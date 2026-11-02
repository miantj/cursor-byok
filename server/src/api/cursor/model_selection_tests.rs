//! Keeps IDE model selection stable across new-chat nudges and official usage limits.
use std::sync::Arc;

use prost::Message;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

use super::*;
use crate::{
    cursor::prompting::{PromptAssets, PromptCompiler},
    model::{ModelConfigInput, ModelInvocation},
    network::NetworkClients,
    provider::{Provider, ProviderStream},
    store::Store,
};

struct UnusedProvider;

impl Provider for UnusedProvider {
    fn stream(&self, _: ModelInvocation, _: CancellationToken) -> ProviderStream {
        panic!("model selection metadata must not invoke a provider")
    }
}

struct Fixture {
    _directory: tempfile::TempDir,
    router: Router,
    model_id: String,
    upstream: tokio::task::JoinHandle<()>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.upstream.abort();
    }
}

impl Fixture {
    async fn new(body: Bytes, configured: bool) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::connect(&format!(
            "sqlite://{}",
            directory.path().join("test.db").display()
        ))
        .await
        .unwrap();
        let model_id = if configured {
            let input: ModelConfigInput = serde_json::from_value(serde_json::json!({
                "display_name":"GPT-6.1", "type":"openai", "base_url":"https://example.com/v1",
                "api_key":"test", "tooltip_data":"GPT-6.1", "model_id":"gpt-6.1-sol",
                "openai_endpoint":"/v1/chat/completions"
            }))
            .unwrap();
            store.create_model(&input).await.unwrap().model_hash
        } else {
            String::new()
        };
        let upstream_router = Router::new().fallback(move || {
            let body = body.clone();
            async move {
                Response::builder()
                    .header(header::CONTENT_TYPE, "application/proto")
                    .body(Body::from(body))
                    .unwrap()
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let upstream = tokio::spawn(async move {
            axum::serve(listener, upstream_router).await.unwrap();
        });
        let compiler = PromptCompiler::new(
            PromptAssets::load(
                &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("prompt/cursor"),
            )
            .unwrap(),
        );
        let clients = NetworkClients::new(store.clone());
        let registry = TransportRegistry::new(store, Arc::new(UnusedProvider), compiler);
        let knowledge =
            knowledge::KnowledgeService::with_root(directory.path().join("rules")).unwrap();
        let router = router_with_proxy(
            registry,
            CursorProxy::for_test(clients, format!("http://{address}")),
            knowledge,
        );
        Self {
            _directory: directory,
            router,
            model_id,
            upstream,
        }
    }

    async fn rpc(&self, path: &str, body: Vec<u8>) -> Bytes {
        let response = self
            .router
            .clone()
            .oneshot(
                Request::post(path)
                    .header(header::CONTENT_TYPE, "application/proto")
                    .header(header::AUTHORIZATION, "Bearer official-pro-token")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        to_bytes(response.into_body(), usize::MAX).await.unwrap()
    }
}

#[derive(Clone, PartialEq, Message)]
struct UsageStatus {
    #[prost(bool, tag = "1")]
    is_in_slow_pool: bool,
    #[prost(string, repeated, tag = "9")]
    allowed_model_ids: Vec<String>,
    #[prost(string, repeated, tag = "10")]
    allowed_model_tags: Vec<String>,
    #[prost(int32, tag = "12")]
    stage: i32,
}

#[derive(Clone, PartialEq, Message)]
struct UsageResponse {
    #[prost(message, optional, tag = "1")]
    status: Option<UsageStatus>,
    #[prost(bytes = "vec", repeated, tag = "2")]
    active_grants: Vec<Vec<u8>>,
}

#[tokio::test]
async fn new_chat_nudges_cannot_silently_replace_the_selected_model() {
    let fixture = Fixture::new(Bytes::from_static(&[0x0a, 0x00]), true).await;
    // A request started before selection may still carry Auto when its response arrives.
    for model_id in [fixture.model_id.as_str(), "default"] {
        let request = agent::GetNewChatNudgeParameterizedModelPickerRequest {
            current_model: Some(agent::RequestedModel {
                model_id: model_id.into(),
                ..Default::default()
            }),
        };
        let body = fixture
            .rpc(
                "/agent.v1.AgentService/GetNewChatNudgeParameterizedModelPicker",
                request.encode_to_vec(),
            )
            .await;
        let response: agent::GetNewChatNudgeParameterizedModelPickerResponse =
            connect::decode_unary(&body).unwrap();
        assert!(response.nudge.is_none());
    }
}

#[tokio::test]
async fn official_auto_only_usage_limits_keep_byok_models_selectable() {
    let original = UsageResponse {
        status: Some(UsageStatus {
            is_in_slow_pool: true,
            allowed_model_ids: vec!["default".into()],
            allowed_model_tags: vec!["auto".into()],
            stage: 2,
        }),
        active_grants: vec![vec![0x0a, 0x04, b't', b'e', b's', b't']],
    };
    for framed in [false, true] {
        let upstream = if framed {
            connect::encode_message(&original).unwrap()
        } else {
            original.encode_to_vec().into()
        };
        let fixture = Fixture::new(upstream.clone(), true).await;
        let body = fixture
            .rpc(
                "/aiserver.v1.DashboardService/GetUsageLimitStatusAndActiveGrants",
                Vec::new(),
            )
            .await;
        let response: UsageResponse = connect::decode_unary(&body).unwrap();
        let status = response.status.unwrap();
        assert!(status.allowed_model_ids.contains(&fixture.model_id));
        assert!(status.allowed_model_ids.contains(&"default".to_owned()));
        assert!(status.is_in_slow_pool);
        assert_eq!(status.stage, 2);
        assert_eq!(status.allowed_model_tags, vec!["auto"]);
        assert_eq!(response.active_grants, original.active_grants);
        let offset = if framed { 5 } else { 0 };
        assert!(body[offset..].starts_with(&upstream[offset..]));
    }
}

#[tokio::test]
async fn unrestricted_usage_and_accounts_without_byok_are_unchanged() {
    for (configured, allowed_model_ids) in [(true, vec![]), (false, vec!["default".into()])] {
        let upstream = UsageResponse {
            status: Some(UsageStatus {
                allowed_model_ids,
                ..Default::default()
            }),
            ..Default::default()
        }
        .encode_to_vec();
        let fixture = Fixture::new(upstream.clone().into(), configured).await;
        let body = fixture
            .rpc(
                "/aiserver.v1.DashboardService/GetUsageLimitStatusAndActiveGrants",
                Vec::new(),
            )
            .await;
        assert_eq!(body.as_ref(), upstream);
    }
}
