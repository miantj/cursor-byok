//! Exercises official Bugbot quota failures and the BYOK continuation on the wire.
use super::*;
use std::sync::{Arc, Mutex};

use crate::{
    cursor::{
        prompting::{PromptAssets, PromptCompiler},
        protocol::{events, proto::agent::v1 as pb},
    },
    model::{ModelConfigInput, ModelInvocation, ModelRequest},
    network::NetworkClients,
    provider::{FinishReason, ModelEvent, Provider, ProviderStream},
    store::Store,
};
use prost::Message;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

#[derive(Clone, Default)]
struct ReviewProvider(Arc<Mutex<Vec<ModelRequest>>>);

impl Provider for ReviewProvider {
    fn stream(&self, invocation: ModelInvocation, _: CancellationToken) -> ProviderStream {
        self.0.lock().unwrap().push(invocation.request);
        Box::pin(tokio_stream::iter([
            Ok(ModelEvent::Start {
                model_call_id: "byok-review-call".into(),
            }),
            Ok(ModelEvent::TextStart),
            Ok(ModelEvent::TextDelta("BYOK review completed".into())),
            Ok(ModelEvent::TextEnd),
            Ok(ModelEvent::Done(FinishReason::Stop)),
        ]))
    }
}

fn quota_frame(message: &str) -> Bytes {
    let payload = serde_json::to_vec(&serde_json::json!({
        "error": {"code":"resource_exhausted", "message":message}
    }))
    .unwrap();
    let mut frame = vec![connect::END_STREAM_FLAG];
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend(payload);
    frame.into()
}

fn run_message(kind: &str) -> pb::AgentClientMessage {
    pb::AgentClientMessage {
        message: Some(pb::agent_client_message::Message::RunRequest(
            pb::AgentRunRequest {
                conversation_id: Some("review-conversation".into()),
                subagent_type_name: Some(kind.into()),
                requested_model: Some(pb::RequestedModel {
                    model_id: "composer-2.5".into(),
                    ..Default::default()
                }),
                custom_system_prompt: Some("Report only concrete bugs. Do not edit files.".into()),
                action: Some(pb::ConversationAction {
                    action: Some(pb::conversation_action::Action::UserMessageAction(
                        pb::UserMessageAction {
                            user_message: Some(pb::UserMessage {
                                text: "Full Repository Path: /repo\nDiff: branch changes".into(),
                                mode: pb::AgentMode::Agent as i32,
                                subagent_system_reminder: Some(
                                    "Keep original parent instructions.".into(),
                                ),
                                ..Default::default()
                            }),
                            request_context: Some(pb::RequestContext::default()),
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )),
    }
}

async fn rpc(router: &Router, path: &str, message: &impl Message) -> Response<Body> {
    router
        .clone()
        .oneshot(
            Request::post(path)
                .header(header::CONTENT_TYPE, "application/proto")
                .body(Body::from(message.encode_to_vec()))
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn append(router: &Router, seqno: i64, message: &pb::AgentClientMessage) -> Response<Body> {
    rpc(
        router,
        "/aiserver.v1.BidiService/BidiAppend",
        &ai::BidiAppendRequest {
            request_id: Some(ai::BidiRequestId {
                request_id: "review-request".into(),
            }),
            append_seqno: seqno,
            data: hex::encode(message.encode_to_vec()),
            data_binary: Vec::new(),
        },
    )
    .await
}

async fn collect(router: &Router, response: Response<Body>) -> Vec<u8> {
    let mut stream = response.into_body().into_data_stream();
    let mut output = Vec::new();
    let mut pending = bytes::BytesMut::new();
    let mut seqno = 2;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.unwrap();
        output.extend_from_slice(&chunk);
        pending.extend_from_slice(&chunk);
        while pending.len() >= 5 {
            let length = u32::from_be_bytes(pending[1..5].try_into().unwrap()) as usize;
            if pending.len() < 5 + length {
                break;
            }
            let frame = pending.split_to(5 + length);
            if frame[0] != 0 {
                continue;
            }
            let message = pb::AgentServerMessage::decode(&frame[5..]).unwrap();
            if let Some(pb::agent_server_message::Message::KvServerMessage(kv)) = message.message {
                let ack = pb::AgentClientMessage {
                    message: Some(pb::agent_client_message::Message::KvClientMessage(
                        pb::KvClientMessage {
                            id: kv.id,
                            message: Some(pb::kv_client_message::Message::SetBlobResult(
                                pb::SetBlobResult { error: None },
                            )),
                        },
                    )),
                };
                assert_eq!(append(router, seqno, &ack).await.status(), StatusCode::OK);
                seqno += 1;
            }
        }
    }
    output
}

#[derive(Clone, Copy)]
enum QuotaSite {
    Stream,
    AppendHttp,
    StreamHttp,
}

async fn scenario(
    kind: &str,
    frames: Vec<Bytes>,
    configured: bool,
    site: QuotaSite,
) -> (Vec<u8>, ReviewProvider) {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::connect(&format!(
        "sqlite://{}",
        directory.path().join("test.db").display()
    ))
    .await
    .unwrap();
    if configured {
        let input: ModelConfigInput = serde_json::from_value(serde_json::json!({
            "sort_order":0,"display_name":"BYOK Reviewer","type":"openai",
            "base_url":"https://example.com/v1","use_full_url":false,"api_key":"test",
            "tooltip_data":"Test review model","model_id":"byok-review-model","openai_endpoint":"/v1/chat/completions"
        })).unwrap();
        store.create_model(&input).await.unwrap();
    }
    store
        .set_proxy_settings(crate::store::ProxySettingsInput {
            mode: crate::store::ProxyMode::Custom,
            address: "http://127.0.0.1:1".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    let error = connect::decode_frames(&frames.concat())
        .unwrap()
        .into_iter()
        .find(|(flags, _)| *flags == connect::END_STREAM_FLAG)
        .map(|(_, payload)| payload)
        .unwrap_or_default();
    let append_error = error.clone();
    let upstream_frames = Arc::new(frames);
    let mock = Router::new()
        .route(
            "/aiserver.v1.BidiService/BidiAppend",
            post(move || {
                let error = append_error.clone();
                async move {
                    if matches!(site, QuotaSite::AppendHttp) {
                        Response::builder()
                            .status(StatusCode::TOO_MANY_REQUESTS)
                            .body(Body::from(error))
                            .unwrap()
                    } else {
                        Response::new(Body::empty())
                    }
                }
            }),
        )
        .route(
            "/agent.v1.AgentService/RunSSE",
            post(move || {
                let frames = upstream_frames.as_ref().clone();
                let error = error.clone();
                async move {
                    if matches!(site, QuotaSite::StreamHttp) {
                        return Response::builder()
                            .status(StatusCode::TOO_MANY_REQUESTS)
                            .body(Body::from(error))
                            .unwrap();
                    }
                    Response::builder()
                        .header(header::CONTENT_TYPE, "text/event-stream")
                        .body(Body::from_stream(tokio_stream::iter(
                            frames.into_iter().map(Ok::<_, std::convert::Infallible>),
                        )))
                        .unwrap()
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let upstream = tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });
    let provider = ReviewProvider::default();
    let compiler = PromptCompiler::new(
        PromptAssets::load(&std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("prompt/cursor"))
            .unwrap(),
    );
    let clients = NetworkClients::new(store.clone());
    let registry = TransportRegistry::new(store, Arc::new(provider.clone()), compiler);
    let knowledge = knowledge::KnowledgeService::with_root(directory.path().join("rules")).unwrap();
    let router = router_with_proxy(
        registry.clone(),
        CursorProxy::for_test(clients, format!("http://{addr}")),
        knowledge,
    );
    let first = append(&router, 0, &run_message(kind)).await;
    let status = first.status();
    let body = to_bytes(first.into_body(), usize::MAX).await.unwrap();
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let heartbeat = pb::AgentClientMessage {
        message: Some(pb::agent_client_message::Message::ClientHeartbeat(
            pb::ClientHeartbeat {},
        )),
    };
    assert_eq!(
        append(&router, 1, &heartbeat).await.status(),
        StatusCode::OK
    );
    let response = rpc(
        &router,
        "/agent.v1.AgentService/RunSSE",
        &ai::BidiRequestId {
            request_id: "review-request".into(),
        },
    )
    .await;
    let output = tokio::time::timeout(
        std::time::Duration::from_secs(8),
        collect(&router, response),
    )
    .await
    .unwrap();
    registry.shutdown().await;
    upstream.abort();
    (output, provider)
}

#[tokio::test]
async fn bugbot_quota_error_switches_to_byok_and_keeps_review_instructions() {
    let frame = quota_frame("You're out of usage. Switch to Auto, or ask your admin to increase your limit to continue.");
    for site in [
        QuotaSite::Stream,
        QuotaSite::AppendHttp,
        QuotaSite::StreamHttp,
    ] {
        let heartbeat = connect::encode_message(&events::heartbeat()).unwrap();
        let (output, provider) = scenario(
            "bugbot",
            vec![
                heartbeat,
                frame.slice(..3),
                frame.slice(3..11),
                frame.slice(11..),
            ],
            true,
            site,
        )
        .await;
        let requests = provider.0.lock().unwrap();
        assert_eq!(
            requests.len(),
            1,
            "quota failure must start the BYOK review exactly once"
        );
        let history = requests[0]
            .history
            .iter()
            .map(|message| format!("{:?}", message.content))
            .collect::<String>();
        assert!(history.contains("Report only concrete bugs. Do not edit files."));
        assert!(history.contains("Full Repository Path: /repo"));
        assert!(history.contains("Keep original parent instructions."));
        assert_eq!(
            requests[0].model.display_name.as_deref(),
            Some("BYOK Reviewer")
        );
        assert!(!String::from_utf8_lossy(&output).contains("You're out of usage"));
        assert!(String::from_utf8_lossy(&output).contains("BYOK review completed"));
    }
}

#[tokio::test]
async fn other_failures_and_started_reviews_are_not_replayed() {
    let quota = quota_frame("You're out of usage.");
    let text = connect::encode_message(
        &events::response_event(
            &ModelEvent::TextDelta("official review".into()),
            "official",
            &Default::default(),
        )
        .unwrap()
        .unwrap(),
    )
    .unwrap();
    for (kind, frames, configured) in [
        ("explore", vec![quota.clone()], true),
        (
            "bugbot",
            vec![quota_frame("Too many requests. Retry later.")],
            true,
        ),
        ("bugbot", vec![text, quota.clone()], true),
        ("bugbot", vec![quota], false),
        ("bugbot", vec![connect::encode_end_stream()], true),
    ] {
        let expected = frames.concat();
        let (output, provider) = scenario(kind, frames, configured, QuotaSite::Stream).await;
        assert_eq!(output, expected);
        assert!(provider.0.lock().unwrap().is_empty());
    }
}

#[test]
fn cancellation_stops_quota_replay_and_usage_details_are_decoded() {
    use base64::Engine;
    let mut replay = BugbotReplay::new(0, &run_message("bugbot"), None).unwrap();
    replay.record(
        1,
        &pb::AgentClientMessage {
            message: Some(pb::agent_client_message::Message::ConversationAction(
                pb::ConversationAction {
                    action: Some(pb::conversation_action::Action::CancelAction(
                        pb::CancelAction::default(),
                    )),
                    ..Default::default()
                },
            )),
        },
    );
    assert!(replay.appends.is_none());
    for (code, quota) in [
        (9, true),
        (10, true),
        (44, true),
        (45, true),
        (29, false),
        (57, false),
    ] {
        let detail = ai::ErrorDetails {
            error: code,
            details: None,
            is_expected: Some(true),
        };
        let payload = serde_json::to_vec(&serde_json::json!({"error":{
            "code":"unknown", "details":[{"type":"aiserver.v1.ErrorDetails", "value":base64::engine::general_purpose::STANDARD.encode(detail.encode_to_vec())}]
        }})).unwrap();
        assert_eq!(bugbot_fallback::quota_error(&payload), quota);
    }
    assert!(!bugbot_fallback::quota_error(
        br#"{"code":"unauthenticated","message":"Please log in"}"#
    ));
}
