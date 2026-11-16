//! Verifies registry ownership follows the transport actor rather than output subscriptions.

#[path = "support/fake_provider.rs"]
mod fake_provider;
#[path = "support/fixtures.rs"]
mod fixtures;

use std::{sync::Arc, time::Duration};

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cursor_server::{
    api::cursor::{
        self,
        bidi::{self, DecodedAppend},
    },
    cursor::{
        conversation::TransportCommand,
        prompting::{PromptAssets, PromptCompiler},
        protocol::{connect, events, proto::agent::v1 as pb},
        transport::{TransportParent, TransportRegistry},
    },
    network::NetworkClients,
    Error,
};
use prost::Message;
use tokio_stream::StreamExt;
use tower::ServiceExt;

async fn registry() -> (tempfile::TempDir, TransportRegistry) {
    let (directory, store) = fixtures::temp_store().await;
    let assets = PromptAssets::load(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("prompt/cursor")
            .as_path(),
    )
    .unwrap();
    (
        directory,
        TransportRegistry::new(
            store,
            Arc::new(fake_provider::FakeProvider::default()),
            PromptCompiler::new(assets),
        ),
    )
}

#[tokio::test]
async fn invalid_first_append_does_not_create_a_transport() {
    let (_directory, registry) = registry().await;
    let mut leaked = Vec::new();
    for (request_id, conversation_id, parent) in [
        ("empty-conversation", Some(""), None),
        (
            "empty-parent-request",
            Some("conversation"),
            Some(TransportParent {
                request_id: String::new(),
                tool_call_id: "tool".into(),
            }),
        ),
        (
            "empty-parent-tool",
            Some("conversation"),
            Some(TransportParent {
                request_id: "parent".into(),
                tool_call_id: String::new(),
            }),
        ),
    ] {
        let error = bidi::append(
            &registry,
            DecodedAppend {
                request_id: request_id.into(),
                seqno: 0,
                message: pb::AgentClientMessage {
                    message: Some(pb::agent_client_message::Message::RunRequest(
                        pb::AgentRunRequest {
                            conversation_id: conversation_id.map(str::to_owned),
                            ..Default::default()
                        },
                    )),
                },
            },
            parent,
        )
        .await
        .unwrap_err();
        assert!(matches!(error, Error::Protocol(_)));
        if registry.local(request_id).await.is_some() {
            leaked.push(request_id);
        }
    }
    registry.shutdown().await;
    assert!(
        leaked.is_empty(),
        "invalid first appends left transports: {leaked:?}"
    );
}

#[tokio::test]
async fn overlapping_run_sse_http_body_drop_preserves_the_other_subscriber() {
    let (_directory, registry) = registry().await;
    let request_id = "http-body-overlap";
    let handle = registry.get_or_create(request_id).await.unwrap();
    let mut observer = handle.subscribe();
    let app = cursor::router(
        registry.clone(),
        NetworkClients::new(registry.store().clone()),
    )
    .unwrap();
    let request = || {
        Request::post("/agent.v1.AgentService/RunSSE")
            .body(Body::from(
                pb::BidiRequestId {
                    request_id: request_id.into(),
                }
                .encode_to_vec(),
            ))
            .unwrap()
    };
    let first = app.clone().oneshot(request()).await.unwrap();
    let second = app.oneshot(request()).await.unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(second.status(), StatusCode::OK);
    let mut first = first.into_body().into_data_stream();
    let mut second = second.into_body().into_data_stream();
    handle.emit(&events::heartbeat()).unwrap();
    first.next().await.unwrap().unwrap();
    second.next().await.unwrap().unwrap();

    drop(first);
    tokio::spawn(async {}).await.unwrap();
    handle
        .emit(&events::heartbeat())
        .expect("old HTTP body must not cancel the current subscriber");
    let frame = second.next().await.unwrap().unwrap();
    assert_eq!(frame[0] & connect::END_STREAM_FLAG, 0);

    drop(second);
    tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(frame) = observer.recv().await {
            if frame[0] & connect::END_STREAM_FLAG != 0 {
                let (_, payload) = connect::decode_frames(&frame).unwrap().pop().unwrap();
                let value: serde_json::Value = serde_json::from_slice(&payload).unwrap();
                assert_eq!(value["error"]["code"], "canceled");
                return;
            }
        }
        panic!("final HTTP body drop must emit canceled EndStream");
    })
    .await
    .unwrap();
    registry.shutdown().await;
}

#[tokio::test]
async fn actor_exit_removes_the_matching_transport_and_allows_a_new_generation() {
    let (_directory, registry) = registry().await;
    let first = registry.get_or_create("lifecycle-request").await.unwrap();
    assert!(registry.local("lifecycle-request").await.is_some());

    first.command(TransportCommand::Disconnect).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if registry.local("lifecycle-request").await.is_none() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    let second = registry.get_or_create("lifecycle-request").await.unwrap();
    assert_eq!(second.request_id(), "lifecycle-request");
    assert!(registry.local("lifecycle-request").await.is_some());
    second.command(TransportCommand::Disconnect).await.unwrap();
}

#[tokio::test]
async fn dropping_an_output_subscription_does_not_remove_the_transport() {
    let (_directory, registry) = registry().await;
    let handle = registry
        .get_or_create("subscription-request")
        .await
        .unwrap();
    let subscription = handle.subscribe();
    drop(subscription);

    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(registry.local("subscription-request").await.is_some());

    handle.command(TransportCommand::Disconnect).await.unwrap();
}
