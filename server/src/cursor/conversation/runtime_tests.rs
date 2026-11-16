//! Covers a follow-up admitted before the previous turn closes.
use std::{sync::Arc, time::Duration};

use prost::Message;

use crate::{
    cursor::{
        prompting::{PromptAssets, PromptCompiler},
        protocol::{connect, proto::agent::v1 as pb},
        transport::TransportRegistry,
    },
    model::ModelInvocation,
    provider::{FinishReason, ModelEvent, Provider, ProviderStream},
    store::Store,
};

use super::TransportCommand;

struct TextProvider;

impl Provider for TextProvider {
    fn stream(
        &self,
        _invocation: ModelInvocation,
        _cancellation: tokio_util::sync::CancellationToken,
    ) -> ProviderStream {
        Box::pin(futures_util::stream::iter([
            Ok(ModelEvent::Start {
                model_call_id: "reply".into(),
            }),
            Ok(ModelEvent::TextStart),
            Ok(ModelEvent::TextDelta("done".into())),
            Ok(ModelEvent::TextEnd),
            Ok(ModelEvent::Done(FinishReason::Stop)),
        ]))
    }
}

#[tokio::test]
async fn admitted_follow_up_reopens_the_actor_after_its_idle_timeout() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::connect(&format!(
        "sqlite://{}",
        directory.path().join("test.db").display()
    ))
    .await
    .unwrap();
    let registry = TransportRegistry::new(
        store,
        Arc::new(TextProvider),
        PromptCompiler::new(PromptAssets::embedded().unwrap()),
    );
    let handle = registry
        .get_or_create("admitted-continuation")
        .await
        .unwrap();
    let mut output = handle.subscribe();
    let mut seqno = 0;
    let action = |id: &str| pb::ConversationAction {
        action: Some(pb::conversation_action::Action::UserMessageAction(
            pb::UserMessageAction {
                user_message: Some(pb::UserMessage {
                    message_id: id.into(),
                    text: id.into(),
                    mode: pb::AgentMode::Agent as i32,
                    ..Default::default()
                }),
                ..Default::default()
            },
        )),
        ..Default::default()
    };
    handle
        .command(TransportCommand::Append {
            seqno,
            message: Box::new(pb::AgentClientMessage {
                message: Some(pb::agent_client_message::Message::RunRequest(
                    pb::AgentRunRequest {
                        conversation_id: Some("admitted-conversation".into()),
                        requested_model: Some(pb::RequestedModel {
                            model_id: "test-model".into(),
                            ..Default::default()
                        }),
                        action: Some(action("first")),
                        ..Default::default()
                    },
                )),
            }),
        })
        .await
        .unwrap();
    seqno += 1;

    for turn in 0..2 {
        loop {
            let frame = tokio::time::timeout(Duration::from_secs(5), output.recv())
                .await
                .expect("turn must finish")
                .expect("output must remain open");
            let (flags, payload) = connect::decode_frames(&frame).unwrap().pop().unwrap();
            assert_eq!(
                flags & connect::END_STREAM_FLAG,
                0,
                "actor closed before the admitted follow-up finished"
            );
            let message = pb::AgentServerMessage::decode(payload).unwrap();
            match message.message {
                Some(pb::agent_server_message::Message::KvServerMessage(kv)) => {
                    handle
                        .command(TransportCommand::Append {
                            seqno,
                            message: Box::new(pb::AgentClientMessage {
                                message: Some(pb::agent_client_message::Message::KvClientMessage(
                                    pb::KvClientMessage {
                                        id: kv.id,
                                        message: Some(
                                            pb::kv_client_message::Message::SetBlobResult(
                                                pb::SetBlobResult { error: None },
                                            ),
                                        ),
                                    },
                                )),
                            }),
                        })
                        .await
                        .unwrap();
                    seqno += 1;
                }
                Some(pb::agent_server_message::Message::InteractionUpdate(
                    pb::InteractionUpdate {
                        message: Some(pb::interaction_update::Message::TurnEnded(_)),
                    },
                )) => break,
                _ => {}
            }
        }
        if turn == 0 {
            let admission = handle.admit().unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                while handle.accepting_appends() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("first turn must reach its closing boundary");
            handle
                .command(TransportCommand::Append {
                    seqno,
                    message: Box::new(pb::AgentClientMessage {
                        message: Some(pb::agent_client_message::Message::ConversationAction(
                            action("second"),
                        )),
                    }),
                })
                .await
                .unwrap();
            seqno += 1;
            drop(admission);
        }
    }
    handle.disconnect().await;
    registry.shutdown().await;
}
