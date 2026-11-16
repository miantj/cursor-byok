//! Covers context updates and follow-ups across preparation and active runs.
#[path = "support/fake_provider.rs"]
mod fake_provider;
#[path = "support/fixtures.rs"]
mod fixtures;

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use cursor_server::{
    cursor::{
        prompting::{PromptAssets, PromptCompiler},
        protocol::{connect, proto::agent::v1 as pb},
        transport::TransportHandle,
        TransportCommand, TransportRegistry,
    },
    provider::{FinishReason, ModelEvent},
};
use prost::Message;
use tokio::sync::mpsc;

#[tokio::test]
async fn follow_up_during_preparation_preserves_the_original_question() {
    let (_directory, store) = fixtures::temp_store().await;
    let provider = fake_provider::FakeProvider::default();
    provider.push(text_response());
    provider.push(text_response());
    let registry = registry(store, &provider);
    let handle = registry.get_or_create("preparing-follow-up").await.unwrap();
    let mut output = handle.subscribe();
    let mut seqno = 0;
    send(
        &handle,
        &mut seqno,
        run_request("first", Some(missing_context())),
    )
    .await;
    let context_id = wait_for_context(&mut output).await;
    send(
        &handle,
        &mut seqno,
        user_action("second", "second question", None),
    )
    .await;
    send(
        &handle,
        &mut seqno,
        context_result(context_id, "original rules"),
    )
    .await;
    finish_turn(&handle, &mut output, &mut seqno).await;

    let requests = provider.requests();
    let last = requests
        .last()
        .expect("provider must receive both questions");
    let history = serde_json::to_string(&last.history).unwrap();
    assert!(
        history.contains("first question"),
        "preparation lost the original question"
    );
    assert!(
        history.contains("second question"),
        "preparation lost the follow-up"
    );
    assert!(history.find("first question") < history.find("second question"));
}

#[tokio::test]
async fn replacing_context_preparation_releases_the_waiter_and_ignores_late_results() {
    let (_directory, store) = fixtures::temp_store().await;
    let provider = fake_provider::FakeProvider::default();
    provider.push(text_response());
    let registry = registry(store, &provider);
    let handle = registry.get_or_create("replaced-context").await.unwrap();
    let mut output = handle.subscribe();
    let mut seqno = 0;
    send(
        &handle,
        &mut seqno,
        run_request("first", Some(missing_context())),
    )
    .await;
    let old_id = wait_for_context(&mut output).await;
    send(
        &handle,
        &mut seqno,
        run_request("replacement", Some(missing_context())),
    )
    .await;
    let new_id = wait_for_context(&mut output).await;
    assert_ne!(
        old_id, new_id,
        "replacement must have a distinct context reply identity"
    );
    send(&handle, &mut seqno, context_result(old_id, "stale rules")).await;
    send(&handle, &mut seqno, context_result(new_id, "fresh rules")).await;
    finish_turn(&handle, &mut output, &mut seqno).await;
    let history = serde_json::to_string(&provider.requests()[0].history).unwrap();
    assert!(history.contains("fresh rules"));
    assert!(!history.contains("stale rules"));
}

#[tokio::test]
async fn runtime_context_changes_are_append_only_deduplicated_and_allow_reversion() {
    let (_directory, store) = fixtures::temp_store().await;
    let provider = fake_provider::FakeProvider::default();
    for _ in 0..3 {
        provider.push_pending();
    }
    provider.push(text_response());
    let registry = registry(store, &provider);
    let handle = registry.get_or_create("runtime-context").await.unwrap();
    let mut output = handle.subscribe();
    let mut seqno = 0;
    let mut first = run_request("first", None);
    let Some(pb::agent_client_message::Message::RunRequest(request)) = first.message.as_mut()
    else {
        unreachable!()
    };
    let Some(pb::conversation_action::Action::UserMessageAction(action)) =
        request.action.as_mut().unwrap().action.as_mut()
    else {
        unreachable!()
    };
    action.request_context = Some(rules("rules A"));
    send(&handle, &mut seqno, first).await;
    wait_for_calls(&provider, 1, &handle, &mut output, &mut seqno).await;
    for (index, rule) in ["rules B", "rules A", "rules A"].iter().enumerate() {
        send(
            &handle,
            &mut seqno,
            user_action(&format!("follow-{index}"), "follow-up", Some(rules(rule))),
        )
        .await;
        wait_for_calls(&provider, index + 2, &handle, &mut output, &mut seqno).await;
    }
    finish_turn(&handle, &mut output, &mut seqno).await;
    let requests = provider.requests();
    assert_eq!(requests.len(), 4);
    for pair in requests.windows(2) {
        assert_eq!(pair[0].history, pair[1].history[..pair[0].history.len()]);
        assert_eq!(pair[0].prompt, pair[1].prompt);
    }
    let contexts = requests[3]
        .history
        .iter()
        .filter(|message| message.message_id.starts_with("request-context:"))
        .collect::<Vec<_>>();
    assert_eq!(
        contexts.len(),
        3,
        "A → B → A appends three contexts; unchanged A is deduplicated"
    );
    for (message, expected) in contexts.iter().zip(["rules A", "rules B", "rules A"]) {
        assert!(serde_json::to_string(&message.content)
            .unwrap()
            .contains(expected));
    }
    assert_ne!(contexts[0].message_id, contexts[2].message_id);
}

#[tokio::test]
async fn runtime_context_parts_are_hydrated_before_the_follow_up() {
    let (_directory, store) = fixtures::temp_store().await;
    let provider = fake_provider::FakeProvider::default();
    provider.push_pending();
    provider.push(text_response());
    let registry = registry(store, &provider);
    let handle = registry.get_or_create("runtime-parts").await.unwrap();
    let mut output = handle.subscribe();
    let mut seqno = 0;
    send(&handle, &mut seqno, run_request("first", None)).await;
    wait_for_calls(&provider, 1, &handle, &mut output, &mut seqno).await;
    let mut follow_up = user_action("parts-follow-up", "use refreshed context", None);
    let Some(pb::agent_client_message::Message::ConversationAction(action)) =
        follow_up.message.as_mut()
    else {
        unreachable!()
    };
    action.request_context_parts = Some(missing_context());
    send(&handle, &mut seqno, follow_up).await;
    let context_id = wait_for_context(&mut output).await;
    send(
        &handle,
        &mut seqno,
        context_result(context_id, "refreshed runtime rules"),
    )
    .await;
    finish_turn(&handle, &mut output, &mut seqno).await;
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0].history,
        requests[1].history[..requests[0].history.len()]
    );
    let history = serde_json::to_string(&requests[1].history).unwrap();
    assert!(history.contains("refreshed runtime rules"));
    assert!(history.find("refreshed runtime rules") < history.find("use refreshed context"));
}

#[tokio::test]
async fn injected_user_context_updates_rules_before_its_runtime_message() {
    let (_directory, store) = fixtures::temp_store().await;
    let provider = fake_provider::FakeProvider::default();
    provider.push_pending();
    provider.push(text_response());
    let registry = registry(store, &provider);
    let handle = registry.get_or_create("injected-rules").await.unwrap();
    let mut output = handle.subscribe();
    let mut seqno = 0;
    send(&handle, &mut seqno, run_request("first", None)).await;
    wait_for_calls(&provider, 1, &handle, &mut output, &mut seqno).await;
    send(
        &handle,
        &mut seqno,
        pb::AgentClientMessage {
            message: Some(pb::agent_client_message::Message::ConversationAction(
                pb::ConversationAction {
                    action: Some(pb::conversation_action::Action::InjectContextAction(
                        pb::InjectContextAction {
                            injection_id: "new-rules".into(),
                            expected_run_id: "injected-rules".into(),
                            payload: Some(pb::inject_context_action::Payload::UserContext(
                                pb::UserContextInjection {
                                    user_message: Some(pb::UserMessage {
                                        message_id: "injected-user".into(),
                                        text: "injected question".into(),
                                        ..Default::default()
                                    }),
                                    request_context: Some(rules("injected rules")),
                                },
                            )),
                        },
                    )),
                    ..Default::default()
                },
            )),
        },
    )
    .await;
    finish_turn(&handle, &mut output, &mut seqno).await;
    let requests = provider.requests();
    let history = serde_json::to_string(&requests[1].history).unwrap();
    assert!(history.contains("injected rules"));
    assert!(history.find("injected rules") < history.find("injected question"));
    assert_eq!(
        requests[0].history,
        requests[1].history[..requests[0].history.len()]
    );
}

#[tokio::test]
async fn resume_context_is_projected_and_unchanged_context_is_not_repeated() {
    let (_directory, store) = fixtures::temp_store().await;
    let provider = fake_provider::FakeProvider::default();
    for _ in 0..3 {
        provider.push(text_response());
    }
    let registry = registry(store, &provider);
    for (index, rule) in ["resume rules A", "resume rules B", "resume rules B"]
        .iter()
        .enumerate()
    {
        let handle = registry
            .get_or_create(&format!("resume-context-{index}"))
            .await
            .unwrap();
        let mut output = handle.subscribe();
        let mut seqno = 0;
        let mut message = run_request(&format!("user-{index}"), None);
        let Some(pb::agent_client_message::Message::RunRequest(request)) = message.message.as_mut()
        else {
            unreachable!()
        };
        request.action = Some(pb::ConversationAction {
            action: Some(if index == 0 {
                pb::conversation_action::Action::UserMessageAction(pb::UserMessageAction {
                    user_message: Some(pb::UserMessage {
                        message_id: "first-resume-user".into(),
                        text: "first question".into(),
                        mode: pb::AgentMode::Agent as i32,
                        ..Default::default()
                    }),
                    request_context: Some(rules(rule)),
                    ..Default::default()
                })
            } else {
                pb::conversation_action::Action::ResumeAction(pb::ResumeAction {
                    request_context: Some(rules(rule)),
                    ..Default::default()
                })
            }),
            ..Default::default()
        });
        send(&handle, &mut seqno, message).await;
        finish_turn(&handle, &mut output, &mut seqno).await;
    }
    let requests = provider.requests();
    assert_eq!(requests.len(), 3);
    for pair in requests.windows(2) {
        assert_eq!(pair[0].history, pair[1].history[..pair[0].history.len()]);
    }
    let contexts = requests[2]
        .history
        .iter()
        .filter(|message| message.message_id.starts_with("request-context:"))
        .collect::<Vec<_>>();
    assert_eq!(contexts.len(), 2);
    assert!(serde_json::to_string(&contexts[1].content)
        .unwrap()
        .contains("resume rules B"));
}

fn registry(
    store: cursor_server::store::Store,
    provider: &fake_provider::FakeProvider,
) -> TransportRegistry {
    TransportRegistry::new(
        store,
        Arc::new(provider.clone()),
        PromptCompiler::new(PromptAssets::embedded().unwrap()),
    )
}

fn missing_context() -> pb::RequestContextPartReferences {
    pb::RequestContextPartReferences {
        rules_blob_id: vec![7; 32],
        rules_byte_length: 1,
        ..Default::default()
    }
}

fn rules(content: &str) -> pb::RequestContext {
    pb::RequestContext {
        non_file_rules: vec![pb::CursorRule {
            content: content.into(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn user_action(
    id: &str,
    text: &str,
    context: Option<pb::RequestContext>,
) -> pb::AgentClientMessage {
    pb::AgentClientMessage {
        message: Some(pb::agent_client_message::Message::ConversationAction(
            pb::ConversationAction {
                action: Some(pb::conversation_action::Action::UserMessageAction(
                    pb::UserMessageAction {
                        user_message: Some(pb::UserMessage {
                            message_id: id.into(),
                            text: text.into(),
                            mode: pb::AgentMode::Agent as i32,
                            ..Default::default()
                        }),
                        request_context: context,
                        ..Default::default()
                    },
                )),
                ..Default::default()
            },
        )),
    }
}

fn run_request(
    id: &str,
    parts: Option<pb::RequestContextPartReferences>,
) -> pb::AgentClientMessage {
    let Some(pb::agent_client_message::Message::ConversationAction(mut action)) =
        user_action(id, "first question", None).message
    else {
        unreachable!()
    };
    action.request_context_parts = parts;
    pb::AgentClientMessage {
        message: Some(pb::agent_client_message::Message::RunRequest(
            pb::AgentRunRequest {
                action: Some(action),
                conversation_id: Some("context-conversation".into()),
                requested_model: Some(pb::RequestedModel {
                    model_id: "test-model".into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )),
    }
}

fn context_result(id: u32, content: &str) -> pb::AgentClientMessage {
    pb::AgentClientMessage {
        message: Some(pb::agent_client_message::Message::ExecClientMessage(
            pb::ExecClientMessage {
                id,
                message: Some(pb::exec_client_message::Message::RequestContextResult(
                    pb::RequestContextResult {
                        result: Some(pb::request_context_result::Result::Success(
                            pb::RequestContextSuccess {
                                request_context: Some(rules(content)),
                                ..Default::default()
                            },
                        )),
                    },
                )),
                ..Default::default()
            },
        )),
    }
}

fn text_response() -> Vec<ModelEvent> {
    vec![
        ModelEvent::Start {
            model_call_id: "response".into(),
        },
        ModelEvent::TextStart,
        ModelEvent::TextDelta("done".into()),
        ModelEvent::TextEnd,
        ModelEvent::Done(FinishReason::Stop),
    ]
}

async fn send(handle: &TransportHandle, seqno: &mut i64, message: pb::AgentClientMessage) {
    handle
        .command(TransportCommand::Append {
            seqno: *seqno,
            message: Box::new(message),
        })
        .await
        .unwrap();
    *seqno += 1;
}

async fn next(output: &mut mpsc::UnboundedReceiver<Bytes>) -> pb::AgentServerMessage {
    let frame = tokio::time::timeout(Duration::from_secs(5), output.recv())
        .await
        .expect("server response timed out")
        .expect("output closed before completion");
    let (flags, payload) = connect::decode_frames(&frame).unwrap().pop().unwrap();
    assert_eq!(
        flags & connect::END_STREAM_FLAG,
        0,
        "unexpected terminal response: {}",
        String::from_utf8_lossy(&payload)
    );
    pb::AgentServerMessage::decode(payload).unwrap()
}

async fn wait_for_context(output: &mut mpsc::UnboundedReceiver<Bytes>) -> u32 {
    loop {
        if let Some(pb::agent_server_message::Message::ExecServerMessage(exec)) =
            next(output).await.message
        {
            if matches!(
                exec.message,
                Some(pb::exec_server_message::Message::RequestContextArgs(_))
            ) {
                return exec.id;
            }
        }
    }
}

async fn acknowledge(handle: &TransportHandle, seqno: &mut i64, message: &pb::AgentServerMessage) {
    if let Some(pb::agent_server_message::Message::KvServerMessage(kv)) = &message.message {
        assert!(matches!(
            kv.message,
            Some(pb::kv_server_message::Message::SetBlobArgs(_))
        ));
        send(
            handle,
            seqno,
            pb::AgentClientMessage {
                message: Some(pb::agent_client_message::Message::KvClientMessage(
                    pb::KvClientMessage {
                        id: kv.id,
                        message: Some(pb::kv_client_message::Message::SetBlobResult(
                            pb::SetBlobResult { error: None },
                        )),
                    },
                )),
            },
        )
        .await;
    }
}

async fn wait_for_calls(
    provider: &fake_provider::FakeProvider,
    count: usize,
    handle: &TransportHandle,
    output: &mut mpsc::UnboundedReceiver<Bytes>,
    seqno: &mut i64,
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while provider.requests().len() < count {
            if let Ok(message) = tokio::time::timeout(Duration::from_millis(10), next(output)).await
            {
                acknowledge(handle, seqno, &message).await;
            }
        }
    })
    .await
    .expect("model did not receive the next input");
}

async fn finish_turn(
    handle: &TransportHandle,
    output: &mut mpsc::UnboundedReceiver<Bytes>,
    seqno: &mut i64,
) {
    loop {
        let message = next(output).await;
        acknowledge(handle, seqno, &message).await;
        if matches!(
            message.message,
            Some(pb::agent_server_message::Message::InteractionUpdate(
                pb::InteractionUpdate {
                    message: Some(pb::interaction_update::Message::TurnEnded(_))
                }
            ))
        ) {
            handle.disconnect().await;
            break;
        }
    }
}
