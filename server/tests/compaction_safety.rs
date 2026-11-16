//! Keeps failed or interrupted automatic compaction from replacing active history.
#[path = "support/fake_provider.rs"]
mod fake_provider;
#[path = "support/fixtures.rs"]
mod fixtures;

use std::{sync::Arc, time::Duration};

use cursor_server::{
    model::{
        CanonicalMessage, CheckpointId, ConversationId, ModelSpec, Origin, PreparedRun, PromptSpec,
        Role, RunAction, RunId, RunKind,
    },
    provider::{FinishReason, ModelEvent},
    run::{self, CommandResult, CommitCause, RunEngine, RunEvent, RunOutcome},
    store::Store,
    Error,
};

async fn prepared(store: &Store) -> (PreparedRun, Vec<CanonicalMessage>) {
    let conversation_id = ConversationId::new("compaction-safety");
    let messages = vec![
        fixtures::user(
            "original-goal",
            "Preserve the original goal and its constraints.",
        ),
        CanonicalMessage::text(
            "large-answer",
            Role::Assistant,
            Origin::Assistant,
            "x".repeat(400_000),
        ),
    ];
    let base_checkpoint_id = store
        .import_checkpoint(&conversation_id, &messages)
        .await
        .unwrap();
    let mut model = ModelSpec::new("model");
    model.context_window_tokens = Some(100_000);
    let initial = CanonicalMessage::text("current-user", Role::User, Origin::User, "Keep working.");
    let mut expected = messages;
    expected.push(initial.clone());
    (
        PreparedRun {
            run_id: RunId::new("compaction-safety-run"),
            cursor_request_id: None,
            conversation_id,
            kind: RunKind::Root,
            model,
            prompt: PromptSpec {
                instructions: String::new(),
                tools: Vec::new(),
            },
            initial_messages: vec![initial],
            action: RunAction::Start,
            base_checkpoint_id,
        },
        expected,
    )
}

fn text_response(text: &str) -> Vec<ModelEvent> {
    vec![
        ModelEvent::Start {
            model_call_id: "summary-call".into(),
        },
        ModelEvent::TextStart,
        ModelEvent::TextDelta(text.into()),
        ModelEvent::TextEnd,
        ModelEvent::Done(FinishReason::Stop),
    ]
}

async fn assert_failed_summary(response: Result<Vec<ModelEvent>, Error>) {
    let (_directory, store) = fixtures::temp_store().await;
    let (prepared, expected) = prepared(&store).await;
    let conversation_id = prepared.conversation_id.clone();
    let provider = fake_provider::FakeProvider::default();
    match response {
        Ok(events) => provider.push(events),
        Err(error) => provider.push_error(error),
    }
    provider.push(text_response("must not continue after a failed summary"));
    let (port, mut session, handle) = run::channel(prepared.run_id.clone(), 32);
    let cancellation = handle.cancellation();
    let engine_store = store.clone();
    let engine_provider = provider.clone();
    let task = tokio::spawn(async move {
        RunEngine::new(engine_store, Arc::new(engine_provider))
            .run(prepared, port, cancellation)
            .await
    });
    let mut active_checkpoint = CheckpointId(0);
    let mut completed = 0;
    let outcome = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match session.events.recv().await.unwrap() {
                RunEvent::MessagesCommitted(committed) => {
                    if committed.cause == CommitCause::InitialMessages {
                        active_checkpoint = committed.checkpoint_id;
                    }
                    committed.barrier.complete(Ok(()));
                }
                RunEvent::AutoCompactionCompleted => completed += 1,
                RunEvent::Ended(outcome) => break outcome,
                _ => {}
            }
        }
    })
    .await
    .unwrap();
    assert!(
        matches!(outcome, RunOutcome::Failed(_)),
        "unusable summary must fail explicitly: {outcome:?}"
    );
    assert_eq!(task.await.unwrap(), outcome);
    assert_eq!(
        completed, 0,
        "failed compaction must not report summary_completed"
    );
    assert_eq!(
        provider.requests().len(),
        1,
        "failed compaction must not dispatch the main model"
    );
    assert_eq!(
        store.ensure_conversation(&conversation_id).await.unwrap(),
        active_checkpoint
    );
    assert_eq!(
        store.load_current_messages(&conversation_id).await.unwrap(),
        expected
    );
}

#[tokio::test]
async fn automatic_compaction_provider_failure_preserves_the_active_checkpoint() {
    assert_failed_summary(Err(Error::Provider("summarizer unavailable".into()))).await;
}

#[tokio::test]
async fn automatic_compaction_empty_summary_preserves_the_active_checkpoint() {
    assert_failed_summary(Ok(text_response(" \n\t"))).await;
}

#[tokio::test]
async fn automatic_compaction_tool_calls_preserve_the_active_checkpoint() {
    assert_failed_summary(Ok(vec![
        ModelEvent::Start {
            model_call_id: "tool-summary".into(),
        },
        ModelEvent::ToolCallStart {
            index: 0,
            call_id: "unexpected-tool".into(),
            name: "Read".into(),
        },
        ModelEvent::ToolCallEnd { index: 0 },
        ModelEvent::Done(FinishReason::ToolUse),
    ]))
    .await;
}

#[tokio::test]
async fn breaking_automatic_compaction_commits_without_replacing_history_then_summarizes_again() {
    let (_directory, store) = fixtures::temp_store().await;
    let (prepared, mut expected) = prepared(&store).await;
    let conversation_id = prepared.conversation_id.clone();
    let provider = fake_provider::FakeProvider::default();
    provider.push_pending();
    provider.push(text_response(
        "Valid summary including the newer user goal.",
    ));
    provider.push(text_response("continued after the valid summary"));
    let (port, mut session, handle) = run::channel(prepared.run_id.clone(), 32);
    let cancellation = handle.cancellation();
    let task = tokio::spawn({
        let store = store.clone();
        let provider = provider.clone();
        async move {
            RunEngine::new(store, Arc::new(provider))
                .run(prepared, port, cancellation)
                .await
        }
    });
    loop {
        match session.events.recv().await.unwrap() {
            RunEvent::MessagesCommitted(committed) => committed.barrier.complete(Ok(())),
            RunEvent::AutoCompactionStarted => break,
            _ => {}
        }
    }
    let mut message = fixtures::user("newer-user-goal", "The newer user goal must be preserved.");
    message.runtime_event_id = Some("newer-user-event".into());
    expected.push(message.clone());
    let command = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .break_messages("newer-user-event".into(), vec![message])
                .await
        }
    });
    let mut lifecycle = Vec::new();
    let outcome = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match session.events.recv().await.unwrap() {
                RunEvent::CycleInterrupted => lifecycle.push("interrupted"),
                RunEvent::MessagesCommitted(committed) => {
                    match &committed.cause {
                        CommitCause::RuntimeEvent { event_id }
                            if event_id == "newer-user-event" =>
                        {
                            lifecycle.push("new-message-committed");
                            assert_eq!(
                                store.load_current_messages(&conversation_id).await.unwrap(),
                                expected
                            );
                            assert_eq!(
                                provider.requests().len(),
                                1,
                                "the next model call must wait for the message commit barrier"
                            );
                        }
                        CommitCause::Compaction { summary } => {
                            assert_eq!(summary, "Valid summary including the newer user goal.");
                            lifecycle.push("summary-committed");
                        }
                        _ => {}
                    }
                    committed.barrier.complete(Ok(()));
                }
                RunEvent::AutoCompactionStarted => lifecycle.push("summary-restarted"),
                RunEvent::AutoCompactionCompleted => lifecycle.push("summary-completed"),
                RunEvent::Ended(outcome) => break outcome,
                _ => {}
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(outcome, RunOutcome::Completed);
    assert_eq!(task.await.unwrap(), outcome);
    assert_eq!(command.await.unwrap(), CommandResult::Applied);
    assert_eq!(
        lifecycle,
        [
            "interrupted",
            "new-message-committed",
            "summary-restarted",
            "summary-committed",
            "summary-completed"
        ]
    );
    let requests = provider.requests();
    assert_eq!(requests.len(), 3);
    let summary_input = serde_json::to_string(&requests[1].history).unwrap();
    assert!(summary_input.contains("original goal"));
    assert!(summary_input.contains("newer user goal"));
    assert!(store
        .load_current_messages(&conversation_id)
        .await
        .unwrap()
        .iter()
        .all(|message| !serde_json::to_string(message)
            .unwrap()
            .contains("Durable recent conversation state")));
}

#[tokio::test]
async fn cancelling_automatic_compaction_keeps_the_active_checkpoint() {
    let (_directory, store) = fixtures::temp_store().await;
    let (prepared, expected) = prepared(&store).await;
    let conversation_id = prepared.conversation_id.clone();
    let provider = fake_provider::FakeProvider::default();
    provider.push_pending();
    let (port, mut session, handle) = run::channel(prepared.run_id.clone(), 32);
    let cancellation = handle.cancellation();
    let task = tokio::spawn({
        let store = store.clone();
        async move {
            RunEngine::new(store, Arc::new(provider))
                .run(prepared, port, cancellation)
                .await
        }
    });
    let mut active_checkpoint = CheckpointId(0);
    let outcome = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match session.events.recv().await.unwrap() {
                RunEvent::MessagesCommitted(committed) => {
                    assert_eq!(committed.cause, CommitCause::InitialMessages);
                    active_checkpoint = committed.checkpoint_id;
                    committed.barrier.complete(Ok(()));
                }
                RunEvent::AutoCompactionStarted => handle.cancel(),
                RunEvent::AutoCompactionCompleted => {
                    panic!("cancelled compaction must not complete")
                }
                RunEvent::Ended(outcome) => break outcome,
                _ => {}
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(outcome, RunOutcome::Cancelled);
    assert_eq!(task.await.unwrap(), outcome);
    assert_eq!(
        store.ensure_conversation(&conversation_id).await.unwrap(),
        active_checkpoint
    );
    assert_eq!(
        store.load_current_messages(&conversation_id).await.unwrap(),
        expected
    );
}
