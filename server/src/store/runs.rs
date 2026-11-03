//! Persists Run ownership, status, and provider call progress.
use sqlx::Row;

use crate::{
    model::{CheckpointId, ConversationId, PreparedRun, RunId, RunKind, SubagentKind, Usage},
    Error, Result,
};

use super::{now_ms, Store};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunStatus {
    Running,
    Completed,
    Cancelled,
    Failed,
}

impl RunStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimedRun {
    pub run_id: RunId,
    pub conversation_id: ConversationId,
    pub head_checkpoint_id: CheckpointId,
    pub replaced_run_id: Option<RunId>,
}

impl Store {
    pub(crate) async fn latest_run_kind(
        &self,
        conversation_id: &ConversationId,
    ) -> Result<Option<RunKind>> {
        let row = sqlx::query(
            "SELECT run_kind, parent_run_id, parent_tool_call_id, subagent_kind
             FROM runs WHERE conversation_id = ?
             ORDER BY created_at_ms DESC, rowid DESC LIMIT 1",
        )
        .bind(conversation_id.as_str())
        .fetch_optional(&self.pool)
        .await?;
        row.map(run_kind_from_row).transpose()
    }

    /// Resolve ownership for a background completion without a transport parent.
    /// Prefer an existing run id (`subagent_id` / `task_id`), then a subagent whose
    /// `parent_tool_call_id` matches the completion, then the run that owns a Shell/Await
    /// call (not Task — Task owners are parents and would drop subagent ownership).
    /// Falls back to [`Self::latest_run_kind`].
    pub(crate) async fn run_kind_for_background_completion(
        &self,
        conversation_id: &ConversationId,
        tool_call_ids: &[String],
        agent_or_task_ids: &[String],
    ) -> Result<Option<RunKind>> {
        for agent_id in agent_or_task_ids.iter().filter(|id| !id.is_empty()) {
            let row = sqlx::query(
                "SELECT run_kind, parent_run_id, parent_tool_call_id, subagent_kind
                 FROM runs
                 WHERE conversation_id = ? AND run_id = ?
                 ORDER BY created_at_ms DESC, rowid DESC
                 LIMIT 1",
            )
            .bind(conversation_id.as_str())
            .bind(agent_id)
            .fetch_optional(&self.pool)
            .await?;
            if let Some(kind) = row.map(run_kind_from_row).transpose()? {
                return Ok(Some(kind));
            }
        }
        for tool_call_id in tool_call_ids.iter().filter(|id| !id.is_empty()) {
            let row = sqlx::query(
                "SELECT run_kind, parent_run_id, parent_tool_call_id, subagent_kind
                 FROM runs
                 WHERE conversation_id = ?
                   AND run_kind = 'subagent'
                   AND parent_tool_call_id = ?
                 ORDER BY created_at_ms DESC, rowid DESC
                 LIMIT 1",
            )
            .bind(conversation_id.as_str())
            .bind(tool_call_id)
            .fetch_optional(&self.pool)
            .await?;
            if let Some(kind) = row.map(run_kind_from_row).transpose()? {
                return Ok(Some(kind));
            }
        }
        for tool_call_id in tool_call_ids.iter().filter(|id| !id.is_empty()) {
            let row = sqlx::query(
                "SELECT r.run_kind, r.parent_run_id, r.parent_tool_call_id, r.subagent_kind
                 FROM runs r
                 JOIN tool_rounds tr ON tr.run_id = r.run_id
                 JOIN tool_round_calls t ON t.round_id = tr.round_id
                 WHERE r.conversation_id = ?
                   AND t.call_id = ?
                   AND lower(t.name) IN ('shell', 'await')
                 ORDER BY tr.created_at_ms DESC, tr.round_id DESC
                 LIMIT 1",
            )
            .bind(conversation_id.as_str())
            .bind(tool_call_id)
            .fetch_optional(&self.pool)
            .await?;
            if let Some(kind) = row.map(run_kind_from_row).transpose()? {
                return Ok(Some(kind));
            }
        }
        self.latest_run_kind(conversation_id).await
    }

    pub(crate) async fn subagent_parent_run(
        &self,
        parent_request_id: &str,
        parent_tool_call_id: &str,
    ) -> Result<Option<RunId>> {
        let run_id: Option<String> = sqlx::query_scalar(
            "SELECT r.run_id
             FROM runs r
             JOIN tool_rounds tr ON tr.run_id = r.run_id
             JOIN tool_round_calls t ON t.round_id = tr.round_id
             WHERE r.cursor_request_id = ? AND t.call_id = ? AND lower(t.name) = 'task'
             ORDER BY tr.created_at_ms DESC, tr.round_id DESC LIMIT 1",
        )
        .bind(parent_request_id)
        .bind(parent_tool_call_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(run_id.map(RunId::new))
    }

    pub async fn claim_run(&self, prepared: &PreparedRun) -> Result<ClaimedRun> {
        let _write = self.writes.lock().await;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let now = now_ms();
        Self::ensure_conversation_tx(&mut tx, &prepared.conversation_id).await?;
        let belongs: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                SELECT 1 FROM conversation_checkpoints
                WHERE checkpoint_id = ? AND conversation_id = ?
             )",
        )
        .bind(prepared.base_checkpoint_id.0)
        .bind(prepared.conversation_id.as_str())
        .fetch_one(&mut *tx)
        .await?;
        if !belongs {
            return Err(Error::Store(format!(
                "base checkpoint {} does not belong to conversation {}",
                prepared.base_checkpoint_id, prepared.conversation_id
            )));
        }

        let replaced: Option<String> =
            sqlx::query_scalar("SELECT active_run_id FROM conversations WHERE conversation_id = ?")
                .bind(prepared.conversation_id.as_str())
                .fetch_one(&mut *tx)
                .await?;
        if let Some(replaced) = replaced.as_deref() {
            if replaced != prepared.run_id.as_str() {
                sqlx::query(
                    "UPDATE runs SET status = 'cancelled', updated_at_ms = ?
                     WHERE run_id = ? AND status = 'running'",
                )
                .bind(now)
                .bind(replaced)
                .execute(&mut *tx)
                .await?;
                sqlx::query(
                    "UPDATE llm_calls SET status = 'cancelled', finished_at_ms = ?,
                     duration_ms = MAX(0, ? - created_at_ms)
                     WHERE run_id = ? AND status = 'running'",
                )
                .bind(now)
                .bind(now)
                .bind(replaced)
                .execute(&mut *tx)
                .await?;
            }
        }

        let (parent_run_id, parent_tool_call_id, run_kind, subagent_kind) =
            run_kind_columns(&prepared.kind);
        sqlx::query(
            "INSERT INTO runs
             (run_id, cursor_request_id, conversation_id, base_checkpoint_id, head_checkpoint_id,
              parent_run_id, parent_tool_call_id, run_kind, subagent_kind,
              status, created_at_ms, updated_at_ms)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'running', ?, ?)",
        )
        .bind(prepared.run_id.as_str())
        .bind(prepared.cursor_request_id.as_deref())
        .bind(prepared.conversation_id.as_str())
        .bind(prepared.base_checkpoint_id.0)
        .bind(prepared.base_checkpoint_id.0)
        .bind(parent_run_id)
        .bind(parent_tool_call_id)
        .bind(run_kind)
        .bind(subagent_kind)
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            "UPDATE conversations
             SET current_checkpoint_id = ?, active_run_id = ?, updated_at_ms = ?
             WHERE conversation_id = ?",
        )
        .bind(prepared.base_checkpoint_id.0)
        .bind(prepared.run_id.as_str())
        .bind(now)
        .bind(prepared.conversation_id.as_str())
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(ClaimedRun {
            run_id: prepared.run_id.clone(),
            conversation_id: prepared.conversation_id.clone(),
            head_checkpoint_id: prepared.base_checkpoint_id,
            replaced_run_id: replaced
                .filter(|run| run != prepared.run_id.as_str())
                .map(RunId),
        })
    }

    pub async fn active_run_for_cursor_request(
        &self,
        cursor_request_id: &str,
    ) -> Result<Option<RunId>> {
        let run_id: Option<String> = sqlx::query_scalar(
            "SELECT run_id FROM runs
             WHERE cursor_request_id = ? AND status = 'running'
             ORDER BY created_at_ms DESC
             LIMIT 1",
        )
        .bind(cursor_request_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(run_id.map(RunId))
    }

    pub async fn begin_provider_call(&self, run_id: &RunId) -> Result<u64> {
        let _write = self.writes.lock().await;
        let index: Option<i64> = sqlx::query_scalar(
            "UPDATE runs SET provider_call_index = provider_call_index + 1, updated_at_ms = ?
             WHERE run_id = ? AND status = 'running'
             RETURNING provider_call_index",
        )
        .bind(now_ms())
        .bind(run_id.as_str())
        .fetch_optional(&self.pool)
        .await?;
        index
            .map(|index| index as u64)
            .ok_or_else(|| Error::Store(format!("run is not active: {run_id}")))
    }

    pub async fn finish_run(
        &self,
        run_id: &RunId,
        status: RunStatus,
        usage: Option<Usage>,
        failure: Option<(&str, &str)>,
    ) -> Result<bool> {
        let usage_json = serde_json::to_string(&usage)?;
        let _write = self.writes.lock().await;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let row = sqlx::query(
            "SELECT conversation_id, status, failure_category, failure_summary
             FROM runs WHERE run_id = ?",
        )
        .bind(run_id.as_str())
        .fetch_optional(&mut *tx)
        .await?;
        let Some(row) = row else {
            return Err(Error::RunNotFound(run_id.to_string()));
        };
        let conversation_id: String = row.get("conversation_id");
        let current_status: String = row.get("status");
        let (requested_category, requested_summary) = failure.unzip();
        let terminal_status = if current_status == "running" {
            status.as_str()
        } else {
            current_status.as_str()
        };
        let stored_category: Option<String> = row.get("failure_category");
        let stored_summary: Option<String> = row.get("failure_summary");
        let (category, summary) = if current_status == "running" {
            (requested_category, requested_summary)
        } else {
            (stored_category.as_deref(), stored_summary.as_deref())
        };
        let now = now_ms();
        sqlx::query(
            "UPDATE runs SET status = ?, turn_usage_json = ?, failure_category = ?,
             failure_summary = ?, updated_at_ms = ?
             WHERE run_id = ? AND status = 'running'",
        )
        .bind(status.as_str())
        .bind(usage_json)
        .bind(category)
        .bind(summary)
        .bind(now)
        .bind(run_id.as_str())
        .execute(&mut *tx)
        .await?;
        let (call_status, call_error_kind, call_error_message) = match terminal_status {
            "cancelled" => ("cancelled", None, None),
            "failed" => ("error", category, summary),
            "completed" => (
                "error",
                Some("internal"),
                Some("Run completed before LLM call reached a terminal state"),
            ),
            value => {
                return Err(Error::Store(format!(
                    "cannot finish LLM calls for non-terminal Run status: {value}"
                )))
            }
        };
        sqlx::query(
            "UPDATE llm_calls SET status = ?, finished_at_ms = ?,
             duration_ms = MAX(0, ? - created_at_ms), error_kind = ?, error_message = ?
             WHERE run_id = ? AND status = 'running'",
        )
        .bind(call_status)
        .bind(now)
        .bind(now)
        .bind(call_error_kind)
        .bind(call_error_message)
        .bind(run_id.as_str())
        .execute(&mut *tx)
        .await?;
        let released = sqlx::query(
            "UPDATE conversations SET active_run_id = NULL, updated_at_ms = ?
             WHERE conversation_id = ? AND active_run_id = ?",
        )
        .bind(now)
        .bind(conversation_id)
        .bind(run_id.as_str())
        .execute(&mut *tx)
        .await?
        .rows_affected()
            == 1;
        tx.commit().await?;
        Ok(released)
    }
}

fn run_kind_from_row(row: sqlx::sqlite::SqliteRow) -> Result<RunKind> {
    match row.try_get::<&str, _>("run_kind")? {
        "root" => Ok(RunKind::Root),
        "subagent" => {
            let kind: String = row.try_get("subagent_kind")?;
            Ok(RunKind::Subagent {
                parent_run_id: row
                    .try_get::<Option<String>, _>("parent_run_id")?
                    .map(RunId::new),
                parent_tool_call_id: row.try_get("parent_tool_call_id")?,
                kind: if kind == "generalPurpose" {
                    SubagentKind::GeneralPurpose
                } else {
                    SubagentKind::Named(kind)
                },
            })
        }
        kind => Err(Error::Store(format!("unknown run kind: {kind}"))),
    }
}

fn run_kind_columns(kind: &RunKind) -> (Option<&str>, Option<&str>, &'static str, Option<String>) {
    match kind {
        RunKind::Root => (None, None, "root", None),
        RunKind::Subagent {
            parent_run_id,
            parent_tool_call_id,
            kind,
        } => (
            parent_run_id.as_ref().map(RunId::as_str),
            parent_tool_call_id.as_deref(),
            "subagent",
            Some(match kind {
                crate::model::SubagentKind::GeneralPurpose => "generalPurpose".into(),
                crate::model::SubagentKind::Named(name) => name.clone(),
            }),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ConversationId;

    #[tokio::test]
    async fn background_completion_prefers_finished_subagent_over_task_launcher() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::connect(&format!(
            "sqlite://{}",
            directory.path().join("test.db").display()
        ))
        .await
        .unwrap();
        let conversation = ConversationId::new("conversation");
        let checkpoint = store.ensure_conversation(&conversation).await.unwrap();
        sqlx::query(
            "INSERT INTO runs (run_id, conversation_id, base_checkpoint_id, head_checkpoint_id,
             run_kind, status, created_at_ms, updated_at_ms)
             VALUES ('root-run', 'conversation', ?, ?, 'root', 'completed', 1, 1)",
        )
        .bind(checkpoint.0)
        .bind(checkpoint.0)
        .execute(store.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO tool_rounds (round_id, run_id, base_checkpoint_id, assistant_json, status,
             created_at_ms, updated_at_ms)
             VALUES ('round-1', 'root-run', ?, '{}', 'settled', 1, 1),
                    ('round-2', 'root-run', ?, '{}', 'settled', 2, 2)",
        )
        .bind(checkpoint.0)
        .bind(checkpoint.0)
        .execute(store.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO tool_round_calls
             (round_id, call_index, call_id, model_call_id, name, arguments_json, status)
             VALUES ('round-1', 0, 'task-call', 'model-call', 'Task', '{}', 'completed'),
                    ('round-2', 0, 'shell-call', 'shell-model', 'Shell', '{}', 'completed')",
        )
        .execute(store.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO runs (run_id, conversation_id, base_checkpoint_id, head_checkpoint_id,
             run_kind, parent_run_id, parent_tool_call_id, subagent_kind, status,
             created_at_ms, updated_at_ms)
             VALUES ('finished-child', 'conversation', ?, ?, 'subagent', 'root-run', 'task-call',
             'explore', 'completed', 2, 2),
                    ('other-child', 'conversation', ?, ?, 'subagent', 'root-run', 'other-task',
             'explore', 'completed', 3, 3)",
        )
        .bind(checkpoint.0)
        .bind(checkpoint.0)
        .bind(checkpoint.0)
        .bind(checkpoint.0)
        .execute(store.pool())
        .await
        .unwrap();

        // Task launcher is Root; completion must keep the finished child's Subagent ownership.
        let by_parent_tool = store
            .run_kind_for_background_completion(
                &conversation,
                &["task-call".into()],
                &["missing-agent".into()],
            )
            .await
            .unwrap();
        assert!(matches!(
            by_parent_tool,
            Some(RunKind::Subagent {
                parent_tool_call_id: Some(ref id),
                ..
            }) if id == "task-call"
        ));

        let by_agent = store
            .run_kind_for_background_completion(
                &conversation,
                &["task-call".into()],
                &["finished-child".into()],
            )
            .await
            .unwrap();
        assert!(matches!(
            by_agent,
            Some(RunKind::Subagent {
                parent_tool_call_id: Some(ref id),
                ..
            }) if id == "task-call"
        ));

        // Shell/Await may continue as the owning run when no subagent matches.
        let by_shell = store
            .run_kind_for_background_completion(
                &conversation,
                &["shell-call".into()],
                &[],
            )
            .await
            .unwrap();
        assert_eq!(by_shell, Some(RunKind::Root));

        let fallback = store
            .run_kind_for_background_completion(&conversation, &[], &[])
            .await
            .unwrap();
        assert!(matches!(
            fallback,
            Some(RunKind::Subagent {
                parent_tool_call_id: Some(ref id),
                ..
            }) if id == "other-task"
        ));
    }
}
