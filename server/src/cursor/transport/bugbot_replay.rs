//! Retains an unstarted official Bugbot request until its quota decision is known.
use std::{collections::BTreeMap, sync::Arc};

use tokio::sync::Mutex;

use crate::cursor::protocol::proto::agent::v1 as pb;

use super::TransportParent;

pub(crate) type SharedBugbotReplay = Arc<Mutex<BugbotReplay>>;

pub(crate) struct BugbotReplay {
    pub appends: Option<BTreeMap<i64, pb::AgentClientMessage>>,
    pub parent: Option<TransportParent>,
    pub switched: bool,
}

impl BugbotReplay {
    pub fn new(
        seqno: i64,
        message: &pb::AgentClientMessage,
        parent: Option<TransportParent>,
    ) -> Option<Self> {
        let Some(pb::agent_client_message::Message::RunRequest(request)) = &message.message else {
            return None;
        };
        if seqno != 0
            || request.subagent_type_name.as_deref() != Some("bugbot")
            || request
                .conversation_state
                .as_ref()
                .is_some_and(|state| !state.turns.is_empty())
            || !matches!(
                request
                    .action
                    .as_ref()
                    .and_then(|action| action.action.as_ref()),
                Some(pb::conversation_action::Action::UserMessageAction(action)) if action.user_message.is_some()
            )
        {
            return None;
        }
        Some(Self {
            appends: Some([(seqno, message.clone())].into_iter().collect()),
            parent,
            switched: false,
        })
    }

    pub fn record(&mut self, seqno: i64, message: &pb::AgentClientMessage) {
        let Some(appends) = &mut self.appends else {
            return;
        };
        if appends.get(&seqno) == Some(message) {
            return;
        }
        // Only heartbeats can safely precede a replay; cancellation or any tool reply ends eligibility.
        if appends.len() >= 64
            || !matches!(
                message.message,
                Some(pb::agent_client_message::Message::ClientHeartbeat(_))
            )
        {
            self.appends = None;
            return;
        }
        appends.insert(seqno, message.clone());
    }
}
