//! Compiles Cursor requests and actions into provider-independent Run inputs.

mod action;
mod break_messages;
mod context;
mod images;
mod insert_messages;
mod model;
mod run;

pub use action::*;
pub(crate) use break_messages::{
    compile_injection, compile_request_context, compile_user_message_action,
};
pub(crate) use context::{hydrate_action, merge_local_rules};
pub use run::*;
