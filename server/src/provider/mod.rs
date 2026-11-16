//! Defines the provider interface and exports provider implementations.
mod anthropic;
mod attempt;
mod event;
mod normalize;
mod openai_chat;
mod openai_responses;
mod recorder;
mod request_template;
mod router;

use std::pin::Pin;

use futures_util::Stream;
use tokio_util::sync::CancellationToken;

use crate::{model::ModelInvocation, Result};

pub use anthropic::AnthropicProvider;
pub use event::*;
pub use openai_chat::OpenAiChatProvider;
pub use openai_responses::OpenAiResponsesProvider;
pub use recorder::CallRecorder;
pub(crate) use router::custom_headers;

pub(crate) fn native_usage(
    kind: crate::model::ProviderType,
    value: &serde_json::Value,
) -> crate::model::Usage {
    match kind {
        crate::model::ProviderType::OpenAiChat => openai_chat::openai_usage(value),
        crate::model::ProviderType::OpenAiResponses => openai_responses::responses_usage(value),
        crate::model::ProviderType::Anthropic => anthropic::anthropic_usage(value),
        crate::model::ProviderType::Plugin => {
            unreachable!("plugin calls have no native HTTP protocol")
        }
    }
}
pub use router::{build as build_provider, ProviderRouter};

pub type ProviderStream = Pin<Box<dyn Stream<Item = Result<ModelEvent>> + Send>>;

pub trait Provider: Send + Sync {
    fn stream(
        &self,
        invocation: ModelInvocation,
        cancellation: CancellationToken,
    ) -> ProviderStream;
}

fn map_sse_error(
    label: &str,
    error: eventsource_stream::EventStreamError<crate::Error>,
) -> crate::Error {
    match error {
        eventsource_stream::EventStreamError::Transport(error) => error,
        eventsource_stream::EventStreamError::Utf8(error) => {
            crate::Error::Protocol(format!("{label} SSE UTF-8 error: {error}"))
        }
        eventsource_stream::EventStreamError::Parser(error) => {
            crate::Error::Protocol(format!("{label} SSE parse error: {error}"))
        }
    }
}

fn provider_event_error(label: &str, value: &serde_json::Value) -> Option<crate::Error> {
    let kind = value.get("type").and_then(serde_json::Value::as_str);
    let direct_error = value.get("error").filter(|error| !error.is_null());
    if !matches!(kind, Some("error" | "response.failed")) && direct_error.is_none() {
        return None;
    }

    let error = provider_error_payload(value);
    let message = error
        .get("message")
        .and_then(serde_json::Value::as_str)
        .or_else(|| error.as_str())
        .or_else(|| value.get("message").and_then(serde_json::Value::as_str))
        .unwrap_or("provider returned an error event without a message");
    let detail = serde_json::json!({
        "message": message,
        "code": error.get("code"),
        "type": error.get("type"),
    });
    Some(crate::Error::ProviderResponse {
        message: format!("{label} error: {detail}"),
        retryable: retryable_provider_response(None, value),
        retry_after: None,
    })
}

fn provider_error_payload(value: &serde_json::Value) -> &serde_json::Value {
    value
        .pointer("/response/error")
        .filter(|error| !error.is_null())
        .or_else(|| value.get("error").filter(|error| !error.is_null()))
        .unwrap_or(value)
}

fn retryable_provider_response(
    status: Option<reqwest::StatusCode>,
    value: &serde_json::Value,
) -> bool {
    if status.is_some_and(|status| {
        !matches!(
            status.as_u16(),
            408 | 425 | 429 | 500 | 502 | 503 | 504 | 529
        )
    }) {
        return false;
    }
    let error = provider_error_payload(value);
    let identities = ["code", "type"].map(|key| error.get(key).and_then(serde_json::Value::as_str));
    if identities.iter().flatten().any(|identity| {
        matches!(
            *identity,
            "authentication_error"
                | "permission_error"
                | "invalid_request_error"
                | "not_found_error"
                | "request_too_large"
                | "invalid_api_key"
                | "invalid_json_schema"
                | "context_length_exceeded"
                | "insufficient_quota"
                | "billing_hard_limit_reached"
                | "model_not_found"
                | "unsupported_parameter"
                | "invalid_parameter"
                | "invalid_value"
                | "content_policy_violation"
        )
    }) {
        return false;
    }
    status.is_some_and(|status| {
        matches!(
            status.as_u16(),
            408 | 425 | 429 | 500 | 502 | 503 | 504 | 529
        )
    }) || identities.iter().flatten().any(|identity| {
        matches!(
            *identity,
            "rate_limit_error"
                | "rate_limit_exceeded"
                | "server_error"
                | "api_error"
                | "overloaded_error"
                | "internal_error"
        )
    })
}

fn merge_extra_params(body: &mut serde_json::Value, extra: &serde_json::Value) -> Result<()> {
    let extra = extra
        .as_object()
        .ok_or_else(|| crate::Error::Config("model extra params must be an object".into()))?;
    let body = body
        .as_object_mut()
        .ok_or_else(|| crate::Error::Provider("provider request body must be an object".into()))?;
    for (name, value) in extra {
        if matches!(
            name.as_str(),
            "model"
                | "stream"
                | "messages"
                | "input"
                | "tools"
                | "system"
                | "instructions"
                | "prompt_cache_key"
        ) {
            return Err(crate::Error::Config(format!(
                "model extra params cannot replace {name}"
            )));
        }
        body.insert(name.clone(), value.clone());
    }
    Ok(())
}

fn apply_body_allowlist(
    body: &mut serde_json::Value,
    allowed: Option<&std::collections::HashSet<String>>,
) -> Result<()> {
    let Some(allowed) = allowed else {
        return Ok(());
    };
    body.as_object_mut()
        .ok_or_else(|| crate::Error::Provider("provider request body must be an object".into()))?
        .retain(|name, _| allowed.contains(name));
    Ok(())
}

fn apply_openai_prompt_cache_key(body: &mut serde_json::Value, model_id: &str) -> Result<()> {
    if !model_id.to_ascii_lowercase().contains("gpt") {
        return Ok(());
    }
    body.as_object_mut()
        .ok_or_else(|| crate::Error::Provider("provider request body must be an object".into()))?
        .insert(
            "prompt_cache_key".into(),
            serde_json::Value::String("cursor-byok".into()),
        );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sse_transport_errors_are_not_relabelled_as_parse_errors() {
        let error = map_sse_error(
            "test provider",
            eventsource_stream::EventStreamError::Transport(crate::Error::Provider(
                "connection closed".into(),
            )),
        );

        let crate::Error::Provider(message) = error else {
            panic!("transport error category must be preserved");
        };
        assert_eq!(message, "connection closed");
    }

    #[test]
    fn provider_error_events_extract_flat_and_nested_messages() {
        assert_provider_error(
            "OpenAI Responses",
            serde_json::json!({
                "type": "error",
                "message": "Internal error during token generation"
            }),
            "Internal error during token generation",
        );
        assert_provider_error(
            "OpenAI Chat",
            serde_json::json!({
                "error": {"message": "quota exceeded", "type": "server_error"}
            }),
            "quota exceeded",
        );
        assert_provider_error(
            "Anthropic",
            serde_json::json!({
                "type": "error",
                "error": {"type": "overloaded_error", "message": "Overloaded"}
            }),
            "Overloaded",
        );
        assert_provider_error(
            "OpenAI Responses",
            serde_json::json!({
                "type": "response.failed",
                "response": {"error": {"message": "generation failed"}}
            }),
            "generation failed",
        );
    }

    #[test]
    fn provider_error_events_preserve_codes_and_types() {
        for value in [
            serde_json::json!({"type": "error", "error": {
                "type": "authentication_error", "message": "key rejected"
            }}),
            serde_json::json!({"type": "error", "code": "invalid_json_schema", "message": "schema rejected"}),
            serde_json::json!({"type": "response.failed", "response": {"error": {
                "code": "insufficient_quota", "message": "quota exhausted"
            }}}),
        ] {
            let error = provider_event_error("test", &value).unwrap();
            let message = error.to_string();
            let expected = value
                .pointer("/error/type")
                .or_else(|| value.get("code"))
                .or_else(|| value.pointer("/response/error/code"))
                .unwrap()
                .as_str()
                .unwrap();
            assert!(
                message.contains(expected),
                "structured identity was lost: {message}"
            );
        }
    }

    #[test]
    fn failed_response_does_not_copy_response_history_into_the_error() {
        let value = serde_json::json!({
            "type": "response.failed",
            "response": {
                "instructions": "private instructions",
                "output": [{"text": "private history"}],
                "error": {"code": "server_error", "message": "generation failed"}
            }
        });
        let error = provider_event_error("test", &value).unwrap();
        assert!(error.is_retryable());
        let message = error.to_string();
        assert!(message.contains("generation failed"));
        assert!(!message.contains("private"));
    }

    #[test]
    fn permanent_provider_identities_override_transient_status_and_type() {
        for identity in [
            "authentication_error",
            "invalid_request_error",
            "invalid_json_schema",
            "insufficient_quota",
            "context_length_exceeded",
        ] {
            let value = serde_json::json!({"error": {"code": identity, "type": "server_error", "message": "failed"}});
            assert!(
                !retryable_provider_response(Some(reqwest::StatusCode::TOO_MANY_REQUESTS), &value),
                "{identity}"
            );
            assert!(
                !provider_event_error("test", &value).unwrap().is_retryable(),
                "{identity}"
            );
        }
    }

    #[test]
    fn temporary_provider_errors_retry_but_unknown_errors_are_terminal() {
        for identity in [
            "rate_limit_error",
            "rate_limit_exceeded",
            "server_error",
            "overloaded_error",
            "api_error",
        ] {
            let value = serde_json::json!({"type": "error", "error": {"type": identity, "message": "retry later"}});
            assert!(
                provider_event_error("test", &value).unwrap().is_retryable(),
                "{identity}"
            );
        }
        for status in [408, 425, 429, 500, 502, 503, 504, 529] {
            assert!(retryable_provider_response(
                Some(reqwest::StatusCode::from_u16(status).unwrap()),
                &serde_json::Value::Null
            ));
        }
        for status in [400, 401, 403, 404, 413, 422, 501, 505] {
            assert!(!retryable_provider_response(
                Some(reqwest::StatusCode::from_u16(status).unwrap()),
                &serde_json::json!({"error":{"type":"server_error"}})
            ));
        }
        assert!(!provider_event_error(
            "test",
            &serde_json::json!({"type":"error", "message":"unknown"})
        )
        .unwrap()
        .is_retryable());
    }

    #[test]
    fn successful_provider_events_are_not_errors() {
        assert!(provider_event_error(
            "OpenAI Responses",
            &serde_json::json!({"type": "response.completed", "error": null})
        )
        .is_none());
    }

    fn assert_provider_error(label: &str, value: serde_json::Value, expected: &str) {
        let Some(crate::Error::ProviderResponse { message, .. }) =
            provider_event_error(label, &value)
        else {
            panic!("expected provider error");
        };
        assert!(message.starts_with(label));
        assert!(message.contains(expected), "{message}");
    }
}
