//! Restarts an unstarted Bugbot review on BYOK only after an official usage-limit error.
use axum::{
    body::{Body, Bytes},
    http::Response,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use prost::Message;
use serde_json::Value;
use tokio_stream::StreamExt;

use crate::{
    cursor::{
        protocol::{
            connect,
            proto::{agent::v1 as pb, aiserver::v1 as ai},
        },
        transport::{BugbotReplay, TransportRegistry},
    },
    Result,
};

use super::bidi::{self, DecodedAppend};

pub(super) const MAX_ERROR_BYTES: usize = 1024 * 1024;

pub(super) fn quota_error(payload: &[u8]) -> bool {
    let Ok(value) = serde_json::from_slice::<Value>(payload) else {
        return false;
    };
    let error = value.get("error").unwrap_or(&value);
    if quota_text(
        error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or_default(),
    ) {
        return true;
    }
    error
        .get("details")
        .and_then(Value::as_array)
        .is_some_and(|details| {
            details.iter().any(|detail| {
                if detail.get("type").and_then(Value::as_str) != Some("aiserver.v1.ErrorDetails") {
                    return false;
                }
                let Some(encoded) = detail.get("value").and_then(Value::as_str) else {
                    return false;
                };
                let Some(detail) = STANDARD
                    .decode(encoded)
                    .ok()
                    .and_then(|data| ai::ErrorDetails::decode(data.as_slice()).ok())
                else {
                    return false;
                };
                matches!(
                    ai::error_details::Error::try_from(detail.error),
                    Ok(ai::error_details::Error::FreeUserUsageLimit
                        | ai::error_details::Error::ProUserUsageLimit
                        | ai::error_details::Error::UsagePricingRequired
                        | ai::error_details::Error::UsagePricingRequiredChangeable)
                ) || detail
                    .details
                    .is_some_and(|detail| quota_text(&detail.detail))
            })
        })
}

fn quota_text(message: &str) -> bool {
    let message = message.to_ascii_lowercase().replace('’', "'");
    [
        "out of usage",
        "usage limit exceeded",
        "hit your usage limit",
        "reached your usage limit",
    ]
    .iter()
    .any(|phrase| message.contains(phrase))
}

pub(super) async fn start(
    registry: &TransportRegistry,
    request_id: &str,
    replay: &mut BugbotReplay,
) -> Result<bool> {
    if replay.switched {
        return Ok(true);
    }
    let Some(appends) = &replay.appends else {
        return Ok(false);
    };
    let Some(model) = registry.store().models().await?.into_iter().next() else {
        return Ok(false);
    };
    let mut appends = appends.clone();
    let Some(pb::AgentClientMessage {
        message: Some(pb::agent_client_message::Message::RunRequest(request)),
    }) = appends.get_mut(&0)
    else {
        return Ok(false);
    };
    request.requested_model = Some(pb::RequestedModel {
        model_id: model.model_hash.clone(),
        ..Default::default()
    });
    request.model_details = None;
    if let Some(pb::conversation_action::Action::UserMessageAction(action)) = request
        .action
        .as_mut()
        .and_then(|action| action.action.as_mut())
    {
        if let Some(user) = &mut action.user_message {
            let review_prompt = request.custom_system_prompt.clone().filter(|prompt| !prompt.trim().is_empty())
                .unwrap_or_else(|| "Review the requested diff without editing files. Report only confirmed bugs with file paths and line numbers.".into());
            let reminder = user
                .subagent_system_reminder
                .get_or_insert_with(String::new);
            if !reminder.is_empty() {
                reminder.push_str("\n\n");
            }
            reminder.push_str(&review_prompt);
        }
    }
    tracing::warn!(
        request_id,
        model_id = model.model_id,
        "Cursor Bugbot usage limit reached; switching review to BYOK"
    );
    registry.trace(request_id).artifact("bugbot_fallback", "byok_server", &[], serde_json::json!({
        "reason":"official_usage_limit", "model_hash":model.model_hash, "model_id":model.model_id,
    }));
    for (seqno, message) in appends {
        bidi::append(
            registry,
            DecodedAppend {
                request_id: request_id.into(),
                seqno,
                message,
            },
            replay.parent.clone(),
        )
        .await?;
    }
    replay.switched = true;
    replay.appends = None;
    Ok(true)
}

pub(super) async fn http_error(
    registry: &TransportRegistry,
    request_id: &str,
    response: Response<Body>,
    replay: &mut BugbotReplay,
) -> Result<(Response<Body>, bool)> {
    if response.status().is_success() || replay.appends.is_none() {
        return Ok((response, false));
    }
    let (parts, body) = response.into_parts();
    let mut stream = body.into_data_stream();
    let mut body = bytes::BytesMut::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| {
            crate::Error::Protocol(format!("cannot read Cursor error: {error}"))
        })?;
        body.extend_from_slice(&chunk);
        if body.len() > MAX_ERROR_BYTES {
            replay.appends = None;
            let prefix = tokio_stream::once(Ok::<_, axum::Error>(body.freeze()));
            return Ok((
                Response::from_parts(parts, Body::from_stream(prefix.chain(stream))),
                false,
            ));
        }
    }
    let body = body.freeze();
    let payload = connect::decode_frames(&body)
        .ok()
        .and_then(|frames| {
            frames
                .into_iter()
                .find(|(flags, _)| flags & connect::END_STREAM_FLAG != 0)
                .map(|(_, payload)| payload)
        })
        .unwrap_or_else(|| body.clone());
    let switched = quota_error(&payload) && start(registry, request_id, replay).await?;
    Ok((Response::from_parts(parts, Body::from(body)), switched))
}

pub(super) fn heartbeat(frame: &Bytes) -> bool {
    frame[0] == 0
        && pb::AgentServerMessage::decode(&frame[5..])
            .ok()
            .is_some_and(|message| {
                matches!(
                    message.message,
                    Some(pb::agent_server_message::Message::InteractionUpdate(
                        pb::InteractionUpdate {
                            message: Some(pb::interaction_update::Message::Heartbeat(_)),
                            ..
                        }
                    ))
                )
            })
}
