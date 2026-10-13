//! Firefox Remote Debugging Protocol wire packets and serialization.
//!
//! RDP has no request ids: a reply is matched to its request because it comes
//! from the same actor and in the same order (see [`crate::rdp::client`]).
//! Packets are therefore kept as raw JSON with typed accessors for the few
//! fields routing needs, so a field Firefox adds never breaks parsing.

use crate::error::{BidiError, BidiResult};
use serde::Serialize;
use serde_json::{Map, Value};

/// Notification `type`s: a packet carrying one of these is never a reply.
///
/// Replies carry no `type`, but a console actor answers `evaluateJSAsync` with
/// `{resultID}` and later sends an `evaluationResult` notification from the
/// *same* actor, so being from an actor with a pending request is not enough
/// to tell a reply from a notification.
pub const UNSOLICITED_TYPES: &[&str] = &[
    "target-available-form",
    "target-destroyed-form",
    "resources-available-array",
    "resources-updated-array",
    "resources-destroyed-array",
    "evaluationResult",
    "paused",
    "resumed",
    "newSource",
    "tabListChanged",
    "workerListChanged",
    "descriptor-destroyed",
    // Root sends it when a watcher's forwarding to a content process ends (a
    // debugger detached, a tab closed), unasked and in between root replies.
    "forwardingCancelled",
];

/// Any packet the server sends: greeting, reply, error or notification.
#[derive(Debug, Clone)]
pub struct RdpPacket {
    /// Actor that sent the packet.
    pub from: Option<String>,
    /// Notification type. Replies have none.
    pub r#type: Option<String>,
    /// Error code, e.g. `"noSuchActor"` or `"wrongState"`.
    pub error: Option<String>,
    /// Human-readable description accompanying an error.
    pub message: Option<String>,
    /// The whole packet, `from`, `type` and `error` included.
    pub body: Value,
}

impl RdpPacket {
    /// Parses one text frame. Fails if it is not a JSON object.
    pub fn parse(text: &str) -> Result<Self, serde_json::Error> {
        // Parsed as a map so that anything but an object is rejected, and only once:
        // the typed fields are copied out of it and the map becomes the body.
        let fields: Map<String, Value> = serde_json::from_str(text)?;
        let field = |name: &str| fields.get(name).and_then(Value::as_str).map(str::to_string);
        Ok(Self {
            from: field("from"),
            r#type: field("type"),
            error: field("error"),
            message: field("message"),
            body: Value::Object(fields),
        })
    }

    /// Whether the server reports a failed request.
    pub fn is_error(&self) -> bool {
        self.error.is_some()
    }

    /// Whether the packet is one of the [`UNSOLICITED_TYPES`].
    pub fn is_notification(&self) -> bool {
        self.r#type
            .as_deref()
            .is_some_and(|kind| UNSOLICITED_TYPES.contains(&kind))
    }

    /// The string at the top level of the packet under `field`, if any.
    pub fn str_field(&self, field: &str) -> Option<&str> {
        self.body.get(field)?.as_str()
    }
}

/// Builds `{"to": to, "type": method, ..params}`.
///
/// `params` must serialize to a JSON object, whose keys are merged at the top
/// level, which is where RDP expects them. [`NoParams`](crate::protocol::NoParams)
/// serializes to `{}`. Params that set `to` or `type` are rejected rather than
/// silently overwritten.
pub fn encode_request<P: Serialize>(to: &str, method: &str, params: P) -> BidiResult<String> {
    let Value::Object(mut fields) = serde_json::to_value(params)? else {
        return Err(BidiError::InternalError(
            "RDP params must be a JSON object".to_string(),
        ));
    };
    if fields.contains_key("to") || fields.contains_key("type") {
        return Err(BidiError::InternalError(
            "RDP params must not set `to`/`type`".to_string(),
        ));
    }

    fields.insert("to".to_string(), Value::from(to));
    fields.insert("type".to_string(), Value::from(method));
    Ok(Value::Object(fields).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::NoParams;
    use crate::test_support::fixture;
    use serde_json::json;

    fn encoded<P: Serialize>(to: &str, method: &str, params: P) -> Value {
        let text = encode_request(to, method, params).expect("params should encode");
        serde_json::from_str(&text).expect("an encoded request is JSON")
    }

    fn packet_from(name: &str) -> RdpPacket {
        RdpPacket::parse(&fixture(name).to_string()).expect("a fixture is a packet")
    }

    /// RDP-01: A request is encoded flat with `to` and `type`.
    #[test]
    fn request_is_encoded_flat() {
        // When a request without parameters is encoded
        let list_tabs = encoded("root", "listTabs", NoParams);
        // Then it is exactly to and type
        assert_eq!(list_tabs, json!({ "to": "root", "type": "listTabs" }));

        // And parameters are merged at the top level, not nested
        let resume = encoded(
            "thread1",
            "resume",
            json!({ "resumeLimit": { "type": "next" } }),
        );
        assert_eq!(
            resume,
            json!({ "to": "thread1", "type": "resume", "resumeLimit": { "type": "next" } })
        );
    }

    /// RDP-02: Params that are not an object, or that set `to`/`type`, are rejected.
    #[test]
    fn invalid_request_params_are_rejected() {
        // Then params that are not an object fail with an internal error
        let array = encode_request("a", "m", json!([1]));
        assert!(
            matches!(array, Err(BidiError::InternalError(_))),
            "{array:?}"
        );
        // And so do params that try to overwrite the addressing fields
        let to = encode_request("a", "m", json!({ "to": "b" }));
        assert!(matches!(to, Err(BidiError::InternalError(_))), "{to:?}");
        let kind = encode_request("a", "m", json!({ "type": "x" }));
        assert!(matches!(kind, Err(BidiError::InternalError(_))), "{kind:?}");
    }

    /// RDP-17: A packet keeps its typed fields and the whole body.
    #[test]
    fn packet_keeps_typed_fields_and_the_whole_body() {
        // Given a thread-state notification captured from Firefox
        let packet = packet_from("rdp/thread-state-paused.json");

        // Then the typed fields are read
        assert_eq!(packet.r#type.as_deref(), Some("paused"));
        assert_eq!(
            packet.from.as_deref(),
            Some("server1.conn1.watcher2.process7//thread1")
        );
        assert_eq!(packet.error, None);
        // And the whole body is still there, fields the struct does not know included
        assert_eq!(packet.body, fixture("rdp/thread-state-paused.json"));
        assert_eq!(packet.str_field("type"), Some("paused"));
        assert_eq!(packet.str_field("missing"), None);
    }

    /// RDP-18: A frame that is not a JSON object is not a packet.
    #[test]
    fn non_object_frame_is_not_a_packet() {
        for text in ["garbage", "[1]", "\"text\"", "null", "42"] {
            // Then none of them parses, so the router can discard them
            assert!(RdpPacket::parse(text).is_err(), "{text:?} must be rejected");
        }
    }

    /// RDP-19: Notifications are recognised by their type, replies and errors are not.
    #[test]
    fn notifications_are_recognised_by_type() {
        // Then every notification Firefox sent in the spike is one
        for name in [
            "rdp/target-available.json",
            "rdp/resources-sources.json",
            "rdp/thread-state-paused.json",
            "rdp/thread-state-resumed.json",
            "rdp/evaluation-result.json",
        ] {
            assert!(packet_from(name).is_notification(), "{name}");
        }
        // And replies are not, including the one the console actor sends
        // before its evaluationResult
        for name in [
            "rdp/listTabs.json",
            "rdp/getWatcher.json",
            "rdp/evaluate-reply.json",
            "rdp/frames.json",
        ] {
            assert!(!packet_from(name).is_notification(), "{name}");
        }
        // And an error packet is an error with its code and message
        let failure =
            RdpPacket::parse(r#"{"from":"a","error":"wrongState","message":"not paused"}"#)
                .expect("a packet");
        assert!(failure.is_error());
        assert!(!failure.is_notification());
        assert_eq!(failure.error.as_deref(), Some("wrongState"));
        assert_eq!(failure.message.as_deref(), Some("not paused"));
        assert!(!packet_from("rdp/listTabs.json").is_error());
    }

    /// RDP-27: `forwardingCancelled` is a notification.
    #[test]
    fn forwarding_cancelled_is_a_notification() {
        // Given the packet root sent after a debugger detached and its tab closed
        let packet = packet_from("rdp/forwarding-cancelled.json");

        // Then it is a notification, never a reply
        assert_eq!(packet.from.as_deref(), Some("root"));
        assert!(packet.is_notification());
    }
}
