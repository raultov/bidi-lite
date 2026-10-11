//! WebDriver BiDi wire protocol commands, responses, and parameters.
//!
//! Messages are kept as flat, loosely typed structs: the library is wire-level
//! on purpose and must never reject a message for carrying more than it knows
//! about (Firefox adds `moz:` extension fields, other remote ends add their own).

use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;

/// `type` of a reply that carries a command result.
pub const TYPE_SUCCESS: &str = "success";
/// `type` of a reply that reports a failed command.
pub const TYPE_ERROR: &str = "error";
/// `type` of an unsolicited event.
pub const TYPE_EVENT: &str = "event";

/// Every message the browser sends: command results, errors and events.
///
/// One flat struct (like cdp-lite's) instead of an enum, so unknown fields are
/// ignored instead of failing the parse.
#[derive(Debug, Deserialize, Clone, Default)]
pub struct WsResponse {
    /// `"success"`, `"error"` or `"event"`. `None` only for malformed input.
    #[serde(default)]
    pub r#type: Option<String>,
    /// Id of the command this message answers. `None` for events and for
    /// errors the browser could not attribute to a command (`"id": null`).
    #[serde(default)]
    pub id: Option<u64>,
    /// Result of a successful command.
    #[serde(default)]
    pub result: Option<Value>,
    /// BiDi error code, e.g. `"invalid argument"` or `"no such frame"`.
    #[serde(default)]
    pub error: Option<String>,
    /// Human-readable description accompanying an error.
    #[serde(default)]
    pub message: Option<String>,
    /// Remote-end stack trace accompanying an error, when it sends one.
    #[serde(default)]
    pub stacktrace: Option<String>,
    /// Event name, e.g. `"browsingContext.load"`.
    #[serde(default)]
    pub method: Option<String>,
    /// Event payload.
    #[serde(default)]
    pub params: Option<Value>,
}

impl WsResponse {
    /// Whether this message is an unsolicited event.
    pub fn is_event(&self) -> bool {
        self.r#type.as_deref() == Some(TYPE_EVENT)
    }

    /// Whether this message reports a failed command.
    ///
    /// A message that carries an `error` code counts even if its `type` is
    /// missing, so a sloppy remote end cannot make a failure look like success.
    pub fn is_error(&self) -> bool {
        self.r#type.as_deref() == Some(TYPE_ERROR) || self.error.is_some()
    }

    /// Browsing context an event belongs to: `params.context`, falling back to
    /// `params.source.context` (`log.entryAdded`, `script.message`).
    pub fn event_context(&self) -> Option<&str> {
        let params = self.params.as_ref()?;
        str_at(params, &["context"]).or_else(|| str_at(params, &["source", "context"]))
    }
}

/// A command as written on the wire.
///
/// BiDi requires `params` to be present and to be an object, so unlike
/// cdp-lite there is no `Option` and no `sessionId`: the session is implied by
/// the connection.
#[derive(Debug, Serialize, Clone)]
pub struct WsCommand<P: Serialize> {
    /// Identifier echoed back in the reply, unique per connection.
    pub id: u64,
    /// Command name, e.g. `"browsingContext.navigate"`.
    pub method: String,
    /// Command parameters; must serialize to a JSON object.
    pub params: P,
}

/// Parameters for commands that take none.
///
/// Serializes to `{}`, never `null`: BiDi rejects `"params": null` with
/// `invalid argument`.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoParams;

impl Serialize for NoParams {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // The derive for a unit struct would emit `null`.
        serializer.serialize_map(Some(0))?.end()
    }
}

/// Reads the string at `path` inside `value`, walking nested objects.
///
/// Returns `None` if any step is missing or the final value is not a string.
pub(crate) fn str_at<'a>(value: &'a Value, path: &[&str]) -> Option<&'a str> {
    path.iter()
        .try_fold(value, |current, key| current.get(*key))?
        .as_str()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const EVENT_LOAD: &str = include_str!("../tests/fixtures/bidi/event-load.json");

    fn parse(raw: &str) -> WsResponse {
        serde_json::from_str(raw).expect("message should deserialize")
    }

    /// PRO-01: A command serializes id, method and params.
    #[test]
    fn command_serializes_id_method_and_params() {
        // Given a command with navigation parameters
        let command = WsCommand {
            id: 7,
            method: "browsingContext.navigate".to_string(),
            params: json!({ "context": "C1", "url": "about:blank" }),
        };

        // When it is serialized to JSON
        let encoded = serde_json::to_value(&command).expect("command should serialize");

        // Then it carries exactly id, method and params
        assert_eq!(
            encoded,
            json!({
                "id": 7,
                "method": "browsingContext.navigate",
                "params": { "context": "C1", "url": "about:blank" },
            })
        );
        // And it has no `sessionId` key
        assert!(
            encoded.get("sessionId").is_none(),
            "BiDi routes by connection, never by a per-command session id"
        );
    }

    /// PRO-02: NoParams serializes to an empty object.
    #[test]
    fn no_params_serializes_to_empty_object() {
        // Given a command that takes no parameters
        let command = WsCommand {
            id: 1,
            method: "session.status".to_string(),
            params: NoParams,
        };

        // When it is serialized to JSON
        let encoded = serde_json::to_value(&command).expect("command should serialize");

        // Then `params` is exactly `{}`
        assert_eq!(encoded["params"], json!({}));
        // And it is not null, which BiDi rejects
        assert!(!encoded["params"].is_null(), "params must never be null");
    }

    /// PRO-03: A success response parses.
    #[test]
    fn success_response_parses() {
        // Given a success reply
        let raw = r#"{"type":"success","id":3,"result":{"context":"C1"}}"#;

        // When it is deserialized as WsResponse
        let response = parse(raw);

        // Then type, id and result are read
        assert_eq!(response.r#type.as_deref(), Some("success"));
        assert_eq!(response.id, Some(3));
        assert_eq!(response.result.as_ref().expect("result")["context"], "C1");
        // And it is neither an error nor an event
        assert!(!response.is_error());
        assert!(!response.is_event());
    }

    /// PRO-04: An error response parses with a string code.
    #[test]
    fn error_response_parses_string_code() {
        // Given an error reply whose code is a string
        let raw = r#"{"type":"error","id":4,"error":"no such frame","message":"not found","stacktrace":"at x"}"#;

        // When it is deserialized
        let response = parse(raw);

        // Then code, message and stacktrace are read
        assert_eq!(response.error.as_deref(), Some("no such frame"));
        assert_eq!(response.message.as_deref(), Some("not found"));
        assert_eq!(response.stacktrace.as_deref(), Some("at x"));
        // And it is flagged as an error
        assert!(response.is_error());
    }

    /// PRO-05: An error with `"id": null` parses with id `None`.
    #[test]
    fn error_with_null_id_parses_as_none() {
        // Given an error the browser could not tie to a command
        let raw = r#"{"type":"error","id":null,"error":"invalid argument","message":"bad json"}"#;

        // When it is deserialized
        let response = parse(raw);

        // Then it has no id but is still an error
        assert_eq!(response.id, None);
        assert!(response.is_error());
    }

    /// PRO-06: An event parses (real `browsingContext.load` captured from Firefox).
    #[test]
    fn event_parses() {
        // Given a real event message
        // When it is deserialized
        let response = parse(EVENT_LOAD);

        // Then it is an event without id, naming its method
        assert!(response.is_event());
        assert_eq!(response.id, None);
        assert_eq!(response.method.as_deref(), Some("browsingContext.load"));
    }

    /// PRO-07: The context of an event is read from `params.context`.
    #[test]
    fn event_context_reads_params_context() {
        // Given an event whose params carry a context
        let response = parse(r#"{"type":"event","method":"x.y","params":{"context":"C1"}}"#);

        // Then the context is C1
        assert_eq!(response.event_context(), Some("C1"));
    }

    /// PRO-08: The context of an event falls back to `params.source.context`.
    #[test]
    fn event_context_falls_back_to_source_context() {
        // Given a log.entryAdded-like event that nests its context under `source`
        let raw = r#"{"type":"event","method":"log.entryAdded","params":{"source":{"realm":"R1","context":"C2"}}}"#;

        // Then the context is read from `source`
        assert_eq!(parse(raw).event_context(), Some("C2"));
    }

    /// PRO-09: An event without context yields `None`.
    #[test]
    fn event_context_is_none_without_context() {
        // Given an event with unrelated params, and one without params at all
        let unrelated = parse(r#"{"type":"event","method":"x.y","params":{"url":"u"}}"#);
        let no_params = parse(r#"{"type":"event","method":"x.y"}"#);

        // Then neither belongs to a context
        assert_eq!(unrelated.event_context(), None);
        assert_eq!(no_params.event_context(), None);
    }

    /// PRO-10: Unknown and extension fields are ignored.
    #[test]
    fn unknown_fields_are_ignored() {
        // Given an event carrying vendor extension fields
        let raw = r#"{"type":"event","method":"x.y","params":{},"moz:extra":1,"goog:channel":"c"}"#;

        // When it is deserialized
        let response: Result<WsResponse, _> = serde_json::from_str(raw);

        // Then deserialization succeeds
        assert!(response.is_ok(), "extension fields must not fail the parse");
    }

    /// PRO-11: `str_at` walks nested objects and returns `None` on any miss.
    #[test]
    fn str_at_walks_nested_objects() {
        // Given a nested value
        let value = json!({ "s": "top", "a": { "b": "deep", "n": 1 } });

        // Then strings are found at any depth
        assert_eq!(str_at(&value, &["s"]), Some("top"));
        assert_eq!(str_at(&value, &["a", "b"]), Some("deep"));
        // And every kind of miss is None
        assert_eq!(str_at(&value, &["a", "missing"]), None, "missing key");
        assert_eq!(str_at(&value, &["a", "n"]), None, "not a string");
        assert_eq!(
            str_at(&value, &["s", "x"]),
            None,
            "walks through a non-object"
        );
    }
}
