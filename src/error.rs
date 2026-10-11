//! Error types for the BiDi and RDP protocols.

use crate::protocol::WsResponse;
use std::time::Duration;
use thiserror::Error;

/// Everything that can go wrong while talking to the browser over BiDi.
///
/// Marked `#[non_exhaustive]` so adding variants stays non-breaking: match on
/// the variants you care about and keep a wildcard arm for the rest.
#[non_exhaustive]
#[derive(Error, Debug)]
pub enum BidiError {
    /// The WebSocket failed: handshake refused, connection reset, bad frame.
    #[error("WebSocket error: {0}")]
    Ws(#[from] tokio_tungstenite::tungstenite::Error),

    /// A command could not be serialized or a message could not be parsed.
    #[error("Serialization error: {0}")]
    Json(#[from] serde_json::Error),

    /// The browser did not answer a command within the client's timeout.
    #[error("Command {method} timed out after {timeout:?}")]
    Timeout {
        /// Name of the command that got no answer.
        method: String,
        /// The timeout that elapsed.
        timeout: Duration,
    },

    /// A bug or an unusable state inside the library (poisoned lock, malformed
    /// reply to a command whose shape the library depends on).
    #[error("Internal communication error: {0}")]
    InternalError(String),

    /// The browser answered a command with a BiDi error.
    #[error("Browser returned an error ({error}): {message}")]
    ProtocolError {
        /// BiDi error code, e.g. `"no such frame"`; see [`codes`].
        error: String,
        /// Human-readable description from the browser.
        message: String,
        /// Remote-end stack trace, when the browser sends one.
        stacktrace: Option<String>,
    },

    /// No BiDi session could be started. Firefox allows a single one.
    #[error("Session not created: {0}")]
    SessionNotCreated(String),

    /// The connection is gone: the browser closed it or the process died.
    #[error("Connection lost")]
    Disconnected,

    /// The event channel overran this subscriber's buffer and `skipped`
    /// events were dropped before it could read them.
    ///
    /// The subscriber keeps working: it simply never saw those events. A
    /// consumer that caches event-derived state should treat this as a signal
    /// to resynchronise that state from the source rather than trust the cache.
    #[error("Event stream lagged: {skipped} events were dropped")]
    Lagged {
        /// Number of events that went missing.
        skipped: u64,
    },
}

/// Result alias used across the crate.
pub type BidiResult<T> = Result<T, BidiError>;

/// BiDi error codes worth matching on without typos.
pub mod codes {
    /// A parameter has the wrong type or an unacceptable value.
    pub const INVALID_ARGUMENT: &str = "invalid argument";
    /// The browsing context does not exist (or no longer does).
    pub const NO_SUCH_FRAME: &str = "no such frame";
    /// `session.new` was refused, e.g. a session is already active.
    pub const SESSION_NOT_CREATED: &str = "session not created";
    /// The command name is not known to the remote end.
    pub const UNKNOWN_COMMAND: &str = "unknown command";
    /// Catch-all code; also what this crate reports when a reply names none.
    pub const UNKNOWN_ERROR: &str = "unknown error";
    /// The remote end understands the command but refuses to do it here.
    pub const UNSUPPORTED_OPERATION: &str = "unsupported operation";
    /// The command needs a session and none is active.
    pub const INVALID_SESSION_ID: &str = "invalid session id";
}

impl BidiError {
    /// Builds a [`BidiError::ProtocolError`] from an error reply.
    ///
    /// Missing fields degrade to `"unknown error"` and `"Unknown BiDi error"`
    /// instead of failing, so a sloppy remote end still yields a usable error.
    ///
    /// Public so code working on raw [`WsResponse`] values (for example from
    /// the transport) can turn an error reply into the same error the client
    /// returns from `send_raw_command`.
    pub fn from_response(response: &WsResponse) -> Self {
        Self::ProtocolError {
            error: response
                .error
                .clone()
                .unwrap_or_else(|| codes::UNKNOWN_ERROR.to_string()),
            message: response
                .message
                .clone()
                .unwrap_or_else(|| "Unknown BiDi error".to_string()),
            stacktrace: response.stacktrace.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn error_reply(
        error: Option<&str>,
        message: Option<&str>,
        stacktrace: Option<&str>,
    ) -> WsResponse {
        WsResponse {
            r#type: Some("error".to_string()),
            error: error.map(str::to_string),
            message: message.map(str::to_string),
            stacktrace: stacktrace.map(str::to_string),
            ..Default::default()
        }
    }

    /// ERR-01: A BiDi error response becomes `ProtocolError`.
    #[test]
    fn error_response_maps_to_protocol_error() {
        // Given an error reply with code, message and stacktrace
        let response = error_reply(Some("no such frame"), Some("gone"), Some("s"));

        // When BidiError::from_response is applied
        let error = BidiError::from_response(&response);

        // Then it is a ProtocolError carrying all three
        let BidiError::ProtocolError {
            error: code,
            message,
            stacktrace,
        } = error
        else {
            panic!("expected ProtocolError, got {error:?}");
        };
        assert_eq!(code, "no such frame");
        assert_eq!(message, "gone");
        assert_eq!(stacktrace.as_deref(), Some("s"));
    }

    /// ERR-02: Missing error fields degrade to defaults.
    #[test]
    fn missing_error_fields_degrade_to_defaults() {
        // Given an error reply with no code and no message
        let response = error_reply(None, None, None);

        // When BidiError::from_response is applied
        let error = BidiError::from_response(&response);

        // Then the defaults fill in and there is no stacktrace
        let BidiError::ProtocolError {
            error: code,
            message,
            stacktrace,
        } = error
        else {
            panic!("expected ProtocolError, got {error:?}");
        };
        assert_eq!(code, "unknown error");
        assert_eq!(message, "Unknown BiDi error");
        assert_eq!(stacktrace, None);
    }

    /// ERR-03: Lagged keeps the dropped count through Display.
    #[test]
    fn lagged_error_carries_the_dropped_event_count() {
        // Given a lag error
        let error = BidiError::Lagged { skipped: 7 };

        // Then Display mentions the count, so logs are useful
        assert!(
            error.to_string().contains('7'),
            "the count must survive Display, got: {error}"
        );
        // And matching the variant yields the count
        let BidiError::Lagged { skipped } = error else {
            panic!("expected Lagged, got {error:?}");
        };
        assert_eq!(skipped, 7);
    }

    /// ERR-04: ProtocolError Display names code and message.
    #[test]
    fn protocol_error_display_names_code_and_message() {
        // Given a protocol error
        let error = BidiError::ProtocolError {
            error: "invalid argument".to_string(),
            message: "bad url".to_string(),
            stacktrace: None,
        };

        // Then Display names both
        let shown = error.to_string();
        assert!(shown.contains("invalid argument"), "got: {shown}");
        assert!(shown.contains("bad url"), "got: {shown}");
    }

    /// ERR-05: Timeout Display names the method.
    #[test]
    fn timeout_display_names_the_method() {
        // Given a timeout
        let error = BidiError::Timeout {
            method: "script.evaluate".to_string(),
            timeout: Duration::from_secs(1),
        };

        // Then Display names the command that got no answer
        assert!(
            error.to_string().contains("script.evaluate"),
            "got: {error}"
        );
    }
}
