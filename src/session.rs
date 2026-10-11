//! Session layer of [`BidiClient`]: starting, ending and subscribing.
//!
//! A BiDi connection starts without a session and the browser accepts nothing
//! but `session.status` and `session.new` until one exists. Firefox allows a
//! single session at a time, so this layer is also where "someone else holds
//! the browser" becomes a typed error instead of an opaque protocol failure.
//!
//! The items are re-exported from [`crate::client`], where users look for them.

use crate::client::{BidiClient, EVENT_CHANNEL_CAPACITY, session_url};
use crate::error::{BidiError, BidiResult, codes};
use crate::protocol::{NoParams, WsResponse};
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

/// What `session.new` returned.
#[derive(Debug, Clone)]
pub struct SessionInfo {
    /// Identifier the browser gave to the session.
    pub session_id: String,
    /// Raw `capabilities` object (`browserName`, `browserVersion`, `userAgent`,
    /// and any `moz:` extension). Kept untyped: it differs between browsers.
    pub capabilities: Value,
}

/// Result of `session.status`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SessionStatus {
    /// Whether the browser can start a new session right now. Firefox answers
    /// `false` while another session is active.
    pub ready: bool,
    /// Human-readable detail, e.g. `"Session already started"`.
    #[serde(default)]
    pub message: String,
}

/// The `capabilities` parameter Puppeteer sends to `session.new`.
///
/// Insecure certificates stay rejected and prompts are left alone, so the
/// browser behaves as a user would see it. `webSocketUrl: true` is what makes
/// a browser that supports BiDi over HTTP upgrade the session.
pub fn default_capabilities() -> Value {
    json!({
        "alwaysMatch": {
            "acceptInsecureCerts": false,
            "unhandledPromptBehavior": { "default": "ignore" },
            "webSocketUrl": true,
        }
    })
}

/// Shared slot holding the session of a connection and all its clones.
pub(crate) type SessionSlot = Arc<RwLock<Option<SessionInfo>>>;

impl BidiClient {
    /// Connects to the BiDi endpoint at `host` and starts a session with
    /// [`default_capabilities`].
    ///
    /// `host` is `"127.0.0.1:9222"` or a full URL; see [`session_url`]. End the
    /// session with [`BidiClient::end_session`] when done: Firefox allows only one.
    pub async fn new(host: &str, default_timeout: Duration) -> BidiResult<Self> {
        Self::new_with_capabilities(host, default_timeout, default_capabilities()).await
    }

    /// Like [`BidiClient::new`], with explicit `capabilities`
    /// (`{"alwaysMatch": .., "firstMatch": [..]}`), sent verbatim.
    pub async fn new_with_capabilities(
        host: &str,
        default_timeout: Duration,
        capabilities: Value,
    ) -> BidiResult<Self> {
        Self::new_with_capacity(host, default_timeout, EVENT_CHANNEL_CAPACITY, capabilities).await
    }

    /// Connects with an explicit event-channel capacity and starts a session.
    ///
    /// The one place that pairs a connection with its session, shared by
    /// [`BidiClient::new_with_capabilities`] and `BrowserClient`.
    pub(crate) async fn new_with_capacity(
        host: &str,
        default_timeout: Duration,
        capacity: usize,
        capabilities: Value,
    ) -> BidiResult<Self> {
        let client =
            Self::connect_with_capacity(&session_url(host), default_timeout, capacity).await?;
        // If the session cannot start, `client` is dropped on the way out, which
        // closes the socket: a failed attempt leaves no connection behind.
        client.start_session(capabilities).await?;
        Ok(client)
    }

    /// `session.status`. Works with or without a session.
    pub async fn status(&self) -> BidiResult<SessionStatus> {
        let response = self.send_raw_command("session.status", NoParams).await?;
        let result = response
            .result
            .ok_or_else(|| BidiError::missing_field("session.status", "result"))?;
        Ok(serde_json::from_value(result)?)
    }

    /// `session.new` on an already connected client.
    ///
    /// Fails with [`BidiError::SessionNotCreated`] if this client already holds
    /// a session or the browser is busy with another one.
    pub async fn start_session(&self, capabilities: Value) -> BidiResult<SessionInfo> {
        if self.session().is_some() {
            let reason = "this client already holds a session";
            return Err(BidiError::SessionNotCreated(reason.to_string()));
        }

        // Asking first turns "the browser is busy" into an explained refusal.
        let status = self.status().await?;
        if !status.ready {
            return Err(BidiError::SessionNotCreated(status.message));
        }

        let params = json!({ "capabilities": capabilities });
        let response = match self.send_raw_command("session.new", params).await {
            Ok(response) => response,
            Err(error @ BidiError::Timeout { .. }) => {
                // Firefox may have created it anyway, and an open session nobody holds
                // makes it refuse every new one until it restarts.
                let _ = self.send_raw_command("session.end", NoParams).await;
                return Err(error);
            }
            Err(error) => return Err(session_not_created(error)),
        };
        let info = session_info(&response)?;
        self.store_session(Some(info.clone()));
        Ok(info)
    }

    /// `session.end`. Firefox closes the WebSocket afterwards, so a connection
    /// lost while waiting for the reply still counts as the session having ended.
    ///
    /// Call it when you are done: dropping the client leaves the session open,
    /// and Firefox allows only one. See [`BidiClient`].
    pub async fn end_session(&self) -> BidiResult<()> {
        // On any error the session may still be open: keep reporting it.
        self.send_closing_command("session.end").await?;
        self.store_session(None);
        Ok(())
    }

    /// Sends a command after which Firefox may close the socket before answering.
    ///
    /// A connection lost while waiting counts as success, but one that was already
    /// gone does not: then nothing was sent.
    pub(crate) async fn send_closing_command(&self, method: &str) -> BidiResult<()> {
        if !self.is_connected() {
            return Err(BidiError::Disconnected);
        }
        match self.send_raw_command(method, NoParams).await {
            Ok(_) | Err(BidiError::Disconnected) => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// The session this client holds, seen identically by every clone.
    pub fn session(&self) -> Option<SessionInfo> {
        // A panic while holding this lock cannot leave the `Option` half-written,
        // so a poisoned lock still holds a coherent value and is safe to use.
        self.session
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The identifier of [`BidiClient::session`].
    pub fn session_id(&self) -> Option<String> {
        self.session().map(|info| info.session_id)
    }

    /// `session.subscribe`; returns the subscription id.
    ///
    /// The browser emits no event until it is subscribed to it. `contexts` of
    /// `None` (or an empty slice, which the specification rejects) subscribes
    /// globally. Pair this with [`BidiClient::on_module`], which only filters.
    pub async fn subscribe_events(
        &self,
        events: &[&str],
        contexts: Option<&[&str]>,
    ) -> BidiResult<String> {
        if events.is_empty() {
            return Err(BidiError::ProtocolError {
                error: codes::INVALID_ARGUMENT.to_string(),
                message: "events must not be empty".to_string(),
                stacktrace: None,
            });
        }

        let mut params = json!({ "events": events });
        if let Some(contexts) = contexts.filter(|contexts| !contexts.is_empty()) {
            params["contexts"] = json!(contexts);
        }

        let response = self.send_raw_command("session.subscribe", params).await?;
        response
            .result_str("subscription")
            .map(str::to_string)
            .ok_or_else(|| BidiError::missing_field("session.subscribe", "subscription"))
    }

    /// `session.unsubscribe` for the given subscription ids. No ids is a no-op.
    pub async fn unsubscribe_events(&self, subscription_ids: &[&str]) -> BidiResult<()> {
        if subscription_ids.is_empty() {
            return Ok(());
        }

        let params = json!({ "subscriptions": subscription_ids });
        self.send_raw_command("session.unsubscribe", params).await?;
        Ok(())
    }

    /// Replaces the shared session, for every clone at once.
    fn store_session(&self, session: Option<SessionInfo>) {
        // See `BidiClient::session` for why a poisoned lock is safe to use.
        *self.session.write().unwrap_or_else(PoisonError::into_inner) = session;
    }
}

/// Gives a refusal to create a session its own variant; anything else passes.
fn session_not_created(error: BidiError) -> BidiError {
    match error {
        BidiError::ProtocolError { error, message, .. } if error == codes::SESSION_NOT_CREATED => {
            BidiError::SessionNotCreated(message)
        }
        other => other,
    }
}

/// Reads the session out of a `session.new` reply.
fn session_info(response: &WsResponse) -> BidiResult<SessionInfo> {
    let session_id = response
        .result_str("sessionId")
        .ok_or_else(|| BidiError::missing_field("session.new", "sessionId"))?;
    let capabilities = response
        .result
        .as_ref()
        .and_then(|result| result.get("capabilities"))
        .cloned()
        .unwrap_or_default();

    Ok(SessionInfo {
        session_id: session_id.to_string(),
        capabilities,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        MockServer, answering, close_marker, error, firefox_like, fixture_for,
    };
    use serde_json::json;

    const TIMEOUT: Duration = Duration::from_secs(1);
    /// Long enough for a stray command to show up, short enough not to slow the suite.
    const QUIET: Duration = Duration::from_millis(100);

    /// A client with a session whose setup commands the test no longer sees.
    async fn session_client(server: &mut MockServer) -> BidiClient {
        let client = BidiClient::new(&server.host(), TIMEOUT)
            .await
            .expect("a session should start");
        server.next_command_named("session.new").await;
        client
    }

    /// The error `BidiClient::new` fails with against `server`.
    async fn new_fails(server: &MockServer) -> BidiError {
        let result = BidiClient::new(&server.host(), TIMEOUT).await;
        let Err(error) = result else {
            panic!("expected BidiClient::new to fail");
        };
        error
    }

    /// SES-01: `new` connects to /session and creates a session with Puppeteer's capabilities.
    #[tokio::test]
    async fn new_connects_to_session_path_and_creates_session() {
        // Given a mock Firefox
        let mut server = MockServer::start(firefox_like(vec![])).await;

        // When BidiClient::new is called
        let client = BidiClient::new(&server.host(), TIMEOUT)
            .await
            .expect("a session should start");

        // Then the handshake path is /session
        assert_eq!(server.handshake_path().await, "/session");
        // And the first command is session.status with params {}
        let status = server.next_command().await;
        assert_eq!(status["method"], "session.status");
        assert_eq!(status["params"], json!({}));
        // And the second is session.new with the default capabilities
        let new = server.next_command().await;
        assert_eq!(new["method"], "session.new");
        assert_eq!(
            new["params"],
            json!({ "capabilities": default_capabilities() })
        );
        // And the client holds the session
        assert_eq!(client.session_id().as_deref(), Some("S-1"));
        let info = client.session().expect("a session");
        assert_eq!(info.capabilities["browserName"], "firefox");
    }

    /// SES-02: `new_with_capabilities` forwards the given capabilities verbatim.
    #[tokio::test]
    async fn new_with_capabilities_forwards_them_verbatim() {
        // Given a mock Firefox and custom capabilities
        let mut server = MockServer::start(firefox_like(vec![])).await;
        let capabilities = json!({ "alwaysMatch": { "acceptInsecureCerts": true } });

        // When the client is created with them
        let _client =
            BidiClient::new_with_capabilities(&server.host(), TIMEOUT, capabilities.clone())
                .await
                .expect("a session should start");

        // Then session.new carries exactly those capabilities
        let new = server.next_command_named("session.new").await;
        assert_eq!(new["params"], json!({ "capabilities": capabilities }));
    }

    /// SES-03: A busy browser (`ready: false`) is reported as `SessionNotCreated`.
    #[tokio::test]
    async fn busy_browser_is_session_not_created() {
        // Given Firefox answering session.status like it does with a session open
        let handler = answering(firefox_like(vec![]), "session.status", |command| {
            vec![fixture_for("bidi/session-status-busy.json", command)]
        });
        let mut server = MockServer::start(handler).await;

        // When BidiClient::new is called
        let error = new_fails(&server).await;

        // Then it fails with the browser's own explanation
        assert!(
            matches!(&error, BidiError::SessionNotCreated(message) if message == "Session already started"),
            "{error:?}"
        );
        // And session.new was never sent
        server.next_command_named("session.status").await;
        server.assert_no_command(QUIET).await;
    }

    /// SES-04: A "session not created" error from `session.new` becomes `SessionNotCreated`.
    #[tokio::test]
    async fn session_not_created_error_is_mapped() {
        // Given status ready but session.new refused, as Firefox's own error
        let handler = answering(firefox_like(vec![]), "session.new", |command| {
            vec![fixture_for("bidi/session-new-busy.json", command)]
        });
        let server = MockServer::start(handler).await;

        // When BidiClient::new is called
        let error = new_fails(&server).await;

        // Then it fails with SessionNotCreated carrying the browser's message
        assert!(
            matches!(&error, BidiError::SessionNotCreated(message) if message == "Maximum number of active sessions"),
            "{error:?}"
        );
    }

    /// SES-05: Any other `session.new` error is propagated untouched.
    #[tokio::test]
    async fn other_session_new_errors_propagate() {
        // Given session.new failing with an unrelated error
        let handler = answering(firefox_like(vec![]), "session.new", |command| {
            vec![error(&command["id"], "invalid argument", "bad caps")]
        });
        let server = MockServer::start(handler).await;

        // When BidiClient::new is called
        let error = new_fails(&server).await;

        // Then the ProtocolError reaches the caller as it came
        assert!(
            matches!(&error, BidiError::ProtocolError { error, message, .. }
                if error == "invalid argument" && message == "bad caps"),
            "{error:?}"
        );
    }

    /// SES-06: A `session.new` reply without `sessionId` is an internal error.
    #[tokio::test]
    async fn session_new_without_session_id_is_internal_error() {
        // Given session.new answering capabilities but no session id
        let overrides = vec![("session.new", json!({ "capabilities": {} }))];
        let server = MockServer::start(firefox_like(overrides)).await;

        // When BidiClient::new is called
        let error = new_fails(&server).await;

        // Then it is an InternalError naming the missing field
        assert!(
            matches!(&error, BidiError::InternalError(message) if message.contains("sessionId")),
            "{error:?}"
        );
    }

    /// SES-07: `connect` does not create a session.
    #[tokio::test]
    async fn connect_does_not_create_a_session() {
        // Given a mock Firefox
        let mut server = MockServer::start(firefox_like(vec![])).await;

        // When the client only connects
        let client = BidiClient::connect(&server.ws_url(), TIMEOUT)
            .await
            .expect("connect");

        // Then it holds no session and the server received no command
        assert_eq!(client.session_id(), None);
        server.assert_no_command(QUIET).await;
    }

    /// SES-08: `start_session` refuses a second session on the same client.
    #[tokio::test]
    async fn second_session_on_same_client_is_refused() {
        // Given a client that already holds a session
        let mut server = MockServer::start(firefox_like(vec![])).await;
        let client = session_client(&mut server).await;

        // When start_session is called again
        let again = client.start_session(default_capabilities()).await;

        // Then it fails with SessionNotCreated
        assert!(
            matches!(again, Err(BidiError::SessionNotCreated(_))),
            "{again:?}"
        );
        // And no further command was sent
        server.assert_no_command(QUIET).await;
    }

    /// SES-09: The session is visible from every clone.
    #[tokio::test]
    async fn session_state_is_shared_by_clones() {
        // Given a client with a session and a clone taken before ending it
        let mut server = MockServer::start(firefox_like(vec![])).await;
        let client = session_client(&mut server).await;
        let clone = client.clone();
        assert_eq!(clone.session_id().as_deref(), Some("S-1"));

        // When the session is ended through the original
        client.end_session().await.expect("the session ends");

        // Then the clone no longer sees it
        assert_eq!(clone.session_id(), None);
    }

    /// SES-10: `end_session` sends `session.end` and clears the session.
    #[tokio::test]
    async fn end_session_sends_session_end_and_clears() {
        // Given a client with a session
        let mut server = MockServer::start(firefox_like(vec![])).await;
        let client = session_client(&mut server).await;

        // When the session is ended
        client.end_session().await.expect("the session ends");

        // Then session.end went out with empty params and the session is gone
        let end = server.next_command_named("session.end").await;
        assert_eq!(end["params"], json!({}));
        assert_eq!(client.session_id(), None);
    }

    /// SES-11: `end_session` tolerates the browser closing the socket first.
    #[tokio::test]
    async fn end_session_tolerates_socket_closed_first() {
        // Given a browser that closes the connection instead of answering session.end
        let handler = answering(
            firefox_like(vec![]),
            "session.end",
            |_| vec![close_marker()],
        );
        let mut server = MockServer::start(handler).await;
        let client = session_client(&mut server).await;

        // When the session is ended
        let result = client.end_session().await;

        // Then it still succeeds and the session is cleared
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(client.session_id(), None);
    }

    /// SES-22: `end_session` on a connection that is already gone reports it.
    #[tokio::test]
    async fn end_session_on_a_dead_connection_is_an_error() {
        // Given a client with a session whose connection is gone
        let mut server = MockServer::start(firefox_like(vec![])).await;
        let client = session_client(&mut server).await;
        server.close();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while client.is_connected() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the connection did not end"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // When the session is ended
        let result = client.end_session().await;

        // Then nothing was sent, so the session may still be open in Firefox
        assert!(matches!(result, Err(BidiError::Disconnected)), "{result:?}");
        assert_eq!(client.session_id().as_deref(), Some("S-1"));
    }

    /// SES-23: A `session.new` that times out is followed by `session.end`.
    #[tokio::test]
    async fn timed_out_session_new_is_ended() {
        // Given a Firefox that never answers session.new
        let handler = answering(firefox_like(vec![]), "session.new", |_| vec![]);
        let mut server = MockServer::start(handler).await;

        // When a session is started with a short timeout
        let result = BidiClient::new(&server.host(), Duration::from_millis(300)).await;

        // Then it times out
        assert!(matches!(result, Err(BidiError::Timeout { .. })));
        // And the session Firefox may have created anyway is ended, or it would block Firefox
        server.next_command_named("session.end").await;
    }

    /// SES-21: A `session.end` error other than a lost connection keeps the session.
    #[tokio::test]
    async fn end_session_error_keeps_the_session() {
        // Given a browser that refuses session.end
        let handler = answering(firefox_like(vec![]), "session.end", |command| {
            vec![error(&command["id"], "invalid session id", "gone")]
        });
        let mut server = MockServer::start(handler).await;
        let client = session_client(&mut server).await;

        // When the session is ended
        let result = client.end_session().await;

        // Then the error reaches the caller
        assert!(
            matches!(result, Err(BidiError::ProtocolError { .. })),
            "{result:?}"
        );
        // And the client still reports the session, since it may still be open
        assert_eq!(client.session_id().as_deref(), Some("S-1"));
    }

    /// SES-12: `subscribe_events` sends the events and returns the subscription id.
    #[tokio::test]
    async fn subscribe_events_returns_subscription_id() {
        // Given a client with a session
        let mut server = MockServer::start(firefox_like(vec![])).await;
        let client = session_client(&mut server).await;

        // When it subscribes globally to two events
        let events = ["network.beforeRequestSent", "browsingContext.load"];
        let subscription = client.subscribe_events(&events, None).await;

        // Then the server saw them, with no contexts key
        let command = server.next_command_named("session.subscribe").await;
        assert_eq!(command["params"], json!({ "events": events }));
        assert!(command["params"].get("contexts").is_none());
        // And the call returns the subscription id
        assert_eq!(subscription.expect("a subscription"), "SUB-1");
    }

    /// SES-13: `subscribe_events` scoped to contexts sends them.
    #[tokio::test]
    async fn subscribe_events_scoped_to_contexts() {
        // Given a client with a session
        let mut server = MockServer::start(firefox_like(vec![])).await;
        let client = session_client(&mut server).await;

        // When it subscribes to one event in one context
        client
            .subscribe_events(&["log.entryAdded"], Some(&["C1"]))
            .await
            .expect("a subscription");

        // Then both travel in the params
        let command = server.next_command_named("session.subscribe").await;
        assert_eq!(
            command["params"],
            json!({ "events": ["log.entryAdded"], "contexts": ["C1"] })
        );
    }

    /// SES-14: An empty contexts slice is treated as global.
    #[tokio::test]
    async fn empty_contexts_slice_is_global() {
        // Given a client with a session
        let mut server = MockServer::start(firefox_like(vec![])).await;
        let client = session_client(&mut server).await;

        // When it subscribes with an empty list of contexts
        client
            .subscribe_events(&["log.entryAdded"], Some(&[]))
            .await
            .expect("a subscription");

        // Then no contexts key is sent, since the specification rejects an empty list
        let command = server.next_command_named("session.subscribe").await;
        assert!(command["params"].get("contexts").is_none(), "{command}");
    }

    /// SES-15: Subscribing to no event is rejected locally.
    #[tokio::test]
    async fn subscribing_to_nothing_is_rejected_locally() {
        // Given a client with a session
        let mut server = MockServer::start(firefox_like(vec![])).await;
        let client = session_client(&mut server).await;

        // When it subscribes to no event
        let result = client.subscribe_events(&[], None).await;

        // Then it fails with invalid argument
        assert!(
            matches!(&result, Err(BidiError::ProtocolError { error, .. }) if error == "invalid argument"),
            "{result:?}"
        );
        // And nothing was sent
        server.assert_no_command(QUIET).await;
    }

    /// SES-16: A subscribe reply without subscription id is an internal error.
    #[tokio::test]
    async fn subscribe_reply_without_id_is_internal_error() {
        // Given a browser whose session.subscribe reply has no id
        let overrides = vec![("session.subscribe", json!({}))];
        let mut server = MockServer::start(firefox_like(overrides)).await;
        let client = session_client(&mut server).await;

        // When it subscribes
        let result = client.subscribe_events(&["log.entryAdded"], None).await;

        // Then it is an InternalError naming the missing field
        assert!(
            matches!(&result, Err(BidiError::InternalError(message)) if message.contains("subscription")),
            "{result:?}"
        );
    }

    /// SES-17: `unsubscribe_events` sends the ids.
    #[tokio::test]
    async fn unsubscribe_events_sends_ids() {
        // Given a client with a session
        let mut server = MockServer::start(firefox_like(vec![])).await;
        let client = session_client(&mut server).await;

        // When it unsubscribes two subscriptions
        client
            .unsubscribe_events(&["SUB-1", "SUB-2"])
            .await
            .expect("unsubscribed");

        // Then the server saw both ids
        let command = server.next_command_named("session.unsubscribe").await;
        assert_eq!(
            command["params"],
            json!({ "subscriptions": ["SUB-1", "SUB-2"] })
        );
    }

    /// SES-18: `unsubscribe_events` with no ids is a local no-op.
    #[tokio::test]
    async fn unsubscribe_nothing_is_a_noop() {
        // Given a client with a session
        let mut server = MockServer::start(firefox_like(vec![])).await;
        let client = session_client(&mut server).await;

        // When it unsubscribes nothing
        let result = client.unsubscribe_events(&[]).await;

        // Then it succeeds and nothing was sent
        assert!(result.is_ok(), "{result:?}");
        server.assert_no_command(QUIET).await;
    }

    /// SES-19: `status` parses `ready` and `message`.
    #[tokio::test]
    async fn status_parses_ready_and_message() {
        // Given session.status answering ready with a message
        let overrides = vec![("session.status", json!({ "ready": true, "message": "ok" }))];
        let server = MockServer::start(firefox_like(overrides)).await;
        let client = BidiClient::connect(&server.ws_url(), TIMEOUT)
            .await
            .expect("connect");

        // When the status is read, without any session
        let status = client.status().await.expect("a status");

        // Then both fields are parsed
        assert_eq!(
            status,
            SessionStatus {
                ready: true,
                message: "ok".to_string()
            }
        );
    }

    /// SES-20: `default_capabilities` matches Puppeteer's.
    #[test]
    fn default_capabilities_match_puppeteer() {
        // Then they are exactly what Puppeteer's BiDi browser sends
        assert_eq!(
            default_capabilities(),
            json!({
                "alwaysMatch": {
                    "acceptInsecureCerts": false,
                    "unhandledPromptBehavior": { "default": "ignore" },
                    "webSocketUrl": true,
                }
            })
        );
    }
}
