//! BiDi client: connection, command dispatch and event subscription.
//!
//! [`BidiClient`] is the wire-level entry point: it sends any command with any
//! `Serialize` parameters and hands back the raw reply. One connection carries
//! every command and every event; commands are matched with their reply by id.

use crate::error::{BidiError, BidiResult};
use crate::event_filter::EventFilter;
use crate::protocol::{WsCommand, WsResponse};
use crate::session::SessionSlot;
use crate::transport::{self, Connection, Router};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::timeout;
use tracing::{debug, trace};

pub use crate::session::{SessionInfo, SessionStatus, default_capabilities};

/// Capacity of the event broadcast ring of a single connection.
///
/// One ring carries the events of **every** module of **every** browsing
/// context, so it must be sized for the whole browser session rather than for
/// one page. A subscriber that falls behind the ring loses the skipped events
/// and is told so via [`BidiError::Lagged`]; nothing is silently discarded.
///
/// Overridable per connection with [`BidiClient::connect_with_capacity`] when a
/// session drives many tabs at once.
pub const EVENT_CHANNEL_CAPACITY: usize = 4096;

/// Commands awaiting the reply that carries their `id`.
///
/// One map per connection, shared with the reader task: every tab's replies
/// route through it, which is why ids are allocated per connection and not per
/// browsing context.
type PendingRequests = Arc<Mutex<HashMap<u64, oneshot::Sender<WsResponse>>>>;

/// A connection to a BiDi remote end.
///
/// Cloning is cheap and every clone shares the same connection, id counter and
/// event ring. The socket is closed once the last clone is dropped.
#[derive(Clone)]
pub struct BidiClient {
    connection: Connection,
    pending_requests: PendingRequests,
    event_tx: broadcast::Sender<WsResponse>,
    next_id: Arc<AtomicU64>,
    default_timeout: Duration,
    /// The session this connection holds, shared by every clone. Read and
    /// written by the session layer (`session.rs`).
    pub(crate) session: SessionSlot,
}

impl BidiClient {
    /// Opens the WebSocket only. No session is created: until one exists the
    /// browser accepts nothing but `session.status` and `session.new`.
    ///
    /// `default_timeout` applies to every command sent through this connection.
    pub async fn connect(ws_url: &str, default_timeout: Duration) -> BidiResult<Self> {
        Self::connect_with_capacity(ws_url, default_timeout, EVENT_CHANNEL_CAPACITY).await
    }

    /// Like [`BidiClient::connect`], with an explicit event-channel capacity.
    ///
    /// Raise `capacity` when driving many tabs at once: the ring is shared by
    /// every tab and module of the connection. See [`EVENT_CHANNEL_CAPACITY`].
    ///
    /// `capacity` of `0` is treated as `1`, the smallest usable ring.
    pub async fn connect_with_capacity(
        ws_url: &str,
        default_timeout: Duration,
        capacity: usize,
    ) -> BidiResult<Self> {
        let pending_requests: PendingRequests = Arc::new(Mutex::new(HashMap::new()));
        let (event_tx, _) = broadcast::channel(transport::effective_capacity(capacity));
        let router = BidiRouter {
            pending: Arc::clone(&pending_requests),
            event_tx: event_tx.clone(),
        };

        let connection = transport::open(ws_url, router).await?;

        Ok(Self {
            connection,
            pending_requests,
            event_tx,
            next_id: Arc::new(AtomicU64::new(1)),
            default_timeout,
            session: SessionSlot::default(),
        })
    }

    /// A raw receiver of every event on the connection.
    pub fn subscribe(&self) -> broadcast::Receiver<WsResponse> {
        self.event_tx.subscribe()
    }

    /// The events of one module, e.g. `"browsingContext"`.
    ///
    /// This only filters: the browser sends nothing until the session
    /// subscribes to the events with `session.subscribe`.
    pub fn on_module(&self, module: &'static str) -> EventFilter {
        EventFilter::new(self.subscribe(), module)
    }

    /// Like [`BidiClient::on_module`], restricted to one browsing context.
    pub fn on_module_for_context(&self, module: &'static str, context: &str) -> EventFilter {
        EventFilter::for_context(self.subscribe(), module, context)
    }

    /// Sends any BiDi command and waits for its reply.
    ///
    /// `params` is sent verbatim and must serialize to a JSON object; use
    /// [`NoParams`](crate::protocol::NoParams) for `{}`. A BiDi error reply
    /// becomes [`BidiError::ProtocolError`].
    pub async fn send_raw_command<P: Serialize>(
        &self,
        method: &str,
        params: P,
    ) -> BidiResult<WsResponse> {
        if !self.is_connected() {
            return Err(BidiError::Disconnected);
        }

        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        // Serialised before the id is registered, so a bad payload leaves nothing to clean up.
        let command = WsCommand {
            id,
            method: method.to_string(),
            params,
        };
        let payload = serde_json::to_string(&command)?;

        let reply = self.register_pending(id)?;
        if let Err(error) = transport::send(&self.connection, payload) {
            self.forget(id);
            return Err(error);
        }

        self.await_reply(id, method, reply).await
    }

    /// Whether the reader task still sees an open socket.
    pub fn is_connected(&self) -> bool {
        self.connection.is_alive.load(Ordering::SeqCst)
    }

    fn register_pending(&self, id: u64) -> BidiResult<oneshot::Receiver<WsResponse>> {
        let (responder, reply) = oneshot::channel();
        self.pending_requests
            .lock()
            .map_err(|_| poisoned_lock())?
            .insert(id, responder);
        Ok(reply)
    }

    /// Drops the entry of a command that will never be answered.
    fn forget(&self, id: u64) {
        if let Ok(mut pending) = self.pending_requests.lock() {
            pending.remove(&id);
        }
    }

    async fn await_reply(
        &self,
        id: u64,
        method: &str,
        reply: oneshot::Receiver<WsResponse>,
    ) -> BidiResult<WsResponse> {
        match timeout(self.default_timeout, reply).await {
            Ok(Ok(response)) => into_result(response),
            // The responder was dropped: the connection went away while waiting.
            Ok(Err(_)) => Err(BidiError::Disconnected),
            Err(_) => {
                self.forget(id);
                Err(BidiError::Timeout {
                    method: method.to_string(),
                    timeout: self.default_timeout,
                })
            }
        }
    }
}

/// The WebSocket URL of the BiDi endpoint for a host or URL.
///
/// `"127.0.0.1:9222"` becomes `"ws://127.0.0.1:9222/session"`; a URL that
/// already has a path is left alone.
pub fn session_url(host: &str) -> String {
    transport::ws_url(host, Some("/session"))
}

/// Turns an error reply into the error the caller sees; anything else passes.
fn into_result(response: WsResponse) -> BidiResult<WsResponse> {
    if response.is_error() {
        Err(BidiError::from_response(&response))
    } else {
        Ok(response)
    }
}

/// Feeds the frames of one connection to the callers waiting for them.
struct BidiRouter {
    pending: PendingRequests,
    event_tx: broadcast::Sender<WsResponse>,
}

impl Router for BidiRouter {
    fn route(&mut self, text: &str) {
        // A frame we cannot parse is dropped: a remote end may add fields or messages
        // we do not know, and that must never take the connection down.
        let response = match serde_json::from_str::<WsResponse>(text) {
            Ok(response) => response,
            Err(error) => {
                debug!("Discarded unparsable BiDi message: {error}: {text}");
                return;
            }
        };

        if let Err(error) = route_message(response, &self.pending, &self.event_tx, text) {
            debug!("Could not route a BiDi message: {error}");
        }
    }

    fn on_disconnect(&mut self) {
        fail_pending_requests(&self.pending);
    }
}

/// Hands a message to whoever is waiting for it.
///
/// Anything carrying an `id` answers a specific command; an event goes to every
/// subscriber; an error without an id cannot be tied to any command.
fn route_message(
    response: WsResponse,
    pending: &PendingRequests,
    event_tx: &broadcast::Sender<WsResponse>,
    raw: &str,
) -> BidiResult<()> {
    if let Some(id) = response.id {
        deliver(pending, id, response)?;
    } else if response.is_event() && response.method.is_some() {
        trace!("BiDi event: {:?}", response.method);
        // No subscriber at the moment is fine: the event is simply not wanted.
        let _ = event_tx.send(response);
    } else if response.is_error() {
        debug!("BiDi error not tied to any command: {raw}");
    } else {
        debug!("Unexpected BiDi message: {raw}");
    }

    Ok(())
}

/// Gives a reply to the caller waiting for `id`, if there still is one.
fn deliver(pending: &PendingRequests, id: u64, response: WsResponse) -> BidiResult<()> {
    let responder = pending.lock().map_err(|_| poisoned_lock())?.remove(&id);

    match responder {
        Some(responder) => {
            trace!("Received expected id {id}");
            // The caller may have timed out and gone away; its reply is no longer wanted.
            let _ = responder.send(response);
        }
        None => trace!("Discarded message with id {id} (no listener found)"),
    }

    Ok(())
}

fn poisoned_lock() -> BidiError {
    BidiError::InternalError(
        "Mutex poisoned: another thread panicked while holding the pending-requests lock"
            .to_string(),
    )
}

/// Wakes every caller still waiting for a reply once the socket is gone.
fn fail_pending_requests(pending: &PendingRequests) {
    if let Ok(mut pending) = pending.lock() {
        debug!(
            "Closing {} pending requests due to disconnection",
            pending.len()
        );
        // Dropping the senders wakes every receiver with an error.
        pending.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::NoParams;
    use crate::test_support::{MockServer, firefox_like, silent, success};
    use serde_json::{Value, json};
    use std::sync::atomic::AtomicBool;
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;
    use tokio_stream::StreamExt;

    const WAIT: Duration = Duration::from_secs(2);

    type Call = JoinHandle<BidiResult<WsResponse>>;
    type Commands = mpsc::UnboundedReceiver<String>;

    /// A client whose writer end is under the test's control: no socket involved.
    fn test_client(default_timeout: Duration) -> (BidiClient, Commands) {
        let (command_tx, commands) = mpsc::unbounded_channel();
        let client = BidiClient {
            connection: Connection {
                command_tx,
                is_alive: Arc::new(AtomicBool::new(true)),
            },
            pending_requests: Arc::new(Mutex::new(HashMap::new())),
            event_tx: broadcast::channel(16).0,
            next_id: Arc::new(AtomicU64::new(1)),
            default_timeout,
            session: SessionSlot::default(),
        };
        (client, commands)
    }

    /// The router of a test client, to feed it frames as the reader task would.
    fn router_of(client: &BidiClient) -> BidiRouter {
        BidiRouter {
            pending: Arc::clone(&client.pending_requests),
            event_tx: client.event_tx.clone(),
        }
    }

    /// Starts a command in the background so the test can play the browser.
    fn call<P: Serialize + Send + 'static>(
        client: &BidiClient,
        method: &'static str,
        params: P,
    ) -> Call {
        let client = client.clone();
        tokio::spawn(async move { client.send_raw_command(method, params).await })
    }

    /// The next command written. Once it is out, its id is registered as pending.
    async fn next_payload(commands: &mut Commands) -> Value {
        let payload = timeout(WAIT, commands.recv())
            .await
            .expect("no command was written within 2 s")
            .expect("the writer end was dropped");
        serde_json::from_str(&payload).expect("a command is valid JSON")
    }

    async fn finish(call: Call) -> BidiResult<WsResponse> {
        timeout(WAIT, call)
            .await
            .expect("the call did not finish within 2 s")
            .expect("the call task panicked")
    }

    fn pending_len(client: &BidiClient) -> usize {
        client.pending_requests.lock().expect("lock").len()
    }

    fn parse(text: &str) -> WsResponse {
        serde_json::from_str(text).expect("a valid message")
    }

    fn new_pending() -> PendingRequests {
        Arc::new(Mutex::new(HashMap::new()))
    }

    fn wait_for(pending: &PendingRequests, id: u64) -> oneshot::Receiver<WsResponse> {
        let (tx, rx) = oneshot::channel();
        pending.lock().expect("lock").insert(id, tx);
        rx
    }

    /// CLI-01: Each command gets a distinct, increasing id.
    #[tokio::test]
    async fn ids_are_unique_across_concurrent_commands() {
        // Given a test client
        let (client, mut commands) = test_client(Duration::from_millis(50));

        // When three commands are sent concurrently
        let calls: Vec<Call> = (0..3)
            .map(|_| call(&client, "session.status", json!({})))
            .collect();

        // Then the three payloads written carry three distinct ids
        let mut ids = Vec::new();
        for _ in 0..3 {
            ids.push(
                next_payload(&mut commands).await["id"]
                    .as_u64()
                    .expect("an id"),
            );
        }
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), 3, "a shared pending map needs unique ids");
        for call in calls {
            let _ = finish(call).await;
        }
    }

    /// CLI-02: The payload on the wire is a BiDi command.
    #[tokio::test]
    async fn command_payload_is_bidi_wire_format() {
        // Given a test client
        let (client, mut commands) = test_client(Duration::from_millis(50));

        // When a navigation is sent
        let params = json!({ "context": "C1", "url": "about:blank" });
        let sent = call(&client, "browsingContext.navigate", params);

        // Then the written JSON is exactly a BiDi command
        let payload = next_payload(&mut commands).await;
        assert_eq!(
            payload,
            json!({
                "id": 1,
                "method": "browsingContext.navigate",
                "params": { "context": "C1", "url": "about:blank" },
            })
        );
        // And it carries no sessionId
        assert!(payload.get("sessionId").is_none());
        let _ = finish(sent).await;
    }

    /// CLI-03: NoParams goes out as an empty object.
    #[tokio::test]
    async fn no_params_goes_out_as_empty_object() {
        // Given a test client
        let (client, mut commands) = test_client(Duration::from_millis(50));

        // When a command without parameters is sent
        let sent = call(&client, "session.status", NoParams);

        // Then the written JSON has "params": {}
        assert_eq!(next_payload(&mut commands).await["params"], json!({}));
        let _ = finish(sent).await;
    }

    /// CLI-04: A success reply resolves the caller with the full response.
    #[tokio::test]
    async fn success_reply_resolves_caller() {
        // Given a pending command
        let (client, mut commands) = test_client(WAIT);
        let pending = call(&client, "browsingContext.navigate", json!({}));
        let id = next_payload(&mut commands).await["id"].clone();

        // When the router receives its success reply
        let reply = json!({ "type": "success", "id": id, "result": { "navigation": "N1" } });
        router_of(&client).route(&reply.to_string());

        // Then the call returns the response
        let response = finish(pending).await.expect("a success");
        assert_eq!(response.result.expect("a result")["navigation"], "N1");
    }

    /// CLI-05: An error reply maps to `ProtocolError`.
    #[tokio::test]
    async fn error_reply_maps_to_protocol_error() {
        // Given a pending command
        let (client, mut commands) = test_client(WAIT);
        let pending = call(&client, "browsingContext.navigate", json!({}));
        let id = next_payload(&mut commands).await["id"].clone();

        // When the router receives an error reply for it
        let reply =
            json!({ "type": "error", "id": id, "error": "invalid argument", "message": "bad url" });
        router_of(&client).route(&reply.to_string());

        // Then the call fails with the browser's code and message
        let result = finish(pending).await;
        let Err(BidiError::ProtocolError { error, message, .. }) = result else {
            panic!("expected ProtocolError, got {result:?}");
        };
        assert_eq!(error, "invalid argument");
        assert_eq!(message, "bad url");
    }

    /// CLI-06: A disconnection while waiting yields `Disconnected`.
    #[tokio::test]
    async fn disconnection_while_waiting_yields_disconnected() {
        // Given a pending command
        let (client, mut commands) = test_client(WAIT);
        let pending = call(&client, "session.status", NoParams);
        let _ = next_payload(&mut commands).await;

        // When the connection is lost: the reader clears the pending map
        client.connection.is_alive.store(false, Ordering::SeqCst);
        router_of(&client).on_disconnect();

        // Then the call returns Disconnected instead of waiting for its timeout
        let result = finish(pending).await;
        assert!(matches!(result, Err(BidiError::Disconnected)), "{result:?}");
    }

    /// CLI-07: A command that gets no answer times out and leaves no trace.
    #[tokio::test]
    async fn timed_out_command_is_removed_from_pending_map() {
        // Given a client whose default timeout is 100 ms and nobody answers
        let (client, _commands) = test_client(Duration::from_millis(100));

        // When the user sends a command
        let params = json!({ "context": "C1", "url": "about:blank" });
        let result = client
            .send_raw_command("browsingContext.navigate", params)
            .await;

        // Then the call times out naming the method
        assert!(
            matches!(&result, Err(BidiError::Timeout { method, .. }) if method == "browsingContext.navigate"),
            "{result:?}"
        );
        // And the pending-request map is empty
        assert_eq!(pending_len(&client), 0, "the id must not leak");
    }

    /// CLI-08: Commands on a dead connection fail fast.
    #[tokio::test]
    async fn dead_connection_fails_fast() {
        // Given a client with is_alive == false
        let (client, mut commands) = test_client(WAIT);
        client.connection.is_alive.store(false, Ordering::SeqCst);

        // When any command is sent
        let result = client.send_raw_command("session.status", NoParams).await;

        // Then it returns Disconnected immediately
        assert!(matches!(result, Err(BidiError::Disconnected)), "{result:?}");
        // And nothing was written and the pending map stays empty
        assert!(commands.try_recv().is_err(), "nothing may be written");
        assert_eq!(pending_len(&client), 0);
    }

    /// CLI-09: A dead writer marks the connection as gone.
    #[tokio::test]
    async fn dead_writer_marks_connection_as_gone() {
        // Given a client whose command receiver has been dropped
        let (client, commands) = test_client(WAIT);
        drop(commands);

        // When a command is sent
        let result = client.send_raw_command("session.status", NoParams).await;

        // Then it returns Disconnected
        assert!(matches!(result, Err(BidiError::Disconnected)), "{result:?}");
        // And the connection is reported as gone, with no pending entry left
        assert!(!client.is_connected());
        assert_eq!(pending_len(&client), 0);
    }

    /// CLI-11: The default ring is sized for a multi-tab session.
    #[test]
    fn default_event_capacity_is_roomy() {
        // The ring is shared by every tab and module of a connection.
        const {
            assert!(
                EVENT_CHANNEL_CAPACITY >= 1024,
                "EVENT_CHANNEL_CAPACITY is too small for a multi-tab connection"
            );
        };
    }

    /// CLI-12: `session_url` normalises the endpoint.
    #[test]
    fn session_url_normalises_endpoints() {
        // A bare host gets the scheme and the /session path
        assert_eq!(session_url("127.0.0.1:9222"), "ws://127.0.0.1:9222/session");
        // A URL that already names a path is left alone
        assert_eq!(session_url("ws://h:1/session"), "ws://h:1/session");
    }

    /// CLI-13: Events go to the broadcast channel.
    #[test]
    fn events_are_broadcast() {
        // Given a subscriber on the event channel and a pending command
        let pending = new_pending();
        let _waiting = wait_for(&pending, 1);
        let (event_tx, _) = broadcast::channel(16);
        let mut events = event_tx.subscribe();

        // When the router receives an event
        let raw = r#"{"type":"event","method":"browsingContext.load","params":{"context":"C1"}}"#;
        route_message(parse(raw), &pending, &event_tx, raw).expect("routes");

        // Then the subscriber receives it
        let event = events.try_recv().expect("the event is broadcast");
        assert_eq!(event.method.as_deref(), Some("browsingContext.load"));
        // And no pending request is touched
        assert_eq!(pending.lock().expect("lock").len(), 1);
    }

    /// CLI-14: A reply with an unknown id is discarded.
    #[test]
    fn reply_with_unknown_id_is_discarded() {
        // Given an empty pending map and an event subscriber
        let pending = new_pending();
        let (event_tx, _) = broadcast::channel(16);
        let mut events = event_tx.subscribe();

        // When a reply for an id nobody waits for arrives
        let raw = r#"{"type":"success","id":99,"result":{}}"#;
        route_message(parse(raw), &pending, &event_tx, raw).expect("routes");

        // Then nothing panics and nothing is broadcast
        assert!(events.try_recv().is_err(), "a reply is never an event");
    }

    /// CLI-15: An error with null id is neither delivered nor broadcast.
    #[test]
    fn error_with_null_id_is_not_routed() {
        // Given a pending command with id 1 and an event subscriber
        let pending = new_pending();
        let _waiting = wait_for(&pending, 1);
        let (event_tx, _) = broadcast::channel(16);
        let mut events = event_tx.subscribe();

        // When an error the browser could not attribute arrives
        let raw = r#"{"type":"error","id":null,"error":"invalid argument","message":"x"}"#;
        route_message(parse(raw), &pending, &event_tx, raw).expect("routes");

        // Then the pending command is still pending
        assert_eq!(pending.lock().expect("lock").len(), 1);
        // And the subscriber receives nothing
        assert!(events.try_recv().is_err());
    }

    /// CLI-16: `connect` opens the given URL and round-trips a command.
    #[tokio::test]
    async fn connect_round_trips_a_command() {
        // Given a mock server
        let mut server = MockServer::start(firefox_like(vec![])).await;

        // When the client connects and sends session.status
        let client = BidiClient::connect(&server.ws_url(), Duration::from_secs(1))
            .await
            .expect("connect");
        let response = client
            .send_raw_command("session.status", NoParams)
            .await
            .expect("a reply");

        // Then the server saw the BiDi command
        let expected = json!({ "id": 1, "method": "session.status", "params": {} });
        assert_eq!(server.next_command().await, expected);
        // And the reply has result.ready == true
        assert_eq!(response.result.expect("a result")["ready"], true);
    }

    /// CLI-17: Unparsable messages do not kill the connection.
    #[tokio::test]
    async fn unparsable_message_does_not_kill_connection() {
        // Given a connected client
        let server = MockServer::start(firefox_like(vec![])).await;
        let client = BidiClient::connect(&server.ws_url(), Duration::from_secs(1))
            .await
            .expect("connect");

        // When the server pushes text that is not JSON
        server.push_raw("not json {");

        // Then the client keeps answering commands, before and after it
        for _ in 0..2 {
            let reply = client.send_raw_command("session.status", NoParams).await;
            assert!(reply.is_ok(), "{reply:?}");
        }
        assert!(client.is_connected());
    }

    /// CLI-18: Server close fails in-flight commands with `Disconnected`.
    #[tokio::test]
    async fn server_close_fails_in_flight_commands() {
        // Given a connected client and a silent server
        let mut server = MockServer::start(silent()).await;
        let client = BidiClient::connect(&server.ws_url(), Duration::from_secs(5))
            .await
            .expect("connect");

        // When a command is in flight and the server closes the socket
        let in_flight = call(&client, "session.status", NoParams);
        let _ = server.next_command().await;
        server.close();

        // Then the call returns Disconnected, well before the 5 s timeout
        let result = finish(in_flight).await;
        assert!(matches!(result, Err(BidiError::Disconnected)), "{result:?}");
        // And the connection is reported as gone
        assert!(!client.is_connected());
        // And a later command fails immediately
        let later = client.send_raw_command("session.status", NoParams).await;
        assert!(matches!(later, Err(BidiError::Disconnected)), "{later:?}");
    }

    /// CLI-19: Events pushed by the server reach `on_module` subscribers.
    #[tokio::test]
    async fn pushed_events_reach_module_subscribers() {
        // Given a connected client with on_module("browsingContext")
        let server = MockServer::start(silent()).await;
        let client = BidiClient::connect(&server.ws_url(), Duration::from_secs(1))
            .await
            .expect("connect");
        let mut events = client.on_module("browsingContext");

        // When the server pushes a browsingContext event
        let event = crate::test_support::event("browsingContext.load", json!({ "context": "C1" }));
        server.push(event);

        // Then the filter yields it
        let received = timeout(WAIT, events.next())
            .await
            .expect("no event within 2 s")
            .expect("the stream is open")
            .expect("not a lag");
        assert_eq!(received.method.as_deref(), Some("browsingContext.load"));
    }

    /// CLI-20: Responses can arrive out of order.
    #[tokio::test]
    async fn out_of_order_replies_reach_their_callers() {
        // Given a server that answers nothing and two commands in flight
        let mut server = MockServer::start(silent()).await;
        let client = BidiClient::connect(&server.ws_url(), Duration::from_secs(5))
            .await
            .expect("connect");
        let first = call(&client, "test.first", json!({}));
        let second = call(&client, "test.second", json!({}));
        let mut issued = Vec::new();
        for _ in 0..2 {
            let command = server.next_command().await;
            issued.push((command["method"].clone(), command["id"].clone()));
        }

        // When the test pushes the reply for the later id first
        issued.sort_by_key(|(_, id)| std::cmp::Reverse(id.as_u64()));
        for (method, id) in &issued {
            server.push(success(id, json!({ "method": method })));
        }

        // Then each caller receives its own result
        for (call, method) in [(first, "test.first"), (second, "test.second")] {
            let response = finish(call).await.expect("a reply");
            assert_eq!(response.result.expect("a result")["method"], method);
        }
    }

    /// CLI-21: A command that cannot be serialized leaves no pending entry.
    #[tokio::test]
    async fn unserializable_params_leave_no_pending_entry() {
        // Given params JSON cannot represent: a map whose keys are not strings
        let (client, mut commands) = test_client(WAIT);
        let params: HashMap<(u8, u8), u8> = HashMap::from([((1, 2), 3)]);

        // When the command is sent
        let result = client.send_raw_command("test.bad", params).await;

        // Then it fails with a serialization error
        assert!(matches!(result, Err(BidiError::Json(_))), "{result:?}");
        // And nothing is pending and nothing was written
        assert_eq!(pending_len(&client), 0);
        assert!(commands.try_recv().is_err(), "nothing may be written");
    }
}
