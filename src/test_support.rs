//! In-process mock WebSocket server for testing without a real browser.
//!
//! One server serves both protocols: it knows nothing about BiDi or RDP, it
//! only relays JSON frames to a handler and optionally greets first.

use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot as tokio_oneshot};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{
    Callback, ErrorResponse, Request, Response,
};
use tokio_tungstenite::{WebSocketStream, accept_hdr_async};

/// How long the test-facing calls wait before panicking with a clear message,
/// so a broken test fails fast instead of hanging the suite.
const WAIT: Duration = Duration::from_secs(2);

type MockSink = SplitSink<WebSocketStream<TcpStream>, Message>;

/// Decides what to send back for one received message, in order.
/// An empty vector stays silent.
pub(crate) type Handler = Box<dyn Fn(&Value) -> Vec<Value> + Send + Sync>;

/// One override of [`rdp_firefox_like`]: replies for an `(actor, type)` pair.
pub(crate) type RdpOverride = ((&'static str, &'static str), Vec<Value>);

/// Knobs for [`MockServer::start_with`].
#[derive(Default)]
pub(crate) struct MockOptions {
    /// Sent right after the handshake (the RDP root greeting).
    pub(crate) greeting: Option<Value>,
}

/// Protocol-agnostic mock WebSocket server accepting a single connection.
pub(crate) struct MockServer {
    addr: SocketAddr,
    commands: mpsc::UnboundedReceiver<Value>,
    pushes: mpsc::UnboundedSender<String>,
    path: tokio_oneshot::Receiver<String>,
}

/// Everything the server task owns.
struct Plumbing {
    handler: Handler,
    commands: mpsc::UnboundedSender<Value>,
    pushes: mpsc::UnboundedReceiver<String>,
    greeting: Option<Value>,
}

impl MockServer {
    /// Starts the server with a reply handler and no greeting (BiDi).
    pub(crate) async fn start(handler: Handler) -> Self {
        Self::start_with(handler, MockOptions::default()).await
    }

    /// Starts the server with explicit options.
    pub(crate) async fn start_with(handler: Handler, options: MockOptions) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the mock must bind a loopback port");
        let addr = listener
            .local_addr()
            .expect("a bound listener has an address");
        let (commands_tx, commands) = mpsc::unbounded_channel();
        let (pushes, pushes_rx) = mpsc::unbounded_channel();
        let (path_tx, path) = tokio_oneshot::channel();
        let plumbing = Plumbing {
            handler,
            commands: commands_tx,
            pushes: pushes_rx,
            greeting: options.greeting,
        };

        tokio::spawn(serve(listener, path_tx, plumbing));

        Self {
            addr,
            commands,
            pushes,
            path,
        }
    }

    /// RDP mode: greets with the `rdp/greeting.json` fixture.
    pub(crate) async fn start_rdp(handler: Handler) -> Self {
        let options = MockOptions {
            greeting: Some(fixture("rdp/greeting.json")),
        };
        Self::start_with(handler, options).await
    }

    /// `"127.0.0.1:PORT"`, what hosts-based constructors expect.
    pub(crate) fn host(&self) -> String {
        self.addr.to_string()
    }

    /// `"ws://127.0.0.1:PORT/session"`, what URL-based constructors expect.
    pub(crate) fn ws_url(&self) -> String {
        format!("ws://{}/session", self.addr)
    }

    /// Next command received. Panics after 2 s with a clear message.
    pub(crate) async fn next_command(&mut self) -> Value {
        timeout(WAIT, self.commands.recv())
            .await
            .expect("no command reached the mock within 2 s")
            .expect("the mock server task ended")
    }

    /// Path requested in the WebSocket handshake. Resolves once per server.
    pub(crate) async fn handshake_path(&mut self) -> String {
        timeout(WAIT, &mut self.path)
            .await
            .expect("no WebSocket handshake reached the mock within 2 s")
            .expect("the mock server task ended")
    }

    /// Sends an arbitrary message to the client at any time.
    pub(crate) fn push(&self, message: Value) {
        self.pushes
            .send(message.to_string())
            .expect("the mock server task ended");
    }
}

/// Accepts one connection, then relays frames until either side is done.
async fn serve(
    listener: TcpListener,
    path_tx: tokio_oneshot::Sender<String>,
    mut plumbing: Plumbing,
) {
    let Ok((stream, _)) = listener.accept().await else {
        return;
    };
    let Ok(socket) = accept_hdr_async(stream, PathRecorder(path_tx)).await else {
        return;
    };
    let (mut sink, mut source) = socket.split();

    if let Some(greeting) = plumbing.greeting.take() {
        if send_json(&mut sink, &greeting).await.is_err() {
            return;
        }
    }

    loop {
        let keep_going = tokio::select! {
            frame = source.next() => on_frame(frame, &plumbing, &mut sink).await,
            push = plumbing.pushes.recv() => on_push(push, &mut sink).await,
        };
        if !keep_going {
            break;
        }
    }
}

/// Handshake callback that reports the requested path to the test.
///
/// A named type rather than a closure: tungstenite fixes the callback's
/// signature, whose error type is large, and clippy only lets that pass in a
/// trait implementation (where the signature is not ours to change).
struct PathRecorder(tokio_oneshot::Sender<String>);

impl Callback for PathRecorder {
    fn on_request(self, request: &Request, response: Response) -> Result<Response, ErrorResponse> {
        // The test may never ask for the path; a closed receiver is fine.
        let _ = self.0.send(request.uri().path().to_string());
        Ok(response)
    }
}

/// Handles one frame from the client. Returns whether to keep serving.
async fn on_frame(
    frame: Option<Result<Message, WsError>>,
    plumbing: &Plumbing,
    sink: &mut MockSink,
) -> bool {
    let Some(Ok(message)) = frame else {
        return false;
    };
    match message {
        Message::Text(text) => relay(plumbing, text.as_str(), sink).await,
        Message::Close(_) => false,
        _ => true,
    }
}

/// Hands a command to the test, then sends whatever the handler decides.
async fn relay(plumbing: &Plumbing, text: &str, sink: &mut MockSink) -> bool {
    let Ok(command) = serde_json::from_str::<Value>(text) else {
        return true;
    };
    // The test may have stopped listening for commands; that is fine.
    let _ = plumbing.commands.send(command.clone());

    for reply in (plumbing.handler)(&command) {
        if send_json(sink, &reply).await.is_err() {
            return false;
        }
    }
    true
}

/// Forwards a test-injected message. `None` means the test dropped the handle.
async fn on_push(push: Option<String>, sink: &mut MockSink) -> bool {
    match push {
        Some(text) => sink.send(Message::Text(text.into())).await.is_ok(),
        None => false,
    }
}

async fn send_json(sink: &mut MockSink, value: &Value) -> Result<(), WsError> {
    sink.send(Message::Text(value.to_string().into())).await
}

/// `{"type":"success","id":..,"result":..}`
pub(crate) fn success(id: &Value, result: Value) -> Value {
    json!({ "type": "success", "id": id, "result": result })
}

/// `{"type":"event","method":..,"params":..}`
pub(crate) fn event(method: &str, params: Value) -> Value {
    json!({ "type": "event", "method": method, "params": params })
}

/// `body` with `"from"` set to `from`.
pub(crate) fn rdp_reply(from: &str, mut body: Value) -> Value {
    body.as_object_mut()
        .expect("an RDP message body must be a JSON object")
        .insert("from".to_string(), json!(from));
    body
}

/// Real messages captured by the spike (`tests/fixtures/`), by relative name.
const FIXTURES: &[(&str, &str)] = &[
    (
        "bidi/event-load.json",
        include_str!("../tests/fixtures/bidi/event-load.json"),
    ),
    (
        "bidi/event-log-entry.json",
        include_str!("../tests/fixtures/bidi/event-log-entry.json"),
    ),
    (
        "bidi/get-tree.json",
        include_str!("../tests/fixtures/bidi/get-tree.json"),
    ),
    (
        "bidi/session-new-busy.json",
        include_str!("../tests/fixtures/bidi/session-new-busy.json"),
    ),
    (
        "bidi/session-new.json",
        include_str!("../tests/fixtures/bidi/session-new.json"),
    ),
    (
        "bidi/session-status-busy.json",
        include_str!("../tests/fixtures/bidi/session-status-busy.json"),
    ),
    (
        "rdp/evaluate-reply.json",
        include_str!("../tests/fixtures/rdp/evaluate-reply.json"),
    ),
    (
        "rdp/evaluation-result.json",
        include_str!("../tests/fixtures/rdp/evaluation-result.json"),
    ),
    (
        "rdp/frames.json",
        include_str!("../tests/fixtures/rdp/frames.json"),
    ),
    (
        "rdp/getWatcher.json",
        include_str!("../tests/fixtures/rdp/getWatcher.json"),
    ),
    (
        "rdp/greeting.json",
        include_str!("../tests/fixtures/rdp/greeting.json"),
    ),
    (
        "rdp/listTabs.json",
        include_str!("../tests/fixtures/rdp/listTabs.json"),
    ),
    (
        "rdp/resources-sources.json",
        include_str!("../tests/fixtures/rdp/resources-sources.json"),
    ),
    (
        "rdp/source-long-string.json",
        include_str!("../tests/fixtures/rdp/source-long-string.json"),
    ),
    (
        "rdp/target-available.json",
        include_str!("../tests/fixtures/rdp/target-available.json"),
    ),
    (
        "rdp/thread-state-paused.json",
        include_str!("../tests/fixtures/rdp/thread-state-paused.json"),
    ),
    (
        "rdp/thread-state-resumed.json",
        include_str!("../tests/fixtures/rdp/thread-state-resumed.json"),
    ),
];

/// Parses the named fixture. Panics listing the known names if it is unknown.
pub(crate) fn fixture(name: &str) -> Value {
    let Some((_, raw)) = FIXTURES.iter().find(|(known, _)| *known == name) else {
        let known: Vec<&str> = FIXTURES.iter().map(|(known, _)| *known).collect();
        panic!("unknown fixture {name:?}; known fixtures: {known:?}");
    };
    serde_json::from_str(raw)
        .unwrap_or_else(|error| panic!("fixture {name} is not valid JSON: {error}"))
}

/// Answers like a Firefox with an idle session; per-method result overrides.
///
/// Unknown methods get an empty result, so a test only scripts what it cares
/// about.
pub(crate) fn firefox_like(overrides: Vec<(&'static str, Value)>) -> Handler {
    let subscriptions = AtomicU64::new(0);
    Box::new(move |command| {
        let method = command["method"].as_str().unwrap_or_default();
        let result = overrides
            .iter()
            .find(|(overridden, _)| *overridden == method)
            .map(|(_, result)| result.clone())
            .unwrap_or_else(|| default_result(command, method, &subscriptions));
        vec![success(&command["id"], result)]
    })
}

/// The idle-Firefox result for `method`.
fn default_result(command: &Value, method: &str, subscriptions: &AtomicU64) -> Value {
    match method {
        "session.status" => json!({ "ready": true, "message": "" }),
        "session.new" => json!({
            "sessionId": "S-1",
            "capabilities": {
                "browserName": "firefox",
                "browserVersion": "157.0.1",
                "acceptInsecureCerts": false,
                "userAgent": "Mozilla/5.0 test",
            },
        }),
        "session.subscribe" => {
            let n = subscriptions.fetch_add(1, Ordering::SeqCst) + 1;
            json!({ "subscription": format!("SUB-{n}") })
        }
        "browsingContext.create" => json!({ "context": "CTX-NEW" }),
        "browsingContext.navigate" => {
            json!({ "navigation": "NAV-1", "url": command["params"]["url"] })
        }
        "browsingContext.getTree" => json!({ "contexts": [] }),
        _ => json!({}),
    }
}

/// Answers the RDP watcher bootstrap from the fixtures.
///
/// Like Firefox, `watchTargets` and `watchResources` emit their notification
/// before the reply. Replies carry the requested actor in `from`; unknown
/// requests get an empty reply. `overrides` replace the whole answer for an
/// `(actor, type)` pair.
pub(crate) fn rdp_firefox_like(overrides: Vec<RdpOverride>) -> Handler {
    Box::new(move |packet| {
        let to = packet["to"].as_str().unwrap_or_default();
        let kind = packet["type"].as_str().unwrap_or_default();
        let overridden = overrides
            .iter()
            .find(|((actor, method), _)| *actor == to && *method == kind);
        match overridden {
            Some((_, replies)) => replies.clone(),
            None => default_rdp_replies(to, kind),
        }
    })
}

/// The idle-Firefox answer to an RDP request of type `kind` sent to `to`.
fn default_rdp_replies(to: &str, kind: &str) -> Vec<Value> {
    match kind {
        "listTabs" => vec![rdp_reply(to, fixture("rdp/listTabs.json"))],
        "getWatcher" => vec![rdp_reply(to, fixture("rdp/getWatcher.json"))],
        "getBreakpointListActor" => vec![rdp_reply(
            to,
            json!({ "breakpointList": { "actor": "server1.conn1.breakpoint-list4" } }),
        )],
        "getThreadConfigurationActor" => vec![rdp_reply(
            to,
            json!({ "configuration": { "actor": "server1.conn1.thread-configuration5" } }),
        )],
        "watchTargets" => vec![
            rdp_reply(to, fixture("rdp/target-available.json")),
            rdp_reply(to, json!({})),
        ],
        "watchResources" => vec![
            fixture("rdp/resources-sources.json"),
            rdp_reply(to, json!({})),
        ],
        _ => vec![rdp_reply(to, json!({}))],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use serde_json::json;
    use std::time::Duration;
    use tokio::net::TcpStream;
    use tokio::time::timeout;
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

    type RawClient = WebSocketStream<MaybeTlsStream<TcpStream>>;

    async fn connect_raw(url: &str) -> RawClient {
        let (client, _) = connect_async(url)
            .await
            .expect("the raw client should connect");
        client
    }

    async fn send_raw(client: &mut RawClient, message: Value) {
        let frame = Message::Text(message.to_string().into());
        client
            .send(frame)
            .await
            .expect("the raw client should send");
    }

    async fn read_raw(client: &mut RawClient) -> Value {
        let frame = timeout(Duration::from_secs(2), client.next())
            .await
            .expect("timed out waiting for a frame")
            .expect("the socket closed")
            .expect("the frame should be readable");
        serde_json::from_str(frame.to_text().expect("a text frame")).expect("a JSON frame")
    }

    /// MCK-01: It records the handshake path.
    #[tokio::test]
    async fn mock_records_handshake_path() {
        // Given a mock server started with firefox_like
        let mut server = MockServer::start(firefox_like(vec![])).await;

        // When a raw client connects to ws_url()
        let _client = connect_raw(&server.ws_url()).await;

        // Then the handshake path is /session
        assert_eq!(server.handshake_path().await, "/session");
    }

    /// MCK-02: It hands received commands to the test and replies through the handler.
    #[tokio::test]
    async fn mock_forwards_commands_and_replies() {
        // Given a mock server started with firefox_like and a connected client
        let mut server = MockServer::start(firefox_like(vec![])).await;
        let mut client = connect_raw(&server.ws_url()).await;

        // When the client sends session.status
        let command = json!({ "id": 1, "method": "session.status", "params": {} });
        send_raw(&mut client, command.clone()).await;

        // Then the test sees that command
        assert_eq!(server.next_command().await, command);
        // And the client reads the handler's reply
        assert_eq!(
            read_raw(&mut client).await,
            json!({ "type": "success", "id": 1, "result": { "ready": true, "message": "" } })
        );
    }

    /// MCK-03: Pushed messages reach the client.
    #[tokio::test]
    async fn mock_pushes_messages_to_client() {
        // Given a connected raw client
        let server = MockServer::start(firefox_like(vec![])).await;
        let mut client = connect_raw(&server.ws_url()).await;

        // When the test pushes an event
        let pushed = event("browsingContext.load", json!({ "context": "C1" }));
        server.push(pushed.clone());

        // Then the client reads that event
        assert_eq!(read_raw(&mut client).await, pushed);
    }

    /// MCK-04: RDP mode greets first and can emit notifications before a reply.
    #[tokio::test]
    async fn rdp_mode_greets_and_orders_notifications() {
        // Given a mock started in RDP mode and a connected client
        let server = MockServer::start_rdp(rdp_firefox_like(vec![])).await;
        let mut client = connect_raw(&format!("ws://{}", server.host())).await;

        // Then the first frame is the greeting fixture
        assert_eq!(read_raw(&mut client).await, fixture("rdp/greeting.json"));

        // When the client asks to watch targets
        let request = json!({ "to": "watcher1", "type": "watchTargets", "targetType": "frame" });
        send_raw(&mut client, request).await;

        // Then a target-available-form notification arrives first
        assert_eq!(read_raw(&mut client).await["type"], "target-available-form");
        // And only then the reply from the requested actor
        assert_eq!(read_raw(&mut client).await, json!({ "from": "watcher1" }));
    }

    /// MCK-05: Every declared fixture parses as JSON.
    #[test]
    fn every_fixture_parses() {
        for (name, _) in FIXTURES {
            // When the fixture is loaded
            let value = fixture(name);

            // Then it is a JSON object
            assert!(value.is_object(), "{name} should be a JSON object");
        }
        // And the table is not empty
        assert!(!FIXTURES.is_empty(), "no fixtures are declared");
    }
}
