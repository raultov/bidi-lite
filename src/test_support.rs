//! In-process mock WebSocket server for testing without a real browser.
//!
//! One server serves both protocols: it knows nothing about BiDi or RDP, it
//! only relays JSON frames to a handler and optionally greets first.

use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};
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
    control: mpsc::UnboundedSender<Control>,
    path: tokio_oneshot::Receiver<String>,
    closed: tokio_oneshot::Receiver<()>,
}

/// What the test can ask the server to do once the client is connected.
enum Control {
    /// Send this raw text frame (so malformed JSON can be injected too).
    Push(String),
    /// Close the WebSocket, as a browser that dies or ends the session would.
    Close,
}

/// Everything the server task owns.
struct Plumbing {
    handler: Handler,
    commands: mpsc::UnboundedSender<Value>,
    control: mpsc::UnboundedReceiver<Control>,
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
        let (control, control_rx) = mpsc::unbounded_channel();
        let (path_tx, path) = tokio_oneshot::channel();
        let (closed_tx, closed) = tokio_oneshot::channel();
        let plumbing = Plumbing {
            handler,
            commands: commands_tx,
            control: control_rx,
            greeting: options.greeting,
        };

        tokio::spawn(async move {
            serve(listener, path_tx, plumbing).await;
            // The test may not be waiting for the end of the connection.
            let _ = closed_tx.send(());
        });

        Self {
            addr,
            commands,
            control,
            path,
            closed,
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

    /// Next command whose `method` equals `method`, skipping the others.
    ///
    /// Matches BiDi commands by `method` and RDP requests by `type`.
    pub(crate) async fn next_command_named(&mut self, method: &str) -> Value {
        loop {
            let command = self.next_command().await;
            // BiDi names the command in `method`, RDP in `type`.
            let name = command["method"]
                .as_str()
                .or_else(|| command["type"].as_str());
            if name == Some(method) {
                return command;
            }
        }
    }

    /// Asserts that no command arrives within `wait`.
    ///
    /// Panics with `unexpected command` otherwise, so a test cannot pass by
    /// not waiting long enough for a command it believes was never sent.
    pub(crate) async fn assert_no_command(&mut self, wait: Duration) {
        if let Ok(Some(command)) = timeout(wait, self.commands.recv()).await {
            panic!("unexpected command reached the mock: {command}");
        }
    }

    /// Path requested in the WebSocket handshake. Resolves once per server.
    pub(crate) async fn handshake_path(&mut self) -> String {
        timeout(WAIT, &mut self.path)
            .await
            .expect("no WebSocket handshake reached the mock within 2 s")
            .expect("the mock server task ended")
    }

    /// Resolves once the connection has ended, e.g. because the client closed the
    /// socket. Panics after 2 s, so a client that leaves it open fails the test.
    pub(crate) async fn wait_until_closed(&mut self) {
        timeout(WAIT, &mut self.closed)
            .await
            .expect("the client did not close the socket within 2 s")
            .expect("the mock server task panicked");
    }

    /// Sends an arbitrary message to the client at any time.
    pub(crate) fn push(&self, message: Value) {
        self.push_raw(&message.to_string());
    }

    /// Sends arbitrary text to the client, valid JSON or not.
    pub(crate) fn push_raw(&self, text: &str) {
        self.send_control(Control::Push(text.to_string()));
    }

    /// Closes the WebSocket from the server side.
    pub(crate) fn close(&self) {
        self.send_control(Control::Close);
    }

    fn send_control(&self, control: Control) {
        self.control
            .send(control)
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

    if let Some(greeting) = plumbing.greeting.take()
        && send_json(&mut sink, &greeting).await.is_err()
    {
        return;
    }

    loop {
        let keep_going = tokio::select! {
            frame = source.next() => on_frame(frame, &plumbing, &mut sink).await,
            control = plumbing.control.recv() => on_control(control, &mut sink).await,
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
        if reply == close_marker() {
            // The peer may already be gone; either way the server is done.
            let _ = sink.close().await;
            return false;
        }
        if send_json(sink, &reply).await.is_err() {
            return false;
        }
    }
    true
}

/// Carries out a test request. `None` means the test dropped the handle.
async fn on_control(control: Option<Control>, sink: &mut MockSink) -> bool {
    match control {
        Some(Control::Push(text)) => sink.send(Message::Text(text.into())).await.is_ok(),
        Some(Control::Close) => {
            // The peer may already be gone; either way the server is done.
            let _ = sink.close().await;
            false
        }
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

/// `{"type":"error","id":..,"error":..,"message":..,"stacktrace":""}`
pub(crate) fn error(id: &Value, code: &str, message: &str) -> Value {
    json!({ "type": "error", "id": id, "error": code, "message": message, "stacktrace": "" })
}

/// A reply that is not sent: it closes the socket at that point of a reply list.
///
/// Replies before it are delivered and replies after it are not, which is how
/// Firefox behaves when it answers `session.end` or dies mid-conversation.
pub(crate) fn close_marker() -> Value {
    json!({ "__mock": "close" })
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
        "rdp/breakpoint-positions.json",
        include_str!("../tests/fixtures/rdp/breakpoint-positions.json"),
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
        "rdp/resources-css-message.json",
        include_str!("../tests/fixtures/rdp/resources-css-message.json"),
    ),
    (
        "rdp/resources-error-message.json",
        include_str!("../tests/fixtures/rdp/resources-error-message.json"),
    ),
    (
        "rdp/resources-sources.json",
        include_str!("../tests/fixtures/rdp/resources-sources.json"),
    ),
    (
        "rdp/resources-thread-state-paused.json",
        include_str!("../tests/fixtures/rdp/resources-thread-state-paused.json"),
    ),
    (
        "rdp/resources-thread-state-resumed.json",
        include_str!("../tests/fixtures/rdp/resources-thread-state-resumed.json"),
    ),
    (
        "rdp/resources-websocket.json",
        include_str!("../tests/fixtures/rdp/resources-websocket.json"),
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
        "rdp/target-destroyed.json",
        include_str!("../tests/fixtures/rdp/target-destroyed.json"),
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

/// A captured reply re-addressed to `command`: the same message Firefox sent,
/// with the `id` of the command it now answers.
pub(crate) fn fixture_for(name: &str, command: &Value) -> Value {
    let mut reply = fixture(name);
    reply["id"] = command["id"].clone();
    reply
}

/// Everything the crate logs, at every level, from the moment of capture.
///
/// One subscriber is installed for the whole test binary. A per-thread one would
/// miss log lines whose interest `tracing` cached as disabled while another test
/// ran without a subscriber, and a test checking that a secret is *not* logged
/// would then pass without checking anything. Tests run in parallel, so the text
/// also holds other tests' lines; look for values only your test uses.
pub(crate) struct CapturedLogs {
    start: usize,
}

impl CapturedLogs {
    /// What was logged since the capture started.
    pub(crate) fn text(&self) -> String {
        let buffer = LOGS.lock().unwrap_or_else(PoisonError::into_inner);
        String::from_utf8_lossy(&buffer[self.start..]).into_owned()
    }
}

static LOGS: Mutex<Vec<u8>> = Mutex::new(Vec::new());

/// Starts capturing the crate's logs.
pub(crate) fn capture_logs() -> CapturedLogs {
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(|| {
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_ansi(false)
            .with_writer(|| LogWriter)
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("no other global subscriber in the tests");
    });
    let start = LOGS.lock().unwrap_or_else(PoisonError::into_inner).len();
    CapturedLogs { start }
}

struct LogWriter;

impl std::io::Write for LogWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let mut buffer = LOGS.lock().unwrap_or_else(PoisonError::into_inner);
        buffer.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Never answers anything: for timeouts and for tests that script replies by hand.
pub(crate) fn silent() -> Handler {
    Box::new(|_| Vec::new())
}

/// `inner`, except that `replies` decides what `method` gets.
///
/// The way to script an error, a captured reply or a dying socket for one
/// command without rewriting the whole handler.
pub(crate) fn answering(
    inner: Handler,
    method: &'static str,
    replies: impl Fn(&Value) -> Vec<Value> + Send + Sync + 'static,
) -> Handler {
    Box::new(move |command| {
        if command["method"].as_str() == Some(method) {
            replies(command)
        } else {
            inner(command)
        }
    })
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
            None => default_rdp_replies(packet, to, kind),
        }
    })
}

/// The idle-Firefox answer to an RDP request of type `kind` sent to `to`.
fn default_rdp_replies(packet: &Value, to: &str, kind: &str) -> Vec<Value> {
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
        "watchResources" => watch_resources_replies(packet, to),
        // One-way requests: Firefox never answers them.
        "unwatchTargets" | "unwatchResources" => Vec::new(),
        _ => vec![rdp_reply(to, json!({}))],
    }
}

fn watch_resources_replies(packet: &Value, to: &str) -> Vec<Value> {
    let mut replies = Vec::new();
    let types = packet.get("resourceTypes").and_then(Value::as_array);
    let has =
        |name: &str| types.is_none_or(|arr| arr.iter().any(|item| item.as_str() == Some(name)));
    if has("source") {
        replies.push(fixture("rdp/resources-sources.json"));
    }
    if has("websocket") && types.is_some() {
        replies.push(fixture("rdp/resources-websocket.json"));
    }
    if has("error-message") && types.is_some() {
        replies.push(fixture("rdp/resources-error-message.json"));
    }
    if has("css-message") && types.is_some() {
        replies.push(fixture("rdp/resources-css-message.json"));
    }
    replies.push(rdp_reply(to, json!({})));
    replies
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

    /// Reads frames until the socket ends, returning the JSON ones.
    async fn read_until_closed(client: &mut RawClient) -> Vec<Value> {
        let mut seen = Vec::new();
        loop {
            let frame = timeout(Duration::from_secs(2), client.next())
                .await
                .expect("the socket neither delivered a frame nor closed within 2 s");
            match frame {
                Some(Ok(Message::Text(text))) => {
                    seen.push(serde_json::from_str(text.as_str()).expect("a JSON frame"));
                }
                // A close frame is followed by the end of the stream.
                Some(Ok(_)) => {}
                Some(Err(_)) | None => return seen,
            }
        }
    }

    /// MCK-06: A close marker among the replies closes the socket at that point.
    #[tokio::test]
    async fn close_marker_closes_the_socket_in_order() {
        // Given a handler that answers, then dies, then would answer again
        let handler = answering(firefox_like(vec![]), "x.dies", |command| {
            vec![
                success(&command["id"], json!({})),
                close_marker(),
                event("x.late", json!({})),
            ]
        });
        let server = MockServer::start(handler).await;
        let mut client = connect_raw(&server.ws_url()).await;

        // When the client sends that command
        send_raw(
            &mut client,
            json!({ "id": 1, "method": "x.dies", "params": {} }),
        )
        .await;

        // Then it reads the reply sent before the marker and nothing after it,
        // not even the marker itself
        let seen = read_until_closed(&mut client).await;
        assert_eq!(seen, [success(&json!(1), json!({}))]);
    }

    /// MCK-07: `answering` replaces the replies of one method and leaves the rest alone.
    #[tokio::test]
    async fn answering_overrides_one_method_only() {
        // Given a handler that makes session.new fail
        let handler = answering(firefox_like(vec![]), "session.new", |command| {
            vec![error(&command["id"], "session not created", "busy")]
        });
        let server = MockServer::start(handler).await;
        let mut client = connect_raw(&server.ws_url()).await;

        // When the client sends the overridden method and another one
        send_raw(
            &mut client,
            json!({ "id": 1, "method": "session.new", "params": {} }),
        )
        .await;
        send_raw(
            &mut client,
            json!({ "id": 2, "method": "session.status", "params": {} }),
        )
        .await;

        // Then the first gets the scripted error, shaped like a real one
        let expected = json!({
            "type": "error",
            "id": 1,
            "error": "session not created",
            "message": "busy",
            "stacktrace": "",
        });
        assert_eq!(read_raw(&mut client).await, expected);
        // And the second keeps the default reply
        assert_eq!(read_raw(&mut client).await["result"]["ready"], true);
    }

    /// MCK-08: `fixture_for` re-addresses a captured reply to the command it answers.
    #[test]
    fn fixture_for_readdresses_a_captured_reply() {
        // Given a command and a captured Firefox error
        let command = json!({ "id": 42, "method": "session.new", "params": {} });

        // When the fixture is re-addressed to it
        let reply = fixture_for("bidi/session-new-busy.json", &command);

        // Then it carries the command's id and everything else Firefox sent
        assert_eq!(reply["id"], 42);
        assert_eq!(reply["error"], "session not created");
        assert_eq!(reply["message"], "Maximum number of active sessions");
    }

    /// MCK-09: `next_command_named` skips other commands.
    #[tokio::test]
    async fn next_command_named_skips_other_commands() {
        // Given a server that received two commands
        let mut server = MockServer::start(silent()).await;
        let mut client = connect_raw(&server.ws_url()).await;
        send_raw(
            &mut client,
            json!({ "id": 1, "method": "a.b", "params": {} }),
        )
        .await;
        send_raw(
            &mut client,
            json!({ "id": 2, "method": "c.d", "params": {} }),
        )
        .await;

        // When the second is asked for by name
        let wanted = server.next_command_named("c.d").await;

        // Then it is returned, and the skipped one is not handed out again
        assert_eq!(wanted["id"], 2);
        server.assert_no_command(Duration::from_millis(50)).await;
    }

    /// MCK-10: `assert_no_command` fails when a command does arrive.
    #[tokio::test]
    #[should_panic(expected = "unexpected command")]
    async fn assert_no_command_fails_when_one_arrives() {
        // Given a server that received a command
        let mut server = MockServer::start(silent()).await;
        let mut client = connect_raw(&server.ws_url()).await;
        send_raw(
            &mut client,
            json!({ "id": 1, "method": "a.b", "params": {} }),
        )
        .await;

        // Then asserting that none arrives panics instead of passing vacuously
        server.assert_no_command(WAIT).await;
    }

    /// MCK-11: It reports when the client closes the socket.
    #[tokio::test]
    async fn mock_reports_the_client_closing() {
        // Given a connected raw client
        let mut server = MockServer::start(silent()).await;
        let mut client = connect_raw(&server.ws_url()).await;

        // When it closes the socket
        client
            .close(None)
            .await
            .expect("the raw client should close");

        // Then the server reports the end of the connection
        server.wait_until_closed().await;
    }

    /// MCK-13: `next_command_named` matches RDP requests by their `type`.
    #[tokio::test]
    async fn next_command_named_matches_rdp_types() {
        // Given a server that received a BiDi command and then an RDP request
        let mut server = MockServer::start(silent()).await;
        let mut client = connect_raw(&server.ws_url()).await;
        send_raw(
            &mut client,
            json!({ "id": 1, "method": "a.b", "params": {} }),
        )
        .await;
        send_raw(
            &mut client,
            json!({ "to": "watcher1", "type": "watchTargets" }),
        )
        .await;

        // When the RDP request is asked for by its type
        let wanted = server.next_command_named("watchTargets").await;

        // Then it is returned
        assert_eq!(wanted["to"], "watcher1");
    }

    /// MCK-14: The RDP handler does not answer one-way requests, like Firefox.
    #[test]
    fn rdp_handler_stays_silent_on_oneway_requests() {
        let handler = rdp_firefox_like(vec![]);
        for kind in ["unwatchTargets", "unwatchResources"] {
            // Then nothing is sent back
            assert!(
                handler(&json!({ "to": "watcher1", "type": kind })).is_empty(),
                "{kind}"
            );
        }
        // And an ordinary request still gets its reply
        assert!(!handler(&json!({ "to": "watcher1", "type": "watchTargets" })).is_empty());
    }

    /// MCK-12: `wait_until_closed` fails when the client stays connected.
    #[tokio::test]
    #[should_panic(expected = "did not close")]
    async fn wait_until_closed_fails_if_the_client_stays_connected() {
        // Given a client that stays connected
        let mut server = MockServer::start(silent()).await;
        let _client = connect_raw(&server.ws_url()).await;

        // Then waiting for the close panics instead of passing vacuously
        server.wait_until_closed().await;
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
