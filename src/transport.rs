//! Shared WebSocket transport, writer task, and reader loop.
//!
//! Everything here is protocol-agnostic: [`BidiClient`](crate::client::BidiClient)
//! and the RDP client both open their socket through it and plug in a
//! [`Router`] that knows what the frames mean. That is what keeps the two
//! clients from duplicating the connection plumbing.

use crate::error::{BidiError, BidiResult};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tracing::{debug, error};

pub(crate) type WebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// What the reader loop does with each text frame.
pub(crate) trait Router: Send + 'static {
    /// Handles one text frame. Parsing errors are the router's business
    /// (logged and dropped): a bad frame never fails the connection.
    fn route(&mut self, text: &str);

    /// Called once when the socket is gone: wake every pending caller.
    fn on_disconnect(&mut self);
}

/// Handles shared by every protocol client.
#[derive(Clone, Debug)]
pub(crate) struct Connection {
    /// Queue drained by the writer task.
    pub(crate) command_tx: mpsc::UnboundedSender<String>,
    /// Cleared by the reader loop when the socket goes away.
    pub(crate) is_alive: Arc<AtomicBool>,
}

/// Turns a host or URL into the WebSocket URL to connect to.
///
/// Adds `ws://` when there is no scheme, trims a trailing slash and appends
/// `path` unless the URL already has one.
pub(crate) fn ws_url(host: &str, path: Option<&str>) -> String {
    // Anything with a scheme is already a URL; tungstenite rejects unsupported ones.
    let url = if host.contains("://") {
        host.to_string()
    } else {
        format!("ws://{host}")
    };
    let url = url.trim_end_matches('/');

    match path {
        Some(path) if !has_path(url) => format!("{url}{path}"),
        _ => url.to_string(),
    }
}

/// Whether `url` (scheme included) already names a path after its authority.
fn has_path(url: &str) -> bool {
    let authority_and_path = url.split_once("://").map_or(url, |(_, rest)| rest);
    authority_and_path.contains('/')
}

/// Opens the socket and spawns the writer task and the reader loop.
pub(crate) async fn open<R: Router>(url: &str, router: R) -> BidiResult<Connection> {
    let (socket, _) = connect_async(url).await?;
    Ok(spawn(socket, router))
}

/// Spawns the writer task and the reader loop over an open socket.
pub(crate) fn spawn<R: Router>(socket: WebSocket, router: R) -> Connection {
    let (sink, source) = socket.split();
    let (command_tx, command_rx) = mpsc::unbounded_channel();
    let is_alive = Arc::new(AtomicBool::new(true));

    tokio::spawn(write_commands(sink, command_rx));
    tokio::spawn(read_loop(source, router, Arc::clone(&is_alive)));

    Connection {
        command_tx,
        is_alive,
    }
}

/// Queues one payload for the writer task.
///
/// Marks the connection dead and returns `Disconnected` if the writer is gone.
pub(crate) fn send(connection: &Connection, payload: String) -> BidiResult<()> {
    connection.command_tx.send(payload).map_err(|_| {
        // The writer only stops when the socket is gone.
        connection.is_alive.store(false, Ordering::SeqCst);
        BidiError::Disconnected
    })
}

/// Applies the event-channel capacity policy.
///
/// `tokio::sync::broadcast` panics on a zero-capacity channel, and a ring of
/// one is the smallest thing that can carry a single event, so `0` degrades to
/// `1` instead of failing the connection.
pub(crate) fn effective_capacity(capacity: usize) -> usize {
    capacity.max(1)
}

/// Writer task: drains the command queue onto the socket.
///
/// Owning the sink here is what lets clients stay `Clone`: every clone pushes
/// into the same queue instead of sharing a locked sink.
async fn write_commands(
    mut sink: SplitSink<WebSocket, Message>,
    mut command_rx: mpsc::UnboundedReceiver<String>,
) {
    while let Some(payload) = command_rx.recv().await {
        debug!("About to send command {payload}");
        if let Err(error) = sink.send(Message::Text(payload.into())).await {
            error!("Fatal error in the WebSocket writer task: {error}");
            return;
        }
    }

    // Every handle is gone. Closing the socket here is what ends the reader
    // loop: without it the reader keeps the connection (and, on Firefox, the
    // single BiDi session) alive until the browser closes it.
    let _ = sink.close().await;
}

/// Reader loop: hands every text frame to the router until the socket ends.
async fn read_loop<R: Router>(
    mut source: SplitStream<WebSocket>,
    mut router: R,
    is_alive: Arc<AtomicBool>,
) {
    while let Some(frame) = source.next().await {
        match frame {
            Ok(Message::Text(text)) => router.route(text.as_str()),
            Ok(Message::Close(_)) => break,
            // Pings are answered by tungstenite; binary frames mean nothing here.
            Ok(_) => {}
            Err(error) => {
                debug!("Connection lost due to error: {error}");
                break;
            }
        }
    }

    is_alive.store(false, Ordering::SeqCst);
    router.on_disconnect();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{MockServer, silent};
    use futures_util::StreamExt;
    use serde_json::json;
    use std::sync::atomic::Ordering;
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio::time::timeout;
    use tokio_tungstenite::accept_async;
    use tokio_tungstenite::tungstenite::Message;

    const WAIT: Duration = Duration::from_secs(2);

    /// A router that reports everything it is told through channels.
    struct RecordingRouter {
        frames: mpsc::UnboundedSender<String>,
        disconnects: mpsc::UnboundedSender<()>,
    }

    impl Router for RecordingRouter {
        fn route(&mut self, text: &str) {
            let _ = self.frames.send(text.to_string());
        }

        fn on_disconnect(&mut self) {
            let _ = self.disconnects.send(());
        }
    }

    /// The test's end of a [`RecordingRouter`].
    struct Probe {
        frames: mpsc::UnboundedReceiver<String>,
        disconnects: mpsc::UnboundedReceiver<()>,
    }

    fn recording_router() -> (RecordingRouter, Probe) {
        let (frames_tx, frames) = mpsc::unbounded_channel();
        let (disconnects_tx, disconnects) = mpsc::unbounded_channel();
        let router = RecordingRouter {
            frames: frames_tx,
            disconnects: disconnects_tx,
        };
        (
            router,
            Probe {
                frames,
                disconnects,
            },
        )
    }

    async fn next_frame(probe: &mut Probe) -> String {
        timeout(WAIT, probe.frames.recv())
            .await
            .expect("no frame reached the router within 2 s")
            .expect("the router was dropped")
    }

    /// TRN-01: Text frames reach the router in order.
    #[tokio::test]
    async fn text_frames_reach_router_in_order() {
        // Given a mock server and a recording router
        let server = MockServer::start(silent()).await;
        let (router, mut probe) = recording_router();
        let _connection = open(&server.ws_url(), router).await.expect("connect");

        // When the server pushes "a", "b", "c"
        for text in ["a", "b", "c"] {
            server.push_raw(text);
        }

        // Then the router saw them in that order
        let mut seen = Vec::new();
        for _ in 0..3 {
            seen.push(next_frame(&mut probe).await);
        }
        assert_eq!(seen, ["a", "b", "c"]);
    }

    /// TRN-02: A closed socket marks the connection dead and calls `on_disconnect` once.
    #[tokio::test]
    async fn closed_socket_calls_on_disconnect_once() {
        // Given a connection to a mock server
        let server = MockServer::start(silent()).await;
        let (router, mut probe) = recording_router();
        let connection = open(&server.ws_url(), router).await.expect("connect");

        // When the server closes
        server.close();

        // Then on_disconnect is called
        let first = timeout(WAIT, probe.disconnects.recv())
            .await
            .expect("on_disconnect was not called within 2 s");
        assert_eq!(first, Some(()));
        // And the connection is marked dead
        assert!(!connection.is_alive.load(Ordering::SeqCst));
        // And it was the only call: the router is dropped with the loop
        let second = timeout(WAIT, probe.disconnects.recv()).await;
        assert_eq!(second, Ok(None), "on_disconnect must run exactly once");
    }

    /// TRN-03: `send` on a dead writer returns `Disconnected` and marks the connection dead.
    #[test]
    fn send_on_dead_writer_is_disconnected() {
        // Given a connection whose receiver was dropped
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        drop(command_rx);
        let connection = Connection {
            command_tx,
            is_alive: Arc::new(AtomicBool::new(true)),
        };

        // When a payload is sent
        let result = send(&connection, "{}".to_string());

        // Then it fails with Disconnected
        assert!(matches!(result, Err(BidiError::Disconnected)), "{result:?}");
        // And the connection is marked dead
        assert!(!connection.is_alive.load(Ordering::SeqCst));
    }

    /// TRN-04: Payloads are written as text frames in send order.
    #[tokio::test]
    async fn payloads_are_written_in_order() {
        // Given a connection to a mock server
        let mut server = MockServer::start(silent()).await;
        let (router, _probe) = recording_router();
        let connection = open(&server.ws_url(), router).await.expect("connect");

        // When two payloads are sent
        send(&connection, json!({ "n": 1 }).to_string()).expect("first send");
        send(&connection, json!({ "n": 2 }).to_string()).expect("second send");

        // Then the server receives them in that order
        assert_eq!(server.next_command().await, json!({ "n": 1 }));
        assert_eq!(server.next_command().await, json!({ "n": 2 }));
    }

    /// TRN-05: `ws_url` normalises hosts and paths.
    #[test]
    fn ws_url_normalises_hosts_and_paths() {
        const SESSION: Option<&str> = Some("/session");
        let cases = [
            ("127.0.0.1:9222", SESSION, "ws://127.0.0.1:9222/session"),
            ("localhost:9222/", SESSION, "ws://localhost:9222/session"),
            ("ws://h:1/session", SESSION, "ws://h:1/session"),
            ("wss://remote/x", SESSION, "wss://remote/x"),
            ("ws://h:1", SESSION, "ws://h:1/session"),
            ("ws://h:1/", SESSION, "ws://h:1/session"),
            ("127.0.0.1:6000", None, "ws://127.0.0.1:6000"),
        ];

        for (host, path, expected) in cases {
            // When the endpoint is normalised
            // Then it has a scheme and, when asked, exactly one path
            assert_eq!(ws_url(host, path), expected, "ws_url({host:?}, {path:?})");
        }
    }

    /// TRN-06: `open` on an unreachable address fails with `Ws`.
    #[tokio::test]
    async fn open_unreachable_address_fails_with_ws() {
        // Given a router and an address nobody listens on
        let (router, _probe) = recording_router();

        // When the connection is opened
        let result = open("ws://127.0.0.1:1", router).await;

        // Then it fails with a WebSocket error
        assert!(matches!(result, Err(BidiError::Ws(_))), "{result:?}");
    }

    /// TRN-07: Dropping every handle to the connection closes the socket.
    #[tokio::test]
    async fn dropping_every_handle_closes_the_socket() {
        // Given a raw server that reports how the client's side ended
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!("ws://{}", listener.local_addr().expect("address"));
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut socket = accept_async(stream).await.expect("handshake");
            matches!(socket.next().await, Some(Ok(Message::Close(_))))
        });
        let (router, _probe) = recording_router();
        let connection = open(&url, router).await.expect("connect");

        // When every handle to the connection is dropped
        drop(connection);

        // Then the server sees a close frame, so a BiDi session cannot outlive its client
        let saw_close = timeout(WAIT, server)
            .await
            .expect("the socket stayed open after the last handle was dropped")
            .expect("the raw server task");
        assert!(saw_close, "the client must send a close frame");
    }

    /// CLI-10: Capacity zero degrades to one.
    #[test]
    fn effective_capacity_clamps_zero_to_one() {
        // Then a zero ring would panic tokio, so it degrades to the smallest usable one
        assert_eq!(effective_capacity(0), 1);
        assert_eq!(effective_capacity(1), 1);
        assert_eq!(effective_capacity(128), 128);
    }
}
