//! Firefox RDP client connection and per-actor FIFO request routing.
//!
//! RDP has no request ids. A reply is matched to its request because it comes
//! from the same actor and in the same order, so each actor has a queue with at
//! most one request on the wire. Requests to different actors run in parallel;
//! two requests to the *same* actor are serialised.
//!
//! The connection, the writer task and the reader loop are the ones
//! [`BidiClient`](crate::client::BidiClient) uses (the private `transport`
//! module); only the correlation differs.

use crate::client::EVENT_CHANNEL_CAPACITY;
use crate::error::{BidiError, BidiResult};
use crate::event_filter::EventFilter;
use crate::protocol::NoParams;
use crate::rdp::protocol::{RdpPacket, encode_request};
use crate::transport::{self, Connection, Router, WeakWriter};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::timeout;
use tracing::{debug, trace};

/// What the actor queues guard, for the poisoned-lock error.
const ACTOR_QUEUES: &str = "RDP actor queues";

/// A request waiting for its turn on an actor.
struct Queued {
    payload: String,
    reply: oneshot::Sender<RdpPacket>,
}

/// Requests of one actor: at most one on the wire, the rest queued.
#[derive(Default)]
struct ActorQueue {
    /// Who is waiting for the reply to the request on the wire.
    ///
    /// If that caller timed out the receiver is gone, but the entry stays: Firefox
    /// still owes a reply, and consuming it here is what keeps the next request
    /// from being handed the wrong one.
    active: Option<oneshot::Sender<RdpPacket>>,
    waiting: VecDeque<Queued>,
}

impl ActorQueue {
    /// Nothing on the wire and nothing waiting: the entry can be dropped.
    fn is_idle(&self) -> bool {
        self.active.is_none() && self.waiting.is_empty()
    }
}

/// The queue of every actor with outstanding requests.
type Actors = Arc<Mutex<HashMap<String, ActorQueue>>>;

/// One tab as reported by `root.listTabs`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TabDescriptor {
    /// Actor to ask for the tab's watcher (`getWatcher`).
    pub actor: String,
    /// Current URL.
    #[serde(default)]
    pub url: String,
    /// Page title.
    #[serde(default)]
    pub title: String,
    /// Browser id: an integer, unrelated to the UUID BiDi uses for the same tab.
    #[serde(default)]
    pub browser_id: Option<u64>,
    /// Outer window id. Firefox spells it `outerWindowID`.
    #[serde(default, rename = "outerWindowID")]
    pub outer_window_id: Option<u64>,
    /// Whether the tab is the selected one.
    #[serde(default)]
    pub selected: bool,
}

/// A connection to a Firefox DevTools server (`--start-debugger-server ws:PORT`).
///
/// Cloning is cheap and every clone shares the connection. The socket is closed
/// once the last clone is dropped.
#[derive(Clone)]
pub struct RdpClient {
    connection: Connection,
    actors: Actors,
    event_tx: broadcast::Sender<RdpPacket>,
    greeting: Arc<Value>,
    default_timeout: Duration,
}

impl RdpClient {
    /// Connects to `ws://{host}` (e.g. `"127.0.0.1:6000"`) and waits for the root
    /// greeting, which the server sends unasked.
    ///
    /// Firefox must have been started with `--start-debugger-server ws:6000`
    /// (and `devtools.debugger.remote-enabled` set in the profile).
    ///
    /// Fails with `Timeout { method: "greeting", .. }` if it does not arrive within
    /// `default_timeout`, and with an internal error if the first packet is not a
    /// greeting. `default_timeout` also applies to every request.
    pub async fn connect(host: &str, default_timeout: Duration) -> BidiResult<Self> {
        Self::connect_with_capacity(host, default_timeout, EVENT_CHANNEL_CAPACITY).await
    }

    /// Like [`RdpClient::connect`], with an explicit event-channel capacity.
    ///
    /// `capacity` of `0` is treated as `1`, the smallest usable ring.
    pub async fn connect_with_capacity(
        host: &str,
        default_timeout: Duration,
        capacity: usize,
    ) -> BidiResult<Self> {
        let actors = Actors::default();
        let (event_tx, _) = broadcast::channel(transport::effective_capacity(capacity));
        let (greeting_tx, greeting_rx) = oneshot::channel();

        let router_actors = Arc::clone(&actors);
        let router_events = event_tx.clone();
        let connection = transport::open_with(&rdp_url(host), move |connection| RdpRouter {
            actors: router_actors,
            event_tx: router_events,
            greeting_tx: Some(greeting_tx),
            writer: connection.weak_writer(),
        })
        .await?;

        // If there is no valid greeting `connection` is dropped on the way out,
        // which closes the socket.
        let greeting = wait_for_greeting(greeting_rx, default_timeout).await?;

        Ok(Self {
            connection,
            actors,
            event_tx,
            greeting: Arc::new(greeting.body),
            default_timeout,
        })
    }

    /// The greeting packet, e.g. to read its `traits`.
    pub fn greeting(&self) -> &Value {
        &self.greeting
    }

    /// Sends `{"to": to, "type": method, ..params}` and waits for that actor's reply.
    ///
    /// The raw access, the counterpart of `BidiClient::send_raw_command`. An error
    /// reply becomes [`BidiError::ProtocolError`].
    ///
    /// Requests to one actor are answered in order, so a second request waits for
    /// the first. An actor that never answers therefore blocks its own queue: its
    /// later requests time out until the connection is replaced.
    pub async fn request<P: Serialize>(
        &self,
        to: &str,
        method: &str,
        params: P,
    ) -> BidiResult<RdpPacket> {
        let payload = encode_request(to, method, params)?;
        let reply = self.enqueue(to, payload)?;

        match timeout(self.default_timeout, reply).await {
            Ok(Ok(packet)) => into_result(packet),
            // The responder was dropped: the connection went away while waiting.
            Ok(Err(_)) => Err(BidiError::Disconnected),
            Err(_) => {
                self.abandon(to);
                Err(BidiError::Timeout {
                    method: method.to_string(),
                    timeout: self.default_timeout,
                })
            }
        }
    }

    /// Sends `{"to": to, "type": method, ..params}` and does not wait for a reply.
    ///
    /// For the RDP methods declared one-way, such as the watcher's `unwatchTargets`
    /// and `unwatchResources`: the server never answers them, so going through
    /// [`RdpClient::request`] would wait until the timeout and, since an actor
    /// answers in order, leave that actor's queue blocked behind a reply that will
    /// never come. The packet is written at once, not ordered with respect to
    /// requests still queued for the same actor.
    pub fn send_oneway<P: Serialize>(&self, to: &str, method: &str, params: P) -> BidiResult<()> {
        let payload = encode_request(to, method, params)?;
        if !self.is_connected() {
            return Err(BidiError::Disconnected);
        }
        transport::send(&self.connection, payload)
    }

    /// A raw receiver of every notification on the connection.
    pub fn subscribe(&self) -> broadcast::Receiver<RdpPacket> {
        self.event_tx.subscribe()
    }

    /// Notifications whose `type` equals `kind` (`""` for all of them).
    pub fn on_type(&self, kind: &'static str) -> EventFilter<RdpPacket> {
        EventFilter::rdp_type(self.subscribe(), kind)
    }

    /// Notifications sent by one actor.
    pub fn on_actor(&self, actor: &str) -> EventFilter<RdpPacket> {
        EventFilter::rdp_actor(self.subscribe(), actor)
    }

    /// `root.listTabs`.
    pub async fn list_tabs(&self) -> BidiResult<Vec<TabDescriptor>> {
        let reply = self.request("root", "listTabs", NoParams).await?;
        let tabs = reply
            .body
            .get("tabs")
            .ok_or_else(|| BidiError::missing_field("listTabs", "tabs"))?;
        Ok(serde_json::from_value(tabs.clone())?)
    }

    /// The timeout every request of this client is bounded by.
    pub(crate) fn default_timeout(&self) -> Duration {
        self.default_timeout
    }

    /// Whether the reader task still sees an open socket.
    pub fn is_connected(&self) -> bool {
        self.connection.is_alive.load(Ordering::SeqCst)
    }

    /// Puts a request on the wire if its actor is idle, or in the actor's queue.
    fn enqueue(&self, to: &str, payload: String) -> BidiResult<oneshot::Receiver<RdpPacket>> {
        let (responder, reply) = oneshot::channel();
        let mut actors = self
            .actors
            .lock()
            .map_err(|_| BidiError::poisoned_lock(ACTOR_QUEUES))?;
        // Checked under the lock. The reader clears the flag *before* it takes this
        // lock to fail every request, so a request added after that is turned away
        // here and one added before it is failed there: either way it hears about it.
        if !self.is_connected() {
            return Err(BidiError::Disconnected);
        }

        let queue = actors.entry(to.to_string()).or_default();
        if queue.active.is_some() {
            queue.waiting.push_back(Queued {
                payload,
                reply: responder,
            });
            return Ok(reply);
        }

        match transport::send(&self.connection, payload) {
            Ok(()) => {
                queue.active = Some(responder);
                Ok(reply)
            }
            Err(error) => {
                actors.remove(to);
                Err(error)
            }
        }
    }

    /// Cleans up after a request that timed out.
    fn abandon(&self, to: &str) {
        let Ok(mut actors) = self.actors.lock() else {
            return;
        };
        let Some(queue) = actors.get_mut(to) else {
            return;
        };
        // A request still queued was never sent and now never will be. Our own
        // receiver is gone by now, and so is that of anyone else who gave up.
        queue.waiting.retain(|queued| !queued.reply.is_closed());
        if queue.is_idle() {
            actors.remove(to);
        }
    }
}

/// `"127.0.0.1:6000"` becomes `"ws://127.0.0.1:6000"`; `ws://` and `wss://` URLs
/// pass through.
pub fn rdp_url(host: &str) -> String {
    transport::ws_url(host, None)
}

/// Turns an error packet into the error the caller sees; anything else passes.
fn into_result(packet: RdpPacket) -> BidiResult<RdpPacket> {
    if packet.is_error() {
        Err(BidiError::from_rdp(&packet))
    } else {
        Ok(packet)
    }
}

/// Waits for the root greeting and checks that it is one.
async fn wait_for_greeting(
    greeting: oneshot::Receiver<RdpPacket>,
    limit: Duration,
) -> BidiResult<RdpPacket> {
    let packet = match timeout(limit, greeting).await {
        Ok(Ok(packet)) => packet,
        Ok(Err(_)) => return Err(BidiError::Disconnected),
        Err(_) => {
            return Err(BidiError::Timeout {
                method: "greeting".to_string(),
                timeout: limit,
            });
        }
    };

    if packet.from.as_deref() == Some("root") && packet.str_field("applicationType").is_some() {
        Ok(packet)
    } else {
        Err(BidiError::InternalError(
            "RDP greeting has no applicationType".to_string(),
        ))
    }
}

/// Feeds the frames of one connection to the requests waiting for them.
struct RdpRouter {
    actors: Actors,
    event_tx: broadcast::Sender<RdpPacket>,
    /// Waiting for the first packet; taken when it arrives.
    greeting_tx: Option<oneshot::Sender<RdpPacket>>,
    /// To send the next queued request when a reply arrives, without keeping the
    /// connection open.
    writer: WeakWriter,
}

impl Router for RdpRouter {
    fn route(&mut self, text: &str) {
        // A frame we cannot parse is dropped: it must never take the connection down.
        let packet = match RdpPacket::parse(text) {
            Ok(packet) => packet,
            Err(error) => {
                debug!("Discarded unparsable RDP message: {error}: {text}");
                return;
            }
        };

        // The first packet is the greeting. Whether it really is one is for
        // `connect` to judge, so that a server that starts with something else
        // fails with a precise error instead of a timeout.
        if let Some(greeting_tx) = self.greeting_tx.take() {
            let _ = greeting_tx.send(packet);
            return;
        }

        if let Err(error) = route_packet(packet, &self.actors, &self.writer, &self.event_tx) {
            debug!("Could not route an RDP message: {error}");
        }
    }

    fn on_disconnect(&mut self) {
        transport::fail_pending(&self.actors);
    }
}

/// Hands a packet to the request it answers, or broadcasts it.
fn route_packet(
    packet: RdpPacket,
    actors: &Actors,
    writer: &WeakWriter,
    event_tx: &broadcast::Sender<RdpPacket>,
) -> BidiResult<()> {
    if let Some(notification) = deliver_reply(actors, writer, packet)? {
        trace!("RDP notification: {:?}", notification.r#type);
        // No subscriber at the moment is fine: the notification is simply not wanted.
        let _ = event_tx.send(notification);
    }
    Ok(())
}

/// Gives `packet` to the request on the wire for its actor, if it answers one.
/// Hands it back if it does not.
fn deliver_reply(
    actors: &Actors,
    writer: &WeakWriter,
    packet: RdpPacket,
) -> BidiResult<Option<RdpPacket>> {
    // A packet with a notification type is never a reply, even from an actor
    // that is waiting for one.
    if packet.is_notification() {
        return Ok(Some(packet));
    }
    let Some(from) = packet.from.clone() else {
        return Ok(Some(packet));
    };

    let mut actors = actors
        .lock()
        .map_err(|_| BidiError::poisoned_lock(ACTOR_QUEUES))?;
    let Some(queue) = actors.get_mut(&from) else {
        return Ok(Some(packet));
    };
    let Some(waiting_for_it) = queue.active.take() else {
        return Ok(Some(packet));
    };

    trace!("Received the reply of {from}");
    // A closed receiver means the caller gave up; the reply is consumed all the same.
    let _ = waiting_for_it.send(packet);
    promote_next(queue, writer);
    if queue.is_idle() {
        actors.remove(&from);
    }
    Ok(None)
}

/// Puts the next request that is still wanted on the wire.
fn promote_next(queue: &mut ActorQueue, writer: &WeakWriter) {
    while let Some(Queued { payload, reply }) = queue.waiting.pop_front() {
        // The caller gave up while it was queued: it is never sent.
        if reply.is_closed() {
            continue;
        }
        if writer.send(payload).is_ok() {
            queue.active = Some(reply);
            return;
        }
        // The writer is gone: dropping `reply` tells the caller it was disconnected.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        MockOptions, MockServer, fixture, rdp_firefox_like, rdp_reply, silent,
    };
    use serde_json::json;
    use tokio::task::JoinHandle;
    use tokio_stream::StreamExt;

    const TIMEOUT: Duration = Duration::from_secs(1);
    /// Long enough to leave room around the events a test waits for, short enough
    /// to keep the suite fast.
    const SHORT: Duration = Duration::from_millis(400);
    const QUIET: Duration = Duration::from_millis(50);
    const WAIT: Duration = Duration::from_secs(2);

    type Call = JoinHandle<BidiResult<RdpPacket>>;

    async fn connected(server: &MockServer) -> RdpClient {
        RdpClient::connect(&server.host(), TIMEOUT)
            .await
            .expect("connect")
    }

    /// Starts a request in the background so the test can play the server.
    fn call(client: &RdpClient, to: &'static str, method: &'static str) -> Call {
        let client = client.clone();
        tokio::spawn(async move { client.request(to, method, NoParams).await })
    }

    async fn finish(call: Call) -> BidiResult<RdpPacket> {
        timeout(WAIT, call)
            .await
            .expect("the call did not finish within 2 s")
            .expect("the call task panicked")
    }

    /// The error of a result whose success type has no `Debug`.
    fn failure<T>(result: BidiResult<T>) -> BidiError {
        match result {
            Err(error) => error,
            Ok(_) => panic!("expected an error"),
        }
    }

    async fn next_packet(filter: &mut EventFilter<RdpPacket>) -> RdpPacket {
        timeout(WAIT, filter.next())
            .await
            .expect("no packet within 2 s")
            .expect("the stream is open")
            .expect("not a lag")
    }

    /// `(on the wire, queued)` requests `client` holds for `actor`.
    fn outstanding(client: &RdpClient, actor: &str) -> (bool, usize) {
        let actors = client.actors.lock().expect("lock");
        actors.get(actor).map_or((false, 0), |queue| {
            (queue.active.is_some(), queue.waiting.len())
        })
    }

    /// How many actors `client` is tracking.
    fn tracked_actors(client: &RdpClient) -> usize {
        client.actors.lock().expect("lock").len()
    }

    /// RDP-03: `connect` waits for the root greeting.
    #[tokio::test]
    async fn connect_waits_for_greeting() {
        // Given a mock RDP server that greets on connect
        let server = MockServer::start_rdp(silent()).await;

        // When the client connects
        let client = connected(&server).await;

        // Then it holds the greeting Firefox sent
        assert_eq!(client.greeting()["applicationType"], "browser");
        assert_eq!(*client.greeting(), fixture("rdp/greeting.json"));
        assert!(client.is_connected());
    }

    /// RDP-04: A server that never greets times out.
    #[tokio::test]
    async fn missing_greeting_times_out() {
        // Given a server that sends nothing
        let server = MockServer::start(silent()).await;

        // When the client connects with a short timeout
        let result = RdpClient::connect(&server.host(), Duration::from_millis(100)).await;

        // Then it fails naming the greeting
        let error = failure(result);
        assert!(
            matches!(&error, BidiError::Timeout { method, .. } if method == "greeting"),
            "{error:?}"
        );
    }

    /// RDP-05: A first packet that is not a greeting is rejected.
    #[tokio::test]
    async fn non_greeting_first_packet_is_rejected() {
        // Given a server whose first packet is a reply
        let options = MockOptions {
            greeting: Some(json!({ "from": "root", "tabs": [] })),
        };
        let server = MockServer::start_with(silent(), options).await;

        // When the client connects
        let error = failure(RdpClient::connect(&server.host(), TIMEOUT).await);

        // Then it is an internal error mentioning the greeting
        assert!(
            matches!(&error, BidiError::InternalError(message) if message.contains("greeting")),
            "{error:?}"
        );
    }

    /// RDP-06: A reply resolves the request of its actor.
    #[tokio::test]
    async fn reply_resolves_its_actor_request() {
        // Given a connected client and a server that answers listTabs like Firefox
        let mut server = MockServer::start_rdp(rdp_firefox_like(vec![])).await;
        let client = connected(&server).await;

        // When it asks the root for the tabs
        let reply = client
            .request("root", "listTabs", NoParams)
            .await
            .expect("a reply");

        // Then the server saw a flat request
        assert_eq!(
            server.next_command().await,
            json!({ "to": "root", "type": "listTabs" })
        );
        // And the call returns the packet Firefox sent
        assert_eq!(reply.body, fixture("rdp/listTabs.json"));
        // And an actor with nothing outstanding leaves no entry behind
        assert_eq!(tracked_actors(&client), 0);
    }

    /// RDP-07: Two requests to the same actor are serialized.
    #[tokio::test]
    async fn same_actor_requests_are_serialized() {
        // Given a server that does not answer by itself
        let mut server = MockServer::start_rdp(silent()).await;
        let client = connected(&server).await;

        // When two requests to one actor are made, the second after the first is out
        let first = call(&client, "thread1", "first");
        assert_eq!(server.next_command().await["type"], "first");
        let second = call(&client, "thread1", "second");

        // Then the server has seen only the first
        server.assert_no_command(QUIET).await;
        // And once it is answered the second goes out
        server.push(rdp_reply("thread1", json!({ "answer": 1 })));
        assert_eq!(server.next_command().await["type"], "second");
        server.push(rdp_reply("thread1", json!({ "answer": 2 })));
        // And each caller gets its own reply
        assert_eq!(finish(first).await.expect("reply").body["answer"], 1);
        assert_eq!(finish(second).await.expect("reply").body["answer"], 2);
    }

    /// RDP-08: Requests to different actors are in flight at once.
    #[tokio::test]
    async fn different_actors_run_concurrently() {
        // Given a server that does not answer by itself
        let mut server = MockServer::start_rdp(silent()).await;
        let client = connected(&server).await;

        // When requests to two actors are made
        let a = call(&client, "a", "m");
        let b = call(&client, "b", "m");

        // Then the server sees both before answering either
        let mut targets = vec![
            server.next_command().await["to"].to_string(),
            server.next_command().await["to"].to_string(),
        ];
        targets.sort();
        assert_eq!(targets, ["\"a\"", "\"b\""]);
        // And replies answered in the opposite order reach the right callers
        server.push(rdp_reply("b", json!({ "who": "b" })));
        server.push(rdp_reply("a", json!({ "who": "a" })));
        assert_eq!(finish(a).await.expect("reply").body["who"], "a");
        assert_eq!(finish(b).await.expect("reply").body["who"], "b");
    }

    /// RDP-09: A notification from an actor with a pending request is not taken as its reply.
    #[tokio::test]
    async fn notification_is_not_taken_as_reply() {
        // Given a pending request to the watcher and a filter on its notifications
        let mut server = MockServer::start_rdp(silent()).await;
        let client = connected(&server).await;
        let mut notifications = client.on_type("resources-available-array");
        let pending = call(&client, "watcher1", "watchResources");
        server.next_command().await;

        // When the watcher sends a notification and then the reply
        let note = json!({ "type": "resources-available-array", "array": [] });
        server.push(rdp_reply("watcher1", note));
        server.push(rdp_reply("watcher1", json!({})));

        // Then the notification is broadcast
        let seen = next_packet(&mut notifications).await;
        assert_eq!(seen.r#type.as_deref(), Some("resources-available-array"));
        // And the request resolves with the reply, not with the notification
        let reply = finish(pending).await.expect("a reply");
        assert_eq!(reply.body, json!({ "from": "watcher1" }));
    }

    /// RDP-10: An error reply maps to `ProtocolError`.
    #[tokio::test]
    async fn rdp_error_maps_to_protocol_error() {
        // Given a pending request to the thread
        let mut server = MockServer::start_rdp(silent()).await;
        let client = connected(&server).await;
        let pending = call(&client, "thread1", "resume");
        server.next_command().await;

        // When the thread answers with an error
        let error = json!({ "error": "wrongState", "message": "not paused" });
        server.push(rdp_reply("thread1", error));

        // Then the call fails with the same variant BiDi errors use
        let error = failure(finish(pending).await);
        let BidiError::ProtocolError {
            error: code,
            message,
            stacktrace,
        } = error
        else {
            panic!("expected ProtocolError, got {error:?}");
        };
        assert_eq!(code, "wrongState");
        assert_eq!(message, "not paused");
        assert_eq!(stacktrace, None);
    }

    /// RDP-11: A timed-out request does not break the order of the next one.
    #[tokio::test]
    async fn timeout_keeps_fifo_order() {
        // Given a client with a short timeout and a request that is never answered in time
        let mut server = MockServer::start_rdp(silent()).await;
        let client = RdpClient::connect(&server.host(), SHORT)
            .await
            .expect("connect");
        let first = call(&client, "a", "first");
        server.next_command().await;

        // When it times out
        let error = failure(finish(first).await);
        assert!(
            matches!(&error, BidiError::Timeout { method, .. } if method == "first"),
            "{error:?}"
        );

        // Then a new request to the same actor queues behind the unanswered one
        let second = call(&client, "a", "second");
        server.assert_no_command(QUIET).await;
        // And the late reply to the first is consumed, not given to the second
        server.push(rdp_reply("a", json!({ "answer": "late" })));
        assert_eq!(server.next_command().await["type"], "second");
        // And the second gets the reply that follows
        server.push(rdp_reply("a", json!({ "answer": "second" })));
        assert_eq!(
            finish(second).await.expect("reply").body["answer"],
            "second"
        );
    }

    /// RDP-12: Closing the socket fails every pending and queued request.
    #[tokio::test]
    async fn close_fails_active_and_queued_requests() {
        // Given an active and a queued request to one actor
        let mut server = MockServer::start_rdp(silent()).await;
        let client = connected(&server).await;
        let active = call(&client, "a", "first");
        server.next_command().await;
        let queued = call(&client, "a", "second");
        server.assert_no_command(QUIET).await;

        // When the server closes the connection
        server.close();

        // Then both fail with Disconnected, well before the timeout
        for pending in [active, queued] {
            let error = failure(finish(pending).await);
            assert!(matches!(error, BidiError::Disconnected), "{error:?}");
        }
        assert!(!client.is_connected());
        // And a later request fails at once
        let later = failure(client.request("a", "third", NoParams).await);
        assert!(matches!(later, BidiError::Disconnected), "{later:?}");
    }

    /// RDP-13: Notifications reach the `on_type` and `on_actor` filters.
    #[tokio::test]
    async fn notifications_reach_type_and_actor_filters() {
        // Given a filter by type and one by actor
        let server = MockServer::start_rdp(silent()).await;
        let client = connected(&server).await;
        let mut results = client.on_type("evaluationResult");
        let mut thread = client.on_actor("thread1");

        // When the thread pauses, the console reports a result and the thread resumes
        server.push(rdp_reply("thread1", json!({ "type": "paused" })));
        server.push(rdp_reply("console1", json!({ "type": "evaluationResult" })));
        server.push(rdp_reply("thread1", json!({ "type": "resumed" })));

        // Then the type filter skips what came before its packet
        let result = next_packet(&mut results).await;
        assert_eq!(result.from.as_deref(), Some("console1"));
        // And the actor filter skips the console in the middle
        let first = next_packet(&mut thread).await;
        let second = next_packet(&mut thread).await;
        assert_eq!(first.r#type.as_deref(), Some("paused"));
        assert_eq!(second.r#type.as_deref(), Some("resumed"));
    }

    /// RDP-14: `list_tabs` parses the descriptors.
    #[tokio::test]
    async fn list_tabs_parses_descriptors() {
        // Given a server that answers listTabs with what Firefox sent in the spike
        let server = MockServer::start_rdp(rdp_firefox_like(vec![])).await;
        let client = connected(&server).await;

        // When the tabs are listed
        let tabs = client.list_tabs().await.expect("tabs");

        // Then the descriptor carries what Firefox said, including its own spelling of the ids
        assert_eq!(tabs.len(), 1);
        assert_eq!(tabs[0].actor, "server1.conn1.tabDescriptor1");
        assert_eq!(tabs[0].url, "https://example.com/test.html");
        assert_eq!(tabs[0].title, "BiDi Spike Test");
        assert!(tabs[0].selected);
        assert_eq!(tabs[0].browser_id, Some(2));
        assert_eq!(tabs[0].outer_window_id, Some(24));
    }

    /// RDP-15: Unparsable frames are discarded and the connection survives.
    #[tokio::test]
    async fn unparsable_rdp_frame_is_discarded() {
        // Given a connected client
        let server = MockServer::start_rdp(rdp_firefox_like(vec![])).await;
        let client = connected(&server).await;

        // When the server sends garbage and the client then makes a request
        server.push_raw("garbage");
        let reply = client.request("root", "listTabs", NoParams).await;

        // Then the request succeeds and the client is still connected
        assert!(reply.is_ok(), "{reply:?}");
        assert!(client.is_connected());
    }

    /// RDP-16: `rdp_url` normalises the endpoint.
    #[test]
    fn rdp_url_normalises_endpoints() {
        // A bare host gets the scheme and no path
        assert_eq!(rdp_url("127.0.0.1:6000"), "ws://127.0.0.1:6000");
        // URLs pass through
        assert_eq!(rdp_url("ws://h:1"), "ws://h:1");
        assert_eq!(rdp_url("wss://h"), "wss://h");
    }

    /// RDP-20: A queued request that timed out is never sent.
    #[tokio::test]
    async fn expired_queued_request_is_never_sent() {
        // Given a short timeout, a request on the wire and one queued behind it
        let mut server = MockServer::start_rdp(silent()).await;
        let client = RdpClient::connect(&server.host(), Duration::from_millis(200))
            .await
            .expect("connect");
        let first = call(&client, "a", "first");
        server.next_command().await;
        let second = call(&client, "a", "second");

        // When both time out
        for pending in [first, second] {
            let error = failure(finish(pending).await);
            assert!(matches!(error, BidiError::Timeout { .. }), "{error:?}");
        }

        // Then the first is still owed a reply, but nothing stays queued behind it
        assert_eq!(outstanding(&client, "a"), (true, 0));
        // And the late reply to the first does not trigger the second
        server.push(rdp_reply("a", json!({})));
        server.assert_no_command(QUIET).await;
        // And the actor is forgotten once that reply has been consumed
        assert_eq!(tracked_actors(&client), 0);
    }

    /// RDP-22: A queued request whose caller was cancelled is never sent.
    #[tokio::test]
    async fn cancelled_queued_request_is_never_sent() {
        // Given a request on the wire and one queued behind it
        let mut server = MockServer::start_rdp(silent()).await;
        let client = connected(&server).await;
        let first = call(&client, "thread1", "first");
        server.next_command().await;
        let second = call(&client, "thread1", "resume");
        server.assert_no_command(QUIET).await;

        // When the caller of the queued one is cancelled, not timed out
        second.abort();
        let _ = second.await;

        // Then answering the first does not send the cancelled request,
        // which Firefox would otherwise carry out for nobody
        server.push(rdp_reply("thread1", json!({ "answer": 1 })));
        assert_eq!(finish(first).await.expect("reply").body["answer"], 1);
        server.assert_no_command(QUIET).await;
    }

    /// RDP-23: A one-way request is sent and not waited for.
    #[tokio::test]
    async fn oneway_request_is_sent_and_not_awaited() {
        // Given a connected client and a server that never answers
        let mut server = MockServer::start_rdp(silent()).await;
        let client = connected(&server).await;

        // When a one-way request is sent
        client
            .send_oneway(
                "watcher1",
                "unwatchTargets",
                json!({ "targetType": "frame" }),
            )
            .expect("it is written");

        // Then the server receives it, flat like any request
        assert_eq!(
            server.next_command().await,
            json!({ "to": "watcher1", "type": "unwatchTargets", "targetType": "frame" })
        );
        // And nothing is left waiting for a reply
        assert_eq!(tracked_actors(&client), 0);
    }

    /// RDP-24: A one-way request does not disturb the queue of its actor.
    #[tokio::test]
    async fn oneway_request_does_not_disturb_the_actor_queue() {
        // Given a request on the wire to the watcher
        let mut server = MockServer::start_rdp(silent()).await;
        let client = connected(&server).await;
        let pending = call(&client, "watcher1", "watchResources");
        server.next_command().await;

        // When a one-way request goes to the same actor in the meantime
        client
            .send_oneway("watcher1", "unwatchResources", NoParams)
            .expect("it is written");
        assert_eq!(server.next_command().await["type"], "unwatchResources");

        // Then the reply still resolves the request that was waiting
        server.push(rdp_reply("watcher1", json!({ "answer": 1 })));
        assert_eq!(finish(pending).await.expect("reply").body["answer"], 1);
    }

    /// RDP-25: A one-way request on a dead connection fails with `Disconnected`.
    #[tokio::test]
    async fn oneway_request_on_dead_connection_is_disconnected() {
        // Given a connection the server has closed
        let server = MockServer::start_rdp(silent()).await;
        let client = connected(&server).await;
        server.close();
        let call = call(&client, "a", "m");
        let _ = finish(call).await;

        // When a one-way request is sent
        let result = client.send_oneway("a", "m", NoParams);

        // Then it fails at once
        assert!(matches!(result, Err(BidiError::Disconnected)), "{result:?}");
    }

    /// RDP-21: Dropping every client closes the socket.
    #[tokio::test]
    async fn dropping_every_client_closes_the_socket() {
        // Given a connected client, whose router holds a writer of its own
        let mut server = MockServer::start_rdp(silent()).await;
        let client = connected(&server).await;

        // When every handle to it is dropped
        drop(client);

        // Then the server sees the connection end
        server.wait_until_closed().await;
    }
}
