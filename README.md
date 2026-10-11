# bidi-lite

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Rust](https://img.shields.io/badge/rust-stable-brightgreen.svg)](https://www.rust-lang.org)

**bidi-lite** is a lightweight, asynchronous, and low-overhead Rust client for
**WebDriver BiDi**, focused on Firefox.

It is the sibling of [`cdp-lite`](https://github.com/raultov/cdp-lite) and keeps
its design: a minimal, wire-level interface over WebSockets for developers who
need raw control. Because WebDriver BiDi has no debugger, it also ships a
minimal client for the **Firefox Remote Debugging Protocol (RDP)**, so
JavaScript can still be paused, inspected and stepped.

Validated against **Firefox 157.0.1**.

---

## Features

* **Zero Bloat:** minimal dependencies and no unnecessary abstractions.
* **Fully Asynchronous:** built on top of `tokio` and `tungstenite`.
* **Low Overhead:** command/event dispatching with `oneshot` and `broadcast` channels.
* **Direct Protocol Access:** send any BiDi command (or RDP request) and receive raw JSON.
* **Event Filtering:** by module (`browsingContext`, `network`, `log`...) and by browsing context (tab).
* **Multi-Tab Control:** drive many tabs of one session over a single WebSocket.
* **JavaScript Debugging:** breakpoints, pausing, stepping, frames and evaluation over RDP.

---

## Quick Start

Add this to your `Cargo.toml`:

```toml
[dependencies]
bidi-lite = "0.1"
tokio = { version = "1", features = ["full"] }
```

Start Firefox with BiDi on port 9222, using a dedicated profile:

```bash
firefox --no-remote --profile "$HOME/bidi-lite-profile" --remote-debugging-port=9222
```

With **snap** Firefox (Ubuntu) the profile must live under `$HOME` and not in a
hidden directory or `/tmp`: the snap's AppArmor profile blocks them. In this
repository, `tests/firefox.sh start` does all of this (both ports included).

Then:

```rust
use bidi_lite::browser::BrowserClient;
use bidi_lite::error::BidiResult;
use bidi_lite::tab::ReadinessState;
use std::time::Duration;

#[tokio::main]
async fn main() -> BidiResult<()> {
    let browser = BrowserClient::connect("127.0.0.1:9222", Duration::from_secs(10)).await?;
    if let Some(tab) = browser.attach_to_all_tabs().await?.into_iter().next() {
        let reply = tab.navigate("https://www.rust-lang.org", ReadinessState::Complete).await?;
        println!("Firefox replied: {reply:?}");
    }
    // Always end the session: see "Sessions" below.
    browser.end().await?;
    Ok(())
}
```

---

## Sessions

* `BidiClient::new` (and `BrowserClient::connect`) connects to `ws://host/session`,
  checks `session.status` and creates a session with `session.new`, using
  Puppeteer's capabilities (`client::default_capabilities()`).
  `BidiClient::new_with_capabilities` sends your own.
* `BidiClient::connect` only opens the WebSocket: no session, so the browser
  accepts nothing but `session.status` and `session.new` until `start_session`.
* **Firefox allows a single session.** A second one fails with
  `BidiError::SessionNotCreated("Session already started")`.
* **The session outlives the connection.** Dropping the client closes the socket
  but does *not* end the session: Firefox keeps refusing new ones until the
  browser restarts, and a new connection cannot end it. Always call
  `BrowserClient::end()` (or `BidiClient::end_session()`); `close_browser()`
  closes Firefox altogether.

---

## Event Subscription

Two steps, and both are needed:

```rust
tab.subscribe_events(&["browsingContext.load", "log.entryAdded"]).await?; // tells Firefox
let mut events = tab.on_module("browsingContext");                         // filters locally
```

`subscribe_events` (`session.subscribe`) is what makes Firefox send events; it
is BiDi's equivalent of CDP's `Page.enable`. `on_module` only filters the
connection's event stream: without the subscription it stays silent forever.

* `client.on_module("network")` sees the events of every tab;
  `tab.on_module(..)` and `tab.events()` only those of that browsing context.
* The context is matched exactly (`params.context`, or `params.source.context`
  for `log.entryAdded`). **Iframes** are browsing contexts of their own, so their
  events do not reach the filter of the tab that contains them.

### Event loss and `BidiError::Lagged`

One connection carries the events of every module of every tab through a single
ring (`client::EVENT_CHANNEL_CAPACITY`, 4096 by default; raise it with
`connect_with_capacity` when driving many tabs). If a listener falls behind, the
events it could not read are **dropped** and the stream yields:

```rust
BidiError::Lagged { skipped: 6 }
```

The listener then keeps working and receives everything it did not miss. Nothing
is discarded silently: `skipped` is exactly how many events went missing. If
your listener caches event-derived state, treat `Lagged` as "that cache is now
incomplete" and ask for the state again, for example with
`browsingContext.getTree` or `script.getRealms`.

---

## Multiple Tabs

Every tab shares one connection and one session. Command ids are allocated per
connection, so replies of many tabs can interleave without being mixed up.

| `BrowserClient` | What it does |
| --- | --- |
| `list_tabs()` / `list_contexts()` | Top-level contexts / the whole tree (`browsingContext.getTree`) |
| `attach(context)` | A `Tab` for an existing context (checked with the browser first) |
| `attach_to_all_tabs()` | A `Tab` for every top-level context, in one round trip |
| `new_tab(url)` | Opens a tab and navigates it without waiting; closes it again if navigation fails |
| `close_tab(context)` | `browsingContext.close` |
| `set_discover_contexts(bool)` | Turns `contextCreated` / `contextDestroyed` on or off |
| `end()` / `close_browser()` | Ends the session / closes Firefox |

| `Tab` | What it does |
| --- | --- |
| `send_raw_command(method, params)` | Any command, **verbatim**: put the context where the command expects it |
| `navigate(url, wait)` / `reload(wait)` | `wait` is a `ReadinessState`: `None`, `Interactive` or `Complete` |
| `activate()` / `close()` | Bring to front / close |
| `subscribe_events(events)` / `unsubscribe_events(ids)` | Subscriptions scoped to this tab |
| `on_module(module)` / `events()` | This tab's events, by module or all of them |
| `script_target()` | `{"context": id}`, the `target` of `script.evaluate` / `script.callFunction` |

Firefox refuses top-level navigation to `data:` URLs (`unsupported operation`);
use `http(s):`, `file:` or `about:blank`.

---

## Coming from cdp-lite

| cdp-lite | bidi-lite |
| --- | --- |
| `CdpClient::new(host, t)` | `BidiClient::new(host, t)` (+ `session.new`) |
| `CdpClient::connect(ws, t)` | `BidiClient::connect(ws, t)` (no session) |
| `on_domain("Page")` | `on_module("browsingContext")` (+ `subscribe_events`) |
| `Page.enable` | `session.subscribe` |
| `Tab::session_id()` | `Tab::context_id()` |
| `Tab::send_raw_command` (injects `sessionId`) | `Tab::send_raw_command` (verbatim) + helpers |
| `Tab::detach()` | `Tab::unsubscribe_events(..)` |
| `set_discover_targets` | `set_discover_contexts` |
| `TargetInfo` | `ContextInfo` |
| `CdpError::ProtocolError{code: i64}` | `BidiError::ProtocolError{error: String}` |
| `Runtime.evaluate` | `script.evaluate` + `tab.script_target()` |
| `Debugger.enable` | `RdpClient::connect` + `Debugger::attach*` |
| `Debugger.setBreakpointByUrl` | `Debugger::set_breakpoint` |
| `Debugger.paused` | `Debugger::on_thread_state` + `parse_thread_states` |
| `Debugger.evaluateOnCallFrame` | `Debugger::evaluate_on_frame` |
| `Debugger.resume` / `stepOver` / `stepInto` / `stepOut` | `resume` / `step_over` / `step_into` / `step_out` |
| `Debugger.setPauseOnExceptions` | `set_pause_on_exceptions` |

---

## Architecture

**User, library, browser.** Your code calls typed Rust methods; bidi-lite turns
them into JSON over two WebSockets to the same Firefox, and turns the replies
and events that come back into `Result`s and `Stream`s.

```text
 ┌─────────────────────────── your application ────────────────────────────┐
 │  calls methods, awaits Results, reads event Streams                      │
 │  browser.new_tab(..)   tab.navigate(..)   tab.on_module(..)              │
 │  debugger.set_breakpoint(..)   debugger.on_thread_state()                │
 └──────────────┬───────────────────────────────────────▲──────────────────┘
                │ method calls                          │ Result<WsResponse | RdpPacket>
                ▼                                       │ EventFilter streams
 ┌─────────────────────────────── bidi-lite ───────────┴───────────────────┐
 │  BrowserClient ── Tab                     rdp::Debugger                  │
 │        │  (context placed per command)          │ (watcher, sources,     │
 │        ▼                                        ▼  breakpoints, frames)  │
 │  BidiClient                                RdpClient                     │
 │   ids ↔ replies, session,                   per-actor FIFO queues        │
 │   event ring + filters                      notification ring + filters  │
 │        │                                        │                        │
 │  transport: one WebSocket each; writer task + reader task (shared code)  │
 └────────┼────────────────────────────────────────┼───────────────────────┘
          │ ws://127.0.0.1:9222/session            │ ws://127.0.0.1:6000
          │ JSON commands ⇄ replies + events       │ JSON requests ⇄ replies + notifications
 ┌────────▼────────────────────────────────────────▼───────────────────────┐
 │ Firefox                                                                  │
 │  Remote Agent (WebDriver BiDi)          DevTools server (RDP)            │
 │  tabs, navigation, script,              sources, breakpoints, pause,     │
 │  network, logs                          frames, evaluation               │
 └──────────────────────────────────────────────────────────────────────────┘
```

Both clients are cheap to clone; every clone shares its connection. The two
connections are independent: BiDi works without the debugger port.

**A BiDi command, round trip.** The caller gets a fresh id and waits on a
`oneshot` channel; a writer task owns the socket's sink and a reader task routes
every reply to the waiting caller by id.

```text
 caller task                BidiClient                     background tasks              Firefox
 ───────────                ──────────                     ────────────────              ───────
 send_raw_command(m, p) ─► id = next_id++ (AtomicU64)
                           pending[id] = oneshot::Sender
                           {"id","method","params"} ──► mpsc ──► writer task ──► WS text ──►
                           await oneshot (timeout)                                           │
                                                                                             ▼
                                                           reader task ◄── WS text ◄── {"type":"success","id",..}
                                                           route_message: pending.remove(id)
 Ok(WsResponse) ◄───────── oneshot::Receiver ◄───────────── .send(response)
   or ProtocolError / Timeout (id removed) / Disconnected (socket closed → pending cleared)
```

**BiDi events.** Firefox only sends what the session subscribed to. The reader
task puts every event in one broadcast ring; each `EventFilter` is a subscriber's
view of it.

```text
 tab.subscribe_events(["browsingContext.load"])  ──► session.subscribe {events, contexts:[ctx]}
                                                      (without this, Firefox sends nothing)

 Firefox ── {"type":"event","method":"browsingContext.load","params":{"context":"T1",..}} ──►
   reader task ── route_message ──► broadcast ring (EVENT_CHANNEL_CAPACITY = 4096)
                                       ├──► EventFilter  on_module("browsingContext")       → every tab
                                       ├──► EventFilter  tab(T1).on_module("browsingContext") → T1 only
                                       └──► EventFilter  tab(T2).events()                    → (skipped)
   a slow subscriber that falls behind the ring gets Err(BidiError::Lagged { skipped })
```

**An RDP request.** RDP has no ids: each actor answers its requests in order. So
each actor has at most one request on the wire and the rest wait in a queue;
requests to different actors run in parallel.

```text
 request("thread1","resume",..) ─► actors["thread1"] busy? ─ no ─► send now ──► writer ──► Firefox
                                                         └ yes ─► queue (VecDeque)
 Firefox ── {"from":"thread1", ..no "type"..} ─► reader ─► route_packet:
                                    reply to thread1's active request; send its next queued one
 Firefox ── {"from":"watcher1","type":"resources-available-array",..} ─► broadcast ─► EventFilter<RdpPacket>
                                    (`type` in UNSOLICITED_TYPES ⇒ notification, never a reply)
```

| Where it lives | |
| --- | --- |
| `transport.rs` | WebSocket, writer task, reader loop (shared by both protocols) |
| `client.rs`, `session.rs` | ids, pending replies, routing; session and subscriptions |
| `rdp/client.rs` | per-actor request queues |
| `event_filter.rs` | module, context, type and actor filters; lag |

---

## Debugging JavaScript (RDP)

WebDriver BiDi has no breakpoints, pausing or stack frames. `rdp::debugger::Debugger`
drives the debugger Firefox's own DevTools use, over a second connection.

Firefox needs the DevTools server and three preferences in the profile's `user.js`:

```bash
firefox --no-remote --profile "$HOME/bidi-lite-profile" \
        --remote-debugging-port=9222 --start-debugger-server ws:6000
```

```js
user_pref("devtools.debugger.remote-enabled", true);
user_pref("devtools.chrome.enabled", true);
user_pref("devtools.debugger.prompt-connection", false);
```

Firefox refuses to start the DevTools server unless the first two are `true`.

> **Security:** with these preferences any local process can connect to the
> debugger port without being asked, with the debugger's full power over the
> browser. Use them only with a dedicated profile and with the server on
> `127.0.0.1`.

```rust
let rdp = RdpClient::connect("127.0.0.1:6000", Duration::from_secs(10)).await?;
let debugger = Debugger::attach_selected(&rdp).await?;
if let Some((_, location)) = debugger.find_in_sources("nav_dropdown.value").await? {
    debugger.set_breakpoint(&location, None).await?;
}
let mut states = debugger.on_thread_state();
// ... on ThreadState::Paused: frames(), evaluate_on_frame(), resume(), step_over() ...
debugger.detach().await?;
```

* Breakpoints go through the tab's breakpoint list, so they **survive reloads and
  navigations**. Firefox only breaks at exact breakable positions; without a
  column, `set_breakpoint` takes the first one of the line from the source.
* Every navigation, reload included, gives the tab a new thread and console
  actor; the debugger follows them.
* One thread-state packet can carry several states (after a step: `resumed` and
  the new `paused`): read every state `parse_thread_states` returns.

### Limitations

* **Two ports and two connections.** Without `--start-debugger-server`,
  `RdpClient::connect` fails with a WebSocket error.
* **RDP is not a standard.** It can change between Firefox versions; this crate
  is validated against Firefox 157, and `make e2e` re-checks it.
* **Finding the tab.** BiDi and RDP name tabs differently. Activate the tab and
  use `Debugger::attach_selected`; `attach_by_url` takes the *first* tab whose URL
  matches, which may be another one showing the same page.
* **Iframes:** only the tab's top-level document is followed.
* **Requests to the same RDP actor are serialised**, and one that is never
  answered blocks its actor's queue until the connection is replaced.
* **A breakpoint outlives its debugger.** Unwatching does not remove
  breakpoints, so a `Debugger` dropped without `detach()` leaves them set, and
  hitting one freezes the page until the RDP connection closes. `detach()`
  removes them first. A paused thread stays paused until `resume()`.

### Logging

At `debug` level the transport logs every command it sends, payload included.
Do not enable it in production for commands that carry secrets (for example the
credentials of `network.continueWithAuth`).

---

## Quality gates

* `make check`: `cargo fmt --check`, `clippy --all-targets -D warnings`, unit
  tests and doctests (no browser: an in-process mock WebSocket server stands in
  for Firefox, fed with messages captured from it), `cargo doc -D warnings`, and
  the duplication check at 0. CI runs it on Linux, macOS and Windows.
* `make e2e`: runs every example against a real Firefox started by
  `tests/firefox.sh` (Linux/macOS, needs a display and network). Not a gate.
* `make check-all`: both.

---

## Advanced examples

Each one runs against a real Firefox (`tests/firefox.sh start`):

| Example | What it shows |
| --- | --- |
| `simple` | Start a session, navigate, end it |
| `filter_domains` | Subscribe to events and filter them by module |
| `runtime_usage` | `script.evaluate`, promises, `script.callFunction`, exceptions, console |
| `multi_tab` | Several tabs, per-tab listeners, context discovery |
| `multi_tab_parallel` | Several pages loading at once over one connection |
| `breakpoints_usage` | BiDi drives the page, the RDP debugger breaks in its script |
| `proxy_usage` | A per-user-context authenticated proxy (needs `PROXY_HOST`, `PROXY_PORT`, `PROXY_USER`, `PROXY_PASS`). **Not tested against a real proxy** (see below) |

```bash
cargo run --example breakpoints_usage
```

> **`proxy_usage` is untested with a real proxy.** It was only run against an
> unreachable proxy: Firefox accepted every command and routed the tab through
> the proxy (the navigation was refused by it), and the example cleaned up. The
> authentication step (`network.continueWithAuth` answering a 407) has never run
> against a proxy that asks for credentials.

---

## Why bidi-lite?

WebDriver BiDi is the standard, cross-browser successor of CDP, and Firefox's
way to be automated. `bidi-lite` gives you that protocol directly, with the same
small surface as `cdp-lite`, plus the one thing BiDi lacks: a debugger.
Higher-level libraries can build on both crates to drive Chromium and Firefox
alike.

## License

This project is licensed under the MIT License; see [LICENSE](LICENSE).

## Contributing

Contributions are welcome. Read [AGENTS.md](AGENTS.md) for the conventions
(TDD, no `unsafe`, no `#[allow]`, duplication at 0) and make sure `make check`
passes before opening a pull request.
