# Changelog

All notable changes to `bidi-lite` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Project scaffolding: quality gates (`make check`: fmt, clippy, tests, docs,
  duplication), CI on Linux, macOS and Windows, `AGENTS.md`.
- `protocol` module: `WsCommand`, `WsResponse` (with `is_event`, `is_error` and
  `event_context`) and `NoParams`, the wire types of WebDriver BiDi. `NoParams`
  serializes to `{}` because BiDi rejects `"params": null`.
- `error` module: `BidiError` (`#[non_exhaustive]`), `BidiResult` and the
  `error::codes` constants. BiDi error codes are strings, so
  `ProtocolError { error, message, stacktrace }` replaces cdp-lite's numeric
  `code`. `BidiError::from_response` builds it from an error reply.
- `event_filter::EventFilter<T>`: a `Stream` over the shared event ring that
  keeps the messages a subscriber asked for: by module (`EventFilter::new`), by
  module and browsing context (`for_context`) or with any predicate
  (`with_predicate`). A subscriber that falls behind gets
  `BidiError::Lagged { skipped }` and keeps working with what it did not miss.
- `client::BidiClient`: opens the WebSocket (`connect`, `connect_with_capacity`)
  and sends any command with any `Serialize` params through `send_raw_command`,
  returning the raw reply. Replies are matched to commands by id, so they may
  arrive in any order; a BiDi error reply becomes `BidiError::ProtocolError`, an
  unanswered command `BidiError::Timeout` and a lost connection
  `BidiError::Disconnected`. Events are exposed through `subscribe`, `on_module`
  and `on_module_for_context`; a frame that is not valid JSON is logged and
  dropped without closing the connection. Also `session_url` and
  `EVENT_CHANNEL_CAPACITY`. The socket is closed when the last clone of the
  client is dropped. That does not end the session: Firefox keeps it until
  `end_session` is called and refuses a new one meanwhile, which is documented
  on `BidiClient`.
- Session layer on `BidiClient`: `new` and `new_with_capabilities` connect and
  start a session in one call, `start_session`, `end_session`, `status`,
  `session` and `session_id` manage it, and `subscribe_events` /
  `unsubscribe_events` turn on the events that `on_module` filters. Firefox
  allows a single session, so a busy browser is reported as
  `BidiError::SessionNotCreated` carrying the browser's own explanation, and a
  connection whose session could not start is closed. `SessionInfo`,
  `SessionStatus` and `default_capabilities` (Puppeteer's capabilities) are
  exported from `client`. The session is shared by every clone of the client.
- `browser::BrowserClient`: browser-level control over one session.
  `connect` (or `from_client` for a client built with custom capabilities),
  `list_contexts`, `list_tabs`, `attach`, `attach_to_all_tabs`, `new_tab`,
  `close_tab`, `set_discover_contexts`, `end` and `close_browser`. `attach`
  asks the browser first, so an unknown context fails with `no such frame`
  instead of on the first command; `new_tab` closes the tab it just opened if
  navigating it fails, so no orphan tab is left behind; `close_browser` counts a
  dropped socket as success because Firefox may close it before answering.
  Also `ContextInfo` (tolerant of the fields a browser leaves out) and
  `ReadinessState`.
- `tab::Tab`: the handle `BrowserClient` hands out for one top-level browsing
  context. `send_raw_command` sends exactly what it is given, because BiDi puts
  the context in a different place per command; the helpers `navigate`,
  `reload`, `activate`, `close`, `subscribe_events` / `unsubscribe_events` and
  `script_target` put it where each command expects it. `on_module` and
  `events` are filters limited to the tab (they need `subscribe_events` first).
  Also re-exports `ReadinessState`.
- `rdp` module, the Firefox Remote Debugging Protocol (the part of the debugger
  that BiDi lacks): `RdpPacket`, `encode_request` and `UNSOLICITED_TYPES` for the
  wire, and `RdpClient` (`connect`, `request`, `list_tabs`, `subscribe`, `on_type`,
  `on_actor`). RDP has no request ids, so each actor answers its requests in order:
  `RdpClient` keeps one request per actor on the wire and queues the rest, while
  requests to different actors run in parallel. A request that times out or is
  cancelled keeps its place until Firefox answers it, so a late reply is never
  handed to the next request. An RDP error surfaces as the same
  `BidiError::ProtocolError` as a BiDi one (`BidiError::from_rdp`), and
  `EventFilter` gains `rdp_type` and `rdp_actor` for the notifications.
  `RdpClient::send_oneway` sends the requests Firefox never answers (the
  watcher's `unwatchTargets` and `unwatchResources`).
- `rdp::debugger::Debugger`, JavaScript debugging over RDP: `attach`,
  `attach_by_url`, `attach_selected`; sources (`sources`, `source_text`,
  `find_in_sources`, `on_sources`); `set_breakpoint` / `remove_breakpoint`
  (optionally conditional); `set_pause_on_exceptions`; `pause`, `resume`,
  `step_over`, `step_into`, `step_out`; `on_thread_state`; `frames`,
  `frame_environment`, `evaluate_on_frame` and `evaluate`; `detach`. Breakpoints
  go through the watcher's breakpoint list, so they survive reloads and
  navigations; without a column the first breakable position of the line is
  used, and `find_in_sources` returns a line without a column for that reason:
  Firefox ignores a breakpoint at a column it cannot break at. Every
  navigation, reload included, gives the tab a new thread and
  console actor, which the debugger follows. `detach` removes the breakpoints
  it set before unwatching, because a breakpoint left behind freezes the page.
  Types and pure parsers (`SourceInfo`, `BreakpointLocation`, `FrameInfo`,
  `ThreadState`, `Evaluation`, `PauseOnExceptions`, `parse_thread_states`,
  `parse_sources`, `locate`...) live in `rdp::resources` and are re-exported
  from `rdp::debugger`. Checked against Firefox 157.
- Examples, each runnable against a real Firefox (`tests/firefox.sh start`):
  `simple` (start a session, navigate, end it); `filter_domains` (subscribe to
  events, then filter them by module); `runtime_usage` (`script.evaluate`,
  promises, `script.callFunction`, exceptions versus protocol errors, console);
  `multi_tab` (several tabs on one session, per-tab listeners, context discovery);
  `multi_tab_parallel` (several pages loading at once over one connection);
  `breakpoints_usage` (BiDi drives the page, the RDP debugger breaks in its
  script, reads a variable in the paused frame and pauses on an exception);
  `proxy_usage` (a per-user-context proxy with credentials answered from
  `network.authRequired`; skipped unless `PROXY_*` are set).
- `make e2e` (`tests/run_all_e2e.sh`): runs every example against a real
  Firefox and checks its output; not part of `make check`.
