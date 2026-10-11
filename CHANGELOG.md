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
  client is dropped, so a Firefox session cannot outlive its client.
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
  context, with `context_id` and `client`. Its navigation helpers come next.
