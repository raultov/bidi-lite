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
  client is dropped, so a Firefox session cannot outlive its client. No session
  is created yet: that is the next step.
