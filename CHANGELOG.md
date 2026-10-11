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
