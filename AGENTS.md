# AGENTS.md — conventions for bidi-lite

Rules for autonomous agents working in this repository. Specs and phase plans
(`docs/specs/`, gitignored) are local-only working documents provided by the
human; they are never committed and may be absent in a fresh clone. This file
is the standing contract.

## What this crate is

A minimal, low-level, asynchronous Rust client for **WebDriver BiDi**, focused
on Firefox, mirroring the design and API of its sibling `cdp-lite` (Chromium /
CDP). Because BiDi has no debugger, it also ships a minimal client for the
**Firefox Remote Debugging Protocol** (`src/rdp/`) so JavaScript debugging
(breakpoints, pause, frames, evaluation, stepping) is not lost.

It is wire-level on purpose: commands take any `Serialize` params and return
raw JSON. Do not add typed per-command structs, browser launching, profile
management or reconnection; those belong to the higher-level crate built on
top of `cdp-lite` and `bidi-lite`.

## Ground rules

1. **Write only inside this repository** (plus the E2E home
   `~/.local/share/bidi-lite/`, used by `tests/firefox.sh` for the dedicated
   Firefox profile). Never write into `../cdp-lite`, `../knot-lite` or any other
   checkout; read them freely as references.
2. **Rust:** edition 2024; **no `unsafe`** (denied in `Cargo.toml`); **no
   `#[allow]`** (`allow_attributes` is a warning and warnings are errors);
   `#[expect(..., reason = "...")]` only as a last resort and reported to the
   human. Fix clippy/dupes findings by refactoring — never silence them.
   Never loosen `clippy.toml` or `dupes.toml`; only tighten them, recording the
   measured curve.
3. All code, comments, docs, README and CHANGELOG in **English**.
4. **Cross-platform** (Linux, macOS, Windows): no shelling out and no
   OS-specific paths in `src/`. Shell scripts live only in `tests/` and are
   not part of `make check`.
5. **Library hygiene:** no `unwrap`/`expect`/`panic!` outside tests; every
   public item has `///` docs explaining the *why*; errors are `BidiError`
   variants, never strings in `InternalError` when a typed variant fits.
6. **Tests:** BDD scenarios from the specs become Rust tests inside the module
   they test (`#[cfg(test)] mod tests`), named in snake_case after the
   behaviour, with a `/// <ID>: <scenario>` doc line and `// Given`,
   `// When`, `// Then` comments. TDD order: write the failing test first
   (it must compile and fail for the expected reason), then the code.
   **Hermetic gate:** tests never start Firefox, never touch the network, and
   only bind `127.0.0.1:0` (the in-process mock in `src/test_support.rs`).
   Real browser traffic is captured once into `tests/fixtures/` and loaded
   with `include_str!`.
7. After every task `make check` must be green before committing. Add the
   user-visible change to `CHANGELOG.md` under `[Unreleased]` in the same
   commit. Update `README.md` whenever the public API or behaviour changes.
   Commits: `phaseN(FN.x): <summary>`, one commit per task.
8. If a spec is wrong about the protocol, an API or a path, fix the spec
   first, then the test, then the code, and record the deviation in the
   roadmap's deviation log.
9. Never commit `target/`, `.idea/`, `.knot/` or `docs/specs/`. Stage files
   explicitly (no `git add -A`). Push, merge, tag or `cargo publish` only when
   the human explicitly asks; never push to or merge into `master`/`main`
   unprompted.
10. Use the knot MCP tools (`search_hybrid_context`, `find_callers`,
    `explore_file`) when reading indexed code (e.g. `cdp-lite`).

## Architecture

- **Symmetry with cdp-lite.** Same type and method names where the concept is
  the same (`WsCommand`, `WsResponse`, `NoParams`, `EventFilter`,
  `BrowserClient`, `Tab`, `send_raw_command`, `subscribe`, `list_tabs`,
  `attach`, `new_tab`, `close_tab`, `EVENT_CHANNEL_CAPACITY`,
  `connect_with_capacity`). Different names only where BiDi differs
  (`BidiClient`, `BidiError`, `ContextInfo`, `context_id`, `on_module`,
  `set_discover_contexts`). Check the cdp-lite counterpart before naming
  anything public.
- **One connection = writer task + reader task.** Commands go through an
  unbounded `mpsc` to the writer; the reader routes replies to `oneshot`
  senders and events to a `broadcast` ring. All of this lives once, in
  `transport.rs`, shared by `BidiClient` and `RdpClient`.
- **Correlation:** BiDi by `id`; RDP by actor, FIFO, one request in flight per
  actor.
- **Never rewrite params.** `send_raw_command` and `RdpClient::request` send
  exactly what they are given. Helpers (`Tab::navigate`, `Debugger::set_breakpoint`)
  place the context/actor where each command expects it.
- **Events are opt-in.** `on_module` only filters; the browser emits nothing
  until `subscribe_events` is called. Lag is reported as
  `BidiError::Lagged { skipped }`, never swallowed.
- **A malformed message never kills a connection** (logged with `debug!` and
  dropped).

## Layout

```
src/
├── lib.rs            module list + crate docs
├── protocol.rs       WsCommand, WsResponse, NoParams (BiDi wire)
├── error.rs          BidiError, BidiResult, error codes
├── transport.rs      WebSocket connect, writer task, reader loop (shared)
├── client.rs         BidiClient: dispatch, routing, session lifecycle, subscriptions
├── event_filter.rs   EventFilter<T>: module/context (BiDi) or type/actor (RDP) filters
├── browser.rs        BrowserClient, ContextInfo, ReadinessState
├── tab.rs            Tab: one top-level browsing context
├── rdp/              Firefox RDP: protocol (RdpPacket), client (per-actor FIFO), debugger
└── test_support.rs   #[cfg(test)] in-process mock WebSocket server (BiDi and RDP modes)
examples/             one runnable program per cdp-lite example; need a real Firefox
tests/
├── fixtures/         real BiDi/RDP messages captured from Firefox (loaded by unit tests)
├── firefox.sh        start/stop a dedicated headless Firefox for the smoke run
└── run_all_e2e.sh    runs every example against it (`make e2e`, not a gate)
```

## Gates

```sh
make check        # fmt-check, clippy -D warnings, test (unit + doctests), doc, dupes
make test-lib     # fast loop, not a gate
make e2e          # examples against a real Firefox (Linux/macOS; not in `make check`)
make check-all    # make check + make e2e
make dupes-cleanup
```

Duplication must stay at **0** (see `dupes.toml`): extract a shared,
parameterised helper instead of duplicating code.

## Running Firefox for the examples

```sh
tests/firefox.sh start   # headless, BiDi on :9222/session, RDP on ws:6000
cargo run --example simple
tests/firefox.sh stop
```

Firefox allows a single BiDi session: every example must end it
(`browser.end()`), or the next run fails with `SessionNotCreated`. With snap
Firefox, profiles must live under `$HOME`, never `/tmp`.

## Adding a helper to `Tab`, `BrowserClient` or `Debugger`

1. Confirm the helper is needed by the higher-level crate or an example; the
   default answer is "use `send_raw_command`/`request`".
2. Write the BDD scenario in the spec, then the failing test against the mock
   (assert the exact params on the wire).
3. Implement it on top of the existing private helper
   (`Tab::context_command`, `Debugger::thread_request`...) so dupes stays at 0.
4. `///` docs with the underlying protocol command; README table row;
   CHANGELOG entry.

## Plans

Specs and phase plans (`docs/specs/`, gitignored) are provided by the human
for execution. Their roadmap file holds the phase order, the checklist and the
deviation log.
