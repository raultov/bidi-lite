# Fixture Sources and Transcripts

Notes on the origin of messages captured from real browsers in `tests/fixtures/`.

## WebDriver BiDi (`tests/fixtures/bidi/`)

Captured from Firefox 157.0.1 on Ubuntu 24.04 (Phase 1 spike and Phase 10 smoke run):
- `get-tree.json`: reply to `browsingContext.getTree { maxDepth: 0 }`.
- `session-new.json`: successful reply to `session.new`.
- `session-new-busy.json`: error reply when a session is already active.
- `session-status-busy.json`: `session.status` returning `ready: false`.
- `event-load.json`: `browsingContext.load` event.
- `event-log-entry.json`: `log.entryAdded` event from a page console log.
- `get-tree-two-tabs.json` (0.1.3): `browsingContext.getTree { maxDepth: 0 }` with `about:home` and two
  tabs on the same local URL (`/inline`). `id` set to 0 (re-addressed by `fixture_for`).

## Firefox RDP (`tests/fixtures/rdp/`)

Captured from Firefox 157.0.1 on Ubuntu 24.04 (`--start-debugger-server ws:6000`, visible):
- `greeting.json`: root greeting on connect (`from: "root"`, `applicationType: "browser"`).
- `listTabs.json`: reply from `root` to `listTabs`.
- `getWatcher.json`: watcher form returned by `tabDescriptor.getWatcher`.
- `target-available.json`: `target-available-form` emitted by `watcher.watchTargets`.
- `target-destroyed.json`: `target-destroyed-form` emitted during target switch.
- `resources-sources.json`: `resources-available-array` carrying `source` items.
- `resources-thread-state-paused.json`: `resources-available-array` carrying `thread-state` paused.
- `resources-thread-state-resumed.json`: `resources-available-array` carrying `thread-state` resumed.
- `thread-state-paused.json`: `paused` notification directly from thread actor.
- `thread-state-resumed.json`: `resumed` notification directly from thread actor.
- `breakpoint-positions.json`: reply to `source.getBreakpointPositionsCompressed`.
- `frames.json`: reply to `thread.frames`.
- `evaluate-reply.json`: reply to `webconsole.evaluateJSAsync` (`resultID`).
- `evaluation-result.json`: `evaluationResult` notification.
- `source-long-string.json`: hand-made fixture for `longString` grip.
- `resources-websocket.json`: `resources-available-array` carrying `websocket` resources
  (`webSocketOpened`, `frameSent` [text, binary, close], `frameReceived` [text, binary], `webSocketClosed`).
  Captured against a local WebSocket echo server on 127.0.0.1:8808.
- `resources-error-message.json`: `resources-available-array` carrying `error-message` resources
  (`ReferenceError` script error with stacktrace and `cookieSameSite` warning).
  Captured from page scripts and cookies on http://127.0.0.1:8809/.
- `resources-css-message.json`: `resources-available-array` carrying `css-message` resources
  (CSS Parser warnings: unknown property and invalid value).
  Captured from stylesheet parsing on http://127.0.0.1:8809/.

Captured in the 0.1.3 probe (PROMPT-02, Firefox 157.0.1, visible window), the same session as
`bidi/get-tree-two-tabs.json`:
- `listTabs-two-tabs.json`: `listTabs` with the same three tabs (`browserId` 2/11/12,
  `browsingContextID` 3/12/13).
- `get-process.json`: reply from `root` to `getProcess { id: 0 }` (the parent process descriptor).
- `parent-process-target.json`: reply from the process descriptor to `getTarget` (its `consoleActor`).
- `browser-id-lookup.json`: `evaluationResult` of the `NavigableManager` lookup of the third tab
  (`result: 12`).
- `browser-id-lookup-null.json`: the same lookup for an unknown context (`result: {type: "null"}`).
- `breakpoint-positions-inline-none.json` / `breakpoint-positions-inline-second.json`: replies to
  `getBreakpointPositionsCompressed` for line 9 of a page with two inline `<script>` blocks, from the
  first source (`{}`) and the second (`{"9": [2]}`).
- `forwarding-cancelled.json`: `forwardingCancelled` notification from `root` after a debugger
  detached and its tab closed.

Captured in the 0.1.5 probe (Firefox 159.0, headless, `--start-debugger-server ws:6100`) on a page of
`127.0.0.1` whose iframe is of another origin (`localhost`, same port), which Firefox runs in its own
process. Its script `inner.js` paused at a breakpoint set through the watcher's breakpoint list:
- `frame-target-available.json`: `target-available-form` of the iframe's target
  (`isTopLevelTarget: false`, its own `threadActor` and `consoleActor`, `browsingContextID`, `url`).
- `frame-target-destroyed.json`: `target-destroyed-form` of the iframe's initial `about:blank` target,
  replaced once the iframe loaded its page.
- `frame-resources-sources.json`: `resources-available-array` carrying the iframe's `inner.js` source,
  from the iframe's target.
- `frame-resources-thread-state-paused.json`: `resources-available-array` carrying the iframe thread's
  `paused` state (`why: breakpoint`), from the iframe's target.
