//! JavaScript debugging over RDP: breakpoints, pause, stack frames, and evaluation.
//!
//! WebDriver BiDi has no debugger, so [`Debugger`] drives the one Firefox's own
//! DevTools use, through an [`RdpClient`]. It follows the watcher-based
//! bootstrap of modern DevTools: one watcher per tab announces the tab's
//! top-level target (thread and console actors) and its sources, and keeps
//! announcing the new target every time the tab navigates, reloads included.

use crate::error::{BidiError, BidiResult, codes};
use crate::event_filter::EventFilter;
use crate::protocol::{NoParams, str_at};
use crate::rdp::client::{RdpClient, TabDescriptor};
use crate::rdp::protocol::RdpPacket;
use serde_json::{Value, json};
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::{RecvError, TryRecvError};
use tokio::task::AbortHandle;
use tokio::time::{Instant, timeout, timeout_at};
use tokio_stream::StreamExt;

pub use crate::rdp::resources::{
    BreakpointLocation, Evaluation, FrameInfo, FrameLocation, PauseOnExceptions, SourceInfo,
    ThreadState, locate, parse_resources, parse_sources, parse_thread_states,
};

/// The resources the debugger watches.
const RESOURCE_TYPES: [&str; 2] = ["source", "thread-state"];

/// The actors of the tab's current top-level target.
#[derive(Debug, Clone)]
struct TargetForm {
    actor: String,
    thread_actor: String,
    console_actor: String,
}

/// What the background task keeps up to date, shared by every clone.
#[derive(Default)]
struct State {
    target: Option<TargetForm>,
    sources: Vec<SourceInfo>,
    sources_complete: bool,
    /// The breakpoints set through this debugger, column resolved, so that
    /// `remove_breakpoint` and `detach` send the same location.
    breakpoints: Vec<BreakpointLocation>,
}

type Shared = Arc<RwLock<State>>;

// A panic while holding the lock cannot leave `State` half-written in a way that
// matters (each field is replaced whole), so a poisoned lock is still usable.
fn read(state: &Shared) -> RwLockReadGuard<'_, State> {
    state.read().unwrap_or_else(PoisonError::into_inner)
}

fn write(state: &Shared) -> RwLockWriteGuard<'_, State> {
    state.write().unwrap_or_else(PoisonError::into_inner)
}

/// The debugger of one tab.
///
/// Cloning is cheap; every clone shares the same state. Breakpoints are set
/// through the watcher's breakpoint list, so they survive reloads and
/// navigations. Call [`Debugger::detach`] when done: a breakpoint left behind
/// freezes the page the next time it is hit.
#[derive(Clone)]
pub struct Debugger {
    client: RdpClient,
    watcher: String,
    breakpoint_list: String,
    thread_configuration: String,
    state: Shared,
    follower: AbortHandle,
}

impl Debugger {
    /// Attaches to a tab: watcher bootstrap, first top-level target, sources.
    ///
    /// Fails with `Timeout { method: "watchTargets", .. }` if no top-level target
    /// is announced within the client's timeout.
    pub async fn attach(client: &RdpClient, tab: &TabDescriptor) -> BidiResult<Self> {
        // Subscribed first: the target and the sources are announced before the
        // replies of `watchTargets` and `watchResources`.
        let mut events = client.subscribe();
        let switching =
            json!({ "isServerTargetSwitchingEnabled": true, "isPopupDebuggingEnabled": false });
        let watcher = actor_in(
            client.request(&tab.actor, "getWatcher", switching).await?,
            &["actor"],
            "getWatcher",
        )?;
        let breakpoint_list = actor_in(
            client
                .request(&watcher, "getBreakpointListActor", NoParams)
                .await?,
            &["breakpointList", "actor"],
            "getBreakpointListActor",
        )?;
        let thread_configuration = actor_in(
            client
                .request(&watcher, "getThreadConfigurationActor", NoParams)
                .await?,
            &["configuration", "actor"],
            "getThreadConfigurationActor",
        )?;

        let state = Shared::default();
        client
            .request(&watcher, "watchTargets", json!({ "targetType": "frame" }))
            .await?;
        wait_for_target(&mut events, &watcher, &state, client.default_timeout()).await?;
        client
            .request(
                &watcher,
                "watchResources",
                json!({ "resourceTypes": RESOURCE_TYPES }),
            )
            .await?;
        // The reader broadcasts a notification before it routes the reply that
        // follows it, so the bootstrap sources are already queued here.
        drain(&mut events, &watcher, &state);

        let follower =
            tokio::spawn(follow(events, watcher.clone(), Arc::clone(&state))).abort_handle();
        Ok(Self {
            client: client.clone(),
            watcher,
            breakpoint_list,
            thread_configuration,
            state,
            follower,
        })
    }

    /// Attaches to the first tab whose URL starts with `url_prefix`.
    ///
    /// BiDi and RDP name tabs differently (a UUID and an actor), so the URL is
    /// how a tab driven with BiDi is found here.
    pub async fn attach_by_url(client: &RdpClient, url_prefix: &str) -> BidiResult<Self> {
        let tabs = client.list_tabs().await?;
        let tab = tabs
            .iter()
            .find(|tab| tab.url.starts_with(url_prefix))
            .ok_or_else(|| {
                BidiError::InternalError(format!("no tab has a URL starting with {url_prefix}"))
            })?;
        Self::attach(client, tab).await
    }

    /// Attaches to the selected tab.
    pub async fn attach_selected(client: &RdpClient) -> BidiResult<Self> {
        let tabs = client.list_tabs().await?;
        let tab = tabs
            .iter()
            .find(|tab| tab.selected)
            .ok_or_else(|| BidiError::InternalError("no tab is selected".to_string()))?;
        Self::attach(client, tab).await
    }

    /// The connection, for requests this type does not wrap.
    pub fn client(&self) -> &RdpClient {
        &self.client
    }

    /// The watcher actor of the tab.
    pub fn watcher(&self) -> &str {
        &self.watcher
    }

    /// The breakpoint list actor (watcher-level: its breakpoints survive navigations).
    pub fn breakpoint_list_actor(&self) -> &str {
        &self.breakpoint_list
    }

    /// The thread configuration actor (pause on exceptions).
    pub fn thread_configuration_actor(&self) -> &str {
        &self.thread_configuration
    }

    /// The thread actor of the current target. It changes on every navigation.
    pub fn thread_actor(&self) -> String {
        self.target()
            .map(|target| target.thread_actor)
            .unwrap_or_default()
    }

    /// The console actor of the current target. It changes on every navigation.
    pub fn console_actor(&self) -> String {
        self.target()
            .map(|target| target.console_actor)
            .unwrap_or_default()
    }

    /// The sources of the current target known so far.
    ///
    /// Filled by a background task as Firefox announces them, and emptied when the
    /// tab navigates. Sources created by `evaluate*` are left out.
    pub fn sources(&self) -> Vec<SourceInfo> {
        read(&self.state).sources.clone()
    }

    /// `false` if announcements were missed (the event ring lagged), so
    /// [`Debugger::sources`] may lack some until the next navigation.
    pub fn sources_complete(&self) -> bool {
        read(&self.state).sources_complete
    }

    /// The current target's `resources-available-array` packets that announce sources.
    pub fn on_sources(&self) -> EventFilter<RdpPacket> {
        self.on_resource("source")
    }

    /// The current target's `resources-available-array` packets that announce a
    /// thread state; read them with [`parse_thread_states`].
    ///
    /// One packet can carry several states: after a step Firefox sends `resumed`
    /// and the new `paused` together, so read every state, not only the first.
    pub fn on_thread_state(&self) -> EventFilter<RdpPacket> {
        self.on_resource("thread-state")
    }

    /// The text of a source. Big scripts come as a `longString`, fetched whole.
    pub async fn source_text(&self, source_actor: &str) -> BidiResult<String> {
        let reply = self
            .client
            .request(source_actor, "source", NoParams)
            .await?;
        let source = reply
            .body
            .get("source")
            .ok_or_else(|| BidiError::missing_field("source", "source"))?;
        if let Some(text) = source.as_str() {
            return Ok(text.to_string());
        }

        let (Some(actor), Some(length)) = (str_at(source, &["actor"]), source["length"].as_u64())
        else {
            return Err(BidiError::InternalError(
                "source returned neither a string nor a longString grip".to_string(),
            ));
        };
        let whole = self
            .client
            .request(actor, "substring", json!({ "start": 0, "end": length }))
            .await?;
        whole
            .str_field("substring")
            .map(str::to_string)
            .ok_or_else(|| BidiError::missing_field("substring", "substring"))
    }

    /// The first cached source whose text contains `needle`, and where.
    ///
    /// Texts are fetched one by one; a source whose actor is gone is skipped.
    pub async fn find_in_sources(
        &self,
        needle: &str,
    ) -> BidiResult<Option<(SourceInfo, BreakpointLocation)>> {
        for source in self.sources() {
            let Some(url) = source.url.clone() else {
                continue;
            };
            let text = match self.source_text(&source.actor).await {
                Ok(text) => text,
                Err(BidiError::ProtocolError { .. }) => continue,
                Err(error) => return Err(error),
            };
            if let Some(location) = locate(&url, &text, needle) {
                return Ok(Some((source, location)));
            }
        }
        Ok(None)
    }

    /// Sets a breakpoint, optionally conditional.
    ///
    /// Firefox needs the exact column of a breakable position. With `column: None`
    /// it is taken from the source, which must then be loaded; a column given by
    /// the caller needs no source, so a breakpoint can be set before its script
    /// loads.
    pub async fn set_breakpoint(
        &self,
        location: &BreakpointLocation,
        condition: Option<&str>,
    ) -> BidiResult<()> {
        let resolved = self.resolve(location).await?;
        let mut options = json!({});
        if let Some(condition) = condition {
            options["condition"] = json!(condition);
        }
        let params = json!({ "location": location_json(&resolved), "options": options });
        self.client
            .request(&self.breakpoint_list, "setBreakpoint", params)
            .await?;

        let mut state = write(&self.state);
        if !state.breakpoints.contains(&resolved) {
            state.breakpoints.push(resolved);
        }
        Ok(())
    }

    /// Removes a breakpoint set with [`Debugger::set_breakpoint`].
    pub async fn remove_breakpoint(&self, location: &BreakpointLocation) -> BidiResult<()> {
        let resolved = match self.remembered(location) {
            Some(resolved) => resolved,
            None => self.resolve(location).await?,
        };
        let params = json!({ "location": location_json(&resolved) });
        self.client
            .request(&self.breakpoint_list, "removeBreakpoint", params)
            .await?;
        write(&self.state)
            .breakpoints
            .retain(|known| *known != resolved);
        Ok(())
    }

    /// When to pause on exceptions. The setting survives navigations.
    pub async fn set_pause_on_exceptions(&self, mode: PauseOnExceptions) -> BidiResult<()> {
        let params = json!({ "configuration": mode.configuration() });
        self.client
            .request(&self.thread_configuration, "updateConfiguration", params)
            .await?;
        Ok(())
    }

    /// Pauses at the next JavaScript to run (`interrupt`).
    pub async fn pause(&self) -> BidiResult<()> {
        self.thread_request("interrupt", json!({ "when": "onNext" }))
            .await
    }

    /// Resumes. Fails with `wrongState` if the thread is not paused.
    pub async fn resume(&self) -> BidiResult<()> {
        self.thread_request("resume", json!({ "resumeLimit": null }))
            .await
    }

    /// Runs to the next line of the current function.
    pub async fn step_over(&self) -> BidiResult<()> {
        self.step("next").await
    }

    /// Steps into the call on the current line.
    pub async fn step_into(&self) -> BidiResult<()> {
        self.step("step").await
    }

    /// Runs until the current function returns, pausing in its caller.
    pub async fn step_out(&self) -> BidiResult<()> {
        self.step("finish").await
    }

    /// `count` stack frames from `start`, while paused.
    pub async fn frames(&self, start: u32, count: u32) -> BidiResult<Vec<FrameInfo>> {
        let params = json!({ "start": start, "count": count });
        let reply = self
            .client
            .request(&self.thread_actor(), "frames", params)
            .await?;
        let frames = reply
            .body
            .get("frames")
            .and_then(Value::as_array)
            .ok_or_else(|| BidiError::missing_field("frames", "frames"))?;
        Ok(frames.iter().filter_map(FrameInfo::from_value).collect())
    }

    /// The scopes of a frame (`getEnvironment`), as Firefox sends them.
    pub async fn frame_environment(&self, frame_actor: &str) -> BidiResult<Value> {
        let reply = self
            .client
            .request(frame_actor, "getEnvironment", NoParams)
            .await?;
        Ok(reply.body)
    }

    /// Evaluates `expression` in a paused frame.
    pub async fn evaluate_on_frame(
        &self,
        frame_actor: &str,
        expression: &str,
    ) -> BidiResult<Evaluation> {
        self.evaluate_with(json!({ "text": expression, "frameActor": frame_actor }))
            .await
    }

    /// Evaluates `expression` in the page.
    pub async fn evaluate(&self, expression: &str) -> BidiResult<Evaluation> {
        self.evaluate_with(json!({ "text": expression })).await
    }

    /// Removes the breakpoints this debugger set, then stops watching targets and
    /// resources.
    ///
    /// Unwatching does not remove breakpoints, hence the order. The unwatch
    /// requests are one-way: Firefox does not answer them. A paused thread stays
    /// paused: resume it first. A breakpoint list that is already gone (closed tab)
    /// counts as success.
    pub async fn detach(self) -> BidiResult<()> {
        let breakpoints = std::mem::take(&mut write(&self.state).breakpoints);
        let mut first_error = None;
        for breakpoint in &breakpoints {
            let params = json!({ "location": location_json(breakpoint) });
            match self
                .client
                .request(&self.breakpoint_list, "removeBreakpoint", params)
                .await
            {
                Ok(_) | Err(BidiError::Disconnected) => {}
                Err(BidiError::ProtocolError { error, .. }) if error == "noSuchActor" => {}
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }

        // Best effort: a dead connection has already dropped the watcher.
        let _ = self.client.send_oneway(
            &self.watcher,
            "unwatchResources",
            json!({ "resourceTypes": RESOURCE_TYPES }),
        );
        let _ = self.client.send_oneway(
            &self.watcher,
            "unwatchTargets",
            json!({ "targetType": "frame" }),
        );
        self.follower.abort();
        first_error.map_or(Ok(()), Err)
    }

    fn target(&self) -> Option<TargetForm> {
        read(&self.state).target.clone()
    }

    /// Resource packets of `resource_type` from the current target.
    fn on_resource(&self, resource_type: &'static str) -> EventFilter<RdpPacket> {
        let state = Arc::clone(&self.state);
        EventFilter::with_predicate(self.client.subscribe(), move |packet| {
            from_current_target(&state, packet)
                && !parse_resources(packet, resource_type).is_empty()
        })
    }

    async fn thread_request(&self, method: &str, params: Value) -> BidiResult<()> {
        self.client
            .request(&self.thread_actor(), method, params)
            .await?;
        Ok(())
    }

    async fn step(&self, limit: &str) -> BidiResult<()> {
        self.thread_request("resume", json!({ "resumeLimit": { "type": limit } }))
            .await
    }

    /// `evaluateJSAsync`, then the `evaluationResult` that carries its `resultID`.
    async fn evaluate_with(&self, params: Value) -> BidiResult<Evaluation> {
        let console = self.console_actor();
        // Subscribed before sending: the result is a later notification.
        let mut results = self.client.on_actor(&console);
        let reply = self
            .client
            .request(&console, "evaluateJSAsync", params)
            .await?;
        let id = reply
            .str_field("resultID")
            .ok_or_else(|| BidiError::missing_field("evaluateJSAsync", "resultID"))?
            .to_string();

        let limit = self.client.default_timeout();
        let waiting = async {
            while let Some(packet) = results.next().await {
                let packet = packet?;
                if packet.r#type.as_deref() == Some("evaluationResult")
                    && packet.str_field("resultID") == Some(id.as_str())
                {
                    return Ok(Evaluation::from_packet(&packet));
                }
            }
            Err(BidiError::Disconnected)
        };
        timeout(limit, waiting).await.unwrap_or_else(|_| {
            Err(BidiError::Timeout {
                method: "evaluationResult".to_string(),
                timeout: limit,
            })
        })
    }

    /// A breakpoint set earlier for the same place, when the caller gives no column.
    fn remembered(&self, location: &BreakpointLocation) -> Option<BreakpointLocation> {
        read(&self.state)
            .breakpoints
            .iter()
            .find(|known| {
                known.source_url == location.source_url
                    && known.line == location.line
                    && location
                        .column
                        .is_none_or(|column| known.column == Some(column))
            })
            .cloned()
    }

    /// `location` with its column; the first breakable position of the line if none is given.
    async fn resolve(&self, location: &BreakpointLocation) -> BidiResult<BreakpointLocation> {
        if location.column.is_some() {
            return Ok(location.clone());
        }
        let url = &location.source_url;
        let source = self
            .sources()
            .into_iter()
            .find(|source| source.url.as_deref() == Some(url.as_str()))
            .ok_or_else(|| {
                invalid_argument(format!(
                    "no loaded source has the URL {url}; give the column"
                ))
            })?;

        let line = location.line;
        let query = json!({ "query": {
            "start": { "line": line, "column": 0 },
            "end": { "line": line + 1, "column": 0 },
        } });
        let reply = self
            .client
            .request(&source.actor, "getBreakpointPositionsCompressed", query)
            .await?;
        let column = reply.body["positions"][line.to_string()]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_u64)
            .min()
            .and_then(|column| u32::try_from(column).ok())
            .ok_or_else(|| {
                invalid_argument(format!("line {line} of {url} has no breakable position"))
            })?;

        Ok(BreakpointLocation {
            column: Some(column),
            ..location.clone()
        })
    }
}

/// The error for a request the debugger refuses before sending it.
fn invalid_argument(message: String) -> BidiError {
    BidiError::ProtocolError {
        error: codes::INVALID_ARGUMENT.to_string(),
        message,
        stacktrace: None,
    }
}

/// `{sourceUrl, line, column}` as the breakpoint list expects it.
fn location_json(location: &BreakpointLocation) -> Value {
    json!({ "sourceUrl": location.source_url, "line": location.line, "column": location.column })
}

/// The actor at `path` in a bootstrap reply.
fn actor_in(packet: RdpPacket, path: &[&str], method: &str) -> BidiResult<String> {
    str_at(&packet.body, path)
        .map(str::to_string)
        .ok_or_else(|| BidiError::missing_field(method, &path.join(".")))
}

/// Whether `packet` was sent by the current top-level target.
fn from_current_target(state: &Shared, packet: &RdpPacket) -> bool {
    let state = read(state);
    let current = state.target.as_ref().map(|target| target.actor.as_str());
    current.is_some() && packet.from.as_deref() == current
}

/// The top-level target a `target-available-form` announces, if it is one.
fn top_level_target(body: &Value) -> Option<TargetForm> {
    let target = body.get("target")?;
    if target.get("isTopLevelTarget") != Some(&Value::Bool(true)) {
        return None;
    }
    Some(TargetForm {
        actor: str_at(target, &["actor"])?.to_string(),
        thread_actor: str_at(target, &["threadActor"])?.to_string(),
        console_actor: str_at(target, &["consoleActor"])?.to_string(),
    })
}

/// Updates the state with one notification of the connection.
///
/// Several tabs can share a connection, so only this watcher's targets and the
/// current target's resources count.
fn apply(packet: &RdpPacket, watcher: &str, state: &Shared) {
    match packet.r#type.as_deref() {
        Some("target-available-form") if packet.from.as_deref() == Some(watcher) => {
            if let Some(target) = top_level_target(&packet.body) {
                // The sources died with the previous target.
                let mut state = write(state);
                state.target = Some(target);
                state.sources.clear();
                state.sources_complete = true;
            }
        }
        Some("resources-available-array") if from_current_target(state, packet) => {
            let mut state = write(state);
            for source in parse_sources(packet) {
                // Every `evaluate*` announces one of these; they would pile up.
                let evaluation = source.introduction_type.as_deref() == Some("debugger eval");
                if !evaluation
                    && !state
                        .sources
                        .iter()
                        .any(|known| known.actor == source.actor)
                {
                    state.sources.push(source);
                }
            }
        }
        _ => {}
    }
}

/// Waits for the first top-level target, applying what arrives meanwhile.
async fn wait_for_target(
    events: &mut broadcast::Receiver<RdpPacket>,
    watcher: &str,
    state: &Shared,
    limit: Duration,
) -> BidiResult<()> {
    let deadline = Instant::now() + limit;
    while read(state).target.is_none() {
        match timeout_at(deadline, events.recv()).await {
            Ok(Ok(packet)) => apply(&packet, watcher, state),
            Ok(Err(RecvError::Lagged(_))) => write(state).sources_complete = false,
            Ok(Err(RecvError::Closed)) => return Err(BidiError::Disconnected),
            Err(_) => {
                return Err(BidiError::Timeout {
                    method: "watchTargets".to_string(),
                    timeout: limit,
                });
            }
        }
    }
    Ok(())
}

/// Applies every notification already queued.
fn drain(events: &mut broadcast::Receiver<RdpPacket>, watcher: &str, state: &Shared) {
    loop {
        match events.try_recv() {
            Ok(packet) => apply(&packet, watcher, state),
            Err(TryRecvError::Lagged(_)) => write(state).sources_complete = false,
            Err(TryRecvError::Empty | TryRecvError::Closed) => return,
        }
    }
}

/// Background task: keeps the state up to date until the connection closes.
async fn follow(mut events: broadcast::Receiver<RdpPacket>, watcher: String, state: Shared) {
    loop {
        match events.recv().await {
            Ok(packet) => apply(&packet, &watcher, &state),
            // Announcements were missed: the cache may lack sources.
            Err(RecvError::Lagged(_)) => write(&state).sources_complete = false,
            Err(RecvError::Closed) => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::BidiError;
    use crate::test_support::{MockServer, RdpOverride, fixture, rdp_firefox_like, rdp_reply};
    use serde_json::json;
    use std::time::Duration;
    use tokio::time::{Instant, sleep, timeout};
    use tokio_stream::StreamExt;

    const TIMEOUT: Duration = Duration::from_secs(1);
    const QUIET: Duration = Duration::from_millis(100);
    const WAIT: Duration = Duration::from_secs(2);

    // Actors of the fixtures the mock answers the bootstrap with.
    const TAB: &str = "server1.conn1.tabDescriptor1";
    const WATCHER: &str = "server1.conn1.watcher3";
    const BP_LIST: &str = "server1.conn1.breakpoint-list4";
    const THREAD_CFG: &str = "server1.conn1.thread-configuration5";
    const TARGET: &str = "server1.conn1.watcher2.process7//windowGlobalTarget2";
    const THREAD: &str = "server1.conn1.watcher2.process7//thread1";
    const CONSOLE: &str = "server1.conn1.watcher2.process7//consoleActor3";
    const SOURCE: &str = "server1.conn1.watcher2.process7//source18";
    const PAGE: &str = "https://example.com/test.html";

    async fn client_for(server: &MockServer, limit: Duration) -> RdpClient {
        RdpClient::connect(&server.host(), limit)
            .await
            .expect("connect")
    }

    /// A debugger attached through the mock, with the bootstrap commands consumed.
    async fn attached(overrides: Vec<RdpOverride>) -> (MockServer, Debugger) {
        let mut server = MockServer::start_rdp(rdp_firefox_like(overrides)).await;
        let client = client_for(&server, TIMEOUT).await;
        let debugger = Debugger::attach_selected(&client).await.expect("attach");
        server.next_command_named("watchResources").await;
        (server, debugger)
    }

    fn failure<T>(result: BidiResult<T>) -> BidiError {
        match result {
            Err(error) => error,
            Ok(_) => panic!("expected an error"),
        }
    }

    fn location(source_url: &str, line: u32, column: Option<u32>) -> BreakpointLocation {
        BreakpointLocation {
            source_url: source_url.to_string(),
            line,
            column,
        }
    }

    /// Waits until `condition` holds, failing the test after 2 s.
    async fn eventually(what: &str, condition: impl Fn() -> bool) {
        let deadline = Instant::now() + WAIT;
        while !condition() {
            assert!(
                Instant::now() < deadline,
                "{what} did not happen within 2 s"
            );
            sleep(Duration::from_millis(10)).await;
        }
    }

    fn sources_announced(from: &str, sources: Value) -> Value {
        json!({ "type": "resources-available-array", "from": from, "array": [["source", sources]] })
    }

    fn is_invalid_argument(error: &BidiError) -> bool {
        matches!(error, BidiError::ProtocolError { error, .. } if error == "invalid argument")
    }

    /// DBG-01: `attach` runs the watcher bootstrap in order, without `thread.attach`.
    #[tokio::test]
    async fn attach_runs_watcher_bootstrap_in_order() {
        // Given a mock Firefox and its tab
        let mut server = MockServer::start_rdp(rdp_firefox_like(vec![])).await;
        let client = client_for(&server, TIMEOUT).await;
        let tabs = client.list_tabs().await.expect("tabs");
        server.next_command_named("listTabs").await;

        // When the debugger attaches
        let debugger = Debugger::attach(&client, &tabs[0]).await.expect("attach");

        // Then the server saw the watcher bootstrap, in order
        let get_watcher = server.next_command().await;
        assert_eq!(get_watcher["to"], TAB);
        assert_eq!(get_watcher["type"], "getWatcher");
        assert_eq!(get_watcher["isServerTargetSwitchingEnabled"], true);
        for kind in ["getBreakpointListActor", "getThreadConfigurationActor"] {
            let command = server.next_command().await;
            assert_eq!(
                (command["to"].as_str(), command["type"].as_str()),
                (Some(WATCHER), Some(kind))
            );
        }
        let targets = server.next_command().await;
        assert_eq!(targets["type"], "watchTargets");
        assert_eq!(targets["targetType"], "frame");
        let resources = server.next_command().await;
        assert_eq!(resources["type"], "watchResources");
        assert_eq!(
            resources["resourceTypes"],
            json!(["source", "thread-state"])
        );
        // And nothing else, no thread.attach in particular
        server.assert_no_command(QUIET).await;
        // And the actors come from the target form
        assert_eq!(debugger.watcher(), WATCHER);
        assert_eq!(debugger.thread_actor(), THREAD);
        assert_eq!(debugger.console_actor(), CONSOLE);
    }

    /// DBG-02: `attach` finds the target whether it is announced before or after the reply.
    #[tokio::test]
    async fn attach_catches_early_target() {
        // Given a Firefox that announces the target only after answering watchTargets
        let late = vec![
            rdp_reply(WATCHER, json!({})),
            rdp_reply(WATCHER, fixture("rdp/target-available.json")),
        ];
        let (_server, debugger) = attached(vec![((WATCHER, "watchTargets"), late)]).await;

        // Then attaching still found it (the early case is DBG-01)
        assert_eq!(debugger.thread_actor(), THREAD);
    }

    /// DBG-03: `attach` fails clearly when no top-level target appears.
    #[tokio::test]
    async fn attach_without_top_level_target_times_out() {
        // Given a Firefox that announces no target
        let silent_targets = vec![(
            (WATCHER, "watchTargets"),
            vec![rdp_reply(WATCHER, json!({}))],
        )];
        let server = MockServer::start_rdp(rdp_firefox_like(silent_targets)).await;
        let client = client_for(&server, Duration::from_millis(300)).await;

        // When the debugger attaches
        let error = failure(Debugger::attach_selected(&client).await);

        // Then it times out naming watchTargets
        assert!(
            matches!(&error, BidiError::Timeout { method, .. } if method == "watchTargets"),
            "{error:?}"
        );
    }

    fn tabs(list: Value) -> Vec<RdpOverride> {
        vec![(
            ("root", "listTabs"),
            vec![rdp_reply("root", json!({ "tabs": list }))],
        )]
    }

    /// DBG-04: `attach_by_url` picks the descriptor by URL prefix.
    #[tokio::test]
    async fn attach_by_url_picks_by_prefix() {
        // Given two tabs
        let list = json!([
            { "actor": "tabA", "url": "about:blank", "selected": true },
            { "actor": "tabB", "url": "https://www.rust-lang.org/" },
        ]);
        let mut server = MockServer::start_rdp(rdp_firefox_like(tabs(list))).await;
        let client = client_for(&server, TIMEOUT).await;

        // When attaching by the prefix of the second
        Debugger::attach_by_url(&client, "https://www.rust-lang.org")
            .await
            .expect("attach");

        // Then its watcher was asked for
        assert_eq!(server.next_command_named("getWatcher").await["to"], "tabB");
        // And a prefix no tab has fails naming it
        let error = failure(Debugger::attach_by_url(&client, "https://nope").await);
        assert!(
            matches!(&error, BidiError::InternalError(message) if message.contains("https://nope")),
            "{error:?}"
        );
    }

    /// DBG-05: `attach_selected` picks the selected descriptor.
    #[tokio::test]
    async fn attach_selected_picks_selected_tab() {
        // Given two tabs, the second selected
        let list = json!([{ "actor": "tabA", "url": "a" }, { "actor": "tabB", "url": "b", "selected": true }]);
        let mut server = MockServer::start_rdp(rdp_firefox_like(tabs(list))).await;
        let client = client_for(&server, TIMEOUT).await;

        // When attaching to the selected one
        Debugger::attach_selected(&client).await.expect("attach");

        // Then it is the second
        assert_eq!(server.next_command_named("getWatcher").await["to"], "tabB");
        // And with no selected tab it fails
        let none = json!([{ "actor": "tabA", "url": "a" }]);
        let server = MockServer::start_rdp(rdp_firefox_like(tabs(none))).await;
        let client = client_for(&server, TIMEOUT).await;
        let error = failure(Debugger::attach_selected(&client).await);
        assert!(matches!(error, BidiError::InternalError(_)), "{error:?}");
    }

    /// DBG-06: Sources from the bootstrap and from later notifications are cached.
    #[tokio::test]
    async fn sources_are_cached_from_notifications() {
        // Given an attached debugger, whose bootstrap announced one source
        let (server, debugger) = attached(vec![]).await;
        assert_eq!(
            debugger.sources().len(),
            1,
            "the bootstrap source is cached by attach"
        );
        assert!(debugger.sources_complete());

        // When the target announces another source
        server.push(sources_announced(
            TARGET,
            json!([{ "actor": "source99", "url": "https://x/app.js" }]),
        ));

        // Then the cache eventually holds both
        eventually("the second source", || debugger.sources().len() == 2).await;
        assert!(
            debugger
                .sources()
                .iter()
                .any(|source| source.actor == "source99")
        );
    }

    /// DBG-07: `source_text` returns plain strings and resolves `longString` grips.
    #[tokio::test]
    async fn source_text_resolves_long_strings() {
        // Given a short source and a long one, as Firefox sends them
        let grip = "server1.conn0.longString2";
        let (mut server, debugger) = attached(vec![
            (
                (SOURCE, "source"),
                vec![rdp_reply(SOURCE, json!({ "source": "let a = 1;" }))],
            ),
            (
                ("source8", "source"),
                vec![rdp_reply("source8", fixture("rdp/source-long-string.json"))],
            ),
            (
                (grip, "substring"),
                vec![rdp_reply(
                    grip,
                    json!({ "substring": "function hugeScript() {}" }),
                )],
            ),
        ])
        .await;

        // Then the short one is returned as is
        assert_eq!(
            debugger.source_text(SOURCE).await.expect("text"),
            "let a = 1;"
        );
        // And the long one is fetched whole through its grip
        let long = debugger.source_text("source8").await.expect("text");
        assert_eq!(long, "function hugeScript() {}");
        let substring = server.next_command_named("substring").await;
        assert_eq!(
            (substring["start"].as_u64(), substring["end"].as_u64()),
            (Some(0), Some(150_000))
        );
    }

    /// DBG-08: `set_breakpoint` sends the location to the breakpoint list.
    #[tokio::test]
    async fn set_breakpoint_sends_location_and_options() {
        // Given an attached debugger
        let (mut server, debugger) = attached(vec![]).await;

        // When a breakpoint with a column and a condition is set
        let at = location("https://x/app.js", 12, Some(4));
        debugger
            .set_breakpoint(&at, Some("a > 1"))
            .await
            .expect("set");

        // Then the breakpoint list received it, with no source needed
        let command = server.next_command().await;
        assert_eq!(command["to"], BP_LIST);
        assert_eq!(command["type"], "setBreakpoint");
        assert_eq!(
            command["location"],
            json!({ "sourceUrl": "https://x/app.js", "line": 12, "column": 4 })
        );
        assert_eq!(command["options"], json!({ "condition": "a > 1" }));
        // And without a condition the options are empty
        debugger.set_breakpoint(&at, None).await.expect("set");
        assert_eq!(server.next_command().await["options"], json!({}));
    }

    /// DBG-09: `remove_breakpoint` sends the same location.
    #[tokio::test]
    async fn remove_breakpoint_sends_location() {
        // Given a breakpoint that was set
        let (mut server, debugger) = attached(vec![]).await;
        let at = location("https://x/app.js", 12, Some(4));
        debugger.set_breakpoint(&at, None).await.expect("set");
        server.next_command().await;

        // When it is removed
        debugger.remove_breakpoint(&at).await.expect("removed");

        // Then the breakpoint list saw the same location
        let command = server.next_command().await;
        assert_eq!(
            (command["to"].as_str(), command["type"].as_str()),
            (Some(BP_LIST), Some("removeBreakpoint"))
        );
        assert_eq!(
            command["location"],
            json!({ "sourceUrl": "https://x/app.js", "line": 12, "column": 4 })
        );
        // And it is no longer remembered
        assert!(debugger.state.read().expect("lock").breakpoints.is_empty());
    }

    /// DBG-10: `set_pause_on_exceptions` maps the mode.
    #[tokio::test]
    async fn pause_on_exceptions_modes_map_to_configuration() {
        let (mut server, debugger) = attached(vec![]).await;
        for mode in [
            PauseOnExceptions::None,
            PauseOnExceptions::Uncaught,
            PauseOnExceptions::All,
        ] {
            debugger.set_pause_on_exceptions(mode).await.expect("set");
            let command = server.next_command().await;
            assert_eq!(command["to"], THREAD_CFG);
            assert_eq!(command["type"], "updateConfiguration");
            assert_eq!(command["configuration"], mode.configuration(), "{mode:?}");
        }
    }

    /// DBG-11: `resume` and the step helpers send the right `resumeLimit`.
    #[tokio::test]
    async fn resume_and_steps_send_resume_limit() {
        let (mut server, debugger) = attached(vec![]).await;
        debugger.resume().await.expect("resume");
        debugger.step_over().await.expect("next");
        debugger.step_into().await.expect("step");
        debugger.step_out().await.expect("finish");
        let expected = [
            Value::Null,
            json!({ "type": "next" }),
            json!({ "type": "step" }),
            json!({ "type": "finish" }),
        ];
        for limit in expected {
            let command = server.next_command().await;
            assert_eq!(
                (command["to"].as_str(), command["type"].as_str()),
                (Some(THREAD), Some("resume"))
            );
            // `resumeLimit: null` must be present: a plain resume
            assert_eq!(command.get("resumeLimit"), Some(&limit));
        }
    }

    /// DBG-12: `pause` interrupts the thread.
    #[tokio::test]
    async fn pause_interrupts_thread() {
        let (mut server, debugger) = attached(vec![]).await;
        debugger.pause().await.expect("pause");
        let command = server.next_command().await;
        assert_eq!(
            (command["to"].as_str(), command["type"].as_str()),
            (Some(THREAD), Some("interrupt"))
        );
        assert_eq!(command["when"], "onNext");
    }

    fn console_answers(packets: Vec<Value>) -> Vec<RdpOverride> {
        let replies = packets
            .into_iter()
            .map(|packet| rdp_reply(CONSOLE, packet))
            .collect();
        vec![((CONSOLE, "evaluateJSAsync"), replies)]
    }

    /// DBG-13: `evaluate_on_frame` correlates `evaluationResult` by `resultID`.
    #[tokio::test]
    async fn evaluate_on_frame_correlates_result_id() {
        // Given a console that reports someone else's result first
        let (mut server, debugger) = attached(console_answers(vec![
            json!({ "resultID": "r-2" }),
            json!({ "type": "evaluationResult", "resultID": "r-1", "result": "other" }),
            json!({ "type": "evaluationResult", "resultID": "r-2", "result": "en-US" }),
        ]))
        .await;

        // When an expression is evaluated on a frame
        let evaluation = debugger
            .evaluate_on_frame("frame5", "current_lang")
            .await
            .expect("evaluated");

        // Then the console saw the text and the frame
        let command = server.next_command().await;
        assert_eq!(command["text"], "current_lang");
        assert_eq!(command["frameActor"], "frame5");
        // And the result is the one with our resultID
        assert_eq!(evaluation.result, "en-US");
        assert!(evaluation.exception.is_none());
    }

    /// DBG-14: An expression that throws is reported in `Evaluation::exception`, not as an error.
    #[tokio::test]
    async fn thrown_expression_is_reported_as_exception() {
        let (_server, debugger) = attached(console_answers(vec![
            json!({ "resultID": "r-1" }),
            json!({ "type": "evaluationResult", "resultID": "r-1", "exception": { "type": "object", "class": "Error" } }),
        ]))
        .await;
        let evaluation = debugger
            .evaluate("throw new Error('x')")
            .await
            .expect("not an error");
        assert_eq!(
            evaluation.exception.expect("an exception")["class"],
            "Error"
        );
    }

    /// DBG-30: `evaluate` without a frame omits `frameActor`.
    #[tokio::test]
    async fn evaluate_without_frame_omits_frame_actor() {
        let (mut server, debugger) = attached(console_answers(vec![
            json!({ "resultID": "r-1" }),
            json!({ "type": "evaluationResult", "resultID": "r-1", "result": 2 }),
        ]))
        .await;
        assert_eq!(
            debugger.evaluate("1 + 1").await.expect("evaluated").result,
            2
        );
        let command = server.next_command().await;
        assert_eq!(command["text"], "1 + 1");
        assert!(command.get("frameActor").is_none(), "{command}");
    }

    fn new_target(actor: &str, thread: &str, console: &str) -> Value {
        json!({
            "type": "target-available-form",
            "from": WATCHER,
            "target": { "actor": actor, "threadActor": thread, "consoleActor": console, "isTopLevelTarget": true },
        })
    }

    /// DBG-20: A navigation swaps the target, whether it is a reload or cross-origin.
    #[tokio::test]
    async fn cross_origin_navigation_swaps_target() {
        // Given an attached debugger with a breakpoint
        let (mut server, debugger) = attached(vec![]).await;
        debugger
            .set_breakpoint(&location(PAGE, 7, Some(10)), None)
            .await
            .expect("set");
        server.next_command().await;

        // When the target is destroyed and a new top-level one appears, with its source
        server.push(fixture("rdp/target-destroyed.json"));
        server.push(new_target("T2", "thread2", "console2"));
        server.push(sources_announced(
            "T2",
            json!([{ "actor": "source2", "url": PAGE }]),
        ));

        // Then the actors follow the new target
        eventually("the new thread actor", || {
            debugger.thread_actor() == "thread2"
        })
        .await;
        assert_eq!(debugger.console_actor(), "console2");
        // And only the new target's source is cached
        eventually("the new source", || {
            debugger
                .sources()
                .iter()
                .any(|source| source.actor == "source2")
        })
        .await;
        assert!(
            debugger
                .sources()
                .iter()
                .all(|source| source.actor != SOURCE)
        );
        // And no breakpoint was sent again: the breakpoint list keeps it
        server.assert_no_command(QUIET).await;
    }

    /// DBG-21: `detach` tolerates a closed tab.
    #[tokio::test]
    async fn detach_tolerates_closed_tab() {
        // Given a breakpoint, and a breakpoint list that is already gone
        let gone = rdp_reply(
            BP_LIST,
            json!({ "error": "noSuchActor", "message": "gone" }),
        );
        let (_server, debugger) = attached(vec![((BP_LIST, "removeBreakpoint"), vec![gone])]).await;
        debugger
            .set_breakpoint(&location(PAGE, 7, Some(10)), None)
            .await
            .expect("set");

        // Then detaching succeeds
        let result = debugger.detach().await;
        assert!(result.is_ok(), "{result:?}");
    }

    /// DBG-26: `detach` removes the breakpoints it set, then unwatches without waiting.
    #[tokio::test]
    async fn detach_removes_breakpoints_then_unwatches() {
        // Given two breakpoints
        let (mut server, debugger) = attached(vec![]).await;
        for line in [7, 8] {
            debugger
                .set_breakpoint(&location(PAGE, line, Some(2)), None)
                .await
                .expect("set");
            server.next_command().await;
        }

        // When the debugger detaches, against a server that never answers the unwatch requests
        timeout(Duration::from_millis(500), debugger.detach())
            .await
            .expect("detach must not wait for the one-way unwatch requests")
            .expect("detached");

        // Then both breakpoints were removed, and only then the watcher was unwatched
        let mut seen = Vec::new();
        for _ in 0..4 {
            let command = server.next_command().await;
            seen.push((
                command["to"].as_str().map(str::to_string),
                command["type"].as_str().map(str::to_string),
            ));
        }
        let expect = |to: &str, kind: &str| (Some(to.to_string()), Some(kind.to_string()));
        assert_eq!(
            seen,
            [
                expect(BP_LIST, "removeBreakpoint"),
                expect(BP_LIST, "removeBreakpoint"),
                expect(WATCHER, "unwatchResources"),
                expect(WATCHER, "unwatchTargets"),
            ]
        );
    }

    /// DBG-22: `frames` parses the frame list.
    #[tokio::test]
    async fn frames_parses_frame_list() {
        let frames = vec![rdp_reply(THREAD, fixture("rdp/frames.json"))];
        let (mut server, debugger) = attached(vec![((THREAD, "frames"), frames)]).await;
        let infos = debugger.frames(0, 10).await.expect("frames");
        let command = server.next_command().await;
        assert_eq!(
            (command["start"].as_u64(), command["count"].as_u64()),
            (Some(0), Some(10))
        );
        assert_eq!(infos.len(), 2);
        assert_eq!(infos[0].display_name.as_deref(), Some("f"));
        assert_eq!(
            infos[0].location.as_ref().map(|location| location.line),
            Some(8)
        );
    }

    fn positions(reply: Value) -> Vec<RdpOverride> {
        vec![(
            (SOURCE, "getBreakpointPositionsCompressed"),
            vec![rdp_reply(SOURCE, reply)],
        )]
    }

    /// DBG-23: `set_breakpoint` resolves the column from the source when none is given.
    #[tokio::test]
    async fn set_breakpoint_resolves_the_column() {
        // Given a cached source whose line 7 has its position at column 10
        let (mut server, debugger) =
            attached(positions(fixture("rdp/breakpoint-positions.json"))).await;

        // When a breakpoint is set on that line without a column
        debugger
            .set_breakpoint(&location(PAGE, 7, None), None)
            .await
            .expect("set");

        // Then the source was asked for the positions of that line
        let query = server.next_command().await;
        assert_eq!(
            (query["to"].as_str(), query["type"].as_str()),
            (Some(SOURCE), Some("getBreakpointPositionsCompressed"))
        );
        assert_eq!(
            query["query"],
            json!({ "start": { "line": 7, "column": 0 }, "end": { "line": 8, "column": 0 } })
        );
        // And the breakpoint list got the exact column
        let set = server.next_command().await;
        assert_eq!(
            set["location"],
            json!({ "sourceUrl": PAGE, "line": 7, "column": 10 })
        );
    }

    /// DBG-24: `set_breakpoint` without a column needs a loaded source.
    #[tokio::test]
    async fn set_breakpoint_without_column_needs_a_loaded_source() {
        // Given no cached source has the URL
        let (mut server, debugger) = attached(vec![]).await;

        // Then a breakpoint without a column fails naming the URL, and nothing is sent
        let error = failure(
            debugger
                .set_breakpoint(&location("https://x/later.js", 3, None), None)
                .await,
        );
        assert!(is_invalid_argument(&error), "{error:?}");
        assert!(error.to_string().contains("https://x/later.js"), "{error}");
        server.assert_no_command(QUIET).await;
        // And with a column it needs no source
        debugger
            .set_breakpoint(&location("https://x/later.js", 3, Some(0)), None)
            .await
            .expect("set");
        assert_eq!(server.next_command().await["type"], "setBreakpoint");
    }

    /// DBG-25: `set_breakpoint` on a line without code fails.
    #[tokio::test]
    async fn set_breakpoint_on_a_line_without_code_fails() {
        let (mut server, debugger) = attached(positions(json!({ "positions": {} }))).await;
        let error = failure(
            debugger
                .set_breakpoint(&location(PAGE, 3, None), None)
                .await,
        );
        assert!(is_invalid_argument(&error), "{error:?}");
        server
            .next_command_named("getBreakpointPositionsCompressed")
            .await;
        server.assert_no_command(QUIET).await;
        assert!(debugger.state.read().expect("lock").breakpoints.is_empty());
    }

    /// DBG-27: The source cache skips `debugger eval` sources and sources of other targets.
    #[tokio::test]
    async fn source_cache_follows_the_target() {
        // Given an attached debugger
        let (server, debugger) = attached(vec![]).await;

        // When an evaluation source, a source of another target and a real one are announced
        server.push(sources_announced(
            TARGET,
            json!([{ "actor": "eval1", "url": null, "introductionType": "debugger eval" }]),
        ));
        server.push(sources_announced(
            "otherTarget",
            json!([{ "actor": "foreign", "url": "https://y/" }]),
        ));
        server.push(sources_announced(
            TARGET,
            json!([{ "actor": "real", "url": "https://x/app.js" }]),
        ));

        // Then only the real one joins the cache
        eventually("the real source", || {
            debugger
                .sources()
                .iter()
                .any(|source| source.actor == "real")
        })
        .await;
        let actors: Vec<String> = debugger
            .sources()
            .into_iter()
            .map(|source| source.actor)
            .collect();
        assert_eq!(actors, [SOURCE, "real"]);
    }

    /// DBG-28: `find_in_sources` locates the needle.
    #[tokio::test]
    async fn find_in_sources_locates_the_needle() {
        let text = rdp_reply(SOURCE, json!({ "source": "a\nbb\n  needle();" }));
        let (_server, debugger) = attached(vec![((SOURCE, "source"), vec![text])]).await;
        let (source, at) = debugger
            .find_in_sources("needle")
            .await
            .expect("searched")
            .expect("found");
        assert_eq!(source.actor, SOURCE);
        assert_eq!(at, location(PAGE, 3, Some(2)));
        assert!(
            debugger
                .find_in_sources("absent")
                .await
                .expect("searched")
                .is_none()
        );
    }

    /// DBG-29: `frame_environment` returns the environment as sent.
    #[tokio::test]
    async fn frame_environment_returns_the_environment() {
        let environment = rdp_reply(
            "frame5",
            json!({ "actor": "env1", "bindings": { "variables": {} } }),
        );
        let (mut server, debugger) =
            attached(vec![(("frame5", "getEnvironment"), vec![environment])]).await;
        let reply = debugger
            .frame_environment("frame5")
            .await
            .expect("environment");
        assert_eq!(reply["actor"], "env1");
        assert_eq!(server.next_command().await["type"], "getEnvironment");
    }

    /// DBG-35: `on_thread_state` only yields this target's thread states.
    #[tokio::test]
    async fn on_thread_state_only_yields_this_target() {
        // Given a filter on the thread states
        let (server, debugger) = attached(vec![]).await;
        let mut states = debugger.on_thread_state();

        // When another target pauses, this target announces a source, and this target pauses
        server.push(rdp_reply(
            "otherTarget",
            fixture("rdp/resources-thread-state-paused.json"),
        ));
        server.push(sources_announced(
            TARGET,
            json!([{ "actor": "s2", "url": "u" }]),
        ));
        server.push(rdp_reply(
            TARGET,
            fixture("rdp/resources-thread-state-paused.json"),
        ));

        // Then the first packet yielded is this target's pause
        let packet = timeout(WAIT, states.next())
            .await
            .expect("a packet")
            .expect("open")
            .expect("no lag");
        assert_eq!(packet.from.as_deref(), Some(TARGET));
        assert!(matches!(
            parse_thread_states(&packet).as_slice(),
            [ThreadState::Paused { .. }]
        ));
    }

    /// DBG-37: Iframes and targets of other watchers do not replace the target.
    #[tokio::test]
    async fn iframes_and_other_watchers_do_not_swap_the_target() {
        // Given an attached debugger
        let (server, debugger) = attached(vec![]).await;

        // When an iframe of this tab and the top-level target of another tab appear
        let mut iframe = new_target("F1", "threadF", "consoleF");
        iframe["target"]["isTopLevelTarget"] = json!(false);
        let mut foreign = new_target("T9", "thread9", "console9");
        foreign["from"] = json!("otherWatcher");
        server.push(iframe);
        server.push(foreign);
        // And then a source of the current target, which marks the point they were processed
        server.push(sources_announced(
            TARGET,
            json!([{ "actor": "sentinel", "url": "u" }]),
        ));

        // Then the target is still the same, with its sources
        eventually("the sentinel", || {
            debugger
                .sources()
                .iter()
                .any(|source| source.actor == "sentinel")
        })
        .await;
        assert_eq!(debugger.thread_actor(), THREAD);
        assert_eq!(debugger.sources().len(), 2);
    }

    /// DBG-38: Without a column, the first position of the line is taken.
    #[tokio::test]
    async fn set_breakpoint_takes_the_first_position_of_the_line() {
        let reply = json!({ "positions": { "7": [24, 10], "8": [2] } });
        let (mut server, debugger) = attached(positions(reply)).await;
        debugger
            .set_breakpoint(&location(PAGE, 7, None), None)
            .await
            .expect("set");
        let set = server.next_command_named("setBreakpoint").await;
        assert_eq!(set["location"]["column"], 10);
    }

    /// DBG-39: `remove_breakpoint` without a column reuses the column it was set with.
    #[tokio::test]
    async fn remove_breakpoint_reuses_the_resolved_column() {
        // Given a breakpoint set without a column, resolved to column 10
        let (mut server, debugger) =
            attached(positions(fixture("rdp/breakpoint-positions.json"))).await;
        debugger
            .set_breakpoint(&location(PAGE, 7, None), None)
            .await
            .expect("set");
        server.next_command_named("setBreakpoint").await;

        // When it is removed without a column either
        debugger
            .remove_breakpoint(&location(PAGE, 7, None))
            .await
            .expect("removed");

        // Then the same location goes out, without asking the source again
        let command = server.next_command().await;
        assert_eq!(command["type"], "removeBreakpoint");
        assert_eq!(command["location"]["column"], 10);
    }

    /// DBG-40: `find_in_sources` skips a source whose actor is gone.
    #[tokio::test]
    async fn find_in_sources_skips_a_dead_source() {
        // Given a first source whose actor is gone and a second one with the needle
        let gone = rdp_reply(SOURCE, json!({ "error": "noSuchActor", "message": "gone" }));
        let text = rdp_reply("s2", json!({ "source": "needle();" }));
        let (server, debugger) = attached(vec![
            ((SOURCE, "source"), vec![gone]),
            (("s2", "source"), vec![text]),
        ])
        .await;
        server.push(sources_announced(
            TARGET,
            json!([{ "actor": "s2", "url": "https://x/app.js" }]),
        ));
        eventually("the second source", || debugger.sources().len() == 2).await;

        // Then the search goes on to the second
        let (source, _) = debugger
            .find_in_sources("needle")
            .await
            .expect("searched")
            .expect("found");
        assert_eq!(source.actor, "s2");
    }

    /// DBG-41: A source announced twice is cached once.
    #[tokio::test]
    async fn a_source_announced_twice_is_cached_once() {
        let (server, debugger) = attached(vec![]).await;
        let source = json!([{ "actor": SOURCE, "url": PAGE }]);
        server.push(sources_announced(TARGET, source));
        server.push(sources_announced(
            TARGET,
            json!([{ "actor": "sentinel", "url": "u" }]),
        ));
        eventually("the sentinel", || {
            debugger
                .sources()
                .iter()
                .any(|source| source.actor == "sentinel")
        })
        .await;
        assert_eq!(debugger.sources().len(), 2);
    }

    /// DBG-36: `on_sources` only yields this target's source announcements.
    #[tokio::test]
    async fn on_sources_only_yields_this_target() {
        let (server, debugger) = attached(vec![]).await;
        let mut sources = debugger.on_sources();
        server.push(sources_announced(
            "otherTarget",
            json!([{ "actor": "foreign", "url": "y" }]),
        ));
        server.push(rdp_reply(
            TARGET,
            fixture("rdp/resources-thread-state-paused.json"),
        ));
        server.push(sources_announced(
            TARGET,
            json!([{ "actor": "s2", "url": "u" }]),
        ));
        let packet = timeout(WAIT, sources.next())
            .await
            .expect("a packet")
            .expect("open")
            .expect("no lag");
        assert_eq!(parse_sources(&packet)[0].actor, "s2");
    }
}
