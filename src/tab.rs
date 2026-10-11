//! Handle for driving a single top-level browsing context.

use crate::client::BidiClient;
use crate::error::BidiResult;
use crate::event_filter::EventFilter;
use crate::protocol::WsResponse;
use serde::Serialize;
use serde_json::{Map, Value, json};
use std::fmt;

pub use crate::browser::ReadinessState;

/// A handle to one top-level browsing context over the shared BiDi session.
///
/// # Where the context goes
///
/// Unlike its counterpart in `cdp-lite`, nothing is injected into your
/// commands. BiDi puts the context in a different place per command
/// (`context`, `target.context`, `contexts`, `partition.context`, ...) and there
/// is no generic rule, so [`Tab::send_raw_command`] sends exactly what you give
/// it. The helpers ([`navigate`](Tab::navigate), [`reload`](Tab::reload),
/// [`activate`](Tab::activate), [`close`](Tab::close),
/// [`subscribe_events`](Tab::subscribe_events) and
/// [`script_target`](Tab::script_target)) put the context where each command
/// expects it. Anything else (screenshots, `print`, cookies, input) is a
/// `send_raw_command` away.
///
/// # Events are opt-in
///
/// [`Tab::on_module`] and [`Tab::events`] only filter. The browser emits nothing
/// until [`Tab::subscribe_events`] asks for it.
///
/// Dropping a `Tab` neither closes the browsing context nor removes its
/// subscriptions.
///
/// ```no_run
/// use bidi_lite::browser::BrowserClient;
/// use bidi_lite::tab::ReadinessState;
/// use serde_json::json;
/// use std::time::Duration;
/// use tokio_stream::StreamExt;
///
/// # async fn demo() -> bidi_lite::error::BidiResult<()> {
/// let browser = BrowserClient::connect("127.0.0.1:9222", Duration::from_secs(10)).await?;
/// let tab = browser.new_tab("about:blank").await?;
///
/// // Subscribe first, then read the events through the filter.
/// tab.subscribe_events(&["browsingContext.load"]).await?;
/// let mut loads = tab.on_module("browsingContext");
///
/// tab.navigate("https://example.com", ReadinessState::Complete).await?;
/// let _load = loads.next().await;
///
/// // Everything else goes through `send_raw_command`, which places nothing for you.
/// let title = tab
///     .send_raw_command(
///         "script.evaluate",
///         json!({
///             "expression": "document.title",
///             "target": tab.script_target(),
///             "awaitPromise": false,
///         }),
///     )
///     .await?;
/// # let _ = title;
///
/// tab.close().await?;
/// browser.end().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Tab {
    client: BidiClient,
    context_id: String,
}

impl Tab {
    /// Wraps a browsing context that the browser has confirmed exists.
    pub(crate) fn new(client: BidiClient, context_id: String) -> Self {
        Self { client, context_id }
    }

    /// The browsing context id, the identifier BiDi commands use for this tab.
    pub fn context_id(&self) -> &str {
        &self.context_id
    }

    /// The connection this tab shares with every other tab of the session.
    pub fn client(&self) -> &BidiClient {
        &self.client
    }

    /// Sends any command, verbatim: `params` goes out exactly as given.
    ///
    /// Put the context where the command expects it, e.g.
    /// `{"target": {"context": tab.context_id()}}` for `script.evaluate`.
    pub async fn send_raw_command<P: Serialize>(
        &self,
        method: &str,
        params: P,
    ) -> BidiResult<WsResponse> {
        self.client.send_raw_command(method, params).await
    }

    /// The events of one module emitted by this tab, e.g. `"browsingContext"`.
    ///
    /// The context is matched exactly: events from an iframe carry the iframe's
    /// own id and do not reach this filter. Only filters; see
    /// [`Tab::subscribe_events`].
    pub fn on_module(&self, module: &'static str) -> EventFilter {
        self.client.on_module_for_context(module, &self.context_id)
    }

    /// Every event emitted by this tab, whatever its module.
    pub fn events(&self) -> EventFilter {
        // An empty module name matches every module.
        self.on_module("")
    }

    /// `session.subscribe` scoped to this tab; returns the subscription id.
    pub async fn subscribe_events(&self, events: &[&str]) -> BidiResult<String> {
        let contexts = [self.context_id.as_str()];
        self.client.subscribe_events(events, Some(&contexts)).await
    }

    /// `session.unsubscribe` for the given subscription ids.
    pub async fn unsubscribe_events(&self, subscription_ids: &[&str]) -> BidiResult<()> {
        self.client.unsubscribe_events(subscription_ids).await
    }

    /// `browsingContext.navigate`; returns the raw reply (`result.navigation`,
    /// `result.url`).
    ///
    /// Firefox refuses top-level navigation to `data:` URLs.
    pub async fn navigate(&self, url: &str, wait: ReadinessState) -> BidiResult<WsResponse> {
        let fields = [("url", json!(url)), ("wait", json!(wait))];
        self.context_command("browsingContext.navigate", fields)
            .await
    }

    /// `browsingContext.reload`.
    pub async fn reload(&self, wait: ReadinessState) -> BidiResult<WsResponse> {
        self.context_command("browsingContext.reload", [("wait", json!(wait))])
            .await
    }

    /// `browsingContext.activate`: brings the tab to the foreground.
    pub async fn activate(&self) -> BidiResult<()> {
        self.context_command("browsingContext.activate", []).await?;
        Ok(())
    }

    /// `browsingContext.close`. Consumes the handle: there is nothing left to drive.
    pub async fn close(self) -> BidiResult<()> {
        self.context_command("browsingContext.close", []).await?;
        Ok(())
    }

    /// `{"context": id}`, ready to use as the `target` of `script.evaluate` and
    /// `script.callFunction`.
    pub fn script_target(&self) -> Value {
        json!({ "context": self.context_id })
    }

    /// Sends `method` with this tab's context plus `fields`.
    ///
    /// The one place that puts `context` at the top level of the params, so the
    /// helpers above cannot disagree about it.
    async fn context_command(
        &self,
        method: &str,
        fields: impl IntoIterator<Item = (&'static str, Value)>,
    ) -> BidiResult<WsResponse> {
        let mut params = Map::new();
        params.insert("context".to_string(), json!(self.context_id));
        params.extend(
            fields
                .into_iter()
                .map(|(key, value)| (key.to_string(), value)),
        );
        self.client
            .send_raw_command(method, Value::Object(params))
            .await
    }
}

impl fmt::Debug for Tab {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Tab")
            .field("context_id", &self.context_id)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::browser::BrowserClient;
    use crate::client::test_client;
    use crate::test_support::{MockServer, event, firefox_like};
    use std::time::Duration;
    use tokio::time::timeout;
    use tokio_stream::StreamExt;

    const TIMEOUT: Duration = Duration::from_secs(1);
    const WAIT: Duration = Duration::from_secs(2);

    /// A tab attached to `T1`, with the commands that set it up already consumed.
    async fn tab(server: &mut MockServer) -> Tab {
        let browser = BrowserClient::connect(&server.host(), TIMEOUT)
            .await
            .expect("connect");
        let tab = browser.attach("T1").await.expect("attach");
        server.next_command_named("browsingContext.getTree").await;
        tab
    }

    /// A tab that talks to nobody: for the tests that never send anything.
    fn offline_tab() -> Tab {
        let (client, _commands) = test_client(Duration::from_millis(50));
        Tab::new(client, "T1".to_string())
    }

    async fn first_event(filter: &mut EventFilter) -> WsResponse {
        timeout(WAIT, filter.next())
            .await
            .expect("no event within 2 s")
            .expect("the stream is open")
            .expect("not a lag")
    }

    /// TAB-01: `send_raw_command` is verbatim.
    #[tokio::test]
    async fn send_raw_command_is_verbatim() {
        // Given a tab
        let mut server = MockServer::start(firefox_like(vec![])).await;
        let tab = tab(&mut server).await;

        // When it sends a command that carries its own target
        let params = json!({
            "expression": "1+1",
            "target": { "context": "T1" },
            "awaitPromise": false,
        });
        tab.send_raw_command("script.evaluate", params.clone())
            .await
            .expect("a reply");

        // Then the server saw exactly those params
        let command = server.next_command_named("script.evaluate").await;
        assert_eq!(command["params"], params);
        // And no context key was added at the top level
        assert!(command["params"].get("context").is_none());
    }

    /// TAB-02: `navigate` places the context and the wait state.
    #[tokio::test]
    async fn navigate_places_context_and_wait() {
        // Given a tab
        let mut server = MockServer::start(firefox_like(vec![])).await;
        let tab = tab(&mut server).await;

        // When it navigates
        let response = tab
            .navigate("https://crates.io", ReadinessState::Complete)
            .await
            .expect("a reply");

        // Then the server saw the context, the url and the wait state
        let command = server.next_command_named("browsingContext.navigate").await;
        assert_eq!(
            command["params"],
            json!({ "context": "T1", "url": "https://crates.io", "wait": "complete" })
        );
        // And the raw reply is returned
        let result = response.result.as_ref().expect("a result");
        assert_eq!(result["url"], "https://crates.io");
    }

    /// TAB-03: `reload` places the context.
    #[tokio::test]
    async fn reload_places_context() {
        // Given a tab
        let mut server = MockServer::start(firefox_like(vec![])).await;
        let tab = tab(&mut server).await;

        // When it reloads
        tab.reload(ReadinessState::Interactive)
            .await
            .expect("a reply");

        // Then the server saw the context and the wait state
        let command = server.next_command_named("browsingContext.reload").await;
        assert_eq!(
            command["params"],
            json!({ "context": "T1", "wait": "interactive" })
        );
    }

    /// TAB-04: `activate` places the context.
    #[tokio::test]
    async fn activate_places_context() {
        // Given a tab
        let mut server = MockServer::start(firefox_like(vec![])).await;
        let tab = tab(&mut server).await;

        // When it is activated
        tab.activate().await.expect("activated");

        // Then the server saw only the context
        let command = server.next_command_named("browsingContext.activate").await;
        assert_eq!(command["params"], json!({ "context": "T1" }));
    }

    /// TAB-05: `close` places the context.
    #[tokio::test]
    async fn close_places_context() {
        // Given a tab
        let mut server = MockServer::start(firefox_like(vec![])).await;
        let tab = tab(&mut server).await;

        // When it is closed
        tab.close().await.expect("closed");

        // Then the server saw only the context
        let command = server.next_command_named("browsingContext.close").await;
        assert_eq!(command["params"], json!({ "context": "T1" }));
    }

    /// TAB-06: `subscribe_events` is scoped to the tab.
    #[tokio::test]
    async fn subscribe_events_is_scoped_to_tab() {
        // Given a tab
        let mut server = MockServer::start(firefox_like(vec![])).await;
        let tab = tab(&mut server).await;

        // When it subscribes to an event
        let subscription = tab.subscribe_events(&["log.entryAdded"]).await;

        // Then the subscription is limited to this context
        let command = server.next_command_named("session.subscribe").await;
        assert_eq!(
            command["params"],
            json!({ "events": ["log.entryAdded"], "contexts": ["T1"] })
        );
        // And the id comes back
        assert_eq!(subscription.expect("a subscription"), "SUB-1");
    }

    /// TAB-12: `unsubscribe_events` sends the ids.
    #[tokio::test]
    async fn unsubscribe_events_sends_ids() {
        // Given a tab
        let mut server = MockServer::start(firefox_like(vec![])).await;
        let tab = tab(&mut server).await;

        // When it unsubscribes a subscription
        tab.unsubscribe_events(&["SUB-1"])
            .await
            .expect("unsubscribed");

        // Then the server saw the id, which is global rather than per context
        let command = server.next_command_named("session.unsubscribe").await;
        assert_eq!(command["params"], json!({ "subscriptions": ["SUB-1"] }));
    }

    /// TAB-07: `on_module` only yields this tab's events.
    #[tokio::test]
    async fn on_module_yields_only_this_tab() {
        // Given a filter on browsingContext
        let mut server = MockServer::start(firefox_like(vec![])).await;
        let tab = tab(&mut server).await;
        let mut loads = tab.on_module("browsingContext");

        // When the server pushes a load for T2 and then one for T1
        server.push(event("browsingContext.load", json!({ "context": "T2" })));
        server.push(event("browsingContext.load", json!({ "context": "T1" })));

        // Then the first event yielded is the one for T1
        let first = first_event(&mut loads).await;
        assert_eq!(first.event_context(), Some("T1"));
    }

    /// TAB-08: `events` yields every module of this tab only.
    #[tokio::test]
    async fn events_yields_every_module_of_this_tab() {
        // Given a filter on every event of the tab
        let mut server = MockServer::start(firefox_like(vec![])).await;
        let tab = tab(&mut server).await;
        let mut everything = tab.events();

        // When the server pushes a network event for T2 and a log entry for T1
        let other = event("network.beforeRequestSent", json!({ "context": "T2" }));
        let ours = event(
            "log.entryAdded",
            json!({ "source": { "realm": "R", "context": "T1" } }),
        );
        server.push(other);
        server.push(ours);

        // Then the first event yielded is the log entry
        let first = first_event(&mut everything).await;
        assert_eq!(first.method.as_deref(), Some("log.entryAdded"));
    }

    /// TAB-09: `script_target` is `{"context": id}`.
    #[test]
    fn script_target_is_context_object() {
        // Given a tab
        let tab = offline_tab();

        // Then its script target names the context
        assert_eq!(tab.script_target(), json!({ "context": "T1" }));
    }

    /// TAB-10: Clones drive the same tab.
    #[test]
    fn clones_drive_the_same_tab() {
        // Given a tab and a clone of it
        let tab = offline_tab();
        let clone = tab.clone();

        // Then the clone points at the same context
        assert_eq!(clone.context_id(), "T1");
    }

    /// TAB-11: `Debug` shows the context id.
    #[test]
    fn debug_shows_context_id() {
        // Given a tab
        let tab = offline_tab();

        // Then its Debug output names the context
        assert!(format!("{tab:?}").contains("T1"));
    }
}
