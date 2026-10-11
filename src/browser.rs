//! Browser-level client and context tree management.
//!
//! In BiDi every connection is browser-level: there is no per-page connection
//! like the one `cdp-lite` opens. [`BrowserClient`] is therefore a thin layer
//! over a [`BidiClient`] that holds a session, specialised in the
//! `browsingContext` module.

use crate::client::{BidiClient, default_capabilities};
use crate::error::{BidiError, BidiResult};
use crate::protocol::{NoParams, WsResponse};
use crate::tab::Tab;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// The events that announce browsing contexts coming and going.
const CONTEXT_EVENTS: [&str; 2] = [
    "browsingContext.contextCreated",
    "browsingContext.contextDestroyed",
];

/// One node of `browsingContext.getTree` (`browsingContext.Info`).
///
/// Every field but `context` is optional on the wire and defaults here, so a
/// remote end that omits one never fails the whole tree.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextInfo {
    /// Browsing context id.
    pub context: String,
    /// Current URL.
    #[serde(default)]
    pub url: String,
    /// Parent context; `None` for top-level contexts (tabs and windows).
    #[serde(default)]
    pub parent: Option<String>,
    /// User context (container) the browsing context belongs to.
    #[serde(default)]
    pub user_context: Option<String>,
    /// Context that opened this one, when there is one.
    #[serde(default)]
    pub original_opener: Option<String>,
    /// Window that hosts the context.
    #[serde(default)]
    pub client_window: Option<String>,
    /// Child contexts (iframes); `None` when `maxDepth` cut the tree here.
    #[serde(default)]
    pub children: Option<Vec<ContextInfo>>,
}

impl ContextInfo {
    /// Whether this is a top-level context, i.e. a tab or a window.
    pub fn is_tab(&self) -> bool {
        self.parent.is_none()
    }
}

/// Which load state `navigate` and `reload` wait for before replying.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ReadinessState {
    /// Reply as soon as the navigation starts.
    None,
    /// Reply once the document is parsed (`DOMContentLoaded`).
    Interactive,
    /// Reply once the page has finished loading (`load`).
    #[default]
    Complete,
}

/// A BiDi session driven at browser level: tabs, contexts and the browser itself.
///
/// Cloning is cheap; every clone shares the connection, the session and the
/// context-discovery state.
#[derive(Clone)]
pub struct BrowserClient {
    client: BidiClient,
    /// Subscription id of [`BrowserClient::set_discover_contexts`], if on.
    ///
    /// A `tokio` mutex because the guard is held across the subscribe await.
    discovery: Arc<Mutex<Option<String>>>,
}

impl BrowserClient {
    /// Connects to the BiDi endpoint at `host` and starts a session with the
    /// default capabilities. See [`BidiClient::new`].
    ///
    /// Call [`BrowserClient::end`] when done: Firefox keeps the session after the
    /// connection drops and allows only one.
    pub async fn connect(host: &str, default_timeout: Duration) -> BidiResult<Self> {
        Ok(Self::from_client(
            BidiClient::new(host, default_timeout).await?,
        ))
    }

    /// Like [`BrowserClient::connect`], with an explicit event-channel capacity.
    /// See [`BidiClient::connect_with_capacity`].
    pub async fn connect_with_capacity(
        host: &str,
        default_timeout: Duration,
        capacity: usize,
    ) -> BidiResult<Self> {
        let capabilities = default_capabilities();
        let client =
            BidiClient::new_with_capacity(host, default_timeout, capacity, capabilities).await?;
        Ok(Self::from_client(client))
    }

    /// Wraps a client that already holds a session, e.g. one built with
    /// [`BidiClient::new_with_capabilities`].
    pub fn from_client(client: BidiClient) -> Self {
        Self {
            client,
            discovery: Arc::new(Mutex::new(None)),
        }
    }

    /// The underlying client, for any command this layer does not wrap.
    pub fn client(&self) -> &BidiClient {
        &self.client
    }

    /// The whole context tree: `browsingContext.getTree {}`.
    pub async fn list_contexts(&self) -> BidiResult<Vec<ContextInfo>> {
        self.get_tree(json!({})).await
    }

    /// Top-level contexts only: `browsingContext.getTree {"maxDepth": 0}`.
    pub async fn list_tabs(&self) -> BidiResult<Vec<ContextInfo>> {
        let contexts = self.get_tree(json!({ "maxDepth": 0 })).await?;
        // `maxDepth: 0` already limits the answer; filtering again keeps a
        // remote end that ignores it from handing out iframes as tabs.
        Ok(contexts.into_iter().filter(ContextInfo::is_tab).collect())
    }

    /// A [`Tab`] for an existing context.
    ///
    /// Asks the browser first (`getTree` rooted at `context`), so an unknown id
    /// fails here with `no such frame` instead of on the first command sent
    /// through the tab.
    pub async fn attach(&self, context: &str) -> BidiResult<Tab> {
        self.get_tree(json!({ "root": context, "maxDepth": 0 }))
            .await?;
        Ok(Tab::new(self.client.clone(), context.to_string()))
    }

    /// A [`Tab`] for every top-level context, in a single round trip.
    pub async fn attach_to_all_tabs(&self) -> BidiResult<Vec<Tab>> {
        let tabs = self.list_tabs().await?;
        Ok(tabs
            .into_iter()
            .map(|info| Tab::new(self.client.clone(), info.context))
            .collect())
    }

    /// Opens a tab: `browsingContext.create {"type": "tab"}` and, unless `url` is
    /// `about:blank`, a `navigate` that does not wait for the page to load.
    ///
    /// If the navigation fails the new tab is closed again, so a failed call
    /// leaves no orphan tab behind and the navigation error is what you get.
    /// Firefox refuses top-level navigation to `data:` URLs.
    pub async fn new_tab(&self, url: &str) -> BidiResult<Tab> {
        let created = self
            .client
            .send_raw_command("browsingContext.create", json!({ "type": "tab" }))
            .await?;
        let context = created
            .result_str("context")
            .ok_or_else(|| BidiError::missing_field("browsingContext.create", "context"))?
            .to_string();

        let tab = Tab::new(self.client.clone(), context);
        if url != "about:blank" {
            if let Err(error) = tab.navigate(url, ReadinessState::None).await {
                // The navigation error is the one that matters; failing to close
                // the tab we just opened would only hide it.
                let _ = tab.close().await;
                return Err(error);
            }
        }

        Ok(tab)
    }

    /// `browsingContext.close {context}`.
    pub async fn close_tab(&self, context: &str) -> BidiResult<()> {
        Tab::new(self.client.clone(), context.to_string())
            .close()
            .await
    }

    /// Turns on or off the `contextCreated` and `contextDestroyed` events.
    ///
    /// Idempotent. Read the events from `client().on_module("browsingContext")`.
    /// Turning it off forgets the subscription even if the browser refuses to
    /// drop it, so a stale id can never make every later call fail.
    pub async fn set_discover_contexts(&self, discover: bool) -> BidiResult<()> {
        let mut subscription = self.discovery.lock().await;
        if discover {
            if subscription.is_none() {
                let id = self.client.subscribe_events(&CONTEXT_EVENTS, None).await?;
                *subscription = Some(id);
            }
        } else if let Some(id) = subscription.take() {
            self.client.unsubscribe_events(&[id.as_str()]).await?;
        }
        Ok(())
    }

    /// `session.end`. The browser stays open.
    pub async fn end(&self) -> BidiResult<()> {
        self.client.end_session().await
    }

    /// `browser.close`. The browser may drop the socket before answering, which
    /// counts as success.
    pub async fn close_browser(&self) -> BidiResult<()> {
        match self
            .client
            .send_raw_command("browser.close", NoParams)
            .await
        {
            Ok(_) | Err(BidiError::Disconnected) => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// `browsingContext.getTree` with `params`, parsed.
    async fn get_tree(&self, params: Value) -> BidiResult<Vec<ContextInfo>> {
        let response = self
            .client
            .send_raw_command("browsingContext.getTree", params)
            .await?;
        parse_contexts(&response)
    }
}

/// Reads `result.contexts` out of a `browsingContext.getTree` reply.
fn parse_contexts(response: &WsResponse) -> BidiResult<Vec<ContextInfo>> {
    let contexts = response
        .result
        .as_ref()
        .and_then(|result| result.get("contexts"))
        .ok_or_else(|| BidiError::missing_field("browsingContext.getTree", "contexts"))?;
    Ok(serde_json::from_value(contexts.clone())?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        Handler, MockServer, answering, close_marker, error, firefox_like, fixture,
    };

    const TIMEOUT: Duration = Duration::from_secs(1);
    /// Long enough for a stray command to show up, short enough not to slow the suite.
    const QUIET: Duration = Duration::from_millis(100);

    /// A mock Firefox whose `getTree` answers `contexts`.
    fn with_tree(contexts: Value) -> Handler {
        let overrides = vec![("browsingContext.getTree", json!({ "contexts": contexts }))];
        firefox_like(overrides)
    }

    /// A browser whose setup commands the test no longer sees.
    async fn browser(server: &mut MockServer) -> BrowserClient {
        let browser = BrowserClient::connect(&server.host(), TIMEOUT)
            .await
            .expect("connect");
        server.next_command_named("session.new").await;
        browser
    }

    /// The error of a result whose success type has no `Debug`.
    fn failure<T>(result: BidiResult<T>) -> BidiError {
        match result {
            Err(error) => error,
            Ok(_) => panic!("expected an error"),
        }
    }

    fn context_ids(contexts: &[ContextInfo]) -> Vec<&str> {
        contexts.iter().map(|info| info.context.as_str()).collect()
    }

    fn tab_ids(tabs: &[Tab]) -> Vec<&str> {
        tabs.iter().map(Tab::context_id).collect()
    }

    /// BRW-01: `connect` yields a client holding a session.
    #[tokio::test]
    async fn connect_yields_client_with_session() {
        // Given a mock Firefox
        let server = MockServer::start(firefox_like(vec![])).await;

        // When the browser connects
        let browser = BrowserClient::connect(&server.host(), TIMEOUT)
            .await
            .expect("connect");

        // Then its client holds a session
        assert_eq!(browser.client().session_id().as_deref(), Some("S-1"));
    }

    /// BRW-22: `connect_with_capacity` also starts the session.
    #[tokio::test]
    async fn connect_with_capacity_starts_a_session() {
        // Given a mock Firefox
        let mut server = MockServer::start(firefox_like(vec![])).await;

        // When the browser connects with an explicit (degenerate) ring capacity
        let browser = BrowserClient::connect_with_capacity(&server.host(), TIMEOUT, 0)
            .await
            .expect("connect");

        // Then the session was created, with the default capabilities, over /session
        assert_eq!(browser.client().session_id().as_deref(), Some("S-1"));
        assert_eq!(server.handshake_path().await, "/session");
        let new = server.next_command_named("session.new").await;
        assert_eq!(
            new["params"],
            json!({ "capabilities": default_capabilities() })
        );
    }

    /// BRW-02: `list_contexts` returns the whole tree.
    #[tokio::test]
    async fn list_contexts_returns_whole_tree() {
        // Given a tab with one iframe
        let tree = json!([{
            "context": "T1",
            "url": "https://a/",
            "children": [{ "context": "F1", "parent": "T1", "url": "https://ads/", "children": [] }],
            "parent": null,
            "userContext": "default",
            "originalOpener": null,
            "clientWindow": "W1",
        }]);
        let mut server = MockServer::start(with_tree(tree)).await;
        let browser = browser(&mut server).await;

        // When the contexts are listed
        let contexts = browser.list_contexts().await.expect("a tree");

        // Then getTree was asked with empty params
        let command = server.next_command_named("browsingContext.getTree").await;
        assert_eq!(command["params"], json!({}));
        // And the tab comes back with its iframe as a child
        assert_eq!(context_ids(&contexts), ["T1"]);
        assert_eq!(contexts[0].user_context.as_deref(), Some("default"));
        assert_eq!(contexts[0].client_window.as_deref(), Some("W1"));
        let children = contexts[0].children.as_ref().expect("children");
        assert_eq!(context_ids(children), ["F1"]);
        assert_eq!(children[0].parent.as_deref(), Some("T1"));
    }

    /// BRW-03: `list_tabs` asks for top-level contexts only.
    #[tokio::test]
    async fn list_tabs_requests_top_level_only() {
        // Given two tabs
        let tree =
            json!([{ "context": "T1", "parent": null }, { "context": "T2", "parent": null }]);
        let mut server = MockServer::start(with_tree(tree)).await;
        let browser = browser(&mut server).await;

        // When the tabs are listed
        let tabs = browser.list_tabs().await.expect("tabs");

        // Then getTree was limited to depth 0
        let command = server.next_command_named("browsingContext.getTree").await;
        assert_eq!(command["params"], json!({ "maxDepth": 0 }));
        // And both ids are returned
        assert_eq!(context_ids(&tabs), ["T1", "T2"]);
    }

    /// BRW-04: `ContextInfo` tolerates missing optional fields.
    #[test]
    fn context_info_tolerates_missing_fields() {
        // Given a context that carries only its id
        let info: ContextInfo = serde_json::from_value(json!({ "context": "T1" })).expect("parses");

        // Then every optional field has its default
        assert_eq!(info.url, "");
        assert_eq!(info.parent, None);
        assert_eq!(info.user_context, None);
        assert_eq!(info.original_opener, None);
        assert_eq!(info.client_window, None);
        assert!(info.children.is_none());
        // And, having no parent, it is a tab
        assert!(info.is_tab());
    }

    /// BRW-05: A `getTree` reply without contexts is an internal error.
    #[test]
    fn get_tree_without_contexts_is_internal_error() {
        // Given a getTree reply whose result is empty
        let response = WsResponse {
            result: Some(json!({})),
            ..Default::default()
        };

        // When it is parsed
        let error = failure(parse_contexts(&response));

        // Then it is an InternalError naming the missing field
        assert!(
            matches!(&error, BidiError::InternalError(message) if message.contains("contexts")),
            "{error:?}"
        );
    }

    /// BRW-06: `attach` validates the context and returns a `Tab`.
    #[tokio::test]
    async fn attach_validates_and_returns_tab() {
        // Given a mock Firefox
        let mut server = MockServer::start(firefox_like(vec![])).await;
        let browser = browser(&mut server).await;

        // When attaching to T1
        let tab = browser.attach("T1").await.expect("a tab");

        // Then the browser was asked about exactly that context, without its children
        let command = server.next_command_named("browsingContext.getTree").await;
        assert_eq!(command["params"], json!({ "root": "T1", "maxDepth": 0 }));
        // And the tab is T1
        assert_eq!(tab.context_id(), "T1");
    }

    /// BRW-07: `attach` to an unknown context fails with `no such frame`.
    #[tokio::test]
    async fn attach_unknown_context_fails() {
        // Given a browser that does not know the context
        let handler = answering(firefox_like(vec![]), "browsingContext.getTree", |command| {
            vec![error(&command["id"], "no such frame", "no such context")]
        });
        let mut server = MockServer::start(handler).await;
        let browser = browser(&mut server).await;

        // When attaching to it
        let error = failure(browser.attach("nope").await);

        // Then the browser's error reaches the caller
        assert!(
            matches!(&error, BidiError::ProtocolError { error, .. } if error == "no such frame"),
            "{error:?}"
        );
    }

    /// BRW-08: `attach_to_all_tabs` returns one `Tab` per top-level context in one round trip.
    #[tokio::test]
    async fn attach_to_all_tabs_uses_one_round_trip() {
        // Given two tabs
        let tree =
            json!([{ "context": "T1", "parent": null }, { "context": "T2", "parent": null }]);
        let mut server = MockServer::start(with_tree(tree)).await;
        let browser = browser(&mut server).await;

        // When attaching to all of them
        let tabs = browser.attach_to_all_tabs().await.expect("tabs");

        // Then there is one tab per context
        assert_eq!(tab_ids(&tabs), ["T1", "T2"]);
        // And exactly one getTree went out
        server.next_command_named("browsingContext.getTree").await;
        server.assert_no_command(QUIET).await;
    }

    /// BRW-09: `new_tab` creates a tab and navigates it without waiting.
    #[tokio::test]
    async fn new_tab_creates_then_navigates_without_waiting() {
        // Given a mock Firefox
        let mut server = MockServer::start(firefox_like(vec![])).await;
        let browser = browser(&mut server).await;

        // When a tab is opened on a URL
        let tab = browser.new_tab("https://docs.rs").await.expect("a tab");

        // Then a tab was created
        let create = server.next_command().await;
        assert_eq!(create["method"], "browsingContext.create");
        assert_eq!(create["params"], json!({ "type": "tab" }));
        // And then navigated without waiting for the load
        let navigate = server.next_command().await;
        assert_eq!(navigate["method"], "browsingContext.navigate");
        assert_eq!(
            navigate["params"],
            json!({ "context": "CTX-NEW", "url": "https://docs.rs", "wait": "none" })
        );
        // And the tab is the created context
        assert_eq!(tab.context_id(), "CTX-NEW");
    }

    /// BRW-10: `new_tab` on `about:blank` skips navigation.
    #[tokio::test]
    async fn new_tab_on_about_blank_skips_navigation() {
        // Given a mock Firefox
        let mut server = MockServer::start(firefox_like(vec![])).await;
        let browser = browser(&mut server).await;

        // When a blank tab is opened
        browser.new_tab("about:blank").await.expect("a tab");

        // Then only the creation was sent
        server.next_command_named("browsingContext.create").await;
        server.assert_no_command(QUIET).await;
    }

    /// BRW-11: `new_tab` closes the orphan tab if navigation fails.
    #[tokio::test]
    async fn new_tab_closes_orphan_on_navigation_failure() {
        // Given a browser that refuses to navigate
        let handler = answering(
            firefox_like(vec![]),
            "browsingContext.navigate",
            |command| vec![error(&command["id"], "invalid argument", "bad url")],
        );
        let mut server = MockServer::start(handler).await;
        let browser = browser(&mut server).await;

        // When a tab is opened on a bad URL
        let error = failure(browser.new_tab("::bad::").await);

        // Then the navigation error is what the caller gets
        assert!(
            matches!(&error, BidiError::ProtocolError { error, .. } if error == "invalid argument"),
            "{error:?}"
        );
        // And the tab that had been created was closed
        let close = server.next_command_named("browsingContext.close").await;
        assert_eq!(close["params"], json!({ "context": "CTX-NEW" }));
    }

    /// BRW-12: `new_tab` reports a create reply without context.
    #[tokio::test]
    async fn new_tab_reports_missing_context() {
        // Given a browser whose create reply has no context
        let overrides = vec![("browsingContext.create", json!({}))];
        let mut server = MockServer::start(firefox_like(overrides)).await;
        let browser = browser(&mut server).await;

        // When a tab is opened
        let error = failure(browser.new_tab("https://docs.rs").await);

        // Then it is an InternalError naming the command and the field
        assert!(
            matches!(&error, BidiError::InternalError(message)
                if message.contains("browsingContext.create") && message.contains("context")),
            "{error:?}"
        );
    }

    /// BRW-13: `close_tab` closes by context id.
    #[tokio::test]
    async fn close_tab_closes_by_context_id() {
        // Given a mock Firefox
        let mut server = MockServer::start(firefox_like(vec![])).await;
        let browser = browser(&mut server).await;

        // When T1 is closed
        browser.close_tab("T1").await.expect("closed");

        // Then browsingContext.close names it
        let close = server.next_command_named("browsingContext.close").await;
        assert_eq!(close["params"], json!({ "context": "T1" }));
    }

    /// BRW-14: `set_discover_contexts(true)` subscribes once.
    #[tokio::test]
    async fn discover_contexts_subscribes_once() {
        // Given a mock Firefox
        let mut server = MockServer::start(firefox_like(vec![])).await;
        let browser = browser(&mut server).await;

        // When discovery is turned on twice
        browser.set_discover_contexts(true).await.expect("on");
        browser.set_discover_contexts(true).await.expect("on again");

        // Then exactly one subscription for the two context events was sent
        let subscribe = server.next_command_named("session.subscribe").await;
        assert_eq!(
            subscribe["params"],
            json!({ "events": ["browsingContext.contextCreated", "browsingContext.contextDestroyed"] })
        );
        server.assert_no_command(QUIET).await;
    }

    /// BRW-15: `set_discover_contexts(false)` unsubscribes the stored id.
    #[tokio::test]
    async fn discover_contexts_off_unsubscribes_stored_id() {
        // Given discovery turned on, which got subscription SUB-1
        let mut server = MockServer::start(firefox_like(vec![])).await;
        let browser = browser(&mut server).await;
        browser.set_discover_contexts(true).await.expect("on");
        server.next_command_named("session.subscribe").await;

        // When it is turned off
        browser.set_discover_contexts(false).await.expect("off");

        // Then that subscription was dropped
        let unsubscribe = server.next_command_named("session.unsubscribe").await;
        assert_eq!(unsubscribe["params"], json!({ "subscriptions": ["SUB-1"] }));
        // And turning it off again sends nothing
        browser
            .set_discover_contexts(false)
            .await
            .expect("off again");
        server.assert_no_command(QUIET).await;
    }

    /// BRW-16: `end` ends the session.
    #[tokio::test]
    async fn end_ends_the_session() {
        // Given a browser with a session
        let mut server = MockServer::start(firefox_like(vec![])).await;
        let browser = browser(&mut server).await;

        // When it ends
        browser.end().await.expect("ended");

        // Then session.end was sent and the client holds no session
        server.next_command_named("session.end").await;
        assert_eq!(browser.client().session_id(), None);
    }

    /// BRW-17: `close_browser` treats a dropped socket as success.
    #[tokio::test]
    async fn close_browser_tolerates_dropped_socket() {
        // Given a browser that drops the connection instead of answering browser.close
        let handler = answering(firefox_like(vec![]), "browser.close", |_| {
            vec![close_marker()]
        });
        let mut server = MockServer::start(handler).await;
        let browser = browser(&mut server).await;

        // When the browser is closed
        let result = browser.close_browser().await;

        // Then it still succeeds
        assert!(result.is_ok(), "{result:?}");
    }

    /// BRW-18: `ReadinessState` serializes in lowercase.
    #[test]
    fn readiness_state_serializes_lowercase() {
        // Then each state is the lowercase word BiDi expects
        assert_eq!(
            serde_json::to_value(ReadinessState::None).expect("serializes"),
            "none"
        );
        assert_eq!(
            serde_json::to_value(ReadinessState::Interactive).expect("serializes"),
            "interactive"
        );
        assert_eq!(
            serde_json::to_value(ReadinessState::Complete).expect("serializes"),
            "complete"
        );
        // And waiting for the load is the default
        assert_eq!(ReadinessState::default(), ReadinessState::Complete);
    }

    /// BRW-19: A real Firefox `getTree` reply parses.
    #[test]
    fn firefox_get_tree_reply_parses() {
        // Given the reply captured from Firefox 157, where unset fields are null
        let response: WsResponse =
            serde_json::from_value(fixture("bidi/get-tree.json")).expect("a reply");

        // When it is parsed
        let contexts = parse_contexts(&response).expect("contexts");

        // Then the tab is described as Firefox sent it
        assert_eq!(
            context_ids(&contexts),
            ["595962ab-c645-46d8-b701-bfe0da608911"]
        );
        assert_eq!(contexts[0].url, "https://example.com/test.html");
        assert_eq!(contexts[0].user_context.as_deref(), Some("default"));
        assert_eq!(contexts[0].parent, None);
        assert!(
            contexts[0].children.is_none(),
            "a null children list is None"
        );
        assert!(contexts[0].is_tab());
    }

    /// BRW-20: `list_tabs` drops nested contexts a browser returns anyway.
    #[tokio::test]
    async fn list_tabs_drops_nested_contexts() {
        // Given a browser that ignores maxDepth and returns an iframe too
        let tree = json!([
            { "context": "T1", "parent": null },
            { "context": "F1", "parent": "T1" },
            { "context": "T2", "parent": null },
        ]);
        let mut server = MockServer::start(with_tree(tree)).await;
        let browser = browser(&mut server).await;

        // When the tabs are listed
        let tabs = browser.list_tabs().await.expect("tabs");

        // Then only the top-level contexts remain
        assert_eq!(context_ids(&tabs), ["T1", "T2"]);
    }

    /// BRW-21: `set_discover_contexts(false)` forgets the id even if the browser refuses.
    #[tokio::test]
    async fn discover_contexts_off_forgets_the_id_even_if_unsubscribe_fails() {
        // Given discovery on, and a browser that refuses to drop the subscription
        let handler = answering(firefox_like(vec![]), "session.unsubscribe", |command| {
            vec![error(
                &command["id"],
                "invalid argument",
                "no such subscription",
            )]
        });
        let mut server = MockServer::start(handler).await;
        let browser = browser(&mut server).await;
        browser.set_discover_contexts(true).await.expect("on");
        server.next_command_named("session.subscribe").await;

        // When it is turned off
        let result = browser.set_discover_contexts(false).await;

        // Then the refusal reaches the caller
        assert!(
            matches!(result, Err(BidiError::ProtocolError { .. })),
            "{result:?}"
        );
        server.next_command_named("session.unsubscribe").await;
        // And the stale id is gone, so turning it on again subscribes afresh
        browser.set_discover_contexts(true).await.expect("on again");
        server.next_command_named("session.subscribe").await;
    }
}
