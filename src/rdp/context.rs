//! Which RDP tab descriptor shows a WebDriver BiDi browsing context.
//!
//! BiDi names a tab by a UUID and RDP by an actor, and nothing on either wire
//! links the two: `listTabs` gives integers (`browserId`, `browsingContextID`,
//! `outerWindowID`) that BiDi never shows. The BiDi UUID is the navigable id
//! Firefox's Remote Agent keeps in its `NavigableManager`, which lives in the
//! parent process. RDP can evaluate code there (the DevTools server already
//! needs `devtools.chrome.enabled`), so the lookup asks the `NavigableManager`
//! for the context's `browserId` and picks the descriptor that has it.
//!
//! No tab is activated and no page is touched. Validated on Firefox 157.

use crate::error::{BidiError, BidiResult, codes};
use crate::protocol::NoParams;
use crate::rdp::client::{RdpClient, TabDescriptor};
use crate::rdp::protocol::actor_in;
use crate::rdp::resources::Evaluation;
use serde_json::{Value, json};
use std::sync::PoisonError;

/// Evaluated in the parent process: the `browserId` of a top-level context, or
/// `null`. `CONTEXT` is replaced by the id as a JSON string literal.
const LOOKUP: &str = r#"(() => {
  const { NavigableManager } = ChromeUtils.importESModule(
    "chrome://remote/content/shared/NavigableManager.sys.mjs");
  const context = NavigableManager.getBrowsingContextById(CONTEXT);
  return context && !context.parent ? context.browserId : null;
})()"#;

impl RdpClient {
    /// The `browserId` of the tab showing the BiDi top-level context `context`,
    /// read from the Remote Agent in the parent process.
    ///
    /// `None` when no tab shows it: an unknown or closed context, or a context
    /// that is not top-level (an iframe). Fails if the lookup itself throws, e.g.
    /// on a Firefox whose Remote Agent has no `NavigableManager`.
    pub async fn browser_id_of_context(&self, context: &str) -> BidiResult<Option<u64>> {
        let text = LOOKUP.replace("CONTEXT", &serde_json::to_string(context)?);
        let params = json!({ "text": text });
        let console = self.parent_console().await?;
        let evaluation = match self.evaluate_on(&console, params.clone()).await {
            // The console went away with its target: find the current one, once.
            Err(BidiError::ProtocolError { error, .. }) if error == "noSuchActor" => {
                self.forget_parent_console();
                let console = self.parent_console().await?;
                self.evaluate_on(&console, params).await?
            }
            other => other?,
        };
        browser_id_from(&evaluation)
    }

    /// The tab descriptor showing the BiDi top-level context `context`, matched
    /// by `browserId`; `None` when there is none, so a guess is never returned.
    ///
    /// Two round trips (the lookup, then `listTabs`), three more the first time
    /// to find the parent process's console.
    pub async fn tab_for_context(&self, context: &str) -> BidiResult<Option<TabDescriptor>> {
        let Some(browser_id) = self.browser_id_of_context(context).await? else {
            return Ok(None);
        };
        let tabs = self.list_tabs().await?;
        Ok(tabs
            .into_iter()
            .find(|tab| tab.browser_id == Some(browser_id)))
    }

    /// The parent process's console actor: `getProcess {id: 0}`, then its
    /// target. Cached, because every `getTarget` builds a new target with a
    /// score of actors that live as long as the connection.
    async fn parent_console(&self) -> BidiResult<String> {
        if let Some(console) = self.cached_parent_console() {
            return Ok(console);
        }
        let process = self
            .request("root", "getProcess", json!({ "id": 0 }))
            .await?;
        let descriptor = actor_in(process, &["processDescriptor", "actor"], "getProcess")?;
        let target = self.request(&descriptor, "getTarget", NoParams).await?;
        let console = actor_in(target, &["process", "consoleActor"], "getTarget")?;
        *self
            .parent_console
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(console.clone());
        Ok(console)
    }

    fn cached_parent_console(&self) -> Option<String> {
        self.parent_console
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn forget_parent_console(&self) {
        *self
            .parent_console
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = None;
    }
}

/// The browser id a lookup evaluated to; an exception is an error, not a `None`.
fn browser_id_from(evaluation: &Evaluation) -> BidiResult<Option<u64>> {
    if evaluation.exception.is_some() {
        let message = evaluation.raw["exceptionMessage"]
            .as_str()
            .unwrap_or("no message");
        return Err(BidiError::refused(
            codes::UNSUPPORTED_OPERATION,
            format!("the browsing context lookup threw: {message}"),
        ));
    }
    match &evaluation.result {
        Value::Number(number) => number
            .as_u64()
            .map(Some)
            .ok_or_else(|| BidiError::missing_field("evaluationResult", "result")),
        // Primitives without a JSON form come as grips: `{type: "null"}`.
        grip if grip["type"] == "null" || grip["type"] == "undefined" => Ok(None),
        _ => Err(BidiError::missing_field("evaluationResult", "result")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        Handler, MockServer, PARENT_CONSOLE, PARENT_PROCESS, fixture, rdp_browser_id,
        rdp_context_lookup, rdp_firefox_like, rdp_lookup_answers, rdp_reply,
    };
    use serde_json::{Value, json};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    const TIMEOUT: Duration = Duration::from_secs(1);
    const QUIET: Duration = Duration::from_millis(100);

    /// The third tab of `rdp/listTabs-two-tabs.json`, browserId 12.
    const CONTEXT: &str = "dcd31f9a-26fa-449e-bad3-efcdcbb7980d";

    async fn connected(handler: Handler) -> (MockServer, RdpClient) {
        let server = MockServer::start_rdp(handler).await;
        let client = RdpClient::connect(&server.host(), TIMEOUT)
            .await
            .expect("connect");
        (server, client)
    }

    fn command_of(command: &Value) -> (Option<&str>, Option<&str>) {
        (command["to"].as_str(), command["type"].as_str())
    }

    /// RDP-30: `browser_id_of_context` asks the parent process.
    #[tokio::test]
    async fn browser_id_of_context_asks_the_parent_process() {
        // Given a Firefox whose lookup answers browserId 12
        let lookup = rdp_lookup_answers(fixture("rdp/browser-id-lookup.json"));
        let (mut server, client) = connected(rdp_firefox_like(rdp_context_lookup(lookup))).await;

        // When the browser id of the context is asked for
        let id = client
            .browser_id_of_context(CONTEXT)
            .await
            .expect("looked up");

        // Then the parent process was found, then its console evaluated the lookup
        let process = server.next_command().await;
        assert_eq!(command_of(&process), (Some("root"), Some("getProcess")));
        assert_eq!(process["id"], 0);
        let target = server.next_command().await;
        assert_eq!(
            command_of(&target),
            (Some(PARENT_PROCESS), Some("getTarget"))
        );
        let evaluate = server.next_command().await;
        assert_eq!(
            command_of(&evaluate),
            (Some(PARENT_CONSOLE), Some("evaluateJSAsync"))
        );
        let text = evaluate["text"].as_str().expect("text");
        assert!(text.contains(&format!("\"{CONTEXT}\"")), "{text}");
        assert!(text.contains("NavigableManager"), "{text}");
        // And the answer is the browser id
        assert_eq!(id, Some(12));
    }

    /// RDP-31: An unknown context has no browser id.
    #[tokio::test]
    async fn unknown_context_has_no_browser_id() {
        let lookup = rdp_lookup_answers(fixture("rdp/browser-id-lookup-null.json"));
        let (_server, client) = connected(rdp_firefox_like(rdp_context_lookup(lookup))).await;
        let id = client
            .browser_id_of_context("00000000-0000-0000-0000-000000000000")
            .await
            .expect("looked up");
        assert_eq!(id, None);
    }

    /// The `to type` of the next `count` commands.
    async fn next_commands(server: &mut MockServer, count: usize) -> Vec<String> {
        let mut seen = Vec::with_capacity(count);
        for _ in 0..count {
            let command = server.next_command().await;
            let (to, kind) = command_of(&command);
            seen.push(format!(
                "{} {}",
                to.unwrap_or_default(),
                kind.unwrap_or_default()
            ));
        }
        seen
    }

    /// The parent console's answer to its `nth` (0-based) lookup: alive for two.
    fn dying_console(nth: usize) -> Vec<Value> {
        if nth < 2 {
            return rdp_lookup_answers(fixture("rdp/browser-id-lookup.json"));
        }
        let gone = json!({ "error": "noSuchActor", "message": "gone" });
        vec![rdp_reply(PARENT_CONSOLE, gone)]
    }

    /// A Firefox whose console dies after two lookups, and whose second
    /// `getTarget` hands out `console2`, which answers browserId 11.
    fn console_that_dies() -> Handler {
        let evaluations = Arc::new(AtomicUsize::new(0));
        let targets = Arc::new(AtomicUsize::new(0));
        let inner = rdp_firefox_like(rdp_context_lookup(Vec::new()));
        Box::new(move |packet| {
            let to = packet["to"].as_str().unwrap_or_default();
            match (to, packet["type"].as_str().unwrap_or_default()) {
                (PARENT_CONSOLE, "evaluateJSAsync") => {
                    dying_console(evaluations.fetch_add(1, Ordering::SeqCst))
                }
                ("console2", "evaluateJSAsync") => {
                    let mut answers = rdp_lookup_answers(rdp_browser_id(11));
                    for answer in &mut answers {
                        answer["from"] = json!("console2");
                    }
                    answers
                }
                (PARENT_PROCESS, "getTarget") if targets.fetch_add(1, Ordering::SeqCst) > 0 => {
                    let target = json!({ "process": { "consoleActor": "console2" } });
                    vec![rdp_reply(PARENT_PROCESS, target)]
                }
                _ => inner(packet),
            }
        })
    }

    /// RDP-32: The parent-process console is reused, and fetched again when it is gone.
    #[tokio::test]
    async fn parent_console_is_reused_and_refetched_when_gone() {
        // Given a first lookup, which found the parent process's console
        let (mut server, client) = connected(console_that_dies()).await;
        client.browser_id_of_context(CONTEXT).await.expect("first");
        next_commands(&mut server, 3).await;

        // When a second lookup is made
        let second = client.browser_id_of_context(CONTEXT).await.expect("second");
        // Then only the evaluation is sent
        let console = format!("{PARENT_CONSOLE} evaluateJSAsync");
        assert_eq!(
            next_commands(&mut server, 1).await,
            std::slice::from_ref(&console)
        );
        server.assert_no_command(QUIET).await;
        assert_eq!(second, Some(12));

        // When the console is gone on the third
        let third = client.browser_id_of_context(CONTEXT).await.expect("third");
        // Then the parent process is asked again and the lookup retried on the new console
        let expected = [
            console,
            "root getProcess".to_string(),
            format!("{PARENT_PROCESS} getTarget"),
            "console2 evaluateJSAsync".to_string(),
        ];
        assert_eq!(next_commands(&mut server, 4).await, expected);
        assert_eq!(third, Some(11));
    }

    /// RDP-33: A lookup that throws is an error, not a guess.
    #[tokio::test]
    async fn throwing_lookup_is_an_error() {
        // Given a lookup that throws, as when the module has another name
        let mut thrown = fixture("rdp/browser-id-lookup.json");
        thrown["result"] = json!({ "type": "undefined" });
        thrown["hasException"] = json!(true);
        thrown["exception"] = json!({ "type": "object", "class": "TypeError" });
        thrown["exceptionMessage"] = json!("TypeError: NavigableManager is undefined");
        let (_server, client) = connected(rdp_firefox_like(rdp_context_lookup(
            rdp_lookup_answers(thrown),
        )))
        .await;

        // Then the call fails naming the exception
        let error = client.browser_id_of_context(CONTEXT).await;
        assert!(
            matches!(&error, Err(BidiError::ProtocolError { error, message, .. })
                if error == "unsupported operation" && message.contains("NavigableManager is undefined")),
            "{error:?}"
        );
    }

    /// RDP-34: Two tabs with the same URL map to their own descriptors.
    #[tokio::test]
    async fn same_url_tabs_map_to_their_own_descriptors() {
        for (id, actor) in [
            (12, "server1.conn0.tabDescriptor3"),
            (11, "server1.conn0.tabDescriptor2"),
        ] {
            // Given two tabs on the same URL, the lookup naming one of them
            let lookup = rdp_lookup_answers(rdp_browser_id(id));
            let (_server, client) = connected(rdp_firefox_like(rdp_context_lookup(lookup))).await;

            // When the descriptor of the context is asked for
            let tab = client.tab_for_context(CONTEXT).await.expect("looked up");

            // Then the browser id decides, not the URL nor the position
            let tab = tab.expect("a descriptor");
            assert_eq!(tab.actor, actor);
            assert_eq!(tab.browser_id, Some(id));
        }
    }

    /// RDP-35: A browser id no descriptor has gives `None`.
    #[tokio::test]
    async fn unknown_browser_id_gives_no_descriptor() {
        let lookup = rdp_lookup_answers(rdp_browser_id(99));
        let (_server, client) = connected(rdp_firefox_like(rdp_context_lookup(lookup))).await;
        let tab = client.tab_for_context(CONTEXT).await.expect("looked up");
        assert!(tab.is_none(), "{tab:?}");
    }
}
