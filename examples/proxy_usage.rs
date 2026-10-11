//! Browsing through an authenticated proxy.
//!
//! The proxy is set per user context (`browser.createUserContext {proxy}`), so only
//! the tabs opened in that context use it. Credentials are answered from the
//! `network.authRequired` event. If Firefox did not support a per-context proxy,
//! the alternative is the `proxy` capability of `session.new`:
//! `BidiClient::new_with_capabilities` and then `BrowserClient::from_client`.
//!
//! Configuration comes from `PROXY_HOST`, `PROXY_PORT`, `PROXY_USER` and
//! `PROXY_PASS`; without them the example skips. Needs Firefox with BiDi on 9222.
//!
//! Untested against a real proxy: only checked with an unreachable one (every command
//! accepted, navigation refused by the proxy). The 407 answer has never run.

use bidi_lite::browser::BrowserClient;
use bidi_lite::error::{BidiError, BidiResult};
use bidi_lite::tab::{ReadinessState, Tab};
use serde_json::{Value, json};
use std::time::Duration;
use tokio_stream::StreamExt;
use tracing::{info, warn};
use tracing_subscriber::{EnvFilter, fmt};

struct Proxy {
    address: String,
    username: String,
    password: String,
}

impl Proxy {
    fn from_env() -> Option<Self> {
        let var = |name: &str| std::env::var(name).ok().filter(|value| !value.is_empty());
        Some(Self {
            address: format!("{}:{}", var("PROXY_HOST")?, var("PROXY_PORT")?),
            username: var("PROXY_USER")?,
            password: var("PROXY_PASS")?,
        })
    }
}

#[tokio::main]
async fn main() -> BidiResult<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).init();

    let Some(proxy) = Proxy::from_env() else {
        info!("SKIP: set PROXY_HOST, PROXY_PORT, PROXY_USER and PROXY_PASS to run this example");
        return Ok(());
    };
    let browser = BrowserClient::connect("127.0.0.1:9222", Duration::from_secs(30)).await?;
    // The session is ended even if something below fails: Firefox allows only one.
    let result = run(&browser, proxy).await;
    browser.end().await?;
    result
}

async fn run(browser: &BrowserClient, proxy: Proxy) -> BidiResult<()> {
    let client = browser.client();
    let settings =
        json!({ "proxyType": "manual", "httpProxy": proxy.address, "sslProxy": proxy.address });
    let created = client
        .send_raw_command("browser.createUserContext", json!({ "proxy": settings }))
        .await?;
    let user_context = string_in(created.result, "userContext")?;

    let opened = client
        .send_raw_command(
            "browsingContext.create",
            json!({ "type": "tab", "userContext": user_context }),
        )
        .await?;
    let tab = browser
        .attach(&string_in(opened.result, "context")?)
        .await?;

    let result = browse_through(&tab, proxy).await;
    tab.clone().close().await?;
    client
        .send_raw_command(
            "browser.removeUserContext",
            json!({ "userContext": user_context }),
        )
        .await?;
    result
}

async fn browse_through(tab: &Tab, proxy: Proxy) -> BidiResult<()> {
    // Hold requests that need credentials, and hear about them.
    let intercept = json!({ "phases": ["authRequired"], "contexts": [tab.context_id()] });
    tab.send_raw_command("network.addIntercept", intercept)
        .await?;
    tab.subscribe_events(&["network.authRequired"]).await?;
    spawn_authenticator(tab, proxy);

    tab.navigate(
        "https://api.ipify.org?format=json",
        ReadinessState::Complete,
    )
    .await?;
    let body = json!({
        "expression": "document.body.innerText",
        "target": tab.script_target(),
        "awaitPromise": false,
    });
    let reply = tab.send_raw_command("script.evaluate", body).await?;
    info!(
        "Seen from the proxy: {}",
        reply.result.unwrap_or_default()["result"]["value"]
    );
    Ok(())
}

/// Answers the proxy's authentication challenges (407) with its credentials.
///
/// Only those: a 401 comes from the site itself, and sending it the proxy's
/// credentials would leak them. Other challenges get the browser's default.
fn spawn_authenticator(tab: &Tab, proxy: Proxy) {
    let tab = tab.clone();
    let mut events = tab.on_module("network");
    tokio::spawn(async move {
        while let Some(event) = events.next().await {
            let event = match event {
                Ok(event) => event,
                // Some events were missed; keep answering the next ones.
                Err(error) => {
                    warn!("network events: {error}");
                    continue;
                }
            };
            if event.method.as_deref() == Some("network.authRequired") {
                answer_challenge(&tab, &proxy, event.params.unwrap_or_default()).await;
            }
        }
    });
}

async fn answer_challenge(tab: &Tab, proxy: &Proxy, params: Value) {
    let request = params["request"]["request"].clone();
    let answer = if params["response"]["status"] == 407 {
        json!({
            "request": request,
            "action": "provideCredentials",
            "credentials": { "type": "password", "username": proxy.username, "password": proxy.password },
        })
    } else {
        json!({ "request": request, "action": "default" })
    };
    if let Err(error) = tab
        .send_raw_command("network.continueWithAuth", answer)
        .await
    {
        warn!("could not answer the challenge: {error}");
    }
}

fn string_in(result: Option<Value>, field: &str) -> BidiResult<String> {
    result
        .as_ref()
        .and_then(|result| result[field].as_str())
        .map(str::to_string)
        .ok_or_else(|| BidiError::InternalError(format!("the reply has no `{field}`")))
}
