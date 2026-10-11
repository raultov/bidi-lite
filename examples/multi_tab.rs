//! Several tabs over one session: open, watch, activate, reload and close them.
//!
//! Needs Firefox with BiDi on port 9222 (`tests/firefox.sh start`).

use bidi_lite::browser::BrowserClient;
use bidi_lite::error::BidiResult;
use bidi_lite::protocol::WsResponse;
use bidi_lite::tab::{ReadinessState, Tab};
use std::time::Duration;
use tokio_stream::StreamExt;
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt};

#[tokio::main]
async fn main() -> BidiResult<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).init();

    let browser = BrowserClient::connect("127.0.0.1:9222", Duration::from_secs(20)).await?;
    // The session is ended even if something below fails: Firefox allows only one.
    let result = run(&browser).await;
    browser.end().await?;
    result
}

async fn run(browser: &BrowserClient) -> BidiResult<()> {
    for tab in browser.list_tabs().await? {
        info!("open tab {} -> {}", tab.context, tab.url);
    }

    // Announce every tab that appears from now on.
    browser.set_discover_contexts(true).await?;
    spawn_discovery_listener(browser);

    let (docs, crates) = open_watched_tabs(browser).await?;
    drive_and_close(browser, docs, crates).await?;
    browser.set_discover_contexts(false).await
}

/// Opens two tabs, each with a listener of its own load events.
async fn open_watched_tabs(browser: &BrowserClient) -> BidiResult<(Tab, Tab)> {
    let docs = browser.new_tab("https://docs.rs").await?;
    let crates = browser.new_tab("https://crates.io").await?;
    for (tab, label) in [(&docs, "docs.rs"), (&crates, "crates.io")] {
        tab.subscribe_events(&["browsingContext.load"]).await?;
        spawn_load_listener(tab, label);
    }
    Ok((docs, crates))
}

async fn drive_and_close(browser: &BrowserClient, docs: Tab, crates: Tab) -> BidiResult<()> {
    docs.activate().await?;
    crates.reload(ReadinessState::Complete).await?;
    docs.navigate("https://docs.rs/tokio", ReadinessState::Complete)
        .await?;
    // Let the listeners print before the tabs go away.
    tokio::time::sleep(Duration::from_millis(500)).await;

    docs.close().await?;
    browser.close_tab(crates.context_id()).await
}

fn spawn_discovery_listener(browser: &BrowserClient) {
    let mut contexts = browser.client().on_module("browsingContext");
    tokio::spawn(async move {
        while let Some(Ok(event)) = contexts.next().await {
            if event.method.as_deref() == Some("browsingContext.contextCreated") {
                info!("new {}: {:?}", context_kind(&event), event.event_context());
            }
        }
    });
}

/// Iframes are browsing contexts too; only top-level ones have no parent.
fn context_kind(event: &WsResponse) -> &'static str {
    match event.params.as_ref().map(|params| &params["parent"]) {
        Some(parent) if !parent.is_null() => "frame",
        _ => "tab",
    }
}

/// Each tab's filter only sees that tab's events, even though they share a connection.
fn spawn_load_listener(tab: &Tab, label: &'static str) {
    let mut events = tab.on_module("browsingContext");
    tokio::spawn(async move {
        while let Some(Ok(event)) = events.next().await {
            if event.method.as_deref() == Some("browsingContext.load") {
                let url = event.params.unwrap_or_default()["url"].clone();
                info!("{label} loaded {url}");
            }
        }
    });
}
