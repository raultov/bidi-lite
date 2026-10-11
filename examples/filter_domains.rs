//! Receiving events: subscribe, then filter the shared event stream by module.
//!
//! The name mirrors cdp-lite's example; BiDi calls its domains "modules".
//! Needs Firefox with BiDi on port 9222 (`tests/firefox.sh start`).

use bidi_lite::browser::BrowserClient;
use bidi_lite::error::{BidiError, BidiResult};
use bidi_lite::tab::{ReadinessState, Tab};
use std::time::Duration;
use tokio_stream::StreamExt;
use tracing::{debug, info, warn};
use tracing_subscriber::{EnvFilter, fmt};

const EVENTS: [&str; 4] = [
    "network.beforeRequestSent",
    "network.responseCompleted",
    "browsingContext.load",
    "browsingContext.domContentLoaded",
];

#[tokio::main]
async fn main() -> BidiResult<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).init();

    let browser = BrowserClient::connect("127.0.0.1:9222", Duration::from_secs(10)).await?;
    // The session is ended even if something below fails: Firefox allows only one.
    let result = watch_a_navigation(&browser).await;
    browser.end().await?;
    result
}

async fn watch_a_navigation(browser: &BrowserClient) -> BidiResult<()> {
    let tab = browser
        .attach_to_all_tabs()
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| BidiError::InternalError("Firefox has no tab".to_string()))?;

    // Unlike CDP's `Page.enable`, nothing is sent until the session subscribes:
    // without this call the filters below would stay silent forever.
    tab.subscribe_events(&EVENTS).await?;
    spawn_event_printer(&tab);
    spawn_raw_logger(&tab);

    tab.navigate("https://www.rust-lang.org", ReadinessState::Complete)
        .await?;
    // Give the last events time to arrive before the session ends.
    tokio::time::sleep(Duration::from_secs(1)).await;
    Ok(())
}

/// Prints the method of every network and browsingContext event of the tab.
fn spawn_event_printer(tab: &Tab) {
    let mut activity = tab
        .on_module("network")
        .merge(tab.on_module("browsingContext"));
    tokio::spawn(async move {
        while let Some(event) = activity.next().await {
            match event {
                Ok(event) => info!("event: {}", event.method.unwrap_or_default()),
                Err(error) => warn!("event stream: {error}"),
            }
        }
    });
}

/// A raw receiver sees every event of the connection, unfiltered.
fn spawn_raw_logger(tab: &Tab) {
    let mut raw = tab.client().subscribe();
    tokio::spawn(async move {
        while let Ok(event) = raw.recv().await {
            debug!("raw event: {:?}", event.method);
        }
    });
}
