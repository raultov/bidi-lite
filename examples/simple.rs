//! The minimal round trip: start a BiDi session, navigate the first tab, end the session.
//!
//! Needs a Firefox listening for BiDi on port 9222 (`tests/firefox.sh start`, or
//! `firefox --remote-debugging-port=9222`). Set `RUST_LOG` to change the log level.

use bidi_lite::browser::BrowserClient;
use bidi_lite::error::{BidiError, BidiResult};
use bidi_lite::tab::ReadinessState;
use std::time::Duration;
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt};

#[tokio::main]
async fn main() -> BidiResult<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).init();

    let browser = BrowserClient::connect("127.0.0.1:9222", Duration::from_secs(10)).await?;
    info!("BiDi session started");

    // Firefox allows a single session and keeps it after the connection closes,
    // so it is ended even if navigating fails.
    let result = navigate_first_tab(&browser).await;
    browser.end().await?;
    result
}

async fn navigate_first_tab(browser: &BrowserClient) -> BidiResult<()> {
    let tab = browser
        .attach_to_all_tabs()
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| BidiError::InternalError("Firefox has no tab".to_string()))?;

    let response = tab
        .navigate("https://www.rust-lang.org", ReadinessState::Complete)
        .await?;
    info!("Firefox replied: {response:?}");
    Ok(())
}
