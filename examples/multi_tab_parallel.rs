//! Loading several pages at once, one tab each, over a single connection.
//!
//! Every tab shares one WebSocket: command ids are allocated per connection, so
//! the replies of all the tabs can interleave without being mixed up.
//! Needs Firefox with BiDi on port 9222 (`tests/firefox.sh start`).

use bidi_lite::browser::BrowserClient;
use bidi_lite::error::BidiResult;
use bidi_lite::tab::ReadinessState;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tracing::{info, warn};
use tracing_subscriber::{EnvFilter, fmt};

const URLS: [&str; 4] = [
    "https://www.rust-lang.org",
    "https://docs.rs",
    "https://crates.io",
    "https://blog.rust-lang.org",
];

#[tokio::main]
async fn main() -> BidiResult<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).init();

    let browser = BrowserClient::connect("127.0.0.1:9222", Duration::from_secs(30)).await?;
    // The session is ended even if something below fails: Firefox allows only one.
    let result = load_all(&browser).await;
    browser.end().await?;
    result
}

async fn load_all(browser: &BrowserClient) -> BidiResult<()> {
    let started = Instant::now();
    let loads: Vec<JoinHandle<BidiResult<Duration>>> = URLS
        .iter()
        .map(|url| tokio::spawn(load(browser.clone(), url)))
        .collect();

    for (url, handle) in URLS.iter().zip(loads) {
        match handle.await {
            Ok(Ok(took)) => info!("{url} loaded in {took:?}"),
            Ok(Err(error)) => warn!("{url} failed: {error}"),
            Err(error) => warn!("{url}: the task failed: {error}"),
        }
    }
    info!("all {} pages in {:?}", URLS.len(), started.elapsed());
    Ok(())
}

/// Opens a blank tab, loads `url` in it waiting for the page, and closes it.
async fn load(browser: BrowserClient, url: &'static str) -> BidiResult<Duration> {
    let started = Instant::now();
    let tab = browser.new_tab("about:blank").await?;
    let loaded = tab.navigate(url, ReadinessState::Complete).await;
    let took = started.elapsed();
    tab.close().await?;
    loaded.map(|_| took)
}
