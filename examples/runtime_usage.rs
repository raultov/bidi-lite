//! Running JavaScript with the `script` module, BiDi's counterpart of CDP's `Runtime`.
//!
//! Needs Firefox with BiDi on port 9222 (`tests/firefox.sh start`).

use bidi_lite::browser::BrowserClient;
use bidi_lite::error::{BidiError, BidiResult};
use bidi_lite::tab::{ReadinessState, Tab};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::time::timeout;
use tokio_stream::StreamExt;
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt};

#[tokio::main]
async fn main() -> BidiResult<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).init();

    let browser = BrowserClient::connect("127.0.0.1:9222", Duration::from_secs(10)).await?;
    // The session is ended even if something below fails: Firefox allows only one.
    let result = run(&browser).await;
    browser.end().await?;
    result
}

async fn run(browser: &BrowserClient) -> BidiResult<()> {
    let tab = browser
        .attach_to_all_tabs()
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| BidiError::InternalError("Firefox has no tab".to_string()))?;
    tab.navigate("https://www.rust-lang.org", ReadinessState::Complete)
        .await?;

    let sum = evaluate(&tab, "1 + 2", false).await?;
    info!("1 + 2 = {}", sum["result"]);

    let status = evaluate(&tab, "fetch(location.href).then(r => r.status)", true).await?;
    info!("fetch status = {}", status["result"]);

    call_a_function(&tab).await?;
    tell_exceptions_from_errors(&tab).await?;
    read_the_console(&tab).await
}

/// `script.evaluate`. The context goes in `target`, which `script_target` builds.
async fn evaluate(tab: &Tab, expression: &str, await_promise: bool) -> BidiResult<Value> {
    let params = json!({
        "expression": expression,
        "target": tab.script_target(),
        "awaitPromise": await_promise,
    });
    let reply = tab.send_raw_command("script.evaluate", params).await?;
    Ok(reply.result.unwrap_or_default())
}

async fn call_a_function(tab: &Tab) -> BidiResult<()> {
    let params = json!({
        "functionDeclaration": "(a, b) => a * b",
        "arguments": [{ "type": "number", "value": 6 }, { "type": "number", "value": 7 }],
        "target": tab.script_target(),
        "awaitPromise": false,
    });
    let reply = tab.send_raw_command("script.callFunction", params).await?;
    info!("6 * 7 = {}", reply.result.unwrap_or_default()["result"]);
    Ok(())
}

/// A script that throws is a normal reply whose `type` is `"exception"`; a
/// `ProtocolError` means the command itself was wrong.
async fn tell_exceptions_from_errors(tab: &Tab) -> BidiResult<()> {
    let thrown = evaluate(tab, "throw new Error('boom')", false).await?;
    if thrown["type"] == "exception" {
        info!("the script threw: {}", thrown["exceptionDetails"]["text"]);
    }

    let wrong = tab
        .send_raw_command("script.evaluate", json!({ "expression": "1" }))
        .await;
    if let Err(BidiError::ProtocolError { error, message, .. }) = wrong {
        info!("the command was refused: {error}: {message}");
    }
    Ok(())
}

async fn read_the_console(tab: &Tab) -> BidiResult<()> {
    // Subscribe first, then create the filter, then make the page log.
    tab.subscribe_events(&["log.entryAdded"]).await?;
    let mut logs = tab.on_module("log");
    evaluate(tab, "console.log('hi from bidi-lite')", false).await?;

    match timeout(Duration::from_secs(5), logs.next()).await {
        Ok(Some(entry)) => info!("console: {}", entry?.params.unwrap_or_default()["text"]),
        _ => info!("no console entry arrived"),
    }
    Ok(())
}
