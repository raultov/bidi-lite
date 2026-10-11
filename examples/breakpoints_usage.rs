//! Debugging a page's JavaScript: BiDi drives the page, RDP debugs it.
//!
//! The flow of cdp-lite's example: find `nav_dropdown.value` in rust-lang.org's
//! scripts, break there, reload, read `current_lang` in the paused frame, resume.
//! Then pause on an uncaught exception.
//!
//! Needs Firefox with both servers: BiDi on 9222 and the DevTools server on 6000
//! (`tests/firefox.sh start`, which also sets the profile preferences RDP needs).
//!
//! Stepping is not used here. The flow would be: `Paused` -> `step_over()` ->
//! `Paused` with `why.type == "resumeLimit"`, one line further. After a step,
//! Firefox announces the resume and the new pause in one packet, so read every
//! state that `parse_thread_states` returns, not only the first.

use bidi_lite::browser::BrowserClient;
use bidi_lite::error::{BidiError, BidiResult};
use bidi_lite::rdp::client::RdpClient;
use bidi_lite::rdp::debugger::{
    BreakpointLocation, Debugger, FrameInfo, PauseOnExceptions, ThreadState, parse_thread_states,
};
use bidi_lite::tab::{ReadinessState, Tab};
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::timeout;
use tokio_stream::StreamExt;
use tracing::{error, info, warn};
use tracing_subscriber::{EnvFilter, fmt};

const PAGE: &str = "https://www.rust-lang.org";
const NEEDLE: &str = "nav_dropdown.value";

/// What the debugger task tells `main` about.
#[derive(Default)]
struct Signals {
    breakpoint: Notify,
    exception: Notify,
}

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
    let tab = browser.new_tab("about:blank").await?;
    tab.navigate(PAGE, ReadinessState::Complete).await?;
    // BiDi and RDP name tabs differently. Another tab may show the same URL, so
    // the tab is brought to the front and the debugger attaches to the selected one.
    tab.activate().await?;

    let rdp = RdpClient::connect("127.0.0.1:6000", Duration::from_secs(10)).await?;
    let debugger = Debugger::attach_selected(&rdp).await?;
    info!("Debugger attached");

    let result = debug(&tab, &debugger).await;
    // Remove the breakpoints first: one left behind freezes the page.
    debugger.detach().await?;
    tab.close().await?;
    result
}

async fn debug(tab: &Tab, debugger: &Debugger) -> BidiResult<()> {
    let Some(location) = plant_breakpoint(debugger).await? else {
        return Ok(());
    };

    let signals = Arc::new(Signals::default());
    spawn_debugger_task(debugger.clone(), Arc::clone(&signals));

    // Do not wait for the load: the page is about to pause.
    tab.reload(ReadinessState::None).await?;
    info!("Waiting for breakpoint resolution...");
    wait_for(&signals.breakpoint, "the breakpoint").await;
    debugger.remove_breakpoint(&location).await?;

    pause_on_an_exception(tab, debugger, &signals).await
}

/// Breaks where `NEEDLE` appears. `None` if the site no longer has it: the
/// example must not hang because rust-lang.org changed its scripts.
async fn plant_breakpoint(debugger: &Debugger) -> BidiResult<Option<BreakpointLocation>> {
    let Some((source, location)) = debugger.find_in_sources(NEEDLE).await? else {
        warn!("`{NEEDLE}` was not found in the page's scripts");
        return Ok(None);
    };
    info!(
        "Found `{NEEDLE}` in {} at line {}",
        source.url.unwrap_or_default(),
        location.line
    );
    debugger.set_breakpoint(&location, None).await?;
    Ok(Some(location))
}

async fn pause_on_an_exception(
    tab: &Tab,
    debugger: &Debugger,
    signals: &Signals,
) -> BidiResult<()> {
    debugger
        .set_pause_on_exceptions(PauseOnExceptions::Uncaught)
        .await?;
    let throw_later = json!({
        "expression": "setTimeout(() => { throw new Error('boom') })",
        "target": tab.script_target(),
        "awaitPromise": false,
    });
    tab.send_raw_command("script.evaluate", throw_later).await?;
    wait_for(&signals.exception, "the exception pause").await;
    debugger
        .set_pause_on_exceptions(PauseOnExceptions::None)
        .await
}

/// Runs the pause handler in the background.
fn spawn_debugger_task(debugger: Debugger, signals: Arc<Signals>) {
    let mut states = debugger.on_thread_state();
    tokio::spawn(async move {
        while let Some(packet) = states.next().await {
            let result = match packet {
                Ok(packet) => {
                    handle_states(&debugger, parse_thread_states(&packet), &signals).await
                }
                Err(error) => Err(error),
            };
            if let Err(error) = result {
                error!("Fatal error in the debugger task: {error}");
                return;
            }
        }
    });
}

async fn handle_states(
    debugger: &Debugger,
    states: Vec<ThreadState>,
    signals: &Signals,
) -> BidiResult<()> {
    for state in states {
        let ThreadState::Paused { why, frame } = state else {
            continue;
        };
        match why["type"].as_str() {
            Some("breakpoint") => {
                inspect_paused_frame(debugger, frame).await?;
                signals.breakpoint.notify_one();
            }
            Some("exception") => {
                info!("Paused on an exception: {}", exception_text(&why));
                signals.exception.notify_one();
            }
            other => info!("Paused ({other:?})"),
        }
        debugger.resume().await?;
    }
    Ok(())
}

async fn inspect_paused_frame(debugger: &Debugger, frame: Option<FrameInfo>) -> BidiResult<()> {
    let frame =
        frame.ok_or_else(|| BidiError::InternalError("a pause without a frame".to_string()))?;
    let evaluation = debugger
        .evaluate_on_frame(&frame.actor, "current_lang")
        .await?;
    info!("Evaluation result: current_lang = {}", evaluation.result);

    let stack: Vec<String> = debugger
        .frames(0, 5)
        .await?
        .into_iter()
        .map(|frame| {
            frame
                .display_name
                .unwrap_or_else(|| "(anonymous)".to_string())
        })
        .collect();
    info!("Stack: {stack:?}");
    Ok(())
}

fn exception_text(why: &Value) -> String {
    why["exception"]["preview"]["message"]
        .as_str()
        .unwrap_or("(no message)")
        .to_string()
}

async fn wait_for(signal: &Notify, what: &str) {
    match timeout(Duration::from_secs(10), signal.notified()).await {
        Ok(()) if what == "the breakpoint" => info!("Breakpoint was resolved!"),
        Ok(()) => info!("Saw {what}"),
        Err(_) => warn!("Timed out waiting for {what}"),
    }
}
