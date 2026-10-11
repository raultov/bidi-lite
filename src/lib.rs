//! # bidi-lite
//!
//! A lightweight, asynchronous, and low-overhead Rust client for WebDriver BiDi,
//! focused on Firefox, with a minimal client for the Firefox Remote Debugging
//! Protocol (RDP) so JavaScript can be debugged too.
//!
//! It is wire-level on purpose: commands take any `Serialize` parameters and
//! return raw JSON. Firefox must be started with `--remote-debugging-port=9222`
//! (BiDi) and, for the debugger, `--start-debugger-server ws:6000` (RDP).
//!
//! **Firefox allows a single BiDi session, and it outlives the connection.**
//! Always end it ([`BrowserClient::end`](browser::BrowserClient::end)): a client
//! dropped without doing so leaves Firefox refusing new sessions until restarted.
//!
//! ## Basic navigation
//!
//! ```no_run
//! use bidi_lite::browser::BrowserClient;
//! use bidi_lite::tab::ReadinessState;
//! use std::time::Duration;
//!
//! # async fn doc_example() -> bidi_lite::error::BidiResult<()> {
//! let browser = BrowserClient::connect("127.0.0.1:9222", Duration::from_secs(10)).await?;
//! if let Some(tab) = browser.attach_to_all_tabs().await?.into_iter().next() {
//!     let reply = tab.navigate("https://www.rust-lang.org", ReadinessState::Complete).await?;
//!     println!("{:?}", reply.result);
//! }
//! browser.end().await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Listening to events
//!
//! Filters only filter: the browser sends no event until the session subscribes
//! to it, which is what CDP's `Page.enable` does.
//!
//! ```no_run
//! use bidi_lite::browser::BrowserClient;
//! use bidi_lite::tab::ReadinessState;
//! use std::time::Duration;
//! use tokio_stream::StreamExt;
//!
//! # async fn doc_example() -> bidi_lite::error::BidiResult<()> {
//! let browser = BrowserClient::connect("127.0.0.1:9222", Duration::from_secs(10)).await?;
//! let tab = browser.new_tab("about:blank").await?;
//! tab.subscribe_events(&["browsingContext.load", "network.responseCompleted"]).await?;
//! let mut events = tab.on_module("browsingContext").merge(tab.on_module("network"));
//!
//! tab.navigate("https://www.rust-lang.org", ReadinessState::None).await?;
//! while let Some(event) = events.next().await {
//!     let event = event?; // `BidiError::Lagged` if this subscriber fell behind
//!     if event.method.as_deref() == Some("browsingContext.load") {
//!         break;
//!     }
//! }
//! tab.close().await?;
//! browser.end().await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Driving several tabs at once
//!
//! Every tab shares one connection and one session; each tab's filters only see
//! that tab's events.
//!
//! ```no_run
//! use bidi_lite::browser::BrowserClient;
//! use bidi_lite::tab::ReadinessState;
//! use std::time::Duration;
//!
//! # async fn doc_example() -> bidi_lite::error::BidiResult<()> {
//! let browser = BrowserClient::connect("127.0.0.1:9222", Duration::from_secs(30)).await?;
//! let docs = browser.new_tab("about:blank").await?;
//! let crates = browser.new_tab("about:blank").await?;
//! let (a, b) = tokio::join!(
//!     docs.navigate("https://docs.rs", ReadinessState::Complete),
//!     crates.navigate("https://crates.io", ReadinessState::Complete),
//! );
//! a?;
//! b?;
//! docs.close().await?;
//! crates.close().await?;
//! browser.end().await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Raw access
//!
//! Any BiDi command, with the parameters sent exactly as given.
//!
//! ```no_run
//! use bidi_lite::client::BidiClient;
//! use serde_json::json;
//! use std::time::Duration;
//!
//! # async fn doc_example() -> bidi_lite::error::BidiResult<()> {
//! let client = BidiClient::new("127.0.0.1:9222", Duration::from_secs(10)).await?;
//! let tree = client.send_raw_command("browsingContext.getTree", json!({})).await?;
//! println!("{:?}", tree.result);
//! client.end_session().await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Debugging JavaScript
//!
//! BiDi has no debugger, so [`rdp::debugger::Debugger`] drives Firefox's own
//! (RDP). BiDi drives the page; the debugger attaches to the selected tab.
//!
//! ```no_run
//! use bidi_lite::rdp::client::RdpClient;
//! use bidi_lite::rdp::debugger::{Debugger, ThreadState, parse_thread_states};
//! use std::time::Duration;
//! use tokio_stream::StreamExt;
//!
//! # async fn doc_example() -> bidi_lite::error::BidiResult<()> {
//! let rdp = RdpClient::connect("127.0.0.1:6000", Duration::from_secs(10)).await?;
//! let debugger = Debugger::attach_selected(&rdp).await?;
//! if let Some((_, location)) = debugger.find_in_sources("nav_dropdown.value").await? {
//!     debugger.set_breakpoint(&location, None).await?;
//! }
//!
//! let mut states = debugger.on_thread_state();
//! while let Some(packet) = states.next().await {
//!     // One packet can carry several states: read them all.
//!     for state in parse_thread_states(&packet?) {
//!         if let ThreadState::Paused { frame: Some(frame), .. } = state {
//!             let value = debugger.evaluate_on_frame(&frame.actor, "current_lang").await?;
//!             println!("current_lang = {}", value.result);
//!             debugger.resume().await?;
//!         }
//!     }
//! }
//! debugger.detach().await?; // removes the breakpoints it set
//! # Ok(())
//! # }
//! ```

pub mod browser;
pub mod client;
pub mod error;
pub mod event_filter;
pub mod protocol;
pub mod rdp;
mod session;
pub mod tab;
mod transport;

#[cfg(test)]
mod test_support;
