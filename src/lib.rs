//! # bidi-lite
//!
//! A lightweight, asynchronous, and low-overhead Rust client for WebDriver BiDi,
//! focused on Firefox.

pub mod browser;
pub mod client;
pub mod error;
pub mod event_filter;
pub mod protocol;
pub mod rdp;
pub mod tab;
// Compiled for tests only until `client.rs` uses it (phase 7): until then every
// item is dead code, which fails the `-D warnings` gate.
#[cfg(test)]
mod transport;

#[cfg(test)]
mod test_support;
