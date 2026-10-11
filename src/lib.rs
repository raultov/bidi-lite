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
mod session;
pub mod tab;
mod transport;

#[cfg(test)]
mod test_support;
