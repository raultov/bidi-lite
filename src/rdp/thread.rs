//! Requests to one JavaScript thread of a tab: its top-level document's, or
//! the thread of one of its frames of another origin (which Firefox runs in
//! another process).
//!
//! [`Debugger`](crate::rdp::debugger::Debugger) hands them out
//! ([`Debugger::thread`](crate::rdp::debugger::Debugger::thread),
//! [`Debugger::thread_of`](crate::rdp::debugger::Debugger::thread_of)), and
//! its own `resume`, `step_over`... go through the current one.

use crate::error::{BidiError, BidiResult};
use crate::rdp::client::RdpClient;
use crate::rdp::resources::{Evaluation, FrameInfo};
use serde_json::{Value, json};

/// The thread and console actors of one target, and the connection.
///
/// Cloning is cheap. The actors die with their target: a thread taken before
/// a navigation answers `noSuchActor` after it; take a new one.
#[derive(Clone)]
pub struct TargetThread {
    client: RdpClient,
    thread_actor: String,
    console_actor: String,
}

impl TargetThread {
    /// The thread of a target whose thread and console actors are given.
    pub fn new(client: &RdpClient, thread_actor: &str, console_actor: &str) -> Self {
        Self {
            client: client.clone(),
            thread_actor: thread_actor.to_string(),
            console_actor: console_actor.to_string(),
        }
    }

    /// The thread actor.
    pub fn thread_actor(&self) -> &str {
        &self.thread_actor
    }

    /// The console actor (evaluations).
    pub fn console_actor(&self) -> &str {
        &self.console_actor
    }

    /// Pauses at the next JavaScript to run (`interrupt`).
    pub async fn pause(&self) -> BidiResult<()> {
        self.request("interrupt", json!({ "when": "onNext" })).await
    }

    /// Resumes. Fails with `wrongState` if the thread is not paused.
    pub async fn resume(&self) -> BidiResult<()> {
        self.request("resume", json!({ "resumeLimit": null })).await
    }

    /// Runs to the next line of the current function.
    pub async fn step_over(&self) -> BidiResult<()> {
        self.step("next").await
    }

    /// Steps into the call on the current line.
    pub async fn step_into(&self) -> BidiResult<()> {
        self.step("step").await
    }

    /// Runs until the current function returns, pausing in its caller.
    pub async fn step_out(&self) -> BidiResult<()> {
        self.step("finish").await
    }

    /// `count` stack frames from `start`, while paused.
    pub async fn frames(&self, start: u32, count: u32) -> BidiResult<Vec<FrameInfo>> {
        let params = json!({ "start": start, "count": count });
        let reply = self
            .client
            .request(&self.thread_actor, "frames", params)
            .await?;
        let frames = reply
            .body
            .get("frames")
            .and_then(Value::as_array)
            .ok_or_else(|| BidiError::missing_field("frames", "frames"))?;
        Ok(frames.iter().filter_map(FrameInfo::from_value).collect())
    }

    /// Evaluates `expression` in a paused frame of this thread.
    ///
    /// The client's timeout applies twice: to the request and then to the
    /// `evaluationResult` that follows it.
    pub async fn evaluate_on_frame(
        &self,
        frame_actor: &str,
        expression: &str,
    ) -> BidiResult<Evaluation> {
        self.evaluate_with(json!({ "text": expression, "frameActor": frame_actor }))
            .await
    }

    /// Evaluates `expression` in this target's document.
    pub async fn evaluate(&self, expression: &str) -> BidiResult<Evaluation> {
        self.evaluate_with(json!({ "text": expression })).await
    }

    async fn request(&self, method: &str, params: Value) -> BidiResult<()> {
        self.client
            .request(&self.thread_actor, method, params)
            .await?;
        Ok(())
    }

    async fn step(&self, limit: &str) -> BidiResult<()> {
        self.request("resume", json!({ "resumeLimit": { "type": limit } }))
            .await
    }

    /// `evaluateJSAsync` on this target's console.
    async fn evaluate_with(&self, params: Value) -> BidiResult<Evaluation> {
        self.client.evaluate_on(&self.console_actor, params).await
    }
}
