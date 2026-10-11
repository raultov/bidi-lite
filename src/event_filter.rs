//! Stream filter for broadcast events by module, context, or custom predicate.
//!
//! Every event of a connection travels through one broadcast ring; an
//! [`EventFilter`] is one subscriber's view of it.

use crate::error::{BidiError, BidiResult};
use crate::protocol::WsResponse;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::broadcast;
use tokio_stream::Stream;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

/// Decides whether one message belongs to this subscriber.
type Predicate<T> = Box<dyn Fn(&T) -> bool + Send + Sync>;

/// A subscriber's view of the shared event stream: only the messages its
/// predicate accepts.
///
/// The type parameter is the message type, so the BiDi and the RDP clients
/// share one implementation. It defaults to [`WsResponse`], which is what a bare
/// `EventFilter` means.
///
/// # Events are opt-in
///
/// A filter only filters. The browser emits no event until the session
/// subscribes to it with `session.subscribe`, so a filter created without that
/// call stays silent forever. This is the main difference with CDP, where
/// `Page.enable` and friends play that role.
///
/// # Iframes
///
/// A context filter compares the context id exactly. Events from an iframe
/// carry the iframe's own context id, so they do not reach the filter of the
/// tab that contains it.
///
/// # Lag
///
/// If the subscriber falls behind the ring, the stream yields
/// [`BidiError::Lagged`] with the number of events it missed and then keeps
/// working with what it did not miss.
pub struct EventFilter<T: Clone + Send + 'static = WsResponse> {
    inner: BroadcastStream<T>,
    predicate: Predicate<T>,
}

impl<T: Clone + Send + 'static> EventFilter<T> {
    /// Filters `receiver` with any predicate over the message type.
    pub fn with_predicate(
        receiver: broadcast::Receiver<T>,
        predicate: impl Fn(&T) -> bool + Send + Sync + 'static,
    ) -> Self {
        Self {
            inner: BroadcastStream::new(receiver),
            predicate: Box::new(predicate),
        }
    }
}

impl EventFilter<WsResponse> {
    /// Events whose method starts with `"{module}."`; `""` matches every module.
    pub fn new(receiver: broadcast::Receiver<WsResponse>, module: &'static str) -> Self {
        let prefix = module_prefix(module);
        Self::with_predicate(receiver, move |event| bidi_matches(&prefix, None, event))
    }

    /// Like [`EventFilter::new`], but only for events whose context equals
    /// `context` (see [`WsResponse::event_context`]).
    ///
    /// Events that carry no context belong to no tab and are dropped.
    pub fn for_context(
        receiver: broadcast::Receiver<WsResponse>,
        module: &'static str,
        context: impl Into<String>,
    ) -> Self {
        let prefix = module_prefix(module);
        let context = context.into();
        Self::with_predicate(receiver, move |event| {
            bidi_matches(&prefix, Some(&context), event)
        })
    }
}

/// `"{module}."`, or nothing to match every module.
///
/// The dot matters: `"log"` must not match a module named `"logger"`.
fn module_prefix(module: &str) -> String {
    if module.is_empty() {
        String::new()
    } else {
        format!("{module}.")
    }
}

/// Whether `event` is a message of the module `prefix` names, optionally
/// restricted to one browsing context.
fn bidi_matches(prefix: &str, context: Option<&str>, event: &WsResponse) -> bool {
    let Some(method) = event.method.as_deref() else {
        return false;
    };
    if !method.starts_with(prefix) {
        return false;
    }

    match (context, event.event_context()) {
        // Not scoped to a context: every context's events.
        (None, _) => true,
        (Some(wanted), Some(actual)) => wanted == actual,
        // Events that carry no context (e.g. `script.realmDestroyed`) belong to no tab.
        (Some(_), None) => false,
    }
}

impl<T: Clone + Send + 'static> Stream for EventFilter<T> {
    type Item = BidiResult<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match Pin::new(&mut self.inner).poll_next(cx) {
                Poll::Ready(Some(Ok(item))) => {
                    if (self.predicate)(&item) {
                        return Poll::Ready(Some(Ok(item)));
                    }
                    // Not what this subscriber wants: poll the next one.
                }
                Poll::Ready(Some(Err(BroadcastStreamRecvError::Lagged(skipped)))) => {
                    // The ring overran this subscriber and `skipped` events are gone.
                    // A typed error lets a consumer caching event-derived state
                    // resynchronise it instead of silently going stale.
                    return Poll::Ready(Some(Err(BidiError::Lagged { skipped })));
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support;
    use serde_json::{Value, json};
    use std::time::Duration;
    use tokio::time::timeout;
    use tokio_stream::StreamExt;

    const WAIT: Duration = Duration::from_secs(2);

    fn event(method: &str, params: Value) -> WsResponse {
        serde_json::from_value(test_support::event(method, params)).expect("a valid event")
    }

    fn load(context: &str) -> WsResponse {
        event("browsingContext.load", json!({ "context": context }))
    }

    fn contexts(events: &[WsResponse]) -> Vec<Option<&str>> {
        events.iter().map(WsResponse::event_context).collect()
    }

    fn methods(events: &[WsResponse]) -> Vec<&str> {
        events
            .iter()
            .map(|event| event.method.as_deref().expect("events carry a method"))
            .collect()
    }

    /// Reads exactly `count` items, failing the test on a lag, a closed stream or a hang.
    async fn take<T: Clone + Send + 'static>(filter: &mut EventFilter<T>, count: usize) -> Vec<T> {
        let mut items = Vec::new();
        while items.len() < count {
            let item = timeout(WAIT, filter.next())
                .await
                .expect("timed out waiting for an event")
                .expect("the stream closed early")
                .expect("unexpected filter error");
            items.push(item);
        }
        items
    }

    /// EVF-01: A module filter forwards only that module.
    #[tokio::test]
    async fn module_filter_forwards_only_that_module() {
        // Given a filter on "browsingContext"
        let (tx, rx) = broadcast::channel(16);
        let mut filter = EventFilter::new(rx, "browsingContext");

        // When events of two modules are broadcast
        let _ = tx.send(event("network.beforeRequestSent", json!({})));
        let _ = tx.send(event("browsingContext.load", json!({})));
        let _ = tx.send(event("browsingContext.domContentLoaded", json!({})));

        // Then it yields only the browsingContext ones, in order
        let events = take(&mut filter, 2).await;
        assert_eq!(
            methods(&events),
            ["browsingContext.load", "browsingContext.domContentLoaded"]
        );
    }

    /// EVF-02: The module prefix requires the dot.
    #[tokio::test]
    async fn module_prefix_requires_the_dot() {
        // Given a filter on "log"
        let (tx, rx) = broadcast::channel(16);
        let mut filter = EventFilter::new(rx, "log");

        // When "logger.x" and "log.entryAdded" are broadcast
        let _ = tx.send(event("logger.x", json!({})));
        let _ = tx.send(event("log.entryAdded", json!({})));
        drop(tx);

        // Then it yields only "log.entryAdded"
        let events = take(&mut filter, 1).await;
        assert_eq!(methods(&events), ["log.entryAdded"]);
        assert!(
            filter.next().await.is_none(),
            "logger.x must not leak through"
        );
    }

    /// EVF-03: An unscoped filter forwards every context.
    #[tokio::test]
    async fn unscoped_filter_forwards_every_context() {
        // Given a filter on "browsingContext" without a context
        let (tx, rx) = broadcast::channel(16);
        let mut filter = EventFilter::new(rx, "browsingContext");

        // When loads for contexts A and B and one without context are broadcast
        let _ = tx.send(load("A"));
        let _ = tx.send(load("B"));
        let _ = tx.send(event("browsingContext.load", json!({})));

        // Then all three are yielded
        let events = take(&mut filter, 3).await;
        assert_eq!(contexts(&events), [Some("A"), Some("B"), None]);
    }

    /// EVF-04: A context filter drops other contexts.
    #[tokio::test]
    async fn context_filter_drops_other_contexts() {
        // Given a filter on "browsingContext" for context A
        let (tx, rx) = broadcast::channel(16);
        let mut filter = EventFilter::for_context(rx, "browsingContext", "A");

        // When a load for B then a load for A are broadcast
        let _ = tx.send(load("B"));
        let _ = tx.send(load("A"));

        // Then the first yielded event is for A
        let events = take(&mut filter, 1).await;
        assert_eq!(contexts(&events), [Some("A")]);
    }

    /// EVF-05: A context filter drops context-less events.
    #[tokio::test]
    async fn context_filter_drops_context_less_events() {
        // Given a filter on every module for context A
        let (tx, rx) = broadcast::channel(16);
        let mut filter = EventFilter::for_context(rx, "", "A");

        // When an event with no context, then one for A, are broadcast
        let _ = tx.send(event("script.realmDestroyed", json!({ "realm": "R" })));
        let _ = tx.send(load("A"));

        // Then the first yielded event is for A
        let events = take(&mut filter, 1).await;
        assert_eq!(contexts(&events), [Some("A")]);
    }

    /// EVF-06: A context filter matches `params.source.context`.
    #[tokio::test]
    async fn context_filter_matches_source_context() {
        // Given a filter on "log" for context A
        let (tx, rx) = broadcast::channel(16);
        let mut filter = EventFilter::for_context(rx, "log", "A");

        // When a log.entryAdded nesting its context under `source` is broadcast
        let source = json!({ "source": { "realm": "R", "context": "A" } });
        let _ = tx.send(event("log.entryAdded", source));

        // Then it is yielded
        let events = take(&mut filter, 1).await;
        assert_eq!(methods(&events), ["log.entryAdded"]);
    }

    /// EVF-07: An empty module matches every module of one context.
    #[tokio::test]
    async fn empty_module_matches_every_module_of_one_context() {
        // Given a filter on every module for context A
        let (tx, rx) = broadcast::channel(16);
        let mut filter = EventFilter::for_context(rx, "", "A");

        // When events of two modules and two contexts are broadcast
        let _ = tx.send(load("B"));
        let _ = tx.send(load("A"));
        let _ = tx.send(event(
            "network.responseCompleted",
            json!({ "context": "A" }),
        ));

        // Then it yields the two events for A
        let events = take(&mut filter, 2).await;
        assert_eq!(
            methods(&events),
            ["browsingContext.load", "network.responseCompleted"]
        );
    }

    /// EVF-08: Messages without method are ignored.
    #[tokio::test]
    async fn messages_without_method_are_ignored() {
        // Given a filter on every module
        let (tx, rx) = broadcast::channel(16);
        let mut filter = EventFilter::new(rx, "");

        // When a message with no method, then an event, are broadcast
        let _ = tx.send(WsResponse::default());
        let _ = tx.send(load("A"));

        // Then only the event is yielded
        let events = take(&mut filter, 1).await;
        assert_eq!(methods(&events), ["browsingContext.load"]);
    }

    /// EVF-09: Overrunning the ring surfaces a typed `Lagged` with the exact count.
    #[tokio::test]
    async fn overflowing_the_ring_reports_exact_lag() {
        // Given a 4-slot ring and a filter that has not read
        let (tx, rx) = broadcast::channel(4);
        let mut filter = EventFilter::new(rx, "browsingContext");

        // When 12 events are broadcast
        for i in 0..12 {
            let _ = tx.send(event(&format!("browsingContext.e{i}"), json!({})));
        }

        // Then the first item is a lag of exactly the 8 events that did not fit
        let first = filter.next().await.expect("the stream stays open");
        let Err(BidiError::Lagged { skipped }) = first else {
            panic!("expected Lagged, got {first:?}");
        };
        assert_eq!(skipped, 8, "12 sent into a 4-slot ring drops 8");
    }

    /// EVF-10: The filter keeps working after a lag.
    #[tokio::test]
    async fn filter_keeps_working_after_a_lag() {
        // Given a 2-slot ring overrun with 5 events and the sender dropped
        let (tx, rx) = broadcast::channel(2);
        let mut filter = EventFilter::new(rx, "browsingContext");
        for i in 0..5 {
            let _ = tx.send(event(&format!("browsingContext.e{i}"), json!({})));
        }
        drop(tx);

        // When the Lagged item is consumed
        let lag = filter.next().await.expect("the stream stays open");
        assert!(matches!(lag, Err(BidiError::Lagged { .. })), "{lag:?}");

        // Then the remaining items are events, at most 2, and the last is the newest
        let mut rest = Vec::new();
        while let Some(item) = filter.next().await {
            rest.push(item.expect("no further errors after the lag"));
        }
        assert!(rest.len() <= 2, "a 2-slot ring holds 2 events: {rest:?}");
        assert_eq!(methods(&rest).last(), Some(&"browsingContext.e4"));
        // And the stream then ended, since the loop above only stops on None
    }

    /// EVF-11: A closed channel ends the stream.
    #[tokio::test]
    async fn closed_channel_ends_the_stream() {
        // Given a filter whose sender is dropped with nothing pending
        let (tx, rx) = broadcast::channel::<WsResponse>(16);
        let mut filter = EventFilter::new(rx, "");
        drop(tx);

        // Then the stream ends
        assert!(filter.next().await.is_none());
    }

    /// EVF-12: A custom predicate over any message type works and lags the same way.
    #[tokio::test]
    async fn custom_predicate_filters_any_type() {
        // Given a filter keeping the even numbers
        let (tx, rx) = broadcast::channel::<u32>(4);
        let mut evens = EventFilter::with_predicate(rx, |n| n % 2 == 0);

        // When 1, 2, 3, 4 are broadcast
        for n in 1..=4 {
            let _ = tx.send(n);
        }

        // Then it yields 2 then 4
        assert_eq!(take(&mut evens, 2).await, [2, 4]);

        // And a ring overrun is reported as Lagged for this type too
        let (tx, rx) = broadcast::channel::<u32>(2);
        let mut lagging = EventFilter::with_predicate(rx, |_| true);
        for n in 0..5 {
            let _ = tx.send(n);
        }
        let first = lagging.next().await.expect("the stream stays open");
        assert!(
            matches!(first, Err(BidiError::Lagged { skipped: 3 })),
            "{first:?}"
        );
    }
}
