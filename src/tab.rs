//! Handle for driving a single top-level browsing context.

use crate::client::BidiClient;

/// A handle to one top-level browsing context over the shared BiDi session.
///
/// Created by [`BrowserClient`](crate::browser::BrowserClient). Dropping a
/// `Tab` neither closes the browsing context nor removes its subscriptions.
pub struct Tab {
    client: BidiClient,
    context_id: String,
}

impl Tab {
    /// Wraps a browsing context that the browser has confirmed exists.
    pub(crate) fn new(client: BidiClient, context_id: String) -> Self {
        Self { client, context_id }
    }

    /// The browsing context id, the identifier BiDi commands use for this tab.
    pub fn context_id(&self) -> &str {
        &self.context_id
    }

    /// The connection this tab shares with every other tab of the session.
    pub fn client(&self) -> &BidiClient {
        &self.client
    }
}
