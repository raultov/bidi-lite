# bidi-lite

A lightweight, asynchronous, and low-overhead Rust client for **WebDriver BiDi**, focused on Firefox.

*Work in progress: not usable yet. Implemented so far: the BiDi wire types
(`protocol`), the error type (`error`), event filtering (`event_filter`), the
connection with raw command dispatch and session handling
(`client::BidiClient`), browser-level control of tabs and contexts
(`browser::BrowserClient`) and driving a single tab (`tab::Tab`). The Firefox
debugger is still to come.*

**Firefox allows a single BiDi session, and it outlives the connection.** Always
end it (`BrowserClient::end`, or `BidiClient::end_session`) when you are done:
dropping the client leaves it open and Firefox then refuses new sessions with
`Session already started` until the browser is restarted.
