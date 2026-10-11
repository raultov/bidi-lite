# bidi-lite

A lightweight, asynchronous, and low-overhead Rust client for **WebDriver BiDi**, focused on Firefox.

*Work in progress: not usable yet. Implemented so far: the BiDi wire types
(`protocol`), the error type (`error`), event filtering (`event_filter`) and the
connection with raw command dispatch (`client::BidiClient`). Sessions, tabs and
the Firefox debugger are still to come.*
