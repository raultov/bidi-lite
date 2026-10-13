//! Types and pure parsers of the Firefox debugger.
//!
//! Everything here is a function of a packet or a string, with no connection,
//! so it is tested against packets captured from Firefox. The
//! [`Debugger`](crate::rdp::debugger::Debugger) re-exports it.

use crate::rdp::protocol::RdpPacket;
use serde::Deserialize;
use serde_json::{Value, json};

/// A script, as announced by a `source` resource.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceInfo {
    /// Actor to ask for the source's text and breakable positions.
    pub actor: String,
    /// URL of the script. An inline script has the URL of its document; a source
    /// created by `eval` has none.
    #[serde(default)]
    pub url: Option<String>,
    /// URL of the source map, if the script declares one.
    #[serde(default)]
    pub source_map_url: Option<String>,
    /// How the script came to be: `"scriptElement"`, `"eval"`, `"debugger eval"`...
    #[serde(default)]
    pub introduction_type: Option<String>,
}

/// Where to break.
///
/// `line` is **1-based** and `column` **0-based**, as in RDP. CDP counts lines
/// from 0: [`BreakpointLocation::from_cdp`] converts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BreakpointLocation {
    /// URL of the script.
    pub source_url: String,
    /// 1-based line.
    pub line: u32,
    /// 0-based column. `None` lets the debugger pick the first breakable position
    /// of the line.
    pub column: Option<u32>,
}

impl BreakpointLocation {
    /// A location from CDP coordinates: both `line` and `column` 0-based.
    pub fn from_cdp(source_url: impl Into<String>, line: u32, column: u32) -> Self {
        Self {
            source_url: source_url.into(),
            line: line + 1,
            column: Some(column),
        }
    }
}

/// Where [`Debugger::set_breakpoint_resolved`](crate::rdp::debugger::Debugger::set_breakpoint_resolved)
/// bound a breakpoint, so a caller can report the column Firefox uses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundBreakpoint {
    /// The location sent to the breakpoint list; its `column` is always `Some`.
    pub location: BreakpointLocation,
    /// Whether a loaded source has the URL. `false` means the breakpoint is set
    /// but binds only when a script with that URL loads.
    pub resolved: bool,
}

/// What [`Debugger::breakable_column`](crate::rdp::debugger::Debugger::breakable_column)
/// found for a line.
///
/// A page has one source per inline `<script>`, all with the page's URL, so a
/// line can be breakable in one of them and not in the others.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Breakable {
    /// No loaded source has the URL.
    NotLoaded,
    /// The first breakable column of the line, in the first source with the URL
    /// that has one.
    At(u32),
    /// Sources with the URL are loaded, but none can break on that line.
    Nowhere,
}

/// When to pause on a thrown exception.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PauseOnExceptions {
    /// Never.
    None,
    /// Only on exceptions nothing catches.
    Uncaught,
    /// On every exception, caught or not.
    All,
}

impl PauseOnExceptions {
    /// The `configuration` object `threadConfiguration.updateConfiguration` expects.
    pub fn configuration(self) -> Value {
        match self {
            Self::None => json!({ "pauseOnExceptions": false }),
            Self::Uncaught => json!({ "pauseOnExceptions": true, "ignoreCaughtExceptions": true }),
            Self::All => json!({ "pauseOnExceptions": true, "ignoreCaughtExceptions": false }),
        }
    }
}

/// `frame.where`: the source actor and the position inside it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct FrameLocation {
    /// Actor of the source the frame is executing.
    pub actor: String,
    /// 1-based line.
    pub line: u32,
    /// 0-based column.
    pub column: u32,
}

/// One stack frame.
#[derive(Debug, Clone)]
pub struct FrameInfo {
    /// Frame actor, for [`Debugger::evaluate_on_frame`](crate::rdp::debugger::Debugger::evaluate_on_frame)
    /// and `frame_environment`.
    pub actor: String,
    /// Function name, if the frame has one.
    pub display_name: Option<String>,
    /// Where the frame is executing.
    pub location: Option<FrameLocation>,
    /// The whole frame object: `this`, `arguments`, `type`, `state`...
    pub raw: Value,
}

impl FrameInfo {
    /// Reads a frame object; `None` if it has no actor.
    pub fn from_value(frame: &Value) -> Option<Self> {
        Some(Self {
            actor: frame.get("actor")?.as_str()?.to_string(),
            display_name: frame
                .get("displayName")
                .and_then(Value::as_str)
                .map(str::to_string),
            location: frame
                .get("where")
                .and_then(|location| serde_json::from_value(location.clone()).ok()),
            raw: frame.clone(),
        })
    }
}

/// The state of the debuggee thread.
#[derive(Debug, Clone)]
pub enum ThreadState {
    /// Stopped. `why.type` is `"breakpoint"`, `"resumeLimit"`, `"interrupted"`,
    /// `"exception"`, `"debuggerStatement"`...
    Paused {
        /// Why it stopped, as sent.
        why: Value,
        /// The frame it stopped in.
        frame: Option<FrameInfo>,
    },
    /// Running again.
    Resumed,
}

/// The outcome of an evaluation: the raw grip, plus whether it threw.
#[derive(Debug, Clone)]
pub struct Evaluation {
    /// The `result` grip: primitives come as JSON values, objects as
    /// `{type: "object", actor, class, preview}` and `undefined` as `{type: "undefined"}`.
    pub result: Value,
    /// The `exception` grip, when the expression threw. `result` is then meaningless.
    pub exception: Option<Value>,
    /// The whole `evaluationResult` packet.
    pub raw: Value,
}

impl Evaluation {
    /// Reads an `evaluationResult` packet.
    pub fn from_packet(packet: &RdpPacket) -> Self {
        Self {
            result: packet.body.get("result").cloned().unwrap_or(Value::Null),
            exception: packet
                .body
                .get("exception")
                .filter(|exception| !exception.is_null())
                .cloned(),
            raw: packet.body.clone(),
        }
    }
}

/// The entries of one resource type in a resource notification packet.
///
/// Reads `resources-available-array`, `resources-updated-array`, and
/// `resources-destroyed-array` packets. Any other packet, or another resource
/// type, yields nothing.
pub fn parse_resources(packet: &RdpPacket, resource_type: &str) -> Vec<Value> {
    match packet.r#type.as_deref() {
        Some(
            "resources-available-array" | "resources-updated-array" | "resources-destroyed-array",
        ) => {}
        _ => return Vec::new(),
    }
    let entries = packet.body.get("array").and_then(Value::as_array);
    entries
        .into_iter()
        .flatten()
        .filter_map(|entry| match entry.as_array()?.as_slice() {
            [kind, items] if kind == resource_type => items.as_array(),
            _ => None,
        })
        .flatten()
        .cloned()
        .collect()
}

/// The thread states a packet announces.
///
/// Firefox announces every pause twice: as a `thread-state` resource and as a
/// `paused` or `resumed` packet from the thread. Both are read; subscribe to one.
pub fn parse_thread_states(packet: &RdpPacket) -> Vec<ThreadState> {
    match packet.r#type.as_deref() {
        Some("resources-available-array") => parse_resources(packet, "thread-state")
            .iter()
            .filter_map(|entry| thread_state(entry, entry.get("state")?.as_str()?))
            .collect(),
        // The packet the thread itself sends carries the state as its type.
        Some(kind @ ("paused" | "resumed")) => {
            thread_state(&packet.body, kind).into_iter().collect()
        }
        _ => Vec::new(),
    }
}

/// The state `kind` names, read from `entry` (a `thread-state` resource or a thread packet).
fn thread_state(entry: &Value, kind: &str) -> Option<ThreadState> {
    match kind {
        "paused" => Some(ThreadState::Paused {
            why: entry.get("why").cloned().unwrap_or(Value::Null),
            frame: entry.get("frame").and_then(FrameInfo::from_value),
        }),
        "resumed" => Some(ThreadState::Resumed),
        _ => None,
    }
}

/// The sources a packet announces.
pub fn parse_sources(packet: &RdpPacket) -> Vec<SourceInfo> {
    parse_resources(packet, "source")
        .into_iter()
        .filter_map(|source| serde_json::from_value(source).ok())
        .collect()
}

/// Where `needle` first appears in `source`, as a breakpoint location.
///
/// The line is 1-based and the column 0-based, counted in UTF-16 units like
/// JavaScript. Only a needle within one line is found. The text of an inline
/// script is the whole document, so the line is the one the breakpoint uses.
pub fn locate(source_url: &str, source: &str, needle: &str) -> Option<BreakpointLocation> {
    // An empty needle would "match" everywhere, which says nothing.
    if needle.is_empty() {
        return None;
    }
    javascript_lines(source)
        .into_iter()
        .enumerate()
        .find_map(|(index, line)| {
            let offset = line.find(needle)?;
            Some(BreakpointLocation {
                source_url: source_url.to_string(),
                line: u32::try_from(index + 1).ok()?,
                column: Some(u32::try_from(line[..offset].encode_utf16().count()).ok()?),
            })
        })
}

/// The lines of `source` as JavaScript counts them: LF, CR, CRLF, U+2028 and
/// U+2029 all end a line, so the numbers match Firefox's.
fn javascript_lines(source: &str) -> Vec<&str> {
    let mut lines = Vec::new();
    let mut start = 0;
    let mut chars = source.char_indices().peekable();
    while let Some((index, c)) = chars.next() {
        if !matches!(c, '\n' | '\r' | '\u{2028}' | '\u{2029}') {
            continue;
        }
        lines.push(&source[start..index]);
        start = index + c.len_utf8();
        if c == '\r' && chars.next_if(|&(_, next)| next == '\n').is_some() {
            start += 1;
        }
    }
    lines.push(&source[start..]);
    lines
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::fixture;

    fn packet_from(name: &str) -> RdpPacket {
        RdpPacket::parse(&fixture(name).to_string()).expect("a fixture is a packet")
    }

    /// DBG-15: `parse_thread_states` reads a paused state, from either announcement.
    #[test]
    fn parse_thread_states_reads_paused() {
        // Given the pause as Firefox announces it, in its two forms
        for name in [
            "rdp/resources-thread-state-paused.json",
            "rdp/thread-state-paused.json",
        ] {
            // When it is parsed
            let states = parse_thread_states(&packet_from(name));

            // Then there is one paused state, with why and the frame it stopped in
            assert_eq!(states.len(), 1, "{name}");
            let ThreadState::Paused { why, frame } = &states[0] else {
                panic!("{name}: expected Paused, got {:?}", states[0]);
            };
            assert_eq!(why["type"], "breakpoint", "{name}");
            let frame = frame.as_ref().expect("a frame");
            assert_eq!(frame.display_name.as_deref(), Some("f"), "{name}");
            let location = frame.location.as_ref().expect("a location");
            assert!(location.line >= 7, "{name}: line {}", location.line);
        }
    }

    /// DBG-16: `parse_thread_states` reads a resumed state, from either announcement.
    #[test]
    fn parse_thread_states_reads_resumed() {
        for name in [
            "rdp/resources-thread-state-resumed.json",
            "rdp/thread-state-resumed.json",
        ] {
            let states = parse_thread_states(&packet_from(name));
            assert!(
                matches!(states.as_slice(), [ThreadState::Resumed]),
                "{name}: {states:?}"
            );
        }
    }

    /// DBG-42: Several states in one packet are all read, in order.
    #[test]
    fn parse_thread_states_reads_every_state_of_a_packet() {
        // Given what Firefox sends after a step: the resume and the new pause together
        let packet = RdpPacket::parse(
            &json!({
                "type": "resources-available-array",
                "from": "target1",
                "array": [["thread-state", [
                    { "state": "resumed" },
                    { "state": "paused", "why": { "type": "resumeLimit" } },
                ]]],
            })
            .to_string(),
        )
        .expect("a packet");

        // Then both come out, the resume first
        let states = parse_thread_states(&packet);
        assert!(
            matches!(states.as_slice(), [ThreadState::Resumed, ThreadState::Paused { why, .. }] if why["type"] == "resumeLimit"),
            "{states:?}"
        );
    }

    /// DBG-33: Packets that announce no thread state yield none.
    #[test]
    fn parse_thread_states_ignores_other_packets() {
        // Then neither a source announcement nor a plain reply is a thread state
        assert!(parse_thread_states(&packet_from("rdp/resources-sources.json")).is_empty());
        assert!(parse_thread_states(&packet_from("rdp/listTabs.json")).is_empty());
    }

    /// DBG-17: `parse_sources` ignores other resource types.
    #[test]
    fn parse_sources_ignores_other_resources() {
        // Given a packet carrying a console message and a source
        let packet = RdpPacket::parse(
            &json!({
                "type": "resources-available-array",
                "from": "target1",
                "array": [
                    ["console-message", [{ "message": "hi" }]],
                    ["source", [{ "actor": "source7", "url": "https://x/app.js" }]],
                ],
            })
            .to_string(),
        )
        .expect("a packet");

        // Then only the source is returned
        let sources = parse_sources(&packet);
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].actor, "source7");
        assert_eq!(sources[0].url.as_deref(), Some("https://x/app.js"));
        // And a source announcement captured from Firefox parses too
        let real = parse_sources(&packet_from("rdp/resources-sources.json"));
        assert_eq!(real.len(), 1);
        assert_eq!(
            real[0].url.as_deref(),
            Some("https://example.com/test.html")
        );
        assert_eq!(real[0].introduction_type.as_deref(), Some("scriptElement"));
    }

    /// DBG-33: `parse_resources` only reads resource arrays.
    #[test]
    fn parse_resources_only_reads_resource_arrays() {
        // Then a reply that happens to carry an `array` is not a resource announcement
        let reply = RdpPacket::parse(r#"{"from":"a","array":[["source",[{"actor":"s"}]]]}"#)
            .expect("a packet");
        assert!(parse_resources(&reply, "source").is_empty());
        // And an entry that is not a [type, items] pair is skipped, not a panic
        let odd = RdpPacket::parse(
            r#"{"from":"a","type":"resources-available-array","array":[1,["source"],["source",[{"actor":"s"}]]]}"#,
        )
        .expect("a packet");
        assert_eq!(parse_resources(&odd, "source").len(), 1);
    }

    /// DBG-54: `parse_resources` reads resources from available, updated, and destroyed arrays.
    #[test]
    fn parse_resources_reads_available_updated_and_destroyed() {
        // Given notifications for available, updated, and destroyed arrays
        for kind in [
            "resources-available-array",
            "resources-updated-array",
            "resources-destroyed-array",
        ] {
            let packet = RdpPacket::parse(
                &json!({
                    "from": "target1",
                    "type": kind,
                    "array": [["websocket", [{"wsMessageType": "frameSent", "data": {"payload": "hello"}}]]],
                })
                .to_string(),
            )
            .expect("a packet");

            // Then items are extracted regardless of notification flavor
            let items = parse_resources(&packet, "websocket");
            assert_eq!(items.len(), 1, "{kind}");
            assert_eq!(items[0]["wsMessageType"], "frameSent", "{kind}");
        }
    }

    /// DBG-18: `locate` finds the 1-based line and the 0-based column.
    #[test]
    fn locate_returns_one_based_line() {
        // Given a source with the needle on its third line, after two spaces
        let source = "a\nbb\n  nav_dropdown.value";

        // Then it is found at line 3, column 2
        let found = locate("https://x/app.js", source, "nav_dropdown.value").expect("found");
        assert_eq!(found.line, 3);
        assert_eq!(found.column, Some(2));
        assert_eq!(found.source_url, "https://x/app.js");
        // And an absent needle is None
        assert_eq!(locate("https://x/app.js", source, "missing"), None);
        // And an empty needle finds nothing
        assert_eq!(locate("https://x/app.js", source, ""), None);
        // And the column counts UTF-16 units, like JavaScript
        let wide = locate("u", "\u{1F600} x", "x").expect("found");
        assert_eq!(wide.column, Some(3));
    }

    /// DBG-45: `locate` counts every JavaScript line terminator.
    #[test]
    fn locate_counts_every_javascript_line_terminator() {
        // Then CR, CRLF, LINE SEPARATOR and PARAGRAPH SEPARATOR each end a line
        for source in [
            "a\rb\rneedle",
            "a\r\nb\r\nneedle",
            "a\u{2028}b\u{2028}needle",
            "a\u{2029}b\nneedle",
        ] {
            let found = locate("u", source, "needle").expect("found");
            assert_eq!(found.line, 3, "{source:?}");
        }
    }

    /// DBG-19: `BreakpointLocation::from_cdp` converts 0-based lines.
    #[test]
    fn breakpoint_location_from_cdp_converts_lines() {
        // Then CDP's line 11 is RDP's line 12, and the column is unchanged
        let location = BreakpointLocation::from_cdp("https://x/app.js", 11, 4);
        assert_eq!(
            location,
            BreakpointLocation {
                source_url: "https://x/app.js".to_string(),
                line: 12,
                column: Some(4),
            }
        );
    }

    /// DBG-31: `FrameInfo` reads actor, name and location from a captured frame.
    #[test]
    fn frame_info_reads_actor_name_and_location() {
        // Given the frames Firefox listed in the spike
        let packet = packet_from("rdp/frames.json");
        let frames = packet.body["frames"].as_array().expect("frames");

        // When they are read
        let infos: Vec<FrameInfo> = frames.iter().filter_map(FrameInfo::from_value).collect();

        // Then both come out, the first with its function and position
        assert_eq!(infos.len(), 2);
        assert_eq!(infos[0].display_name.as_deref(), Some("f"));
        let location = infos[0].location.as_ref().expect("a location");
        assert_eq!((location.line, location.column), (8, 12));
        assert!(location.actor.ends_with("source18"));
        // And the whole frame stays available
        assert_eq!(infos[0].raw["arguments"], json!([10]));
        // And a value that is not a frame is not one
        assert!(FrameInfo::from_value(&json!({ "displayName": "x" })).is_none());
    }

    /// DBG-32: `Evaluation` separates a result from an exception.
    #[test]
    fn evaluation_reports_result_and_exception() {
        // Given what Firefox sent for a successful evaluation
        let ok = Evaluation::from_packet(&packet_from("rdp/evaluation-result.json"));

        // Then there is a result and no exception
        assert!(ok.exception.is_none());
        assert_eq!(ok.raw["type"], "evaluationResult");

        // And a packet carrying an exception reports it, not an error
        let thrown = RdpPacket::parse(
            &json!({
                "from": "console1",
                "type": "evaluationResult",
                "exception": { "type": "object", "class": "Error" },
                "exceptionMessage": "Error: x",
            })
            .to_string(),
        )
        .expect("a packet");
        let thrown = Evaluation::from_packet(&thrown);
        assert_eq!(thrown.exception.expect("an exception")["class"], "Error");
        assert!(thrown.result.is_null());
    }

    /// DBG-34: `PauseOnExceptions` maps to the configuration Firefox expects.
    #[test]
    fn pause_on_exceptions_maps_to_configuration() {
        assert_eq!(
            PauseOnExceptions::None.configuration(),
            json!({ "pauseOnExceptions": false })
        );
        assert_eq!(
            PauseOnExceptions::Uncaught.configuration(),
            json!({ "pauseOnExceptions": true, "ignoreCaughtExceptions": true })
        );
        assert_eq!(
            PauseOnExceptions::All.configuration(),
            json!({ "pauseOnExceptions": true, "ignoreCaughtExceptions": false })
        );
    }
}
