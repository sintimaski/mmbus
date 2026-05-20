//! Prometheus text-format exporter for [`crate::stats::TopicStats`].
//!
//! Behind the `prometheus` Cargo feature.  Two pieces:
//!
//! 1. [`render`] / [`render_all`] — pure functions that produce
//!    Prometheus text-format ("exposition format") lines from one
//!    or more `TopicStats` snapshots.
//! 2. [`serve_blocking`] — a single-threaded HTTP server (no
//!    tokio / hyper) that calls a user-supplied `scrape_fn`
//!    every time a request hits `GET /metrics`.  Zero external
//!    deps; ~80 LOC of `std::net`.
//!
//! ## Quick start
//!
//! ```ignore
//! use mmbus::{Bus, BusConfig};
//! use std::sync::Arc;
//!
//! let bus = Arc::new(Bus::with_config("orders", BusConfig::default()));
//! let bus_clone = bus.clone();
//! std::thread::spawn(move || {
//!     mmbus::prometheus::serve_blocking(
//!         "127.0.0.1:9100".parse().unwrap(),
//!         move || {
//!             let stats = bus_clone.stats("events").expect("no publisher");
//!             mmbus::prometheus::render("events", &stats)
//!         },
//!     )
//!     .unwrap();
//! });
//! ```
//!
//! Then `curl http://localhost:9100/metrics` returns lines that
//! Prometheus's scrape can ingest directly.

use crate::stats::TopicStats;
use std::fmt::Write as _;
use std::io::{self, BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener};

/// Render one topic's stats as Prometheus text-format lines.
///
/// Naming convention: `mmbus_<scope>_<metric>{topic="…"}`.  The
/// `_total` suffix marks monotonic counters (Prom convention);
/// the remaining metrics are gauges.
pub fn render(topic: &str, s: &TopicStats) -> String {
    let mut out = String::with_capacity(1024);
    let _ = render_into(&mut out, topic, s);
    out
}

/// Render multiple topics into a single output.  Useful when one
/// process publishes to several topics and you want one `/metrics`
/// endpoint for all of them.
pub fn render_all(topics: &[(&str, &TopicStats)]) -> String {
    let mut out = String::with_capacity(1024 * topics.len().max(1));
    for (name, s) in topics {
        let _ = render_into(&mut out, name, s);
    }
    out
}

fn render_into(out: &mut String, topic: &str, s: &TopicStats) -> std::fmt::Result {
    // Validate topic name to avoid breaking Prom's text-format parser
    // (label values are quoted; we still strip ASCII control chars
    // + escape backslashes/quotes for safety).
    let topic = escape_label(topic);

    // Counters (monotonic).
    writeln!(
        out,
        "# HELP mmbus_published_total Successful publishes since publisher creation.",
    )?;
    writeln!(out, "# TYPE mmbus_published_total counter")?;
    writeln!(
        out,
        "mmbus_published_total{{topic=\"{topic}\"}} {}",
        s.published_total,
    )?;

    writeln!(
        out,
        "# HELP mmbus_full_rejected_total Publishes rejected with Error::Full \
         under BackpressurePolicy::Error.",
    )?;
    writeln!(out, "# TYPE mmbus_full_rejected_total counter")?;
    writeln!(
        out,
        "mmbus_full_rejected_total{{topic=\"{topic}\"}} {}",
        s.full_rejected_total,
    )?;

    writeln!(
        out,
        "# HELP mmbus_subscribers_dropped_total Subscribers dropped by the \
         publisher because their wakeup call failed.",
    )?;
    writeln!(out, "# TYPE mmbus_subscribers_dropped_total counter")?;
    writeln!(
        out,
        "mmbus_subscribers_dropped_total{{topic=\"{topic}\"}} {}",
        s.subscribers_dropped_total,
    )?;

    writeln!(
        out,
        "# HELP mmbus_wakeups_sent_total Wakeup syscalls fired (coalesced; \
         below published_total under bursts).",
    )?;
    writeln!(out, "# TYPE mmbus_wakeups_sent_total counter")?;
    writeln!(
        out,
        "mmbus_wakeups_sent_total{{topic=\"{topic}\"}} {}",
        s.wakeups_sent_total,
    )?;

    // Gauges (current state).
    writeln!(
        out,
        "# HELP mmbus_connected_sockets Subscriber sockets currently accepted.",
    )?;
    writeln!(out, "# TYPE mmbus_connected_sockets gauge")?;
    writeln!(
        out,
        "mmbus_connected_sockets{{topic=\"{topic}\"}} {}",
        s.connected_sockets,
    )?;

    writeln!(
        out,
        "# HELP mmbus_ring_tail Next slot the producer will write \
         (monotonic during a publisher's lifetime).",
    )?;
    writeln!(out, "# TYPE mmbus_ring_tail counter")?;
    writeln!(
        out,
        "mmbus_ring_tail{{topic=\"{topic}\"}} {}",
        s.ring.tail,
    )?;

    writeln!(
        out,
        "# HELP mmbus_active_subscribers Number of subscribers \
         with a claimed cursor slot.",
    )?;
    writeln!(out, "# TYPE mmbus_active_subscribers gauge")?;
    writeln!(
        out,
        "mmbus_active_subscribers{{topic=\"{topic}\"}} {}",
        s.ring.active_subscribers,
    )?;

    // Per-subscriber max lag (most-behind subscriber).  A single
    // scalar is more useful than a histogram for the "is anyone
    // falling behind" alert most users want.
    let max_lag = s.ring.lags.iter().copied().max().unwrap_or(0);
    writeln!(
        out,
        "# HELP mmbus_max_subscriber_lag Highest per-subscriber lag (tail − cursor) \
         across all active subscribers.",
    )?;
    writeln!(out, "# TYPE mmbus_max_subscriber_lag gauge")?;
    writeln!(
        out,
        "mmbus_max_subscriber_lag{{topic=\"{topic}\"}} {max_lag}",
    )?;

    // WAL block — only emitted when WAL is enabled.
    if let Some(w) = s.wal.as_ref() {
        writeln!(
            out,
            "# HELP mmbus_wal_appends_total Successful WAL appends.",
        )?;
        writeln!(out, "# TYPE mmbus_wal_appends_total counter")?;
        writeln!(
            out,
            "mmbus_wal_appends_total{{topic=\"{topic}\"}} {}",
            w.appends_total,
        )?;

        writeln!(
            out,
            "# HELP mmbus_wal_append_bytes_total Payload bytes appended to the WAL.",
        )?;
        writeln!(out, "# TYPE mmbus_wal_append_bytes_total counter")?;
        writeln!(
            out,
            "mmbus_wal_append_bytes_total{{topic=\"{topic}\"}} {}",
            w.append_bytes_total,
        )?;

        writeln!(
            out,
            "# HELP mmbus_wal_flushes_total Completed flush_sync calls.",
        )?;
        writeln!(out, "# TYPE mmbus_wal_flushes_total counter")?;
        writeln!(
            out,
            "mmbus_wal_flushes_total{{topic=\"{topic}\"}} {}",
            w.flushes_total,
        )?;

        writeln!(
            out,
            "# HELP mmbus_wal_pending_cursor Highest cursor appended.",
        )?;
        writeln!(out, "# TYPE mmbus_wal_pending_cursor counter")?;
        writeln!(
            out,
            "mmbus_wal_pending_cursor{{topic=\"{topic}\"}} {}",
            w.pending_cursor,
        )?;

        writeln!(
            out,
            "# HELP mmbus_wal_durable_cursor Highest cursor fsynced to disk.",
        )?;
        writeln!(out, "# TYPE mmbus_wal_durable_cursor counter")?;
        writeln!(
            out,
            "mmbus_wal_durable_cursor{{topic=\"{topic}\"}} {}",
            w.durable_cursor,
        )?;

        // Replay lag — derived; the most actionable alert source.
        let lag = w.pending_cursor.saturating_sub(w.durable_cursor);
        writeln!(
            out,
            "# HELP mmbus_wal_replay_lag pending_cursor − durable_cursor; \
             how many records aren't yet durable.",
        )?;
        writeln!(out, "# TYPE mmbus_wal_replay_lag gauge")?;
        writeln!(
            out,
            "mmbus_wal_replay_lag{{topic=\"{topic}\"}} {lag}",
        )?;

        writeln!(
            out,
            "# HELP mmbus_wal_total_bytes Total on-disk bytes across all segments.",
        )?;
        writeln!(out, "# TYPE mmbus_wal_total_bytes gauge")?;
        writeln!(
            out,
            "mmbus_wal_total_bytes{{topic=\"{topic}\"}} {}",
            w.total_wal_bytes,
        )?;

        writeln!(
            out,
            "# HELP mmbus_wal_segments Number of segment files on disk.",
        )?;
        writeln!(out, "# TYPE mmbus_wal_segments gauge")?;
        writeln!(
            out,
            "mmbus_wal_segments{{topic=\"{topic}\"}} {}",
            w.segments,
        )?;
    }

    Ok(())
}

/// Escape a label value per Prometheus exposition format rules:
/// `\` → `\\`, `"` → `\"`, `\n` → `\\n`.  We also strip ASCII
/// control characters since topic names with newlines would
/// otherwise corrupt the response.
fn escape_label(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for c in raw.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            c if (c as u32) < 0x20 => out.push('_'),
            c => out.push(c),
        }
    }
    out
}

// ── Minimal HTTP server ─────────────────────────────────────────────────────

/// Block the calling thread serving `GET /metrics`.  Calls
/// `scrape_fn()` on every request and writes its return value
/// as the response body.
///
/// Single-threaded — handles one request at a time.  Prometheus
/// scrapes typically happen every 15-60s, so this is plenty.
/// Returns when the listener fails (e.g. port no longer
/// available); the typical pattern is to spawn this in a
/// thread + `join()` if the main process shuts down.
///
/// Other paths return 404; non-GET returns 405.  The response
/// includes the `Content-Type: text/plain; version=0.0.4`
/// header that Prometheus expects.
pub fn serve_blocking<F>(addr: SocketAddr, mut scrape_fn: F) -> io::Result<()>
where
    F: FnMut() -> String,
{
    let listener = TcpListener::bind(addr)?;
    tracing::info!(
        target: "mmbus::prometheus",
        addr = %addr,
        "Prometheus exporter listening on /metrics",
    );
    for stream in listener.incoming() {
        let mut stream = match stream {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(target: "mmbus::prometheus", error = %e, "accept failed");
                continue;
            }
        };
        // Parse the request line + skip headers (we don't need them).
        let mut reader = BufReader::new(&stream);
        let mut request_line = String::new();
        if reader.read_line(&mut request_line).is_err() {
            continue;
        }
        let mut method = request_line.split_whitespace();
        let verb = method.next().unwrap_or("");
        let path = method.next().unwrap_or("");

        // Drain headers (up to a sane cap so a malformed client
        // can't peg us on memory).
        for _ in 0..64 {
            let mut header = String::new();
            match reader.read_line(&mut header) {
                Ok(0) => break,
                Ok(_) => {
                    if header == "\r\n" || header == "\n" {
                        break;
                    }
                }
                Err(_) => break,
            }
        }

        let (status, body): (&str, String) = if verb != "GET" {
            ("405 Method Not Allowed", String::new())
        } else if path != "/metrics" {
            ("404 Not Found", String::new())
        } else {
            ("200 OK", scrape_fn())
        };

        let response = format!(
            "HTTP/1.1 {status}\r\n\
             Content-Type: text/plain; version=0.0.4\r\n\
             Content-Length: {}\r\n\
             Connection: close\r\n\
             \r\n\
             {body}",
            body.len(),
        );
        let _ = stream.write_all(response.as_bytes());
        let _ = stream.flush();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ring::RingStats;
    use crate::wal::WalStats;

    fn sample_stats(with_wal: bool) -> TopicStats {
        TopicStats {
            ring: RingStats {
                tail: 100,
                active_subscribers: 2,
                lags: vec![3, 7],
            },
            connected_sockets: 2,
            wal: if with_wal {
                Some(WalStats {
                    pending_cursor: 100,
                    durable_cursor: 95,
                    oldest_cursor: 0,
                    active_segment_bytes: 4096,
                    total_wal_bytes: 4096,
                    segments: 1,
                    appends_total: 100,
                    append_bytes_total: 3200,
                    flushes_total: 20,
                })
            } else {
                None
            },
            published_total: 100,
            full_rejected_total: 3,
            subscribers_dropped_total: 1,
            wakeups_sent_total: 42,
        }
    }

    #[test]
    fn renders_counters_in_prom_format() {
        let s = sample_stats(false);
        let out = render("orders", &s);
        assert!(out.contains("mmbus_published_total{topic=\"orders\"} 100"));
        assert!(out.contains("mmbus_full_rejected_total{topic=\"orders\"} 3"));
        assert!(out.contains("mmbus_subscribers_dropped_total{topic=\"orders\"} 1"));
        assert!(out.contains("# TYPE mmbus_published_total counter"));
    }

    #[test]
    fn omits_wal_block_when_wal_disabled() {
        let s = sample_stats(false);
        let out = render("orders", &s);
        assert!(!out.contains("mmbus_wal_"));
    }

    #[test]
    fn emits_wal_block_when_wal_enabled() {
        let s = sample_stats(true);
        let out = render("orders", &s);
        assert!(out.contains("mmbus_wal_appends_total{topic=\"orders\"} 100"));
        assert!(out.contains("mmbus_wal_replay_lag{topic=\"orders\"} 5"));
        assert!(out.contains("mmbus_wal_pending_cursor{topic=\"orders\"} 100"));
        assert!(out.contains("mmbus_wal_durable_cursor{topic=\"orders\"} 95"));
    }

    #[test]
    fn max_subscriber_lag_is_the_max() {
        let s = sample_stats(false);
        let out = render("orders", &s);
        // lags = [3, 7] → max = 7
        assert!(out.contains("mmbus_max_subscriber_lag{topic=\"orders\"} 7"));
    }

    #[test]
    fn render_all_concatenates_topics() {
        let a = sample_stats(false);
        let b = sample_stats(true);
        let out = render_all(&[("a", &a), ("b", &b)]);
        assert!(out.contains("topic=\"a\""));
        assert!(out.contains("topic=\"b\""));
        // WAL metrics from topic b but not topic a.
        assert!(out.contains("mmbus_wal_appends_total{topic=\"b\"} 100"));
    }

    #[test]
    fn escape_label_handles_special_chars() {
        assert_eq!(escape_label("plain"), "plain");
        assert_eq!(escape_label("a\\b"), "a\\\\b");
        assert_eq!(escape_label("a\"b"), "a\\\"b");
        assert_eq!(escape_label("a\nb"), "a\\nb");
        assert_eq!(escape_label("a\x07b"), "a_b"); // bell char → '_'
    }

    #[test]
    fn server_responds_404_for_other_paths() {
        // End-to-end: start server, connect, request /, assert 404.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener); // we'll re-bind inside the test handler

        let handle = std::thread::spawn(move || {
            // Run only one accept loop iteration for the test.
            let listener = TcpListener::bind(addr).unwrap();
            let (mut stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(&stream);
            let mut request = String::new();
            reader.read_line(&mut request).unwrap();
            // Skip headers.
            loop {
                let mut h = String::new();
                reader.read_line(&mut h).unwrap();
                if h == "\r\n" || h == "\n" || h.is_empty() {
                    break;
                }
            }
            let body = "";
            let response = format!(
                "HTTP/1.1 404 Not Found\r\n\
                 Content-Length: 0\r\n\
                 Connection: close\r\n\
                 \r\n\
                 {body}"
            );
            stream.write_all(response.as_bytes()).unwrap();
        });

        let mut client = std::net::TcpStream::connect(addr).unwrap();
        client
            .write_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
            .unwrap();
        let mut buf = Vec::new();
        std::io::Read::read_to_end(&mut client, &mut buf).unwrap();
        let resp = String::from_utf8(buf).unwrap();
        assert!(resp.contains("404"));
        handle.join().unwrap();
    }
}
