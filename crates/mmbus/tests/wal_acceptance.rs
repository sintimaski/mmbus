//! WAL Phase B acceptance scenarios (RFC §15 / plan W1-f).
//!
//! Each scenario runs against all three fsync policies where the
//! semantics are policy-independent (crash recovery + cursor
//! monotonicity), and against the specific policy whose guarantee is
//! being verified (durable_cursor for Each vs Batched).

use mmbus::wal::{FsyncPolicy, WalConfig};
use mmbus::{BusConfig, Publisher, StartPos, Subscriber};
use std::time::Duration;

fn cfg(name: &str, policy: FsyncPolicy) -> BusConfig {
    BusConfig {
        capacity: 16,
        slot_size: 64,
        base_dir: std::env::temp_dir().join("mmbus_wal_acc").join(name),
        wal: WalConfig {
            enabled: true,
            fsync_policy: policy,
            fsync_interval: Duration::from_millis(10),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn cleanup(cfg: &BusConfig) {
    let _ = std::fs::remove_dir_all(&cfg.base_dir);
}

fn run_policy(label: &str, policy: FsyncPolicy, scenario: impl FnOnce(&BusConfig)) {
    let cfg = cfg(label, policy);
    cleanup(&cfg);
    scenario(&cfg);
    cleanup(&cfg);
}

/// Scenario A: publisher restart preserves every prior record (the
/// core durability promise).  Asserted under Each and Batched (after
/// a fsync_interval grace).
#[test]
fn scenario_a_crash_recovery_preserves_records_each() {
    run_policy("a_each", FsyncPolicy::Each, |cfg| {
        const N: u64 = 25;
        {
            let mut p = Publisher::create("bus", cfg.clone()).unwrap();
            for i in 0..N {
                p.publish(&i.to_le_bytes()).unwrap();
            }
            // Drop without explicit close — simulates abrupt shutdown.
            drop(p);
        }
        let _p2 = Publisher::create("bus", cfg.clone()).unwrap();
        let mut sub =
            Subscriber::connect_with("bus", cfg, Duration::from_secs(5), StartPos::Explicit(0))
                .unwrap();
        for i in 0..N {
            let got = sub
                .receive_timeout(Duration::from_secs(2))
                .unwrap()
                .expect("WAL must replay every prior record");
            assert_eq!(u64::from_le_bytes(got.try_into().unwrap()), i);
        }
    });
}

/// Scenario A under Batched: durable_cursor catches up to pending
/// after the flusher tick, then crash recovery yields every record
/// up to durable_cursor at crash time (here: we explicitly fsync
/// before drop, so == pending).
#[test]
fn scenario_a_crash_recovery_preserves_records_batched() {
    run_policy("a_batched", FsyncPolicy::Batched, |cfg| {
        const N: u64 = 25;
        {
            let mut p = Publisher::create("bus", cfg.clone()).unwrap();
            for i in 0..N {
                p.publish(&i.to_le_bytes()).unwrap();
            }
            // Wait for the batched flusher to catch up before dropping.
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            loop {
                let s = p.stats().wal.unwrap();
                if s.durable_cursor == s.pending_cursor {
                    break;
                }
                if std::time::Instant::now() >= deadline {
                    panic!("batched flusher never caught up");
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            drop(p);
        }
        let _p2 = Publisher::create("bus", cfg.clone()).unwrap();
        let mut sub =
            Subscriber::connect_with("bus", cfg, Duration::from_secs(5), StartPos::Explicit(0))
                .unwrap();
        for i in 0..N {
            let got = sub
                .receive_timeout(Duration::from_secs(2))
                .unwrap()
                .expect("WAL must replay every durable record");
            assert_eq!(u64::from_le_bytes(got.try_into().unwrap()), i);
        }
    });
}

/// Scenario B: cursors are globally monotonic across publisher
/// restarts — the second generation continues numbering where the
/// first left off (W1-d ring/WAL cursor alignment).
#[test]
fn scenario_b_cursors_monotonic_across_restarts() {
    run_policy("b_monotonic", FsyncPolicy::Each, |cfg| {
        {
            let mut p = Publisher::create("bus", cfg.clone()).unwrap();
            for i in 0..5u64 {
                p.publish(&i.to_le_bytes()).unwrap();
            }
            assert_eq!(p.stats().ring.tail, 5);
            drop(p);
        }
        let mut p2 = Publisher::create("bus", cfg.clone()).unwrap();
        assert_eq!(p2.stats().ring.tail, 5, "second-gen tail resumes from WAL");
        for i in 5..10u64 {
            p2.publish(&i.to_le_bytes()).unwrap();
        }
        assert_eq!(p2.stats().ring.tail, 10);
        assert_eq!(p2.stats().wal.unwrap().pending_cursor, 10);
    });
}

/// Scenario C: retention deletes oldest segments past
/// `retention_bytes`; a subscribe_from(cursor) older than that gets
/// `CursorTooOld { oldest: wal.oldest_cursor }`.
#[test]
fn scenario_c_retention_drops_oldest_and_surfaces_too_old() {
    let cfg = BusConfig {
        capacity: 16,
        slot_size: 32,
        base_dir: std::env::temp_dir()
            .join("mmbus_wal_acc")
            .join("c_retention"),
        wal: WalConfig {
            enabled: true,
            fsync_policy: FsyncPolicy::Each,
            segment_size_max: 200,
            retention_bytes: 250,
            fsync_interval: Duration::from_millis(5),
            ..Default::default()
        },
        ..Default::default()
    };
    cleanup(&cfg);
    let mut p = Publisher::create("bus", cfg.clone()).unwrap();
    for i in 0..40u64 {
        p.publish(&[i as u8; 16]).unwrap();
    }
    let stats = p.stats().wal.unwrap();
    assert!(
        stats.oldest_cursor > 0,
        "retention must have dropped older cursors"
    );
    drop(p);

    let _p2 = Publisher::create("bus", cfg.clone()).unwrap();
    match Subscriber::connect_with("bus", &cfg, Duration::from_secs(2), StartPos::Explicit(0)) {
        Err(mmbus::Error::CursorTooOld {
            requested: 0,
            oldest,
        }) => {
            assert!(oldest > 0, "CursorTooOld must surface WAL's oldest, not 0");
        }
        Ok(_) => panic!("expected CursorTooOld for cursor 0"),
        Err(e) => panic!("expected CursorTooOld, got {e:?}"),
    }
    cleanup(&cfg);
}

/// Scenario D: under FsyncPolicy::Each, every successful publish leaves
/// durable_cursor == pending_cursor (no replay-on-restart drift).
#[test]
fn scenario_d_each_policy_keeps_durable_eq_pending() {
    run_policy("d_each_durable", FsyncPolicy::Each, |cfg| {
        let mut p = Publisher::create("bus", cfg.clone()).unwrap();
        for i in 0..10u64 {
            p.publish(&i.to_le_bytes()).unwrap();
            let s = p.stats().wal.unwrap();
            assert_eq!(s.durable_cursor, s.pending_cursor);
            assert_eq!(s.pending_cursor, i + 1);
        }
    });
}

/// Scenario E: under FsyncPolicy::None, pending_cursor advances per
/// publish but durable_cursor stays at the open value — confirming the
/// policy is a pure performance dial with no inline fsync.
#[test]
fn scenario_e_none_policy_does_not_advance_durable_cursor() {
    run_policy("e_none_durable", FsyncPolicy::None, |cfg| {
        let mut p = Publisher::create("bus", cfg.clone()).unwrap();
        let durable_at_open = p.stats().wal.unwrap().durable_cursor;
        for i in 0..5u64 {
            p.publish(&i.to_le_bytes()).unwrap();
        }
        let s = p.stats().wal.unwrap();
        assert_eq!(s.pending_cursor, 5);
        assert_eq!(
            s.durable_cursor, durable_at_open,
            "None policy must NOT advance durable_cursor"
        );
    });
}

/// Scenario F: WAL→ring handoff under sustained publish load — the
/// subscriber catches up via WAL and then continues from the live
/// ring without dropping records in between.
#[test]
fn scenario_f_handoff_under_live_publishing() {
    run_policy("f_handoff", FsyncPolicy::Each, |cfg| {
        {
            let mut p = Publisher::create("bus", cfg.clone()).unwrap();
            for i in 0..10u64 {
                p.publish(&i.to_le_bytes()).unwrap();
            }
            drop(p);
        }
        let mut p2 = Publisher::create("bus", cfg.clone()).unwrap();
        let mut sub =
            Subscriber::connect_with("bus", cfg, Duration::from_secs(5), StartPos::Explicit(0))
                .unwrap();
        p2.wait_for_subscribers(1, Duration::from_secs(5)).unwrap();

        // WAL prefix.
        for i in 0..10u64 {
            let got = sub
                .receive_timeout(Duration::from_secs(2))
                .unwrap()
                .expect("wal record");
            assert_eq!(u64::from_le_bytes(got.try_into().unwrap()), i);
        }
        // Live ring tail.
        for i in 10..20u64 {
            p2.publish(&i.to_le_bytes()).unwrap();
        }
        for i in 10..20u64 {
            let got = sub
                .receive_timeout(Duration::from_secs(2))
                .unwrap()
                .expect("live record");
            assert_eq!(u64::from_le_bytes(got.try_into().unwrap()), i);
        }
    });
}
