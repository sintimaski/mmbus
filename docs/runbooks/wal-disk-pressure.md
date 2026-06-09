# Runbook: WAL disk pressure / retention thrashing

**Severity:** P2 (notify) when `WalStats.total_wal_bytes` stays
within 10% of `WalConfig::retention_bytes` for more than 5 min;
P1 (page) if the filesystem hits 95% capacity.

**Owner:** _whoever owns the bus operationally — usually the
team that turned WAL on for their topic._

## What the alert means

The WAL is at or near its retention cap.  Every publish forces
`enforce_retention_locked` to delete the oldest segment, which
means subscribers calling `subscribe_from(cursor)` with an older
cursor will hit `Error::CursorTooOld { oldest: ... }` even for
cursors that were valid seconds ago.

Symptoms:

- `WalStats.oldest_cursor` advances rapidly.
- Subscriber clients see `CursorTooOldError` exceptions where
  they didn't before.
- Disk-usage dashboards for the WAL volume show flat-near-cap
  utilisation.
- Publisher CPU shows a slight bump (the retention loop is on
  the publish hot path under
  `cfg.wal.retention_bytes < total + active`).

## How to diagnose

1. Snapshot the WAL stats:

   ```python
   import mmbus
   bus = mmbus.Bus("<name>")
   print(bus.stats("<topic>").wal)
   ```

   Check for `total_wal_bytes >= retention_bytes * 0.9` and a
   high segment count.

2. Look at the WAL directory:

   ```bash
   du -sh /tmp/mmbus/<bus_name>/wal/
   ls -la /tmp/mmbus/<bus_name>/wal/ | wc -l
   ```

3. Identify the publish rate:

   ```python
   print(bus.stats("<topic>").ring.tail)  # sample twice 10 s apart
   ```

   `(tail_2 - tail_1) / 10` is the live publish rate.  At 32 B
   payloads + ~60 B framing, ~1 MB/s of WAL traffic per
   ~10k publishes/s.

## How to resolve

Pick one based on the use case:

1. **Bump `retention_bytes`** (default 1 GiB) if disk has room.
   Requires restarting the publisher to take effect; the new
   value applies to subsequent retention checks immediately.

2. **Drop `segment_size_max`** (default 64 MiB) if you want
   finer-grained retention granularity at the cost of more
   segment files on disk.  Helpful when retention is fighting
   over a single fat segment.

3. **Move the WAL to a larger filesystem** (`BusConfig::base_dir`
   pointing at a different volume).  Publisher restart required.

4. **Disable the WAL for this topic** if durability isn't
   actually needed (`WalConfig::disabled()`).  Lose replay across
   restart but eliminate the disk-pressure source entirely.

## How to prevent recurrence

- Add a CHART tracking `WalStats.total_wal_bytes /
  retention_bytes` so the team sees the trend before it pages.
- Capacity-plan: at the topic's measured publish rate, how many
  seconds of retention does the cap give?  If less than 10×
  the longest subscriber outage, the cap is too low.
- Track in `docs/roadmap.md` if Phase-5 metrics (Prometheus
  export) would close the visibility gap.

## Related

- Code: `crates/mmbus/src/wal/wal.rs::enforce_retention_locked` (the loop).
- Code: `crates/mmbus/src/wal/wal.rs::append` (the per-publish pre-check).
- Spec: `docs/rfc-wal-phase-b.md` §7 (Retention).
- Spec: `docs/rfc-wal-v2-lockfree.md` §3 (v0.2 plan — pre-
  allocated segments make this more predictable).
