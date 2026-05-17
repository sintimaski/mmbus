# RFC: Replay for late subscribers

**Status:** Draft.  Phase A is small enough to ship in v0.2; Phase B is a
separate project worth its own RFC.
**Owner:** _unassigned_

## 1. Problem

A subscriber that calls `Bus::subscribe(topic)` claims its cursor at the
current ring tail and sees only messages published from that moment on.
A subscriber that connects late, or restarts after a crash, has no way
to receive messages it missed.  This blocks several common use cases:

| Use case | Current pain |
|---|---|
| **Worker job queue with crash recovery** | Worker crashes mid-job, restarts → never sees the work it was assigned before the crash. Forced to use a separate store (Redis, SQLite) just for durability. |
| **Late-joining log aggregator** | Aggregator starts up → only sees from "now"; the previous 5 minutes of events are gone. Operators rely on tailing logs through other channels until the aggregator "warms up". |
| **State-replica catch-up** | A replica process wants to mirror a primary's event stream; if it falls behind or restarts it cannot resume from a checkpoint. |
| **Debugging / audit** | Reproducing an incident requires replaying yesterday's messages — impossible without an external log capture. |

The common shape: "subscribers want to specify *where* to start reading,
not just *that* they want to start reading."

## 2. Design space

Three points, ordered by cost.

### Option A — In-ring history (size-bounded)

The ring already keeps the most recent `capacity` messages in shared
memory.  Today subscribers always claim a cursor at `current_tail`; if
they could claim at `current_tail - N` instead, they would replay the
last `N` messages on connect (capped at `capacity`).

The existing seqlock (`v4` wire format) already handles the race where
a subscriber requests an offset that the publisher has since overwritten:
`try_receive` detects the seq mismatch and skips forward to the slot's
current logical position.  So the *protocol* is already replay-safe —
only the *API* needs to expose the choice.

**Cost:** ~50 lines of code, no wire-format change, no new storage.

**Tradeoff:** Replay window bounded by `capacity` — typically seconds at
high rate, minutes at low rate.  Best-effort: if you ask for more
history than the ring holds you get whatever's still there.

### Option B — Append-only WAL file (durable)

Each topic gains a sibling file (`wal.log`) that the publisher appends
to on every publish.  The file is never truncated by the publisher
during normal operation; a background segment rotator/compactor manages
size on a separate cadence.

Subscribers can replay from any offset (or any timestamp) by reading the
WAL first, then transitioning to the live ring once they catch up.

**Cost:** Substantial.  Design questions include:
- **File format**: length-prefixed records; magic bytes; CRC per
  record for torn-write detection; per-segment metadata
- **Fsync policy**: every publish? Periodic? Configurable per topic?
- **Rotation**: by size (e.g., 100 MB / segment)? By time?
- **Retention**: rolling delete after N segments / N days?
- **Index**: tail → offset, timestamp → offset; both need accelerated
  lookup (we currently have neither)
- **WAL ↔ ring handoff**: subscriber reads WAL up to offset X, then
  transitions to ring at cursor X.  Race: messages published in the
  gap must not be lost.  Likely needs: (1) WAL fsync before ring write,
  (2) subscriber re-resolves the live cursor under a generation check.
- **Crash semantics**: WAL may have torn records on power loss; need a
  recovery scan + truncate-to-last-valid policy.
- **Disk cost**: 100 MB of small messages = a lot of metadata overhead

**Tradeoff:** Adds Kafka-like complexity.  Most users who want this
already use Kafka or NATS JetStream.

### Option C — Hybrid

Ring for hot path (fast, lossy), WAL for replay (slow, durable),
subscriber decides.  Probably what we'd land on long-term, but it's
just A + B with the handoff race solved.  Not a separate option — it
arrives by default once both A and B exist.

## 3. Recommendation

**Phase A (next release):** Ship Option A.  Implementable in v0.2.

**Phase B (separate project, post-1.0):** Option B with its own RFC,
because the design questions above each have multi-way trade-offs and
the API surface is big enough to warrant a real spec.

The good news: Option A's API is forward-compatible with Option B.  If
we add `Bus::subscribe_from(topic, offset)` now, Option B later just
extends the meaning of `offset` from "ring position" to "WAL position
that may be older than the ring".

## 4. Phase A API sketch

```rust
impl Bus {
    /// Subscribe starting `n` messages back from the current tail
    /// (capped at the ring capacity).  Zero = current behavior.
    pub fn subscribe_with_history(
        &self,
        topic: &str,
        n_messages_back: u64,
    ) -> Result<Subscription>;

    /// Subscribe starting at an explicit cursor.  Returns
    /// `Error::CursorTooOld` if the cursor is older than the oldest
    /// slot still in the ring (caller can decide: retry from `tail`,
    /// fail, or wait).
    pub fn subscribe_from(
        &self,
        topic: &str,
        cursor: u64,
    ) -> Result<Subscription>;
}
```

Implementation outline (one focused diff in `Subscriber::connect`):

1. After socket handshake (existing re-sync point), read `tail`.
2. Compute `start = tail.saturating_sub(n_messages_back)`.
3. `ring.set_cursor(cursor_idx, start)`.
4. The seqlock in `try_receive` handles the case where the slot at
   `start` has since been overwritten — caller sees the new seq and
   skips forward.  This is automatic; no extra code.

Test plan:
- Publish N messages; subscribe with `n_messages_back = N/2`; expect to
  receive `N/2` messages.
- Publish 10×capacity messages; subscribe with `n_messages_back = 5×capacity`;
  expect to receive *some* messages (`<= capacity`), all in order, no panics.
- Publish N; subscribe with explicit cursor 0; expect either all N
  messages OR `CursorTooOld`, depending on whether `N < capacity`.

## 5. Open questions

- Should the API accept a `Duration` ("last 5 minutes") instead of /
  in addition to a message count?  That requires timestamps per slot
  (another wire-format change).  Defer to Phase B; the message-count
  API is the minimum useful surface.
- Cursor stability across publisher restart: today the generation
  counter invalidates everything.  Should an explicit cursor passed to
  `subscribe_from` mean "from the current generation" only?  Yes,
  cleanest semantics; document it.
- For Phase B WAL: file path is `base_dir/bus/topic/wal.log` (sibling
  of `ring.mmap`).  Fixed for forward compat.

## 6. Out of scope

- Cross-bus replay (messages from bus A available to bus B).  That's
  the multi-machine RFC, not this one.
- "Exactly-once" delivery semantics.  Replay gives at-least-once; the
  consumer is responsible for idempotency.
- Encrypted at-rest WAL — separate concern.
