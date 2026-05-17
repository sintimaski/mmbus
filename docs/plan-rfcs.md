# Post-v1 RFC execution plan — 2026-05-17

The three post-v1 RFCs (`docs/rfc-windows.md`, `docs/rfc-multi-machine.md`,
`docs/rfc-wal-replay.md` Phase B) are independent.  This doc records
the recommended order and the staged breakdown of the first.

## Strategic order

| # | RFC | Risk | Effort | Ship value | Why this order |
|---|-----|------|--------|------------|----------------|
| 1 | **Windows port** | Low — mechanical platform port with concrete acceptance criteria; existing `#[cfg]` walls are clean | ~1 focused week | Closes README "not yet"; biggest install-base unlock | Lowest design risk; in-crate; no new sub-project; doesn't depend on the other two |
| 2 | **mmbus-bridge** (multi-machine) | Medium — network protocol + security model, but RFC §7 MVP scope is well-defined (2-peer TCP mesh, drop-oldest, loop prevention) | ~2-3 weeks for MVP | Multi-machine pitch; "no broker" extends across hosts | Separate crate per RFC §1; independent of WAL.  Wait until Windows ships so the bridge has all three host OSes covered |
| 3 | **WAL Phase B** (durable replay) | High — file format, fsync policy, rotation, retention, index, ring↔WAL handoff race are all open design questions | Multi-week; needs its own RFC first | Most users who need this go to Kafka/JetStream anyway | RFC §3 itself says "design questions warrant a real spec"; defer until after Windows + bridge demand it |

## Windows port — staged breakdown

The RFC (`docs/rfc-windows.md`) maps every Linux/macOS primitive to a
Win32 substitute; this plan slices that into review-sized commits.

| Stage | Scope | Verification | Status |
|-------|-------|-------------|--------|
| **W0** | Plan doc + CI scaffold (Windows runner, `continue-on-error: true`) so build signal starts surfacing | `actions/runs` shows Windows job | shipped (b3a2b43) |
| **W1** | `producer_lock.rs` split per platform; `LockFileEx` for Windows.  `config.rs` default `base_dir` → `%LOCALAPPDATA%\mmbus` on Windows | `cargo check --target x86_64-pc-windows-msvc` locally; CI green | shipped (a67260f) |
| **W2** | `src/waker.rs` gains a `windows` module mirroring `linux`: `create_semaphore`, `semaphore_wake`, `semaphore_drain`, `wait_wakeup`, plus named-pipe (`create_pipe_instance`, `accept_pipe`, `connect_pipe`) + handshake (`send_handshake`, `recv_handshake_and_dup`) helpers | Cross-check on Windows target compiles clean | shipped (d849c90) |
| **W3** | `publisher.rs` Windows path: `Client.{_pipe, sem}`, dedicated accept-thread + `mpsc::Receiver<Client>`, `wake()` calls `semaphore_wake`, `Drop` joins the thread | type-check + clippy on Windows target | shipped (7de72b7) |
| **W4** | `subscriber.rs` Windows path: `connect_pipe` + `create_semaphore` + `send_handshake` at connect; `wait_wakeup` on (sem, pipe); `try_drain_wakeup` via `semaphore_drain`; `fileno()` returns `isize` (HANDLE value) on Windows | type-check + clippy on Windows target | shipped (7de72b7) |
| **W5** | Wheels: `windows-latest` × `x86_64-pc-windows-msvc` in `wheels.yml`.  CI: build + install via `pip install .` (uses maturin build backend) + run `python/smoke_test.py` on all three OSes (Windows-incompatible smokes skip themselves).  Windows CI gate flipped from `continue-on-error: true` to required (no flag in matrix) — first push must show Windows green; revert the flip if runtime issues appear that didn't surface in the cross-compile type-check. | First CI run shows Windows green; tagged release builds a Windows wheel | shipped |

### Open design questions for later stages

- **Async on Windows**: `loop.add_reader` doesn't work on Win32 handles; asyncio on Windows uses `ProactorEventLoop` (IOCP-backed) instead.  Implementing `AsyncSubscription` on Windows likely needs a separate code path that uses `ReadFile` with `OVERLAPPED` + IOCP.  Out of W5 scope; track as a follow-up.
- **Pipe namespace collision** (RFC §10): include user SID in pipe name.  Punt to W5 if security-conscious; default OK for v1.
- **Pipe ACL**: `InitializeSecurityDescriptor` with current-user SID — RFC §10 says "yes by default".  Implement in W3 when creating pipe instances.

### Non-goals for this push

- Performance parity with Linux (Win32 handle / pipe overhead is higher; document the gap).
- ARM64 Windows wheels (defer until x64 ships).
- Service Manager / Windows event log integration.

## mmbus-bridge — staged breakdown

| Stage | Scope | Status |
|-------|-------|--------|
| **B0** | New `bridge/` crate (alongside `fuzz/`); binary `mmbus-bridge`; TOML config; wire-frame codec (RFC §3 `Frame` struct) — 20 round-trip + edge-case tests | shipped |
| **B1** | `Bridge::start(config)` spawns one subscriber thread per forward-enabled topic + one TCP forwarder thread per peer.  Forwarders connect with exponential backoff, send `PeerHello` on connect, pump frames from an mpsc channel.  Integration test in `tests/forward_smoke.rs` drives the full chain (mmbus publish → bridge → TcpListener decode) | shipped |
| **B2** | New `listen = "host:port"` config field.  Bridge binds a `TcpListener` (when set), accepts connections, spawns a per-connection reader thread that decodes the frame stream, drops self-originated frames (loop prevention via `origin_id`), and forwards `Msg` frames in receive-listed topics to a single publisher thread that calls `Bus::publish`.  Integration test in `tests/receive_smoke.rs` drives the full chain (test peer → TCP → bridge → local mmbus subscribe) and asserts the loop-prevention drop | shipped |
| **B3** | New `queue` module: SPMC bounded drop-oldest queue (Mutex+Condvar, returns evicted count from `send`).  Per-peer `queue::channel(cfg.peer_buffer_max)` replaces `std::sync::mpsc` so a slow/disconnected peer can't stall the publisher; default cap 4096 messages per peer, overridable.  Mesh integration test in `tests/mesh_smoke.rs` configures 2 peers and asserts both receive the same hello + 4 Msg frames in order | shipped |
| **B4a** | PSK auth on the TCP transport: extend `PeerHello` payload to carry `(origin_id, psk_len, psk)`; receiver builds `accepted_psks: HashSet<Vec<u8>>` from `cfg.peers[*].preshared_key` and rejects any connection whose hello doesn't present a matching PSK.  `parse_peer_hello` codec helper added.  Integration test in `tests/psk_auth_smoke.rs` covers both the accept + the silent-drop-on-mismatch paths | shipped |
| **B4b** | QUIC transport (quinn) behind a feature flag; self-signed peer-pinned certs | open |
| **B5** | `python/mmbus/bridge.py` (subprocess wrapper: `run` foreground, `spawn` background, `BridgeNotFoundError` on missing binary) + `bridge/systemd/mmbus-bridge.service` template with sane hardening defaults; smoke covers the module import + error path | shipped |

The bridge crate is a standalone workspace (its `Cargo.toml` has an
empty `[workspace]` block) so the parent `cargo test` does not pull
in its network deps.  Build via `cd bridge && cargo test`.

## WAL Phase B

RFC drafted at [`rfc-wal-phase-b.md`](rfc-wal-phase-b.md).  Covers:
on-disk format (length-prefixed records, per-record CRC32C, segment
headers), three fsync policies (none / batched / each), rotation
(size-based, 64 MiB segments), retention (size-based, 1 GiB default),
in-memory index, ring↔WAL handoff race, crash recovery via
truncate-on-CRC-mismatch.  Implementation breaks into five staged
PRs (W1a..W1e); each builds on the previous and ships green
independently.
