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
| **W0** | Plan doc + CI scaffold (Windows runner, `continue-on-error: true`) so build signal starts surfacing | `actions/runs` shows Windows job | next |
| **W1** | `producer_lock.rs` — split into `unix` / `windows` modules; `LockFileEx` for Windows.  `config.rs` default `base_dir` → `%LOCALAPPDATA%\mmbus` on Windows | `cargo check --target x86_64-pc-windows-msvc` locally; CI green |  |
| **W2** | Restructure `src/waker.rs` → `src/waker/{linux,windows,socket}.rs`.  Add `windows::{create_semaphore, semaphore_wake, semaphore_drain, wait_multi, bind_pipe, accept_pipe, connect_pipe, dup_handle_to_peer}` | Unit tests in `tests/waker_windows.rs` (Windows-gated) |  |
| **W3** | `publisher.rs` Windows path: `Client.handle: HANDLE` field, `accept_clients` uses `ConnectNamedPipe`, `wake()` releases the semaphore | `tests/spsc.rs` passes on Windows CI |  |
| **W4** | `subscriber.rs` Windows path: `CreateFile` on the pipe, `WaitForMultipleObjects(2, [semaphore, pipe], ...)`, `fileno()` returns `RawHandle`.  PyO3 wrapper: surface `fileno()` as the handle int; document that asyncio `add_reader` won't work on Windows (IOCP-only) — that's a follow-up | `tests/crash_recovery.rs` passes on Windows CI |  |
| **W5** | Wheels: add `windows-latest` × `x86_64-pc-windows-msvc` to `wheels.yml`.  CI: move Windows from `continue-on-error` to required.  Smoke: `python/smoke_test.py` on Windows runner (sync + backpressure paths; async + fastapi gated to non-Windows for now) | Tagged release builds a Windows wheel |  |

### Open design questions for later stages

- **Async on Windows**: `loop.add_reader` doesn't work on Win32 handles; asyncio on Windows uses `ProactorEventLoop` (IOCP-backed) instead.  Implementing `AsyncSubscription` on Windows likely needs a separate code path that uses `ReadFile` with `OVERLAPPED` + IOCP.  Out of W5 scope; track as a follow-up.
- **Pipe namespace collision** (RFC §10): include user SID in pipe name.  Punt to W5 if security-conscious; default OK for v1.
- **Pipe ACL**: `InitializeSecurityDescriptor` with current-user SID — RFC §10 says "yes by default".  Implement in W3 when creating pipe instances.

### Non-goals for this push

- Performance parity with Linux (Win32 handle / pipe overhead is higher; document the gap).
- ARM64 Windows wheels (defer until x64 ships).
- Service Manager / Windows event log integration.

## mmbus-bridge — staged breakdown (sketch only; defer until Windows ships)

| Stage | Scope |
|-------|-------|
| **B0** | New `bridge/` crate (alongside `fuzz/`); binary `mmbus-bridge`; config-file parsing |
| **B1** | Local subscribe + forward over TCP to one hardcoded peer endpoint |
| **B2** | Receive frames from peer, dedupe by `origin_id`, republish locally |
| **B3** | Mesh of N peers; per-peer ring buffer; drop-oldest on overflow |
| **B4** | QUIC transport (quinn) behind a feature flag; preshared-key auth |
| **B5** | Python helper `mmbus.bridge.start(config_path)`; systemd unit |

Wire-format is in RFC §3 (`Frame` struct); flesh into a tested binary
codec in B0.

## WAL Phase B — write a real RFC first

The current `rfc-wal-replay.md` defers Phase B explicitly.  Before
writing code, draft `rfc-wal-phase-b.md` that answers: file format
(length prefix, CRC, magic), fsync policy (per publish vs periodic),
rotation (size vs time), retention, index strategy, ring↔WAL handoff
race, crash recovery scan.  Each question has multi-way trade-offs;
the spec is the deliverable for the first session.
