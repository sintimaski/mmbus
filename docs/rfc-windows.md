# RFC: Windows support

**Status:** Draft.  Identifies every platform-dependent line in the
crate today and proposes the Win32 substitute, so a future contributor
can pick this up without re-discovering the design space.
**Owner:** _unassigned_

## 1. Why this is non-trivial

Five distinct kernel primitives we currently rely on do not exist on
Windows, and not all of them have a one-to-one substitute:

| POSIX primitive | Where we use it | Win32 substitute |
|---|---|---|
| `mmap` shared file | `RingBuffer` (`memmap2`) | `CreateFileMapping` + `MapViewOfFile` — already abstracted by `memmap2` 0.9, no code change |
| `flock(LOCK_EX \| LOCK_NB)` | producer-lock for exclusive publisher | `LockFileEx(LOCKFILE_EXCLUSIVE_LOCK \| LOCKFILE_FAIL_IMMEDIATELY)` — close semantics; needs platform branch |
| `AF_UNIX` listener + connect | publisher↔subscriber handshake socket | Named pipe (`\\.\pipe\mmbus-<bus>-<topic>-signal`) via `CreateNamedPipe` / `ConnectNamedPipe` / `CreateFile` |
| `eventfd(EFD_SEMAPHORE)` (Linux) | low-overhead per-message wakeup | Win32 **semaphore** (`CreateSemaphore`) with `ReleaseSemaphore` / `WaitForSingleObject`.  Closest analog: each release wakes one waiter and decrements count, matching `EFD_SEMAPHORE`. |
| `SCM_RIGHTS` fd passing | handing the subscriber's eventfd to the publisher (Linux) | `DuplicateHandle(GetCurrentProcess(), src_handle, peer_proc, ...)` after exchanging `GetCurrentProcessId()` over the named pipe |
| `poll(2)` on (eventfd, socket) | blocking wait with disconnect detection (Linux) | `WaitForMultipleObjects(2, [semaphore_handle, pipe_handle], FALSE, timeout)` |
| `MSG_NOSIGNAL` send / `SO_NOSIGPIPE` | suppress SIGPIPE on broken socket | Not needed on Windows (no SIGPIPE) |

Wire format (`v4` and beyond): **bytes are bytes**.  The on-disk
header is endian-explicit (`u64`/`u32` little-endian) and contains no
OS-dependent values.  A mmap file written on Linux is byte-identical
to one written on Windows.  This means a future *cross-OS bridge*
could share state, though we don't promise that.

## 2. Process model

Windows lacks `fork(2)`.  We never call it — all our concurrency is
threads + cross-process file/handle sharing — so no change there.

Process death detection (which today drives `POLLHUP` on the
handshake socket so subscribers notice publisher crashes) is provided
on Windows by `WaitForSingleObject(process_handle, 0)` returning
`WAIT_OBJECT_0`, **or** by the named pipe returning
`ERROR_BROKEN_PIPE` on the next op.  We'd prefer the latter — same
event-driven shape as POSIX.

## 3. Producer lock

`LockFileEx` semantics are very close to `flock(LOCK_EX | LOCK_NB)`:

* Held by the *process*, not the fd (matches Linux `flock`; differs
  from POSIX `fcntl` byte-range locks).
* Released when the handle closes OR when the process dies.
* `LOCKFILE_FAIL_IMMEDIATELY` gives us our non-blocking try.

**However**: the macOS `flock` is per-process (all fds opened by the
same process share one lock record), which is why we maintain
`IN_PROCESS_LOCKS` to detect same-process duplicates.  On Windows
`LockFileEx` is *also* per-process — so the same `IN_PROCESS_LOCKS`
HashSet works there too.  No new code; just `#[cfg(unix)]` becomes
`#[cfg(any(unix, windows))]` for the in-process check, and the
`libc::flock` call gets a `windows::Win32::Storage::FileSystem::LockFileEx`
peer behind a `#[cfg(windows)]` branch.

## 4. Wakeup primitive

The Linux `eventfd(EFD_SEMAPHORE)` lets a subscriber wait, and a
publisher signal, with a counter that increments on `write(1)` and
decrements by 1 on each `read`.  Multiple writes queue up; multiple
reads drain them.

Windows semaphore via `CreateSemaphore(NULL, 0, max_count, name)` has
near-identical semantics:

* `ReleaseSemaphore(h, 1, NULL)` increments the count by 1 → matches
  `eventfd_write(fd, 1)`.
* `WaitForSingleObject(h, INFINITE)` blocks until count > 0, then
  decrements by 1 → matches `read(eventfd, ..)` under `EFD_SEMAPHORE`.

**Handle passing**: same SCM_RIGHTS dance, different API.  After the
named-pipe connect handshake, both sides exchange their process IDs
(small fixed message).  The subscriber then calls
`DuplicateHandle(GetCurrentProcess(), semaphore, publisher_proc, ...)`
to give the publisher a usable handle in its own process.  The
duplicated handle value is then sent back over the pipe.

## 5. Listener / connect

Named pipes (`\\.\pipe\mmbus-{bus}-{topic}-signal`) replace
`UnixListener`/`UnixStream`.  Server side:

```rust
let pipe = CreateNamedPipe(
    name, PIPE_ACCESS_DUPLEX | FILE_FLAG_OVERLAPPED,
    PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_REJECT_REMOTE_CLIENTS,
    PIPE_UNLIMITED_INSTANCES,
    /* buffer sizes */ 4096, 4096,
    /* timeout */ 0, ptr::null_mut(),
);
ConnectNamedPipe(pipe, &mut overlapped); // accept analog
```

Client side: `CreateFile(name, ..)` on the pipe name.

Two non-obvious points:

1. **One instance per connection**: unlike `AF_UNIX` listeners,
   each named-pipe `CreateNamedPipe` call returns a single
   instance.  To accept multiple subscribers we either call it in a
   loop or pass `PIPE_UNLIMITED_INSTANCES` and create a new instance
   after each accept.  The current `accept_clients` loop maps cleanly.

2. **Async I/O model**: Windows pipes are most efficient with
   `OVERLAPPED` I/O and IOCP.  For our use case (1-byte handshake +
   handle duplication, then nothing) the synchronous path is fine.

## 6. Cargo features and `#[cfg]` strategy

Today:

```
#[cfg(target_os = "linux")]   eventfd + SCM_RIGHTS path
#[cfg(not(target_os = "linux"))]   socket-byte path (macOS + Linux fallback)
#[cfg(target_os = "macos")]   SO_NOSIGPIPE
```

Proposed:

```
#[cfg(target_os = "linux")]   eventfd + SCM_RIGHTS
#[cfg(target_os = "windows")] semaphore + DuplicateHandle + named pipes
#[cfg(any(target_os = "macos", target_os = "freebsd", ...))]  socket-byte path
#[cfg(unix)]                  UnixListener/UnixStream
#[cfg(windows)]               named pipe equivalents
```

The `Waker` abstraction in `src/waker.rs` would gain a `windows`
module alongside the `linux` one.  `bus.rs`'s `Client` / `Subscriber`
gain a `#[cfg(windows)] handle: HANDLE` field next to the existing
`efd: OwnedFd` Linux field.  Two parallel branches, neatly walled
off.

## 7. CI matrix

`.github/workflows/ci.yml` would add:

```yaml
strategy:
  matrix:
    os: [ubuntu-latest, macos-latest, windows-latest]
```

`.github/workflows/wheels.yml` adds:

```yaml
- os: windows-latest
  target: x86_64-pc-windows-msvc
```

Maturin already supports Windows wheels; no build-system surgery.

## 8. Acceptance criteria — "Windows works" means:

* `cargo test` passes on `windows-latest`
* `cargo bench --bench ring` runs to completion (perf may differ)
* `maturin develop` builds and installs the wheel
* The `python/smoke_test.py` round-trip passes
* A new `tests/windows_crash_recovery.rs` (or windows-gate the existing one)
  exercises the publisher-restart path on the named-pipe + semaphore stack

Out of scope for the *first* Windows release:

* Performance parity with Linux (Windows handle / pipe overhead is
  measurably higher; document the gap, optimise later).
* Service Manager integration / Windows event log.
* ARM64 Windows wheels (defer until the x64 wheel ships).

## 9. Estimated effort

* `Waker::windows` module: ~250 LOC + tests
* `producer_lock` Windows branch: ~80 LOC
* `Publisher` / `Subscriber` `#[cfg]` plumbing: ~150 LOC of diff
* CI matrix expansion: 1 day to debug the inevitable Windows-only
  failures (path lengths, permission flags, etc.)
* Total: **~1 focused week** for someone comfortable with Win32 API.

## 10. Open questions

- **Pipe name namespacing**: `\\.\pipe\` is a flat namespace across
  the system.  Two `mmbus` users on the same machine with the same
  bus name would collide.  Mitigate: include the session ID / user
  SID in the pipe name.  Document.
- **Permission model**: should we apply a security descriptor to
  pipes/semaphores so only the same user can connect?  Yes by default
  (current Unix behaviour is "same user" via dir permissions).  Use
  `InitializeSecurityDescriptor` with the current user's SID.
- **Shared-memory file location**: today `/tmp/mmbus/` — on Windows
  use `%LOCALAPPDATA%\mmbus\` or `%TEMP%\mmbus\`.  Document the
  default, leave `BusConfig.base_dir` overrideable.
