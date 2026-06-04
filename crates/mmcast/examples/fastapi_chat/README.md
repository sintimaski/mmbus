# FastAPI chat — mmcast demo

The "you don't need Redis" demo.  Single-file FastAPI app, WebSocket
broadcast across all connected tabs, history replay on reconnect.

## Run

```bash
pip install -e "../../[fastapi]"      # from this dir, install the example deps
uvicorn app:app --port 8000           # single worker — the demo default
```

Open two browser tabs at http://localhost:8000.  Type in one — it shows
up in both.  Close a tab and reopen it; the last 20 messages replay.

## Multi-worker

mmbus enforces single-publisher-per-topic across processes.  For
`uvicorn --workers 4`, mmcast uses per-worker sharding — each worker
publishes to its own shard topic and subscribes to all peers'.

```bash
# Worker 0
MMCAST_WORKER_ID=w0 MMCAST_PEERS=w0,w1,w2,w3 uvicorn app:app --port 8000
# Worker 1
MMCAST_WORKER_ID=w1 MMCAST_PEERS=w0,w1,w2,w3 uvicorn app:app --port 8000
# ...
```

In practice you'd wrap this in a launcher script or use uvicorn's
worker hook.  Native auto-detection of the uvicorn worker index is
tracked for v0.2 — for now the env-var route is portable and works
under any process supervisor.

## Compare with broadcaster + Redis

[`encode/broadcaster`](https://github.com/encode/broadcaster)'s docs
show essentially the same app with a Redis backend.  The differences:

| | broadcaster + Redis             | mmcast                                                   |
|-|---------------------------------|----------------------------------------------------------|
| Setup | `pip install` + Redis container | `pip install mmbus-cast[fastapi]`                       |
| Containers | App + Redis                  | App                                                      |
| Wakeup latency | ~100 µs (loopback TCP)    | ~720 ns (mmap + eventfd / AF_UNIX byte)                  |
| Reconnect replay | Not built-in (use Streams) | `replay_last=20` per spec                              |
| Cross-host | Native (Redis cluster)        | Add `mmbus[bridge]`                                      |

The side-by-side benchmark for this app lives in
`../benchmark/` — that's where the README's results table comes from.
