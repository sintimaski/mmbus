"""Apples-to-apples comparison: mmbus vs pyzmq over Unix-domain IPC.

Both transports run a publisher + subscriber in separate threads inside
this process, exchanging ``N_MESSAGES`` of fixed size, and we measure
total wall time → throughput + per-message round-trip latency.

Why this layout (not subprocesses)?  pyzmq's ``inproc://`` is a
threads-only fast path; ``ipc://`` is the kernel-mediated Unix socket
transport (the equivalent of what mmbus actually uses cross-process).
We measure ``ipc://`` to be fair.

Run:

    pip install pyzmq
    python benches/compare.py

Tuneables at the top of the file.  Single run takes ~5 seconds.
"""
from __future__ import annotations

import statistics
import tempfile
import threading
import time
import uuid
from pathlib import Path

import zmq

from mmbus import Bus, BusFullError

# ── Tuneables ────────────────────────────────────────────────────────────────

N_MESSAGES = 10_000
PAYLOAD_BYTES = 64
N_REPEATS = 3   # take the median of this many runs to filter noise
PAYLOAD_SIZES_TO_TEST = [64, 1024, 16384]   # show the size sweep


# ── mmbus harness ────────────────────────────────────────────────────────────

def bench_mmbus(payload_bytes: int) -> float:
    """Returns total wall-clock seconds for N_MESSAGES one-way."""
    bus_name = f"bench-{uuid.uuid4().hex[:8]}"
    pub_bus = Bus(bus_name, slot_size=payload_bytes + 64, capacity=4096)
    payload = b"x" * payload_bytes
    done = threading.Event()

    def subscriber():
        sub_bus = Bus(bus_name, slot_size=payload_bytes + 64, capacity=4096)
        sub = sub_bus.subscribe("ch", timeout_secs=10.0)
        for _ in range(N_MESSAGES):
            sub.recv()
        done.set()

    t = threading.Thread(target=subscriber, daemon=True)
    t.start()
    pub_bus.wait_for_subscribers("ch", n=1, timeout_secs=10.0)

    start = time.perf_counter()
    for _ in range(N_MESSAGES):
        # Backpressure: spin on Full so the publisher waits for the
        # subscriber to catch up (matches pyzmq PUSH/PULL HWM blocking).
        while True:
            try:
                pub_bus.publish("ch", payload)
                break
            except BusFullError:
                pass  # spin
    done.wait(timeout=60.0)
    elapsed = time.perf_counter() - start

    pub_bus.clean_topic("ch")
    return elapsed


# ── pyzmq harness ────────────────────────────────────────────────────────────

def bench_pyzmq(payload_bytes: int) -> float:
    """Returns total wall-clock seconds for N_MESSAGES one-way over ipc://."""
    sock_path = Path(tempfile.gettempdir()) / f"zmq-bench-{uuid.uuid4().hex[:8]}.sock"
    endpoint = f"ipc://{sock_path}"
    payload = b"x" * payload_bytes
    done = threading.Event()
    # Slow joiner: subscriber must be bound + subscribed before publisher
    # starts pushing, otherwise ZMQ PUB silently drops.  We use PUSH/PULL
    # (queued, no slow-joiner pitfall) — semantically closer to mmbus,
    # which queues every message until the slot is overwritten or read.

    ctx = zmq.Context.instance()
    try:
        def subscriber():
            sub = ctx.socket(zmq.PULL)
            sub.bind(endpoint)
            for _ in range(N_MESSAGES):
                sub.recv()
            done.set()
            sub.close()

        t = threading.Thread(target=subscriber, daemon=True)
        t.start()

        # Wait briefly for the binder to be ready.
        for _ in range(100):
            if sock_path.exists():
                break
            time.sleep(0.005)

        pub = ctx.socket(zmq.PUSH)
        pub.connect(endpoint)
        # Give ZMQ a moment to wire up the connection.
        time.sleep(0.05)

        start = time.perf_counter()
        for _ in range(N_MESSAGES):
            pub.send(payload)
        done.wait(timeout=30.0)
        elapsed = time.perf_counter() - start

        pub.close()
        return elapsed
    finally:
        sock_path.unlink(missing_ok=True)


# ── Driver ───────────────────────────────────────────────────────────────────

def summarise(label: str, payload: int, runs: list[float]) -> None:
    med = statistics.median(runs)
    throughput = N_MESSAGES / med
    per_msg_ns = med * 1e9 / N_MESSAGES
    bytes_total = N_MESSAGES * payload
    bw = bytes_total / med / 1e6
    print(
        f"  {label:<7s}  {payload:>6d}B  "
        f"{throughput:>10,.0f} msg/s   "
        f"{per_msg_ns:>7,.0f} ns/msg   "
        f"{bw:>7.1f} MB/s",
        flush=True,
    )


def main() -> None:
    print(
        f"\nCross-thread Python IPC: {N_MESSAGES:,} messages, "
        f"median of {N_REPEATS} runs\n",
        flush=True,
    )
    print("  transport  payload  throughput            latency       bandwidth", flush=True)
    print("  ─────────  ───────  ──────────            ───────       ─────────", flush=True)

    results: dict[int, dict[str, float]] = {}
    for payload in PAYLOAD_SIZES_TO_TEST:
        mm = [bench_mmbus(payload) for _ in range(N_REPEATS)]
        zm = [bench_pyzmq(payload) for _ in range(N_REPEATS)]
        summarise("mmbus", payload, mm)
        summarise("pyzmq", payload, zm)
        print(flush=True)
        results[payload] = {
            "mmbus": statistics.median(mm),
            "pyzmq": statistics.median(zm),
        }

    print("Speedup (mmbus / pyzmq):", flush=True)
    for payload, r in results.items():
        ratio = r["pyzmq"] / r["mmbus"]
        verb = "faster" if ratio > 1 else "slower"
        print(
            f"  {payload:>6d}B   mmbus is {ratio:>4.2f}× {verb} than pyzmq",
            flush=True,
        )
    print(flush=True)


if __name__ == "__main__":
    main()
