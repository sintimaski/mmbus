"""ZeroMQ PUSH/PULL over ipc:// — same-host, no durability available.

Usage:
    python zmq_bench.py   # always non-durable (zmq has no built-in durability)
"""

import multiprocessing as mp
import os
import tempfile
import time

import zmq

from _common import (
    PAYLOAD,
    PAYLOAD_SIZE,
    TOTAL_N,
    WARMUP_N,
    Result,
    emit,
    peak_rss_mb_self,
)


def subscriber_proc(endpoint: str, ready_path: str, done_q: mp.Queue) -> None:
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PULL)
    sock.set_hwm(1 << 20)  # 1M-msg buffer — comparable to mmbus 65k ring
    sock.bind(endpoint)
    open(ready_path, "w").close()

    n_received = 0
    start_t = 0.0
    end_t = 0.0
    while True:
        msg = sock.recv()
        if len(msg) != PAYLOAD_SIZE:
            continue  # priming
        if n_received == WARMUP_N - 1:
            start_t = time.perf_counter()
        n_received += 1
        if n_received == TOTAL_N:
            end_t = time.perf_counter()
            break
    sock.close()
    ctx.term()
    done_q.put({"wall_sec": end_t - start_t, "n_received": n_received, "rss_mb": peak_rss_mb_self()})


def main() -> None:
    tmpdir = tempfile.mkdtemp(prefix="zmq_bench_")
    endpoint = f"ipc://{tmpdir}/socket"
    ready_path = os.path.join(tmpdir, "sub_ready")

    try:
        done_q: mp.Queue = mp.Queue()
        sub = mp.Process(target=subscriber_proc, args=(endpoint, ready_path, done_q), daemon=False)
        sub.start()

        # Wait for subscriber socket to bind.
        deadline = time.perf_counter() + 10.0
        while not os.path.exists(ready_path):
            if time.perf_counter() > deadline:
                raise RuntimeError("zmq subscriber didn't signal ready")
            time.sleep(0.005)

        ctx = zmq.Context()
        sock = ctx.socket(zmq.PUSH)
        sock.set_hwm(1 << 20)
        sock.connect(endpoint)
        # Small primer so we know the connection is established.
        sock.send(b"prime")
        # Brief settle.
        time.sleep(0.05)

        pub_start = time.perf_counter()
        for _ in range(WARMUP_N):
            sock.send(PAYLOAD)
        warmup_done = time.perf_counter()
        for _ in range(WARMUP_N, TOTAL_N):
            sock.send(PAYLOAD)
        pub_end = time.perf_counter()

        sub.join(timeout=120.0)
        info = done_q.get_nowait()

        measured = TOTAL_N - WARMUP_N
        pub_wall = pub_end - warmup_done
        sustained = measured / info["wall_sec"] if info["wall_sec"] > 0 else 0.0
        pub_thr = measured / pub_wall if pub_wall > 0 else 0.0
        emit(Result(
            framework="zmq",
            durable=False,
            total_n=TOTAL_N,
            payload_size=PAYLOAD_SIZE,
            sustained_throughput_msgs_per_sec=sustained,
            publisher_throughput_msgs_per_sec=pub_thr,
            consumer_wall_sec=info["wall_sec"],
            peak_rss_mb_pub=peak_rss_mb_self(),
            peak_rss_mb_sub=info["rss_mb"],
            notes="transport=ipc:// PUSH/PULL; no built-in durability",
        ))
        sock.close()
        ctx.term()
    finally:
        import shutil
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    main()
