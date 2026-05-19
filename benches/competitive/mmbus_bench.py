"""mmbus reference benchmark.

Usage:
    python mmbus_bench.py durable      # WAL on (Batched, default)
    python mmbus_bench.py nondurable   # WAL disabled
"""

import multiprocessing as mp
import os
import shutil
import sys
import tempfile
import time

import mmbus
from _common import (
    PAYLOAD,
    PAYLOAD_SIZE,
    TOTAL_N,
    WARMUP_N,
    Result,
    emit,
    peak_rss_mb_self,
)


def subscriber_proc(base_dir: str, durable: bool, ready_path: str, done_q: mp.Queue) -> None:
    """Subscriber process — reads TOTAL_N messages, reports wall + RSS.

    The publisher emits 1 priming msg before the subscriber attaches
    so the producer-lock is held when subscribe() runs.  We discard
    that 1 message before timing.
    """
    kwargs = {"base_dir": base_dir, "capacity": 65536, "slot_size": 512}
    if not durable:
        kwargs["wal_enabled"] = False
    bus = mmbus.Bus("bench", **kwargs)
    sub = bus.subscribe("topic")
    # Signal readiness to the publisher.
    open(ready_path, "w").close()

    n_received = 0
    end_t = 0.0
    start_t = 0.0
    for msg in sub:
        # First payload after subscribe might be the priming msg — skip
        # if it doesn't match the bench PAYLOAD length.
        if n_received == 0 and len(msg) != PAYLOAD_SIZE:
            continue
        if n_received == WARMUP_N - 1:
            start_t = time.perf_counter()
        n_received += 1
        if n_received == TOTAL_N:
            end_t = time.perf_counter()
            break
    rss = peak_rss_mb_self()
    done_q.put({
        "wall_sec": end_t - start_t,
        "n_received": n_received,
        "rss_mb": rss,
    })


def main() -> None:
    durable = sys.argv[1] == "durable" if len(sys.argv) > 1 else True
    base_dir = tempfile.mkdtemp(prefix="mmbus_bench_")
    ready_path = os.path.join(base_dir, "sub_ready")

    try:
        # Publisher comes UP first — mmbus's subscriber.connect()
        # needs an existing producer-lock-holder.  We open the
        # publisher's ring + topic by calling publish on an empty
        # payload (creates the ring lazily); then spawn the
        # subscriber.
        # Bump capacity so the publisher isn't bounded by ring backpressure
        # when the subscriber lags by even a few thousand messages.  This
        # matches the spirit of the bench (sustained throughput, not
        # bounded-buffer dynamics).
        kwargs = {"base_dir": base_dir, "capacity": 65536, "slot_size": 512}
        if not durable:
            kwargs["wal_enabled"] = False
        bus = mmbus.Bus("bench", **kwargs)
        # Force ring creation (publish lazily allocates it).
        bus.publish("topic", b"prime")  # 1 throwaway message before subscriber attaches

        done_q: mp.Queue = mp.Queue()
        sub = mp.Process(
            target=subscriber_proc,
            args=(base_dir, durable, ready_path, done_q),
            daemon=False,
        )
        sub.start()

        # Wait for subscriber to be ready.
        deadline = time.perf_counter() + 10.0
        while not os.path.exists(ready_path):
            if time.perf_counter() > deadline:
                raise RuntimeError("subscriber did not signal ready")
            time.sleep(0.005)

        bus.wait_for_subscribers("topic", n=1, timeout_secs=10.0)

        pub_start = time.perf_counter()
        # Warmup.
        for _ in range(WARMUP_N):
            bus.publish("topic", PAYLOAD)
        warmup_done = time.perf_counter()
        # Measured window.
        for _ in range(WARMUP_N, TOTAL_N):
            bus.publish("topic", PAYLOAD)
        pub_end = time.perf_counter()

        sub.join(timeout=60.0)
        if sub.exitcode != 0:
            raise RuntimeError(f"subscriber exited with {sub.exitcode}")
        info = done_q.get_nowait()

        measured = TOTAL_N - WARMUP_N
        pub_wall = pub_end - warmup_done
        sustained = measured / info["wall_sec"] if info["wall_sec"] > 0 else 0.0
        pub_thr = measured / pub_wall if pub_wall > 0 else 0.0
        result = Result(
            framework="mmbus",
            durable=durable,
            total_n=TOTAL_N,
            payload_size=len(PAYLOAD),
            sustained_throughput_msgs_per_sec=sustained,
            publisher_throughput_msgs_per_sec=pub_thr,
            consumer_wall_sec=info["wall_sec"],
            peak_rss_mb_pub=peak_rss_mb_self(),
            peak_rss_mb_sub=info["rss_mb"],
            notes="ipc=mmap+unix-sock; WAL=Batched" if durable else "ipc=mmap+unix-sock; WAL=disabled",
        )
        emit(result)
    finally:
        shutil.rmtree(base_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
