"""Redis Streams XADD/XREAD over TCP loopback.

Usage:
    python redis_bench.py durable      # appendonly=everysec
    python redis_bench.py nondurable   # rely on Redis config; durable column
                                       # only reflects what server was booted
                                       # with (we don't reconfigure on the fly)
"""

import multiprocessing as mp
import os
import sys
import tempfile
import time

import redis

from _common import (
    PAYLOAD,
    PAYLOAD_SIZE,
    TOTAL_N,
    WARMUP_N,
    Result,
    emit,
    peak_rss_mb_self,
)

REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "16379"))
STREAM_KEY = "bench-stream"


def subscriber_proc(ready_path: str, done_q: mp.Queue) -> None:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
    # Reset the stream so we start from id=0.
    r.delete(STREAM_KEY)
    open(ready_path, "w").close()

    n_received = 0
    start_t = 0.0
    end_t = 0.0
    last_id = "0-0"
    # XREAD with BLOCK so we wait for messages; COUNT=4096 amortizes
    # the round-trip cost.
    while n_received < TOTAL_N:
        batch = r.xread({STREAM_KEY: last_id}, count=4096, block=10_000)
        if not batch:
            continue
        # batch = [(stream_name, [(msg_id, fields), ...])]
        for _stream, entries in batch:
            for msg_id, fields in entries:
                last_id = msg_id
                if n_received == WARMUP_N - 1:
                    start_t = time.perf_counter()
                n_received += 1
                if n_received == TOTAL_N:
                    end_t = time.perf_counter()
                    break
            if n_received == TOTAL_N:
                break

    done_q.put({"wall_sec": end_t - start_t, "n_received": n_received, "rss_mb": peak_rss_mb_self()})


def main() -> None:
    durable = sys.argv[1] == "durable" if len(sys.argv) > 1 else True
    tmpdir = tempfile.mkdtemp(prefix="redis_bench_")
    ready_path = os.path.join(tmpdir, "sub_ready")

    try:
        done_q: mp.Queue = mp.Queue()
        sub = mp.Process(target=subscriber_proc, args=(ready_path, done_q), daemon=False)
        sub.start()

        deadline = time.perf_counter() + 10.0
        while not os.path.exists(ready_path):
            if time.perf_counter() > deadline:
                raise RuntimeError("redis subscriber didn't signal ready")
            time.sleep(0.005)

        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
        # XADD uses a pipeline to amortize the per-message TCP round-trip.
        # Without pipelining you get ~25k msg/s on loopback redis; with
        # batches you can get >1M msg/s.
        BATCH = 1024

        # Warmup.
        pipe = r.pipeline(transaction=False)
        for i in range(WARMUP_N):
            pipe.xadd(STREAM_KEY, {b"d": PAYLOAD})
            if (i + 1) % BATCH == 0:
                pipe.execute()
                pipe = r.pipeline(transaction=False)
        pipe.execute()
        warmup_done = time.perf_counter()

        # Measured.
        pipe = r.pipeline(transaction=False)
        for i in range(WARMUP_N, TOTAL_N):
            pipe.xadd(STREAM_KEY, {b"d": PAYLOAD})
            if (i - WARMUP_N + 1) % BATCH == 0:
                pipe.execute()
                pipe = r.pipeline(transaction=False)
        pipe.execute()
        pub_end = time.perf_counter()

        sub.join(timeout=120.0)
        info = done_q.get_nowait()

        measured = TOTAL_N - WARMUP_N
        pub_wall = pub_end - warmup_done
        sustained = measured / info["wall_sec"] if info["wall_sec"] > 0 else 0.0
        pub_thr = measured / pub_wall if pub_wall > 0 else 0.0
        emit(Result(
            framework="redis_streams",
            durable=durable,
            total_n=TOTAL_N,
            payload_size=PAYLOAD_SIZE,
            sustained_throughput_msgs_per_sec=sustained,
            publisher_throughput_msgs_per_sec=pub_thr,
            consumer_wall_sec=info["wall_sec"],
            peak_rss_mb_pub=peak_rss_mb_self(),
            peak_rss_mb_sub=info["rss_mb"],
            notes=f"transport=tcp:{REDIS_PORT}; XADD pipeline batch={BATCH}; "
                  f"redis appendonly=yes appendfsync=everysec"
                  if durable
                  else f"transport=tcp:{REDIS_PORT}; XADD pipeline batch={BATCH}",
        ))
    finally:
        import shutil
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    main()
