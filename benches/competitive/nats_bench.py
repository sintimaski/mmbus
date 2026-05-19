"""NATS JetStream pub/sub over TCP loopback.

Usage:
    python nats_bench.py durable      # FILE storage
    python nats_bench.py nondurable   # core NATS pub/sub (no JetStream)
"""

import asyncio
import multiprocessing as mp
import os
import sys
import tempfile
import time

import nats
from nats.js.api import StreamConfig, StorageType, RetentionPolicy

import _common
from _common import (
    PAYLOAD,
    PAYLOAD_SIZE,
    Result,
    emit,
    peak_rss_mb_self,
)

# Allow override via env (NATS JetStream FILE storage in Docker on
# macOS is slow enough that 1M takes too long; the env knob lets the
# runner script pass a smaller N for NATS specifically).
TOTAL_N = int(os.environ.get("BENCH_N", _common.TOTAL_N))
WARMUP_N = int(os.environ.get("BENCH_WARMUP_N", _common.WARMUP_N))

NATS_URL = os.environ.get("NATS_URL", "nats://127.0.0.1:14222")
SUBJECT = "bench"
STREAM = "BENCH"


async def subscriber_main(durable: bool, ready_path: str) -> dict:
    nc = await nats.connect(NATS_URL)

    if durable:
        js = nc.jetstream()
        # Re-create the stream fresh.
        try:
            await js.delete_stream(STREAM)
        except Exception:
            pass
        await js.add_stream(StreamConfig(
            name=STREAM,
            subjects=[SUBJECT],
            storage=StorageType.FILE,
            retention=RetentionPolicy.LIMITS,
        ))
        sub = await js.subscribe(SUBJECT, stream=STREAM, manual_ack=False)
    else:
        # Core NATS — no JetStream, fire-and-forget over TCP.
        sub = await nc.subscribe(SUBJECT)

    open(ready_path, "w").close()

    n_received = 0
    start_t = 0.0
    end_t = 0.0
    async for msg in sub.messages:
        if len(msg.data) != PAYLOAD_SIZE:
            continue
        if n_received == WARMUP_N - 1:
            start_t = time.perf_counter()
        n_received += 1
        if n_received == TOTAL_N:
            end_t = time.perf_counter()
            break
    await nc.drain()
    return {"wall_sec": end_t - start_t, "n_received": n_received, "rss_mb": peak_rss_mb_self()}


def subscriber_proc(durable: bool, ready_path: str, done_q: mp.Queue) -> None:
    info = asyncio.run(subscriber_main(durable, ready_path))
    done_q.put(info)


async def publisher_main(durable: bool) -> tuple[float, float]:
    nc = await nats.connect(NATS_URL)

    if durable:
        js = nc.jetstream()
        # Fire publishes WITHOUT awaiting per-message ack so the
        # publisher isn't bounded by per-msg TCP round-trip
        # latency.  In-flight cap of 1024 keeps memory bounded.
        # JetStream ACKs land asynchronously; we await them in
        # bulk at warmup-done and end.
        in_flight: list = []
        IN_FLIGHT_CAP = 1024

        async def send(payload):
            ack = js.publish(SUBJECT, payload)  # returns coroutine
            in_flight.append(asyncio.create_task(ack))
            if len(in_flight) >= IN_FLIGHT_CAP:
                # Drain half so we don't pile up.
                drain, remaining = in_flight[: IN_FLIGHT_CAP // 2], in_flight[IN_FLIGHT_CAP // 2 :]
                await asyncio.gather(*drain)
                in_flight.clear()
                in_flight.extend(remaining)

        async def drain_all():
            if in_flight:
                await asyncio.gather(*in_flight)
                in_flight.clear()
    else:
        async def send(payload):
            await nc.publish(SUBJECT, payload)

        async def drain_all():
            await nc.flush()

    # Warmup.
    for _ in range(WARMUP_N):
        await send(PAYLOAD)
    await drain_all()
    warmup_done = time.perf_counter()

    for _ in range(WARMUP_N, TOTAL_N):
        await send(PAYLOAD)
    await drain_all()
    pub_end = time.perf_counter()

    await nc.drain()
    return warmup_done, pub_end


def main() -> None:
    durable = sys.argv[1] == "durable" if len(sys.argv) > 1 else True
    tmpdir = tempfile.mkdtemp(prefix="nats_bench_")
    ready_path = os.path.join(tmpdir, "sub_ready")

    try:
        done_q: mp.Queue = mp.Queue()
        sub = mp.Process(target=subscriber_proc, args=(durable, ready_path, done_q), daemon=False)
        sub.start()

        deadline = time.perf_counter() + 15.0
        while not os.path.exists(ready_path):
            if time.perf_counter() > deadline:
                raise RuntimeError("nats subscriber didn't signal ready")
            time.sleep(0.005)

        # Brief settle for core NATS — the subscribe call is async; some
        # versions need a moment for the SUB to propagate before PUB
        # arrives.
        time.sleep(0.1)

        warmup_done, pub_end = asyncio.run(publisher_main(durable))

        sub.join(timeout=300.0)
        if sub.exitcode != 0:
            raise RuntimeError(f"nats subscriber exited with {sub.exitcode}")
        info = done_q.get_nowait()

        measured = TOTAL_N - WARMUP_N
        pub_wall = pub_end - warmup_done
        sustained = measured / info["wall_sec"] if info["wall_sec"] > 0 else 0.0
        pub_thr = measured / pub_wall if pub_wall > 0 else 0.0
        emit(Result(
            framework="nats_jetstream" if durable else "nats_core",
            durable=durable,
            total_n=TOTAL_N,
            payload_size=PAYLOAD_SIZE,
            sustained_throughput_msgs_per_sec=sustained,
            publisher_throughput_msgs_per_sec=pub_thr,
            consumer_wall_sec=info["wall_sec"],
            peak_rss_mb_pub=peak_rss_mb_self(),
            peak_rss_mb_sub=info["rss_mb"],
            notes=f"transport=tcp:{NATS_URL.split(':')[-1]}; "
                  f"{'JetStream FILE storage' if durable else 'core NATS pub/sub'}",
        ))
    finally:
        import shutil
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    main()
