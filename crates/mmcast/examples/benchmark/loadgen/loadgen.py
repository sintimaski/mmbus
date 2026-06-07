"""Loadgen — open N WS clients against a target, push M messages from
each, record per-message broadcast latency, write a JSON results file.

Per-message latency is end-to-end fan-out: a publisher tags the payload
with its ``send_time_ns``, every other client records ``recv_time_ns``
on arrival, and we compute ``(recv - send)`` across all delivered
copies.  Throughput is the count of delivered copies / wall-clock.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import struct
import time
from typing import List

import websockets


# 16-byte fixed prefix: u64 send_time_ns + u32 sender_id + u32 seq.
HEADER = struct.Struct(">QII")


async def run_client(
    url: str,
    client_id: int,
    publish_n: int,
    payload_size: int,
    interval_ms: float,
    start_event: asyncio.Event,
    latencies_out: List[float],
    received_count: List[int],
) -> None:
    """One WS client: subscribes (always), publishes ``publish_n`` if
    >0.  Records latency of every message it receives (including its
    own broadcasts coming back)."""
    extra = max(payload_size - HEADER.size, 0)
    pad = b"\x00" * extra
    _ = interval_ms  # forwarded to sender() below

    async with websockets.connect(url, max_size=10 * 1024 * 1024) as ws:
        await start_event.wait()

        async def receiver() -> None:
            try:
                async for msg in ws:
                    now_ns = time.monotonic_ns()
                    if isinstance(msg, str):
                        msg = msg.encode("latin-1")
                    if len(msg) >= HEADER.size:
                        send_ns, sender_id, seq = HEADER.unpack_from(msg)
                        # Convert send timestamp to monotonic frame —
                        # senders also use monotonic_ns so the diff is
                        # meaningful even across coroutines (same process).
                        latencies_out.append((now_ns - send_ns) / 1e6)  # ms
                    received_count[0] += 1
            except websockets.ConnectionClosed:
                pass

        async def sender() -> None:
            # ``interval_ms`` paces publishes so we measure broadcast
            # latency (the metric of interest) rather than the depth of
            # WS send buffers in saturated mode.  ``0`` = no pacing
            # (closed-loop saturation test — throughput, not latency).
            interval_s = max(interval_ms / 1000.0, 0.0)
            for seq in range(publish_n):
                payload = HEADER.pack(
                    time.monotonic_ns(), client_id, seq
                ) + pad
                await ws.send(payload)
                if interval_s > 0:
                    await asyncio.sleep(interval_s)
                else:
                    await asyncio.sleep(0)

        recv_task = asyncio.create_task(receiver())
        send_task = asyncio.create_task(sender())
        # Wait for the sender to finish, then give receivers a beat to
        # drain in-flight broadcasts before closing.
        await send_task
        await asyncio.sleep(0.5)
        recv_task.cancel()
        try:
            await recv_task
        except asyncio.CancelledError:
            pass


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True, help="ws:// target URL")
    ap.add_argument("--clients", type=int, default=10)
    ap.add_argument("--publishers", type=int, default=2,
                    help="how many of the clients publish (rest subscribe only)")
    ap.add_argument("--messages-per-publisher", type=int, default=200)
    ap.add_argument("--payload-size", type=int, default=64)
    ap.add_argument(
        "--interval-ms",
        type=float,
        default=0.0,
        help="ms between publishes per sender (0 = saturate)",
    )
    ap.add_argument("--out", default="results.json")
    ap.add_argument("--label", default="?")
    args = ap.parse_args()

    start = asyncio.Event()
    latencies: List[float] = []
    received_count = [0]

    async def make_client(i: int) -> None:
        await run_client(
            url=args.url,
            client_id=i,
            publish_n=(args.messages_per_publisher if i < args.publishers else 0),
            payload_size=args.payload_size,
            interval_ms=args.interval_ms,
            start_event=start,
            latencies_out=latencies,
            received_count=received_count,
        )

    tasks = [asyncio.create_task(make_client(i)) for i in range(args.clients)]
    # Brief connect window so all clients are ready before the start gun.
    await asyncio.sleep(1.0)
    t0 = time.monotonic()
    start.set()
    await asyncio.gather(*tasks)
    t1 = time.monotonic()

    summary = {
        "label": args.label,
        "clients": args.clients,
        "publishers": args.publishers,
        "messages_per_publisher": args.messages_per_publisher,
        "payload_size_bytes": args.payload_size,
        "wall_clock_s": round(t1 - t0, 3),
        "delivered_total": received_count[0],
        # Throughput: messages delivered per second across all clients.
        # Guard against a zero-duration run (no work / instant completion).
        "delivered_per_sec": (
            round(received_count[0] / (t1 - t0), 1) if (t1 - t0) > 0 else 0.0
        ),
        "latency_ms": {
            "count": len(latencies),
            "p50": round(statistics.median(latencies), 3) if latencies else None,
            "p95": round(_pct(latencies, 95), 3) if latencies else None,
            "p99": round(_pct(latencies, 99), 3) if latencies else None,
            "max": round(max(latencies), 3) if latencies else None,
        },
    }
    with open(args.out, "w") as f:
        json.dump(summary, f, indent=2)
    print(json.dumps(summary, indent=2))


def _pct(values: List[float], pct: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    k = int(len(s) * pct / 100.0)
    return s[min(k, len(s) - 1)]


if __name__ == "__main__":
    asyncio.run(main())
