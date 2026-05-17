"""Numpy tensor pipeline over mmbus.

Shows how to pass arbitrary numpy arrays between processes via the
mmbus byte channel.  The wire payload is just ``arr.tobytes()``;
shape + dtype must be carried out-of-band (here: a 16-byte header
prefix per message).  No pickle, no copies beyond ``recv()``'s
``bytes`` allocation.

Run:

    pip install numpy
    python examples/np_pipeline.py

What it demonstrates:

* Publisher generates 100 random ``float32`` arrays of shape (64, 64)
  and publishes each.
* Subscriber receives, decodes header → reconstructs the array via
  ``np.frombuffer`` + ``reshape``, asserts checksum integrity.
* Throughput is printed so you can compare to your in-process baseline.

For ML pipelines with fixed-shape tensors, you can drop the header
entirely and just store ``arr.tobytes()`` — the consumer knows the
shape statically.  For variable-shape arrays, encode (ndim, dtype,
shape[0..ndim]) in a small struct (this example uses a fixed layout
for simplicity).
"""
from __future__ import annotations

import struct
import threading
import time

import numpy as np

from mmbus import Bus

# 16-byte header: 8 bytes ndim+dtype tag (1+7 padding) + 8 bytes shape (2 * u32).
# For this demo we hardcode dtype=float32 + ndim=2; the reader trusts the layout.
HEADER_FMT = "<II"  # u32 rows, u32 cols
HEADER_LEN = struct.calcsize(HEADER_FMT)

N_FRAMES = 100
SHAPE = (64, 64)
DTYPE = np.float32


def encode(arr: np.ndarray) -> bytes:
    """Pack header + raw array bytes into a single message."""
    assert arr.dtype == DTYPE and arr.ndim == 2
    header = struct.pack(HEADER_FMT, arr.shape[0], arr.shape[1])
    return header + arr.tobytes()


def decode(msg: bytes) -> np.ndarray:
    """Reverse of :func:`encode` — single zero-copy slice into the message bytes
    (``np.frombuffer`` doesn't copy; the resulting array shares memory with msg)."""
    rows, cols = struct.unpack(HEADER_FMT, msg[:HEADER_LEN])
    return np.frombuffer(msg[HEADER_LEN:], dtype=DTYPE).reshape(rows, cols)


BUS_NAME = "np-pipeline"
SLOT_SIZE = HEADER_LEN + 4 * SHAPE[0] * SHAPE[1]


def subscriber(results: list[float]) -> None:
    # Each thread/process needs its own Bus handle — PyO3 enforces
    # exclusive access to a given object at a time, and the publisher
    # thread holds a borrow during wait_for_subscribers.
    sub_bus = Bus(BUS_NAME, slot_size=SLOT_SIZE)
    sub = sub_bus.subscribe("frames", timeout_secs=5.0)
    for _ in range(N_FRAMES):
        msg = sub.recv()
        arr = decode(msg)
        results.append(float(arr.sum()))


def main() -> None:
    bus = Bus(BUS_NAME, slot_size=SLOT_SIZE)

    received_sums: list[float] = []
    t = threading.Thread(target=subscriber, args=(received_sums,), daemon=True)
    t.start()

    bus.wait_for_subscribers("frames", n=1, timeout_secs=5.0)

    rng = np.random.default_rng(seed=42)
    sent_sums = []
    start = time.perf_counter()
    for _ in range(N_FRAMES):
        arr = rng.standard_normal(size=SHAPE).astype(DTYPE)
        sent_sums.append(float(arr.sum()))
        bus.publish("frames", encode(arr))
    pub_elapsed = time.perf_counter() - start

    t.join(timeout=5.0)
    total_elapsed = time.perf_counter() - start

    # Round-trip integrity: every sum must match (within float tolerance).
    assert len(received_sums) == N_FRAMES, (
        f"received {len(received_sums)}/{N_FRAMES}"
    )
    for i, (s, r) in enumerate(zip(sent_sums, received_sums)):
        assert abs(s - r) < 1e-3, f"frame {i}: sum mismatch  sent={s}  recv={r}"

    bytes_per_frame = HEADER_LEN + 4 * SHAPE[0] * SHAPE[1]
    bytes_total = bytes_per_frame * N_FRAMES

    print(
        f"✓ {N_FRAMES} frames of shape {SHAPE} ({DTYPE.__name__}) — "
        f"all checksums match"
    )
    print(
        f"  publish:    {pub_elapsed*1000:7.1f} ms  "
        f"({N_FRAMES/pub_elapsed:8.0f} msg/s, "
        f"{bytes_total/pub_elapsed/1e6:6.1f} MB/s)"
    )
    print(
        f"  round-trip: {total_elapsed*1000:7.1f} ms  "
        f"({N_FRAMES/total_elapsed:8.0f} msg/s, "
        f"{bytes_total/total_elapsed/1e6:6.1f} MB/s)"
    )
    print(f"  payload:    {bytes_per_frame} B/frame ({bytes_total/1024:.1f} KiB total)")


if __name__ == "__main__":
    main()
