"""Shared helpers: workload spec + stats + JSON emission."""

import dataclasses
import json
import resource
import sys
import time

# Workload — keep these in lockstep with SPEC.md.
WARMUP_N = 10_000
TOTAL_N = 1_000_000
PAYLOAD_SIZE = 256
PAYLOAD = b"x" * PAYLOAD_SIZE


@dataclasses.dataclass
class Result:
    framework: str
    durable: bool
    total_n: int
    payload_size: int
    sustained_throughput_msgs_per_sec: float
    publisher_throughput_msgs_per_sec: float
    consumer_wall_sec: float
    peak_rss_mb_pub: float
    peak_rss_mb_sub: float
    notes: str = ""

    def to_json(self) -> str:
        return json.dumps(dataclasses.asdict(self), indent=2)


def peak_rss_mb_self() -> float:
    """Return peak RSS in MB for the current process.

    On Linux `ru_maxrss` is in KB; on macOS it's in bytes.  Detect.
    """
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return rss / (1024 * 1024)
    return rss / 1024


def time_window(n_warmup: int = WARMUP_N, n_total: int = TOTAL_N):
    """Return (start_t, mark_warmup_done_callback, end_t) helpers.

    Usage:
        start_t = time.perf_counter()
        for i in range(WARMUP_N):
            send(i)
        warmup_done_t = time.perf_counter()
        for i in range(WARMUP_N, TOTAL_N):
            send(i)
        end_t = time.perf_counter()
        throughput = (TOTAL_N - WARMUP_N) / (end_t - warmup_done_t)
    """
    pass


def emit(result: Result) -> None:
    """Print JSON to stdout — runner script collects this."""
    print(result.to_json())
