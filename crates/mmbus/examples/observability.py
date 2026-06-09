"""Scrape mmbus stats and print Prometheus-style metric lines.

Run a publisher in one terminal:

    python examples/observability.py serve

…and the scraper in another:

    python examples/observability.py scrape
    # mmbus_publishes_total{topic="events"} 1234
    # mmbus_subscribers_dropped_total{topic="events"} 0
    # mmbus_wal_appends_total{topic="events"} 1234
    # mmbus_wal_bytes_total{topic="events"} 39488
    # mmbus_wal_flushes_total{topic="events"} 7
    # ... (one block per scrape interval)

Pipe to Pushgateway / etc. for a real setup; this script just shows the
shape of what Bus.stats() returns and how to derive Prom-style lines
from it.
"""

import os
import sys
import time

import mmbus


TOPIC = "events"
BUS_NAME = "obs-demo"


def prom_lines(stats: mmbus.TopicStats, topic: str) -> list[str]:
    """Format a TopicStats snapshot as Prometheus text-format lines.

    Skips the WAL block when WAL is disabled.
    """
    lines = [
        f'mmbus_published_total{{topic="{topic}"}} {stats.published_total}',
        f'mmbus_full_rejected_total{{topic="{topic}"}} {stats.full_rejected_total}',
        f'mmbus_subscribers_dropped_total{{topic="{topic}"}} '
        f"{stats.subscribers_dropped_total}",
        f'mmbus_connected_sockets{{topic="{topic}"}} {stats.connected_sockets}',
        f'mmbus_ring_tail{{topic="{topic}"}} {stats.tail}',
        f'mmbus_active_subscribers{{topic="{topic}"}} {stats.active_subscribers}',
    ]
    if stats.wal is not None:
        w = stats.wal
        lines.extend(
            [
                f'mmbus_wal_appends_total{{topic="{topic}"}} {w.appends_total}',
                f'mmbus_wal_append_bytes_total{{topic="{topic}"}} {w.append_bytes_total}',
                f'mmbus_wal_flushes_total{{topic="{topic}"}} {w.flushes_total}',
                f'mmbus_wal_pending_cursor{{topic="{topic}"}} {w.pending_cursor}',
                f'mmbus_wal_durable_cursor{{topic="{topic}"}} {w.durable_cursor}',
                f'mmbus_wal_replay_lag{{topic="{topic}"}} '
                f"{w.pending_cursor - w.durable_cursor}",
                f'mmbus_wal_total_bytes{{topic="{topic}"}} {w.total_wal_bytes}',
                f'mmbus_wal_segments{{topic="{topic}"}} {w.segments}',
            ]
        )
    return lines


def serve() -> None:
    """Run a busy publisher so the scraper has something to read."""
    bus = mmbus.Bus(BUS_NAME, base_dir="/tmp/mmbus-obs")
    print(f"[serve] publishing to bus={BUS_NAME!r} topic={TOPIC!r} every 100 ms")
    i = 0
    while True:
        bus.publish(TOPIC, f"msg-{i}".encode())
        i += 1
        time.sleep(0.1)


def scrape() -> None:
    """Print one block of metrics per second."""
    bus = mmbus.Bus(BUS_NAME, base_dir="/tmp/mmbus-obs")
    while True:
        stats = bus.stats(TOPIC)
        if stats is None:
            print(f"# no publisher yet for {TOPIC!r}")
        else:
            print(f"# {time.strftime('%H:%M:%S')}")
            for line in prom_lines(stats, TOPIC):
                print(line)
        print()
        time.sleep(1.0)


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] not in {"serve", "scrape"}:
        print(__doc__)
        sys.exit(1)
    {"serve": serve, "scrape": scrape}[sys.argv[1]]()


if __name__ == "__main__":
    os.makedirs("/tmp/mmbus-obs", exist_ok=True)
    main()
