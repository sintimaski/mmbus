"""Cross-process publisher example.

Pair with ``sub.py`` to see end-to-end IPC:

    # terminal 1
    python examples/sub.py

    # terminal 2
    python examples/pub.py
"""
import time

from mmbus import Bus


def main() -> None:
    bus = Bus("demo")

    # Block until at least one subscriber is connected. Without this the
    # publisher would publish into the ring before anyone is listening, so
    # late subscribers would only see messages from their connect-time
    # forward (cursors are claimed at the current tail).
    print("publisher: waiting for a subscriber...")
    bus.wait_for_subscribers("ticks", n=1, timeout_secs=30.0)
    print("publisher: subscriber connected, sending 10 messages")

    for i in range(10):
        bus.publish("ticks", f"tick {i}".encode())
        time.sleep(0.2)

    print("publisher: done")


if __name__ == "__main__":
    main()
