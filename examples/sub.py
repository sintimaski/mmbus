"""Cross-process subscriber example.

Pair with ``pub.py`` to see end-to-end IPC:

    # terminal 1
    python examples/sub.py

    # terminal 2
    python examples/pub.py

The subscriber blocks until the publisher exists, then prints every
message it receives until the publisher disconnects.
"""
from mmbus import Bus


def main() -> None:
    bus = Bus("demo")

    print("subscriber: connecting to 'ticks'...")
    with bus.subscribe("ticks", timeout_secs=30.0) as sub:
        print(f"subscriber: connected (fd={sub.fileno()})")
        for msg in sub:
            print(f"subscriber: received {msg!r}")

    print("subscriber: publisher disconnected, exiting")


if __name__ == "__main__":
    main()
