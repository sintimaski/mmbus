"""Linux eventfd smoke test — single-process pub/sub round-trip."""
import threading
import mmbus


def main() -> None:
    received: list[bytes] = []

    def subscriber() -> None:
        bus = mmbus.Bus("docker-test")
        sub = bus.subscribe("ch", timeout_secs=10.0)
        received.append(sub.recv())

    t = threading.Thread(target=subscriber, daemon=True)
    t.start()

    pub = mmbus.Bus("docker-test")
    pub.wait_for_subscribers("ch", n=1, timeout_secs=10.0)
    pub.publish("ch", b"hello-linux")
    t.join(timeout=3.0)

    assert received == [b"hello-linux"], f"got {received}"
    print("eventfd smoke test PASSED")


if __name__ == "__main__":
    main()
