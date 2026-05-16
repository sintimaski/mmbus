"""mmbus — zero-copy pub/sub over mmap.

Quick start
-----------
Publisher process::

    from mmbus import Bus

    bus = Bus("my-app")
    bus.wait_for_subscribers("events", n=1)
    bus.publish("events", b"hello")

Subscriber process::

    from mmbus import Bus

    bus = Bus("my-app")
    for msg in bus.subscribe("events"):
        print(msg)
"""
from mmbus._mmbus import Bus, Subscription, TopicStats  # noqa: F401

__all__ = ["Bus", "Subscription", "TopicStats"]
__version__ = "0.1.0"
