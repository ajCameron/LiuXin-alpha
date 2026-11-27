"""Exercise observer isolation, explicit loss and nonblocking shutdown."""

import threading
import time

import pytest

from LiuXin_alpha.core.event_delivery import EventDelivery
from LiuXin_alpha.core.events import make_core_event


def event(payload=None):
    return make_core_event(core_uuid="test", event_type="changed", payload=payload)


def test_blocked_subscriber_overflows_without_delaying_other_subscribers():
    delivery = EventDelivery()
    entered, release = threading.Event(), threading.Event()
    slow, fast = [], []

    def blocked(value):
        entered.set()
        assert release.wait(3)
        slow.append(value)

    delivery.subscribe(blocked)
    delivery.subscribe(fast.append)
    try:
        delivery.emit(event({"n": 0}))
        assert entered.wait(1)
        started = time.monotonic()
        for n in range(300):
            delivery.emit(event({"n": n}))
        assert time.monotonic() - started < 1
        assert delivery.statistics()["dropped"] > 0
        release.set()
        assert delivery.drain(2)
        assert any(value.event_type == "events.resync_required" for value in slow)
        assert fast
    finally:
        release.set()
        delivery.close()


def test_payloads_are_detached_and_unsubscribe_closures_are_idempotent():
    delivery = EventDelivery()
    observed = []

    def mutate(value):
        value.payload["nested"]["value"] = "changed"

    first = delivery.subscribe(mutate)
    delivery.subscribe(observed.append)
    payload = {"nested": {"value": "original"}}
    delivery.emit(event(payload))
    assert delivery.drain(2)
    assert payload["nested"]["value"] == "original"
    assert observed[0].payload["nested"]["value"] == "original"
    first()
    first()
    assert delivery.statistics()["subscribers"] == 1
    delivery.close()


def test_callback_failure_is_observable_and_requires_resync():
    delivery = EventDelivery()
    received = []

    def flaky(value):
        received.append(value)
        if len(received) == 1:
            raise RuntimeError("observer failure")

    delivery.subscribe(flaky)
    delivery.emit(event())
    assert delivery.drain(2)
    assert delivery.statistics()["callback_failures"] == 1
    delivery.emit(event())
    assert delivery.drain(2)
    assert received[1].event_type == "events.resync_required"
    delivery.close()


def test_unsubscribing_blocked_callbacks_cannot_evade_thread_limit():
    delivery = EventDelivery()
    release = threading.Event()
    entered = [threading.Event() for _ in range(32)]
    try:
        for ready in entered:

            def blocked(value, ready=ready):
                ready.set()
                release.wait(5)

            remove = delivery.subscribe(blocked)
            delivery.emit(event())
            assert ready.wait(1)
            remove()
        with pytest.raises(ValueError, match="limit"):
            delivery.subscribe(lambda value: None)
        assert delivery.statistics()["subscribers"] == 0
        assert delivery.statistics()["retired_threads"] == 32
    finally:
        release.set()
        for subscription in delivery.retired:
            subscription.thread.join(2)
        delivery.close()
