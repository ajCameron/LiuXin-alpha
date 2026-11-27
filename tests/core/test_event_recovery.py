"""Clients must detect missing history and cannot mutate retained notifications."""

from unittest.mock import Mock

import pytest

from LiuXin_alpha.core.events import make_core_event
from LiuXin_alpha.core.transport.http import CoreHttpDaemon


def publish(daemon, count):
    for number in range(count):
        daemon._on_runtime_event(
            make_core_event(
                core_uuid="test",
                event_type="changed",
                payload={"nested": {"number": number}},
            )
        )


def test_overflow_is_explicit_and_resumes_after_resynchronization():
    daemon = CoreHttpDaemon(Mock())
    initial = daemon.get_next_event(after=0, timeout=0)
    publish(daemon, 1005)
    gap = daemon.get_next_event(after=0, stream=initial["stream"], timeout=0)
    assert gap["gap"] and gap["reason"] == "overflow"
    assert gap["event"] is None
    assert gap["oldest_sequence"] == 6
    publish(daemon, 1)
    next_event = daemon.get_next_event(
        after=gap["next_sequence"], stream=gap["stream"], timeout=0
    )
    assert not next_event["gap"]
    assert next_event["event"]["sequence"] == 1006


def test_new_instance_refuses_old_cursor_even_with_matching_sequence():
    first, second = CoreHttpDaemon(Mock()), CoreHttpDaemon(Mock())
    publish(first, 3)
    publish(second, 3)
    old = first.get_next_event(after=0, timeout=0)
    value = second.get_next_event(
        after=old["next_sequence"], stream=old["stream"], timeout=0
    )
    assert value["reason"] == "restart"
    assert value["event"] is None


def test_event_payload_is_detached():
    daemon = CoreHttpDaemon(Mock())
    publish(daemon, 1)
    first = daemon.get_next_event(after=0, timeout=0)
    first["event"]["payload"]["nested"]["number"] = "damaged"
    assert (
        daemon.get_next_event(after=0, timeout=0)["event"]["payload"]["nested"][
            "number"
        ]
        == 0
    )


@pytest.mark.parametrize(
    "after,timeout", ((-1, 0), (True, 0), (0, float("nan")), (0, 61))
)
def test_cursor_limits_apply_to_direct_callers(after, timeout):
    with pytest.raises(ValueError):
        CoreHttpDaemon(Mock()).get_next_event(after=after, timeout=timeout)
