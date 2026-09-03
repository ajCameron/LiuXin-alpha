"""Synchronous storage operations remain ordinary, explicit calls."""

from __future__ import annotations


def test_bound_location_is_a_lazy_sync_facade(manager, location, payload) -> None:
    bound = manager.bind(location)
    assert not bound.exists()

    bound.write_bytes(payload)
    with bound.open_read(offset=9, length=8) as source:
        assert source.read() == payload[9:17]


def test_binding_never_changes_the_durable_value(manager, location) -> None:
    first = manager.bind(location)
    second = manager.bind(location)

    assert first is not second
    assert first.location is location
    assert second.location is location
