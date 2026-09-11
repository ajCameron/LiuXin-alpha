"""
Check bound synchronous reads/writes and address-reference preservation.

These cases use real temporary filesystem bytes and a transient storage manager;
binding itself neither creates a file nor changes the durable address value.
"""

from __future__ import annotations


def test_bound_location_is_a_lazy_sync_facade(manager, location, payload) -> None:
    """
    Bind an unpublished address, confirm absence, then write the payload and read bytes 9 through 16
    using a context-managed bounded stream. The selected write/read operations use the manager's
    filesystem Store.

    Example:
        >>> test_bound_location_is_a_lazy_sync_facade(manager, location, payload)  # doctest: +SKIP


    :param manager: Fixture providing a transient manager attached to the primary and secondary filesystem Stores.
    :param location: Primary Store Location for objects/book.epub; resolving it does not publish bytes.
    :param payload: Fixed nonempty byte payload supplied by the shared fixture.
    :return: None after the stated contract assertions pass.
    """
    bound = manager.bind(location)
    assert not bound.exists()

    bound.write_bytes(payload)
    with bound.open_read(offset=9, length=8) as source:
        assert source.read() == payload[9:17]


def test_binding_never_changes_the_durable_value(manager, location) -> None:
    """
    Bind the same Location twice and require distinct facade objects retaining the exact original
    Location reference. This checks binding identity without probing or writing bytes.

    Example:
        >>> test_binding_never_changes_the_durable_value(manager, location)  # doctest: +SKIP


    :param manager: Fixture providing a transient manager attached to the primary and secondary filesystem Stores.
    :param location: Primary Store Location for objects/book.epub; resolving it does not publish bytes.
    :return: None after the stated contract assertions pass.
    """
    first = manager.bind(location)
    second = manager.bind(location)

    assert first is not second
    assert first.location is location
    assert second.location is location
