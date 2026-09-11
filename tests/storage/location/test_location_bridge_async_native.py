"""
Exercise application-owned coroutine/thread adaptation of synchronous routing.

The coroutine deliberately performs synchronous I/O on its event-loop thread;
threaded range reads use one bound address. Neither case introduces an async API
on Location or BoundLocation or measures event-loop responsiveness.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor


def test_async_code_can_call_the_explicit_sync_boundary_without_a_fake_value(
    manager, location, payload,
) -> None:
    """
    Run a coroutine that writes and reads through one bound Location around a scheduling yield. Both
    I/O calls are synchronous on the event-loop thread; the assertion verifies byte equality, not
    nonblocking adaptation.

    Example:
        >>> test_async_code_can_call_the_explicit_sync_boundary_without_a_fake_value(manager, location, payload)  # doctest: +SKIP


    :param manager: Fixture providing a transient manager attached to the primary and secondary filesystem Stores.
    :param location: Primary Store Location for objects/book.epub; resolving it does not publish bytes.
    :param payload: Fixed nonempty byte payload supplied by the shared fixture.
    :return: None after the stated contract assertions pass.
    """
    async def run() -> bytes:
        """
        Bind the captured address, write the captured payload synchronously, yield once to the event
        loop, then return a synchronous full read. The I/O is not moved to an executor.

        Example:
            >>> value = await run()  # doctest: +SKIP


        :return: Bytes returned by the bound read; write/read failures propagate through the coroutine.
        """
        bound = manager.bind(location)
        bound.write_bytes(payload)
        await asyncio.sleep(0)
        return bound.read_bytes()

    assert asyncio.run(run()) == payload


def test_thread_adaptation_keeps_one_durable_location(manager, location, payload) -> None:
    """
    Publish the payload, submit five three-byte range reads through one bound facade, and compare
    results in submission order with the expected slices. The thread-pool context waits for workers;
    the durable address is shared unchanged.

    Example:
        >>> test_thread_adaptation_keeps_one_durable_location(manager, location, payload)  # doctest: +SKIP


    :param manager: Fixture providing a transient manager attached to the primary and secondary filesystem Stores.
    :param location: Primary Store Location for objects/book.epub; resolving it does not publish bytes.
    :param payload: Fixed nonempty byte payload supplied by the shared fixture.
    :return: None after the stated contract assertions pass.
    """
    manager.write_bytes(location, payload)
    bound = manager.bind(location)
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(bound.read_bytes, offset=i, length=3)
            for i in range(5)
        ]
    assert [future.result() for future in futures] == [
        payload[i : i + 3] for i in range(5)
    ]


def test_location_and_bound_location_do_not_pretend_to_be_async(manager, location) -> None:
    """
    Assert that the address and its bound facade lack aopen, aread_bytes, awrite_bytes, and aexists
    attributes. This is a check of those explicit names without performing I/O.

    Example:
        >>> test_location_and_bound_location_do_not_pretend_to_be_async(manager, location)  # doctest: +SKIP


    :param manager: Fixture providing a transient manager attached to the primary and secondary filesystem Stores.
    :param location: Primary Store Location for objects/book.epub; resolving it does not publish bytes.
    :return: None after the stated contract assertions pass.
    """
    bound = manager.bind(location)
    for operation in ("aopen", "aread_bytes", "awrite_bytes", "aexists"):
        assert not hasattr(location, operation)
        assert not hasattr(bound, operation)
