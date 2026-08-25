"""Async applications keep adaptation outside the durable value object."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor


def test_async_code_can_call_the_explicit_sync_boundary_without_a_fake_value(
    manager, location, payload,
) -> None:
    async def run() -> bytes:
        bound = manager.bind(location)
        bound.write_bytes(payload)
        await asyncio.sleep(0)
        return bound.read_bytes()

    assert asyncio.run(run()) == payload


def test_thread_adaptation_keeps_one_durable_location(manager, location, payload) -> None:
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
    bound = manager.bind(location)
    for operation in ("aopen", "aread_bytes", "awrite_bytes", "aexists"):
        assert not hasattr(location, operation)
        assert not hasattr(bound, operation)
