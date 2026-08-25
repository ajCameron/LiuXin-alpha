from __future__ import annotations

import asyncio
import io
import threading

from typing import Any

import pytest

from LiuXin_alpha.utils.sync_async import (
    AsyncContextFromSync,
    AsyncNativeSyncFacade,
    AsyncOpenFromSync,
    BackgroundEventLoop,
    SyncContextFromAsync,
    SyncNativeAsyncFacade,
    SyncOpenFromAsync,
    call_in_thread,
    iterate_async_synchronously,
    iterate_in_thread,
)


def test_background_event_loop_is_lazy_reusable_and_thread_safe() -> None:
    runner = BackgroundEventLoop(thread_name="test-async-bridge")
    assert runner.running is False

    async def thread_id() -> int:
        return threading.get_ident()

    caller_thread = threading.get_ident()
    first_bridge_thread = runner.run(thread_id())
    assert first_bridge_thread != caller_thread
    assert runner.running is True

    results: list[int] = []
    workers = [threading.Thread(target=lambda: results.append(runner.run(thread_id()))) for _ in range(8)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()

    assert results == [first_bridge_thread] * 8
    runner.close()
    runner.close()
    assert runner.running is False

    second_bridge_thread = runner.run(thread_id())
    assert second_bridge_thread != caller_thread
    runner.close()


def test_background_event_loop_rejects_waiting_from_its_own_thread() -> None:
    runner = BackgroundEventLoop()

    async def misuse_bridge() -> None:
        runner.run(asyncio.sleep(0))

    with pytest.raises(RuntimeError, match="bridge event-loop thread"):
        runner.run(misuse_bridge())
    runner.close()


def test_call_in_thread_runs_blocking_work_off_the_event_loop() -> None:
    async def exercise() -> tuple[int, int]:
        event_loop_thread = threading.get_ident()
        worker_thread = await call_in_thread(threading.get_ident)
        return event_loop_thread, worker_thread

    event_loop_thread, worker_thread = asyncio.run(exercise())
    assert worker_thread != event_loop_thread


def test_iterate_in_thread_streams_on_one_thread_and_propagates_errors() -> None:
    producer_threads: list[int] = []

    def values():
        producer_threads.append(threading.get_ident())
        yield 1
        producer_threads.append(threading.get_ident())
        yield 2
        producer_threads.append(threading.get_ident())
        raise ValueError("remote listing failed")

    async def exercise() -> list[int]:
        observed: list[int] = []
        with pytest.raises(ValueError, match="remote listing failed"):
            async for value in iterate_in_thread(values):
                observed.append(value)
        return observed

    assert asyncio.run(exercise()) == [1, 2]
    assert len(set(producer_threads)) == 1


def test_iterate_in_thread_closes_a_partially_consumed_iterator() -> None:
    closed = threading.Event()

    def values():
        try:
            yield 1
            yield 2
        finally:
            closed.set()

    async def exercise() -> None:
        iterator = iterate_in_thread(values)
        assert await anext(iterator) == 1
        await iterator.aclose()

    asyncio.run(exercise())
    assert closed.is_set()


def test_iterate_async_synchronously_is_lazy_and_closes_early() -> None:
    produced: list[int] = []
    closed = threading.Event()

    async def values():
        try:
            for value in range(3):
                produced.append(value)
                yield value
        finally:
            closed.set()

    iterator = iterate_async_synchronously(values)
    assert produced == []
    assert next(iterator) == 0
    assert produced == [0]
    iterator.close()
    assert closed.is_set()


def test_async_context_from_sync_preserves_exit_semantics() -> None:
    exits: list[type[BaseException] | None] = []

    class SyncContext:
        def __enter__(self) -> str:
            return "entered"

        def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool:
            exits.append(exc_type)
            return exc_type is LookupError

    async def exercise() -> str:
        async with AsyncContextFromSync(SyncContext) as value:
            raise LookupError("suppressed by the sync context")
        return value

    assert asyncio.run(exercise()) == "entered"
    assert exits == [LookupError]


def test_sync_context_from_async_preserves_exit_semantics() -> None:
    exits: list[type[BaseException] | None] = []

    class AsyncContext:
        async def __aenter__(self) -> str:
            return "entered"

        async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool:
            exits.append(exc_type)
            return exc_type is LookupError

    with BackgroundEventLoop() as runner:
        with SyncContextFromAsync(AsyncContext(), runner=runner) as value:
            assert value == "entered"
            raise LookupError("suppressed by the async context")

    assert exits == [LookupError]


def test_file_open_adapters_close_and_forward_operations() -> None:
    sync_buffer = io.BytesIO(b"sync")

    async def read_sync_file() -> bytes:
        async with AsyncOpenFromSync(lambda: sync_buffer) as source:
            return await source.read()

    assert asyncio.run(read_sync_file()) == b"sync"
    assert sync_buffer.closed is True

    class AsyncFile:
        def __init__(self) -> None:
            self.buffer = io.BytesIO(b"async")

        async def read(self, size: int = -1) -> bytes:
            return self.buffer.read(size)

        async def write(self, data: bytes) -> int:
            return self.buffer.write(data)

        async def flush(self) -> None:
            return None

    class AsyncOpen:
        def __init__(self) -> None:
            self.file = AsyncFile()
            self.exits = 0

        async def __aenter__(self) -> AsyncFile:
            return self.file

        async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
            self.exits += 1

    opened = AsyncOpen()
    with BackgroundEventLoop() as runner:
        with SyncOpenFromAsync(lambda: opened, runner=runner) as source:
            assert source.read() == b"async"
            source.flush()

    assert opened.exits == 1


def test_async_native_sync_facade_bridges_calls_iteration_and_files() -> None:
    class AsyncFile:
        async def read(self, size: int = -1) -> bytes:
            return b"async"[:size] if size >= 0 else b"async"

        async def write(self, data: bytes) -> int:
            return len(data)

        async def flush(self) -> None:
            return None

    class AsyncOpen:
        async def __aenter__(self) -> AsyncFile:
            return AsyncFile()

        async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
            return None

    class Driver(AsyncNativeSyncFacade):
        async def astat(self) -> int:
            return 42

        async def aiter(self):
            yield "one"
            yield "two"

    driver = Driver()
    assert driver.run_async(driver.astat()) == 42
    assert list(driver.iterate_async(driver.aiter)) == ["one", "two"]
    with driver.open_async(AsyncOpen) as source:
        assert source.read() == b"async"


def test_sync_native_async_facade_bridges_calls_iteration_and_files() -> None:
    class Driver(SyncNativeAsyncFacade):
        def stat(self) -> int:
            return 42

        def iterate(self):
            return iter(("one", "two"))

    async def exercise() -> tuple[int, list[str], bytes]:
        driver = Driver()
        status = await driver.call_sync(driver.stat)
        values = [value async for value in driver.iterate_sync(driver.iterate)]
        async with driver.open_sync(lambda: io.BytesIO(b"sync")) as source:
            payload = await source.read()
        return status, values, payload

    assert asyncio.run(exercise()) == (42, ["one", "two"], b"sync")
