"""
Provide test sync async utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test sync async through a consuming regression::

        python -m pytest -q tests/utils/test_sync_async.py
"""
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
    """
    Perform the test background event loop is lazy reusable and thread safe utility operation under explicit compatibility rules.

    Example:
        Exercise test background event loop is lazy reusable and thread safe through a consuming regression::

            python -m pytest -q tests/utils/test_sync_async.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    runner = BackgroundEventLoop(thread_name="test-async-bridge")
    assert runner.running is False

    async def thread_id() -> int:
        """
        Perform the thread id utility operation under explicit compatibility rules.

        Example:
            Exercise test background event loop is lazy reusable and thread safe.thread id through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
    """
    Perform the test background event loop rejects waiting from its own thread utility operation under explicit compatibility rules.

    Example:
        Exercise test background event loop rejects waiting from its own thread through a consuming regression::

            python -m pytest -q tests/utils/test_sync_async.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    runner = BackgroundEventLoop()

    async def misuse_bridge() -> None:
        """
        Perform the misuse bridge utility operation under explicit compatibility rules.

        Example:
            Exercise test background event loop rejects waiting from its own thread.misuse bridge through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        runner.run(asyncio.sleep(0))

    with pytest.raises(RuntimeError, match="bridge event-loop thread"):
        runner.run(misuse_bridge())
    runner.close()


def test_call_in_thread_runs_blocking_work_off_the_event_loop() -> None:
    """
    Perform the test call in thread runs blocking work off the event loop utility operation under explicit compatibility rules.

    Example:
        Exercise test call in thread runs blocking work off the event loop through a consuming regression::

            python -m pytest -q tests/utils/test_sync_async.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    async def exercise() -> tuple[int, int]:
        """
        Perform the exercise utility operation under explicit compatibility rules.

        Example:
            Exercise test call in thread runs blocking work off the event loop.exercise through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        event_loop_thread = threading.get_ident()
        worker_thread = await call_in_thread(threading.get_ident)
        return event_loop_thread, worker_thread

    event_loop_thread, worker_thread = asyncio.run(exercise())
    assert worker_thread != event_loop_thread


def test_iterate_in_thread_streams_on_one_thread_and_propagates_errors() -> None:
    """
    Perform the test iterate in thread streams on one thread and propagates errors utility operation under explicit compatibility rules.

    Example:
        Exercise test iterate in thread streams on one thread and propagates errors through a consuming regression::

            python -m pytest -q tests/utils/test_sync_async.py


    :return: An iterator yielding the normalized values described above.
    """
    producer_threads: list[int] = []

    def values():
        """
        Perform the values utility operation under explicit compatibility rules.

        Example:
            Exercise test iterate in thread streams on one thread and propagates errors.values through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :return: An iterator yielding the normalized values described above.
        """
        producer_threads.append(threading.get_ident())
        yield 1
        producer_threads.append(threading.get_ident())
        yield 2
        producer_threads.append(threading.get_ident())
        raise ValueError("remote listing failed")

    async def exercise() -> list[int]:
        """
        Perform the exercise utility operation under explicit compatibility rules.

        Example:
            Exercise test iterate in thread streams on one thread and propagates errors.exercise through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        observed: list[int] = []
        with pytest.raises(ValueError, match="remote listing failed"):
            async for value in iterate_in_thread(values):
                observed.append(value)
        return observed

    assert asyncio.run(exercise()) == [1, 2]
    assert len(set(producer_threads)) == 1


def test_iterate_in_thread_closes_a_partially_consumed_iterator() -> None:
    """
    Perform the test iterate in thread closes a partially consumed iterator utility operation under explicit compatibility rules.

    Example:
        Exercise test iterate in thread closes a partially consumed iterator through a consuming regression::

            python -m pytest -q tests/utils/test_sync_async.py


    :return: An iterator yielding the normalized values described above.
    """
    closed = threading.Event()

    def values():
        """
        Perform the values utility operation under explicit compatibility rules.

        Example:
            Exercise test iterate in thread closes a partially consumed iterator.values through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :return: An iterator yielding the normalized values described above.
        """
        try:
            yield 1
            yield 2
        finally:
            closed.set()

    async def exercise() -> None:
        """
        Perform the exercise utility operation under explicit compatibility rules.

        Example:
            Exercise test iterate in thread closes a partially consumed iterator.exercise through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        iterator = iterate_in_thread(values)
        assert await anext(iterator) == 1
        await iterator.aclose()

    asyncio.run(exercise())
    assert closed.is_set()


def test_iterate_async_synchronously_is_lazy_and_closes_early() -> None:
    """
    Perform the test iterate async synchronously is lazy and closes early utility operation under explicit compatibility rules.

    Example:
        Exercise test iterate async synchronously is lazy and closes early through a consuming regression::

            python -m pytest -q tests/utils/test_sync_async.py


    :return: An iterator yielding the normalized values described above.
    """
    produced: list[int] = []
    closed = threading.Event()

    async def values():
        """
        Perform the values utility operation under explicit compatibility rules.

        Example:
            Exercise test iterate async synchronously is lazy and closes early.values through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :return: An iterator yielding the normalized values described above.
        """
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
    """
    Perform the test async context from sync preserves exit semantics utility operation under explicit compatibility rules.

    Example:
        Exercise test async context from sync preserves exit semantics through a consuming regression::

            python -m pytest -q tests/utils/test_sync_async.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    exits: list[type[BaseException] | None] = []

    class SyncContext:
        """
        Adapt the SyncContext interface while preserving calls, results and cleanup.

        Example:
            Exercise test async context from sync preserves exit semantics.SyncContext through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py
        """
        def __enter__(self) -> str:
            """
            Implement the resource's enter lifecycle operation.

            Example:
                Exercise test async context from sync preserves exit semantics.SyncContext.  enter   through a consuming regression::

                    python -m pytest -q tests/utils/test_sync_async.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return "entered"

        def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool:
            """
            Implement the resource's exit lifecycle operation.

            Example:
                Exercise test async context from sync preserves exit semantics.SyncContext.  exit   through a consuming regression::

                    python -m pytest -q tests/utils/test_sync_async.py


            :param exc_type: Value supplied for exc type under the utility contract.
            :param exc: Value supplied for exc under the utility contract.
            :param traceback: Value supplied for traceback under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            exits.append(exc_type)
            return exc_type is LookupError

    async def exercise() -> str:
        """
        Perform the exercise utility operation under explicit compatibility rules.

        Example:
            Exercise test async context from sync preserves exit semantics.exercise through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        async with AsyncContextFromSync(SyncContext) as value:
            raise LookupError("suppressed by the sync context")
        return value

    assert asyncio.run(exercise()) == "entered"
    assert exits == [LookupError]


def test_sync_context_from_async_preserves_exit_semantics() -> None:
    """
    Perform the test sync context from async preserves exit semantics utility operation under explicit compatibility rules.

    Example:
        Exercise test sync context from async preserves exit semantics through a consuming regression::

            python -m pytest -q tests/utils/test_sync_async.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    exits: list[type[BaseException] | None] = []

    class AsyncContext:
        """
        Adapt the AsyncContext interface while preserving calls, results and cleanup.

        Example:
            Exercise test sync context from async preserves exit semantics.AsyncContext through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py
        """
        async def __aenter__(self) -> str:
            """
            Implement the resource's aenter lifecycle operation.

            Example:
                Exercise test sync context from async preserves exit semantics.AsyncContext.  aenter   through a consuming regression::

                    python -m pytest -q tests/utils/test_sync_async.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return "entered"

        async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool:
            """
            Implement the resource's aexit lifecycle operation.

            Example:
                Exercise test sync context from async preserves exit semantics.AsyncContext.  aexit   through a consuming regression::

                    python -m pytest -q tests/utils/test_sync_async.py


            :param exc_type: Value supplied for exc type under the utility contract.
            :param exc: Value supplied for exc under the utility contract.
            :param traceback: Value supplied for traceback under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            exits.append(exc_type)
            return exc_type is LookupError

    with BackgroundEventLoop() as runner:
        with SyncContextFromAsync(AsyncContext(), runner=runner) as value:
            assert value == "entered"
            raise LookupError("suppressed by the async context")

    assert exits == [LookupError]


def test_file_open_adapters_close_and_forward_operations() -> None:
    """
    Perform the test file open adapters close and forward operations utility operation under explicit compatibility rules.

    Example:
        Exercise test file open adapters close and forward operations through a consuming regression::

            python -m pytest -q tests/utils/test_sync_async.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    sync_buffer = io.BytesIO(b"sync")

    async def read_sync_file() -> bytes:
        """
        Read sync file under the documented compatibility and safety rules.

        Example:
            Exercise test file open adapters close and forward operations.read sync file through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        async with AsyncOpenFromSync(lambda: sync_buffer) as source:
            return await source.read()

    assert asyncio.run(read_sync_file()) == b"sync"
    assert sync_buffer.closed is True

    class AsyncFile:
        """
        Adapt the AsyncFile interface while preserving calls, results and cleanup.

        Example:
            Exercise test file open adapters close and forward operations.AsyncFile through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py
        """
        def __init__(self) -> None:
            """
            Initialize and validate the AsyncFile state.

            Example:
                Exercise test file open adapters close and forward operations.AsyncFile.  init   through a consuming regression::

                    python -m pytest -q tests/utils/test_sync_async.py


            :return: None; validated state is stored on the receiving object.
            """
            self.buffer = io.BytesIO(b"async")

        async def read(self, size: int = -1) -> bytes:
            """
            Forward the read operation while preserving adapter ownership rules.

            Example:
                Exercise test file open adapters close and forward operations.AsyncFile.read through a consuming regression::

                    python -m pytest -q tests/utils/test_sync_async.py


            :param size: Value supplied for size under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.buffer.read(size)

        async def write(self, data: bytes) -> int:
            """
            Forward the write operation while preserving adapter ownership rules.

            Example:
                Exercise test file open adapters close and forward operations.AsyncFile.write through a consuming regression::

                    python -m pytest -q tests/utils/test_sync_async.py


            :param data: Value supplied for data under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.buffer.write(data)

        async def flush(self) -> None:
            """
            Forward the flush operation while preserving adapter ownership rules.

            Example:
                Exercise test file open adapters close and forward operations.AsyncFile.flush through a consuming regression::

                    python -m pytest -q tests/utils/test_sync_async.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return None

    class AsyncOpen:
        """
        Adapt the AsyncOpen interface while preserving calls, results and cleanup.

        Example:
            Exercise test file open adapters close and forward operations.AsyncOpen through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py
        """
        def __init__(self) -> None:
            """
            Initialize and validate the AsyncOpen state.

            Example:
                Exercise test file open adapters close and forward operations.AsyncOpen.  init   through a consuming regression::

                    python -m pytest -q tests/utils/test_sync_async.py


            :return: None; validated state is stored on the receiving object.
            """
            self.file = AsyncFile()
            self.exits = 0

        async def __aenter__(self) -> AsyncFile:
            """
            Implement the resource's aenter lifecycle operation.

            Example:
                Exercise test file open adapters close and forward operations.AsyncOpen.  aenter   through a consuming regression::

                    python -m pytest -q tests/utils/test_sync_async.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.file

        async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
            """
            Implement the resource's aexit lifecycle operation.

            Example:
                Exercise test file open adapters close and forward operations.AsyncOpen.  aexit   through a consuming regression::

                    python -m pytest -q tests/utils/test_sync_async.py


            :param exc_type: Value supplied for exc type under the utility contract.
            :param exc: Value supplied for exc under the utility contract.
            :param traceback: Value supplied for traceback under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.exits += 1

    opened = AsyncOpen()
    with BackgroundEventLoop() as runner:
        with SyncOpenFromAsync(lambda: opened, runner=runner) as source:
            assert source.read() == b"async"
            source.flush()

    assert opened.exits == 1


def test_async_native_sync_facade_bridges_calls_iteration_and_files() -> None:
    """
    Perform the test async native sync facade bridges calls iteration and files utility operation under explicit compatibility rules.

    Example:
        Exercise test async native sync facade bridges calls iteration and files through a consuming regression::

            python -m pytest -q tests/utils/test_sync_async.py


    :return: An iterator yielding the normalized values described above.
    """
    class AsyncFile:
        """
        Adapt the AsyncFile interface while preserving calls, results and cleanup.

        Example:
            Exercise test async native sync facade bridges calls iteration and files.AsyncFile through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py
        """
        async def read(self, size: int = -1) -> bytes:
            """
            Forward the read operation while preserving adapter ownership rules.

            Example:
                Exercise test async native sync facade bridges calls iteration and files.AsyncFile.read through a consuming regression::

                    python -m pytest -q tests/utils/test_sync_async.py


            :param size: Value supplied for size under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return b"async"[:size] if size >= 0 else b"async"

        async def write(self, data: bytes) -> int:
            """
            Forward the write operation while preserving adapter ownership rules.

            Example:
                Exercise test async native sync facade bridges calls iteration and files.AsyncFile.write through a consuming regression::

                    python -m pytest -q tests/utils/test_sync_async.py


            :param data: Value supplied for data under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return len(data)

        async def flush(self) -> None:
            """
            Forward the flush operation while preserving adapter ownership rules.

            Example:
                Exercise test async native sync facade bridges calls iteration and files.AsyncFile.flush through a consuming regression::

                    python -m pytest -q tests/utils/test_sync_async.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return None

    class AsyncOpen:
        """
        Adapt the AsyncOpen interface while preserving calls, results and cleanup.

        Example:
            Exercise test async native sync facade bridges calls iteration and files.AsyncOpen through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py
        """
        async def __aenter__(self) -> AsyncFile:
            """
            Implement the resource's aenter lifecycle operation.

            Example:
                Exercise test async native sync facade bridges calls iteration and files.AsyncOpen.  aenter   through a consuming regression::

                    python -m pytest -q tests/utils/test_sync_async.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return AsyncFile()

        async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
            """
            Implement the resource's aexit lifecycle operation.

            Example:
                Exercise test async native sync facade bridges calls iteration and files.AsyncOpen.  aexit   through a consuming regression::

                    python -m pytest -q tests/utils/test_sync_async.py


            :param exc_type: Value supplied for exc type under the utility contract.
            :param exc: Value supplied for exc under the utility contract.
            :param traceback: Value supplied for traceback under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return None

    class Driver(AsyncNativeSyncFacade):
        """
        Provide the Driver utility contract with explicit state and cleanup behavior.

        Example:
            Exercise test async native sync facade bridges calls iteration and files.Driver through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py
        """
        async def astat(self) -> int:
            """
            Perform the astat utility operation under explicit compatibility rules.

            Example:
                Exercise test async native sync facade bridges calls iteration and files.Driver.astat through a consuming regression::

                    python -m pytest -q tests/utils/test_sync_async.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return 42

        async def aiter(self):
            """
            Perform the aiter utility operation under explicit compatibility rules.

            Example:
                Exercise test async native sync facade bridges calls iteration and files.Driver.aiter through a consuming regression::

                    python -m pytest -q tests/utils/test_sync_async.py


            :return: An iterator yielding the normalized values described above.
            """
            yield "one"
            yield "two"

    driver = Driver()
    assert driver.run_async(driver.astat()) == 42
    assert list(driver.iterate_async(driver.aiter)) == ["one", "two"]
    with driver.open_async(AsyncOpen) as source:
        assert source.read() == b"async"


def test_sync_native_async_facade_bridges_calls_iteration_and_files() -> None:
    """
    Perform the test sync native async facade bridges calls iteration and files utility operation under explicit compatibility rules.

    Example:
        Exercise test sync native async facade bridges calls iteration and files through a consuming regression::

            python -m pytest -q tests/utils/test_sync_async.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    class Driver(SyncNativeAsyncFacade):
        """
        Provide the Driver utility contract with explicit state and cleanup behavior.

        Example:
            Exercise test sync native async facade bridges calls iteration and files.Driver through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py
        """
        def stat(self) -> int:
            """
            Perform the stat utility operation under explicit compatibility rules.

            Example:
                Exercise test sync native async facade bridges calls iteration and files.Driver.stat through a consuming regression::

                    python -m pytest -q tests/utils/test_sync_async.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return 42

        def iterate(self):
            """
            Perform the iterate utility operation under explicit compatibility rules.

            Example:
                Exercise test sync native async facade bridges calls iteration and files.Driver.iterate through a consuming regression::

                    python -m pytest -q tests/utils/test_sync_async.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return iter(("one", "two"))

    async def exercise() -> tuple[int, list[str], bytes]:
        """
        Perform the exercise utility operation under explicit compatibility rules.

        Example:
            Exercise test sync native async facade bridges calls iteration and files.exercise through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        driver = Driver()
        status = await driver.call_sync(driver.stat)
        values = [value async for value in driver.iterate_sync(driver.iterate)]
        async with driver.open_sync(lambda: io.BytesIO(b"sync")) as source:
            payload = await source.read()
        return status, values, payload

    assert asyncio.run(exercise()) == (42, ["one", "two"], b"sync")
