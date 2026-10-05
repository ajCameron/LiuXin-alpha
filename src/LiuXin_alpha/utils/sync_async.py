"""
Bridge synchronous and asynchronous call, context-manager, file and open interfaces.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise sync async through a consuming regression::

        python -m pytest -q tests/utils/test_sync_async.py
"""

from __future__ import annotations

import asyncio
import contextvars
import functools
import threading

from collections.abc import (
    AsyncIterator,
    Callable,
    Coroutine,
    Iterator,
)
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError
from typing import AsyncContextManager, Any, Generic, ParamSpec, TypeVar, cast


P = ParamSpec("P")
T = TypeVar("T")

_WORKER_POOL = ThreadPoolExecutor(thread_name_prefix="LiuXinAsyncWorker")


def _start_event_loop_heartbeat(
    loop: asyncio.AbstractEventLoop,
    *,
    interval: float = 0.05,
) -> Callable[[], None]:
    """
    Bound selector sleep while an operation depends on a worker wake-up.

    Example:
        Exercise  start event loop heartbeat through a consuming regression::

            python -m pytest -q tests/utils/test_sync_async.py


    :param loop: Value supplied for loop under the utility contract.
    :param interval: Value supplied for interval under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    active = True
    handle: asyncio.TimerHandle | None = None

    def heartbeat() -> None:
        """
        Perform the heartbeat utility operation under explicit compatibility rules.

        Example:
            Exercise  start event loop heartbeat.heartbeat through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        nonlocal handle
        if active:
            handle = loop.call_later(interval, heartbeat)

    def stop() -> None:
        """
        Perform the stop utility operation under explicit compatibility rules.

        Example:
            Exercise  start event loop heartbeat.stop through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        nonlocal active
        active = False
        if handle is not None:
            handle.cancel()

    handle = loop.call_later(interval, heartbeat)
    return stop


class BackgroundEventLoop:
    """
    Run coroutines synchronously on one private background event loop.

    Example:
        Exercise BackgroundEventLoop through a consuming regression::

            python -m pytest -q tests/utils/test_sync_async.py
    """

    def __init__(
        self,
        *,
        thread_name: str = "LiuXinAsyncBridge",
        poll_interval: float = 0.05,
    ) -> None:
        """
        Create a lazy runner without starting a thread yet.

        Example:
            Exercise BackgroundEventLoop.  init   through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :param thread_name: Value supplied for thread name under the utility contract.
        :param poll_interval: Value supplied for poll interval under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """

        if poll_interval <= 0:
            raise ValueError("poll_interval must be positive")
        self._thread_name = thread_name
        self._poll_interval = poll_interval
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._started = threading.Event()
        self._start_lock = threading.Lock()
        self._heartbeat: asyncio.TimerHandle | None = None

    @property
    def running(self) -> bool:
        """
        Return whether the background event-loop thread is alive.

        Example:
            Exercise BackgroundEventLoop.running through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        return self._thread is not None and self._thread.is_alive()

    def _thread_main(self) -> None:
        """
        Create and own the event loop inside its dedicated thread.

        Example:
            Exercise BackgroundEventLoop. thread main through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._started.set()

        def heartbeat() -> None:
            # Some embedded/runtime combinations fail to wake a selector through
            # its cross-thread self-pipe.  A short timer bounds that failure
            # without changing coroutine semantics.
            """
            Perform the heartbeat utility operation under explicit compatibility rules.

            Example:
                Exercise BackgroundEventLoop. thread main.heartbeat through a consuming regression::

                    python -m pytest -q tests/utils/test_sync_async.py


            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self._heartbeat = loop.call_later(self._poll_interval, heartbeat)

        self._heartbeat = loop.call_later(self._poll_interval, heartbeat)
        try:
            loop.run_forever()
        finally:
            if self._heartbeat is not None:
                self._heartbeat.cancel()
                self._heartbeat = None
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()

    def ensure_started(self) -> None:
        """
        Start the background loop once, safely under concurrent callers.

        Example:
            Exercise BackgroundEventLoop.ensure started through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

        if self.running:
            return
        with self._start_lock:
            if self.running:
                return
            self._loop = None
            self._started.clear()
            self._thread = threading.Thread(
                target=self._thread_main,
                name=self._thread_name,
                daemon=True,
            )
            self._thread.start()
            self._started.wait()
            if self._loop is None:
                raise RuntimeError("background event loop failed to start")

    def run(
        self,
        coroutine: Coroutine[Any, Any, T],
        *,
        timeout: float | None = None,
    ) -> T:
        """
        Block the caller until ``coroutine`` completes on the private loop.

        Example:
            Exercise BackgroundEventLoop.run through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :param coroutine: Value supplied for coroutine under the utility contract.
        :param timeout: Maximum wait time before the operation fails.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        self.ensure_started()
        if threading.current_thread() is self._thread:
            coroutine.close()
            raise RuntimeError("cannot synchronously wait from the bridge event-loop thread")
        assert self._loop is not None
        future: Future[T] = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        try:
            return future.result(timeout=timeout)
        except TimeoutError:
            future.cancel()
            raise

    def close(self, *, timeout: float | None = None) -> None:
        """
        Stop and join the background loop; repeated calls are safe.

        Example:
            Exercise BackgroundEventLoop.close through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :param timeout: Maximum wait time before the operation fails.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

        with self._start_lock:
            loop = self._loop
            thread = self._thread
            if loop is None or thread is None:
                return
            if threading.current_thread() is thread:
                loop.stop()
                return
            loop.call_soon_threadsafe(loop.stop)
            thread.join(timeout=timeout)
            if thread.is_alive():
                raise TimeoutError("background event loop did not stop before the timeout")
            self._loop = None
            self._thread = None
            self._started.clear()

    def __enter__(self) -> "BackgroundEventLoop":
        """
        Start the runner and return it as a synchronous context manager.

        Example:
            Exercise BackgroundEventLoop.  enter   through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        self.ensure_started()
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        """
        Close the runner when its synchronous context exits.

        Example:
            Exercise BackgroundEventLoop.  exit   through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :param exc_type: Value supplied for exc type under the utility contract.
        :param exc: Value supplied for exc under the utility contract.
        :param traceback: Value supplied for traceback under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

        self.close()


async def call_in_thread(
    function: Callable[P, T],
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> T:
    """
    Call blocking synchronous code without blocking the event loop.

    Example:
        Exercise call in thread through a consuming regression::

            python -m pytest -q tests/utils/test_sync_async.py


    :param function: Value supplied for function under the utility contract.
    :param args: Positional values forwarded to the compatibility implementation.
    :param kwargs: Keyword values forwarded to the compatibility implementation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    loop = asyncio.get_running_loop()
    stop_heartbeat = _start_event_loop_heartbeat(loop)
    context = contextvars.copy_context()
    call = functools.partial(context.run, function, *args, **kwargs)
    try:
        return await loop.run_in_executor(_WORKER_POOL, call)
    finally:
        stop_heartbeat()


async def iterate_in_thread(
    iterator_factory: Callable[[], Iterator[T]],
) -> AsyncIterator[T]:
    """
    Stream one synchronous iterator through an asynchronous interface.

    Example:
        Exercise iterate in thread through a consuming regression::

            python -m pytest -q tests/utils/test_sync_async.py


    :param iterator_factory: Value supplied for iterator factory under the utility
        contract.
    :return: An iterator yielding the normalized values described above.
    """

    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="LiuXinSyncIterator")
    iterator: Iterator[T] | None = None
    exhausted = object()
    stop_heartbeat = _start_event_loop_heartbeat(loop)

    def create_iterator() -> Iterator[T]:
        """
        Perform the create iterator utility operation under explicit compatibility rules.

        Example:
            Exercise iterate in thread.create iterator through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return iter(iterator_factory())

    def next_item() -> T | object:
        """
        Perform the next item utility operation under explicit compatibility rules.

        Example:
            Exercise iterate in thread.next item through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        assert iterator is not None
        try:
            return next(iterator)
        except StopIteration:
            return exhausted

    try:
        iterator = await loop.run_in_executor(executor, create_iterator)
        while True:
            item = await loop.run_in_executor(executor, next_item)
            if item is exhausted:
                break
            yield cast(T, item)
    finally:
        try:
            if iterator is not None:
                close = getattr(iterator, "close", None)
                if callable(close):
                    await loop.run_in_executor(executor, close)
        finally:
            executor.shutdown(wait=True, cancel_futures=True)
            stop_heartbeat()


def iterate_async_synchronously(
    iterator_factory: Callable[[], AsyncIterator[T]],
    *,
    runner: BackgroundEventLoop | None = None,
) -> Iterator[T]:
    """
    Expose an asynchronous iterator as a lazy synchronous iterator.

    Example:
        Exercise iterate async synchronously through a consuming regression::

            python -m pytest -q tests/utils/test_sync_async.py


    :param iterator_factory: Value supplied for iterator factory under the utility
        contract.
    :param runner: Value supplied for runner under the utility contract.
    :return: An iterator yielding the normalized values described above.
    """

    owned_runner = runner is None
    active_runner = runner or BackgroundEventLoop()
    iterator = iterator_factory().__aiter__()
    try:
        while True:
            try:
                yield active_runner.run(anext(iterator))
            except StopAsyncIteration:
                break
    finally:
        close = getattr(iterator, "aclose", None)
        if callable(close):
            active_runner.run(close())
        if owned_runner:
            active_runner.close()


class AsyncContextFromSync(Generic[T]):
    """
    Adapt a synchronous context-manager factory for ``async with``.

    Example:
        Exercise AsyncContextFromSync through a consuming regression::

            python -m pytest -q tests/utils/test_sync_async.py
    """

    def __init__(self, context_factory: Callable[[], Any]) -> None:
        """
        Store a factory so even context construction may block safely.

        Example:
            Exercise AsyncContextFromSync.  init   through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :param context_factory: Value supplied for context factory under the utility
            contract.
        :return: None; validated state is stored on the receiving object.
        """

        self._context_factory = context_factory
        self._context: Any | None = None

    async def __aenter__(self) -> T:
        """
        Construct and enter the synchronous context in a worker thread.

        Example:
            Exercise AsyncContextFromSync.  aenter   through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        self._context = await call_in_thread(self._context_factory)
        enter = getattr(self._context, "__enter__", None)
        if callable(enter):
            return cast(T, await call_in_thread(enter))
        return cast(T, self._context)

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool | None:
        """
        Exit the synchronous context in a worker thread.

        Example:
            Exercise AsyncContextFromSync.  aexit   through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :param exc_type: Value supplied for exc type under the utility contract.
        :param exc: Value supplied for exc under the utility contract.
        :param traceback: Value supplied for traceback under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        if self._context is None:
            return None
        exit_method = getattr(self._context, "__exit__", None)
        if callable(exit_method):
            return await call_in_thread(exit_method, exc_type, exc, traceback)
        close = getattr(self._context, "close", None)
        if callable(close):
            await call_in_thread(close)
        return None


class SyncContextFromAsync(Generic[T]):
    """
    Adapt an asynchronous context manager for synchronous ``with``.

    Example:
        Exercise SyncContextFromAsync through a consuming regression::

            python -m pytest -q tests/utils/test_sync_async.py
    """

    def __init__(
        self,
        context: AsyncContextManager[T],
        *,
        runner: BackgroundEventLoop,
    ) -> None:
        """
        Bind an asynchronous context to an explicit background runner.

        Example:
            Exercise SyncContextFromAsync.  init   through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :param context: Value supplied for context under the utility contract.
        :param runner: Value supplied for runner under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """

        self._context = context
        self._runner = runner
        self._exited = False

    def __enter__(self) -> T:
        """
        Synchronously enter the asynchronous context.

        Example:
            Exercise SyncContextFromAsync.  enter   through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        return self._runner.run(self._context.__aenter__())

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool | None:
        """
        Synchronously exit the asynchronous context once.

        Example:
            Exercise SyncContextFromAsync.  exit   through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :param exc_type: Value supplied for exc type under the utility contract.
        :param exc: Value supplied for exc under the utility contract.
        :param traceback: Value supplied for traceback under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        if self._exited:
            return None
        self._exited = True
        return self._runner.run(self._context.__aexit__(exc_type, exc, traceback))


class AsyncFileFromSync:
    """
    Expose a synchronous file-like object through async methods.

    Example:
        Exercise AsyncFileFromSync through a consuming regression::

            python -m pytest -q tests/utils/test_sync_async.py
    """

    def __init__(self, file_object: Any) -> None:
        """
        Wrap one already-open synchronous file-like object.

        Example:
            Exercise AsyncFileFromSync.  init   through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :param file_object: Value supplied for file object under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """

        self._file = file_object
        self._closed = False

    async def __aenter__(self) -> "AsyncFileFromSync":
        """
        Return this wrapper for use as an asynchronous context manager.

        Example:
            Exercise AsyncFileFromSync.  aenter   through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        return self

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        """
        Close the underlying file on asynchronous context exit.

        Example:
            Exercise AsyncFileFromSync.  aexit   through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :param exc_type: Value supplied for exc type under the utility contract.
        :param exc: Value supplied for exc under the utility contract.
        :param traceback: Value supplied for traceback under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

        await self.close()

    async def read(self, size: int = -1) -> Any:
        """
        Read from the synchronous file in a worker thread.

        Example:
            Exercise AsyncFileFromSync.read through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :param size: Value supplied for size under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        return await call_in_thread(self._file.read, size)

    async def write(self, data: Any) -> int:
        """
        Write to the synchronous file in a worker thread.

        Example:
            Exercise AsyncFileFromSync.write through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :param data: Value supplied for data under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        return await call_in_thread(self._file.write, data)

    async def flush(self) -> None:
        """
        Flush the synchronous file in a worker thread.

        Example:
            Exercise AsyncFileFromSync.flush through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

        await call_in_thread(self._file.flush)

    async def close(self) -> None:
        """
        Close the synchronous file once; repeated calls are safe.

        Example:
            Exercise AsyncFileFromSync.close through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

        if self._closed:
            return
        self._closed = True
        await call_in_thread(self._file.close)


class AsyncOpenFromSync:
    """
    Open a synchronous file lazily and expose it through ``async with``.

    Example:
        Exercise AsyncOpenFromSync through a consuming regression::

            python -m pytest -q tests/utils/test_sync_async.py
    """

    def __init__(self, opener: Callable[[], Any]) -> None:
        """
        Store a synchronous opener for execution in a worker thread.

        Example:
            Exercise AsyncOpenFromSync.  init   through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :param opener: Value supplied for opener under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """

        self._context = AsyncContextFromSync[Any](opener)
        self._file: Any | None = None
        self._wrapper: AsyncFileFromSync | None = None

    async def __aenter__(self) -> AsyncFileFromSync:
        """
        Open the file and return its asynchronous wrapper.

        Example:
            Exercise AsyncOpenFromSync.  aenter   through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        self._file = await self._context.__aenter__()
        self._wrapper = AsyncFileFromSync(self._file)
        return self._wrapper

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool | None:
        """
        Exit the original synchronous file context exactly once.

        Example:
            Exercise AsyncOpenFromSync.  aexit   through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :param exc_type: Value supplied for exc type under the utility contract.
        :param exc: Value supplied for exc under the utility contract.
        :param traceback: Value supplied for traceback under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        if self._wrapper is not None:
            self._wrapper._closed = True
        return await self._context.__aexit__(exc_type, exc, traceback)


class SyncFileFromAsync:
    """
    Expose an entered asynchronous file context as a sync file object.

    Example:
        Exercise SyncFileFromAsync through a consuming regression::

            python -m pytest -q tests/utils/test_sync_async.py
    """

    def __init__(
        self,
        runner: BackgroundEventLoop,
        async_context: AsyncContextManager[Any],
        async_file: Any,
    ) -> None:
        """
        Bind the asynchronous context and file to a background runner.

        Example:
            Exercise SyncFileFromAsync.  init   through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :param runner: Value supplied for runner under the utility contract.
        :param async_context: Value supplied for async context under the utility contract.
        :param async_file: Value supplied for async file under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """

        self._runner = runner
        self._context = async_context
        self._file = async_file
        self._closed = False

    def __enter__(self) -> "SyncFileFromAsync":
        """
        Return this synchronous file wrapper.

        Example:
            Exercise SyncFileFromAsync.  enter   through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool | None:
        """
        Exit the asynchronous context exactly once.

        Example:
            Exercise SyncFileFromAsync.  exit   through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :param exc_type: Value supplied for exc type under the utility contract.
        :param exc: Value supplied for exc under the utility contract.
        :param traceback: Value supplied for traceback under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        if self._closed:
            return None
        self._closed = True
        return self._runner.run(self._context.__aexit__(exc_type, exc, traceback))

    def close(self) -> None:
        """
        Close the asynchronous context; repeated calls are safe.

        Example:
            Exercise SyncFileFromAsync.close through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

        self.__exit__(None, None, None)

    def flush(self) -> None:
        """
        Synchronously flush the asynchronous file object.

        Example:
            Exercise SyncFileFromAsync.flush through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

        self._runner.run(self._file.flush())

    def read(self, size: int = -1) -> Any:
        """
        Synchronously read from the asynchronous file object.

        Example:
            Exercise SyncFileFromAsync.read through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :param size: Value supplied for size under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        return self._runner.run(self._file.read(size))

    def write(self, data: Any) -> int:
        """
        Synchronously write to the asynchronous file object.

        Example:
            Exercise SyncFileFromAsync.write through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :param data: Value supplied for data under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        return self._runner.run(self._file.write(data))


class SyncOpenFromAsync:
    """
    Open an asynchronous file context through a synchronous facade.

    Example:
        Exercise SyncOpenFromAsync through a consuming regression::

            python -m pytest -q tests/utils/test_sync_async.py
    """

    def __init__(
        self,
        opener: Callable[[], AsyncContextManager[Any]],
        *,
        runner: BackgroundEventLoop,
    ) -> None:
        """
        Store an asynchronous opener and the runner that will own it.

        Example:
            Exercise SyncOpenFromAsync.  init   through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :param opener: Value supplied for opener under the utility contract.
        :param runner: Value supplied for runner under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """

        self._opener = opener
        self._runner = runner
        self._file: SyncFileFromAsync | None = None

    def __enter__(self) -> SyncFileFromAsync:
        """
        Enter the async context and return a synchronous file wrapper.

        Example:
            Exercise SyncOpenFromAsync.  enter   through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        async_context = self._opener()
        async_file = self._runner.run(async_context.__aenter__())
        self._file = SyncFileFromAsync(self._runner, async_context, async_file)
        return self._file

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool | None:
        """
        Exit the entered asynchronous file context.

        Example:
            Exercise SyncOpenFromAsync.  exit   through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :param exc_type: Value supplied for exc type under the utility contract.
        :param exc: Value supplied for exc under the utility contract.
        :param traceback: Value supplied for traceback under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        if self._file is None:
            return None
        return self._file.__exit__(exc_type, exc, traceback)


class AsyncNativeSyncFacade:
    """
    Reusable sync facade for a naturally asynchronous implementation.

    Example:
        Exercise AsyncNativeSyncFacade through a consuming regression::

            python -m pytest -q tests/utils/test_sync_async.py
    """

    sync_bridge_runner = BackgroundEventLoop(thread_name="LiuXinAsyncNativeFacade")

    def run_async(
        self,
        coroutine: Coroutine[Any, Any, T],
        *,
        timeout: float | None = None,
    ) -> T:
        """
        Run one asynchronous operation through the synchronous facade.

        Example:
            Exercise AsyncNativeSyncFacade.run async through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :param coroutine: Value supplied for coroutine under the utility contract.
        :param timeout: Maximum wait time before the operation fails.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        return self.sync_bridge_runner.run(coroutine, timeout=timeout)

    def iterate_async(
        self,
        iterator_factory: Callable[[], AsyncIterator[T]],
    ) -> Iterator[T]:
        """
        Expose an asynchronous iterator lazily to synchronous callers.

        Example:
            Exercise AsyncNativeSyncFacade.iterate async through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :param iterator_factory: Value supplied for iterator factory under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        return iterate_async_synchronously(
            iterator_factory,
            runner=self.sync_bridge_runner,
        )

    def open_async(
        self,
        context_factory: Callable[[], AsyncContextManager[Any]],
    ) -> SyncFileFromAsync:
        """
        Enter an asynchronous file context and return a sync file wrapper.

        Example:
            Exercise AsyncNativeSyncFacade.open async through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :param context_factory: Value supplied for context factory under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        async_context = context_factory()
        async_file = self.run_async(async_context.__aenter__())
        return SyncFileFromAsync(
            self.sync_bridge_runner,
            async_context,
            async_file,
        )


class SyncNativeAsyncFacade:
    """
    Reusable async facade for a naturally synchronous implementation.

    Example:
        Exercise SyncNativeAsyncFacade through a consuming regression::

            python -m pytest -q tests/utils/test_sync_async.py
    """

    async def call_sync(
        self,
        function: Callable[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        """
        Run one synchronous operation through the async facade.

        Example:
            Exercise SyncNativeAsyncFacade.call sync through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :param function: Value supplied for function under the utility contract.
        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        return await call_in_thread(function, *args, **kwargs)

    def iterate_sync(
        self,
        iterator_factory: Callable[[], Iterator[T]],
    ) -> AsyncIterator[T]:
        """
        Expose a synchronous iterator lazily to asynchronous callers.

        Example:
            Exercise SyncNativeAsyncFacade.iterate sync through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :param iterator_factory: Value supplied for iterator factory under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        return iterate_in_thread(iterator_factory)

    def open_sync(self, opener: Callable[[], Any]) -> AsyncOpenFromSync:
        """
        Return an async context manager around a synchronous file opener.

        Example:
            Exercise SyncNativeAsyncFacade.open sync through a consuming regression::

                python -m pytest -q tests/utils/test_sync_async.py


        :param opener: Value supplied for opener under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        return AsyncOpenFromSync(opener)


__all__ = [
    "AsyncContextFromSync",
    "AsyncFileFromSync",
    "AsyncNativeSyncFacade",
    "AsyncOpenFromSync",
    "BackgroundEventLoop",
    "SyncContextFromAsync",
    "SyncFileFromAsync",
    "SyncNativeAsyncFacade",
    "SyncOpenFromAsync",
    "call_in_thread",
    "iterate_async_synchronously",
    "iterate_in_thread",
]
