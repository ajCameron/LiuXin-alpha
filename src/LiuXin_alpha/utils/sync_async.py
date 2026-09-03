"""Bridges for exposing synchronous and asynchronous implementations together.

The helpers in this module are deliberately independent of storage.  A driver
may implement its natural I/O style and use these adapters at its public
boundary without teaching generic code about its event loop or worker threads.
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
    """Bound selector sleep while an operation depends on a worker wake-up."""

    active = True
    handle: asyncio.TimerHandle | None = None

    def heartbeat() -> None:
        nonlocal handle
        if active:
            handle = loop.call_later(interval, heartbeat)

    def stop() -> None:
        nonlocal active
        active = False
        if handle is not None:
            handle.cancel()

    handle = loop.call_later(interval, heartbeat)
    return stop


class BackgroundEventLoop:
    """Run coroutines synchronously on one private background event loop.

    Calls from multiple synchronous threads are safe.  ``close()`` is
    idempotent, and the runner can be started again by a later call to
    ``run()``.

    Example:
        >>> async def answer() -> int:
        ...     return 42
        >>> with BackgroundEventLoop() as runner:
        ...     runner.run(answer())
        42
    """

    def __init__(
        self,
        *,
        thread_name: str = "LiuXinAsyncBridge",
        poll_interval: float = 0.05,
    ) -> None:
        """Create a lazy runner without starting a thread yet.

        Example:
            >>> runner = BackgroundEventLoop(thread_name="example-bridge")
            >>> runner.running
            False
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
        """Return whether the background event-loop thread is alive.

        Example:
            >>> runner = BackgroundEventLoop()
            >>> runner.running
            False
        """

        return self._thread is not None and self._thread.is_alive()

    def _thread_main(self) -> None:
        """Create and own the event loop inside its dedicated thread.

        Example:
            This private worker is started by ``run()`` rather than directly.

            >>> runner = BackgroundEventLoop()
            >>> runner.running
            False
        """

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._started.set()

        def heartbeat() -> None:
            # Some embedded/runtime combinations fail to wake a selector through
            # its cross-thread self-pipe.  A short timer bounds that failure
            # without changing coroutine semantics.
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
        """Start the background loop once, safely under concurrent callers.

        Example:
            >>> runner = BackgroundEventLoop()
            >>> runner.ensure_started()
            >>> runner.running
            True
            >>> runner.close()
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
        """Block the caller until ``coroutine`` completes on the private loop.

        ``run()`` must not be called from the runner's own thread because that
        would deadlock.  A timeout cancels the submitted coroutine.

        Example:
            >>> async def value() -> str:
            ...     return "ready"
            >>> runner = BackgroundEventLoop()
            >>> runner.run(value())
            'ready'
            >>> runner.close()
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
        """Stop and join the background loop; repeated calls are safe.

        Example:
            >>> runner = BackgroundEventLoop()
            >>> runner.close()
            >>> runner.close()
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
        """Start the runner and return it as a synchronous context manager.

        Example:
            >>> with BackgroundEventLoop() as runner:
            ...     runner.running
            True
        """

        self.ensure_started()
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        """Close the runner when its synchronous context exits.

        Example:
            >>> runner = BackgroundEventLoop()
            >>> with runner:
            ...     pass
            >>> runner.running
            False
        """

        self.close()


async def call_in_thread(
    function: Callable[P, T],
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> T:
    """Call blocking synchronous code without blocking the event loop.

    Context variables are propagated like ``asyncio.to_thread``.  A dedicated
    module pool avoids coupling bridge cleanup to an event loop's default
    executor, which is important in embedded runtimes with unreliable
    cross-thread selector wake-ups.

    Example:
        >>> asyncio.run(call_in_thread(lambda left, right: left + right, 20, 22))
        42
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
    """Stream one synchronous iterator through an asynchronous interface.

    The factory and every ``next()`` call execute on the same dedicated worker
    thread.  This preserves thread affinity for cursors and remote client
    iterators while providing natural one-item backpressure.

    Example:
        >>> async def collect() -> list[int]:
        ...     return [item async for item in iterate_in_thread(lambda: iter(range(3)))]
        >>> asyncio.run(collect())
        [0, 1, 2]
    """

    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="LiuXinSyncIterator")
    iterator: Iterator[T] | None = None
    exhausted = object()
    stop_heartbeat = _start_event_loop_heartbeat(loop)

    def create_iterator() -> Iterator[T]:
        return iter(iterator_factory())

    def next_item() -> T | object:
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
    """Expose an asynchronous iterator as a lazy synchronous iterator.

    A supplied runner can be shared by a facade.  Without one, the generator
    owns a temporary runner and closes it even when iteration stops early.

    Example:
        >>> async def values():
        ...     for value in range(3):
        ...         yield value
        >>> list(iterate_async_synchronously(values))
        [0, 1, 2]
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
    """Adapt a synchronous context-manager factory for ``async with``.

    Entry and exit both run off the event-loop thread.

    Example:
        >>> import io
        >>> async def read() -> bytes:
        ...     async with AsyncContextFromSync(lambda: io.BytesIO(b"data")) as source:
        ...         return source.read()
        >>> asyncio.run(read())
        b'data'
    """

    def __init__(self, context_factory: Callable[[], Any]) -> None:
        """Store a factory so even context construction may block safely.

        Example:
            >>> adapter = AsyncContextFromSync(lambda: object())
        """

        self._context_factory = context_factory
        self._context: Any | None = None

    async def __aenter__(self) -> T:
        """Construct and enter the synchronous context in a worker thread.

        Example:
            >>> adapter = AsyncContextFromSync(lambda: object())
            >>> hasattr(adapter, "__aenter__")
            True
        """

        self._context = await call_in_thread(self._context_factory)
        enter = getattr(self._context, "__enter__", None)
        if callable(enter):
            return cast(T, await call_in_thread(enter))
        return cast(T, self._context)

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool | None:
        """Exit the synchronous context in a worker thread.

        Example:
            >>> adapter = AsyncContextFromSync(lambda: object())
            >>> hasattr(adapter, "__aexit__")
            True
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
    """Adapt an asynchronous context manager for synchronous ``with``.

    Example:
        >>> class Context:
        ...     async def __aenter__(self): return 42
        ...     async def __aexit__(self, *args): return None
        >>> with BackgroundEventLoop() as runner:
        ...     with SyncContextFromAsync(Context(), runner=runner) as value:
        ...         value
        42
    """

    def __init__(
        self,
        context: AsyncContextManager[T],
        *,
        runner: BackgroundEventLoop,
    ) -> None:
        """Bind an asynchronous context to an explicit background runner.

        Example:
            >>> runner = BackgroundEventLoop()
            >>> hasattr(SyncContextFromAsync, "__enter__")
            True
            >>> runner.close()
        """

        self._context = context
        self._runner = runner
        self._exited = False

    def __enter__(self) -> T:
        """Synchronously enter the asynchronous context.

        Example:
            Entry is normally invoked by a ``with`` statement.

            >>> hasattr(SyncContextFromAsync, "__enter__")
            True
        """

        return self._runner.run(self._context.__aenter__())

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool | None:
        """Synchronously exit the asynchronous context once.

        Example:
            >>> hasattr(SyncContextFromAsync, "__exit__")
            True
        """

        if self._exited:
            return None
        self._exited = True
        return self._runner.run(self._context.__aexit__(exc_type, exc, traceback))


class AsyncFileFromSync:
    """Expose a synchronous file-like object through async methods.

    Example:
        >>> import io
        >>> async def read() -> bytes:
        ...     source = AsyncFileFromSync(io.BytesIO(b"data"))
        ...     return await source.read()
        >>> asyncio.run(read())
        b'data'
    """

    def __init__(self, file_object: Any) -> None:
        """Wrap one already-open synchronous file-like object.

        Example:
            >>> import io
            >>> wrapped = AsyncFileFromSync(io.BytesIO())
        """

        self._file = file_object
        self._closed = False

    async def __aenter__(self) -> "AsyncFileFromSync":
        """Return this wrapper for use as an asynchronous context manager.

        Example:
            >>> hasattr(AsyncFileFromSync, "__aenter__")
            True
        """

        return self

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        """Close the underlying file on asynchronous context exit.

        Example:
            >>> hasattr(AsyncFileFromSync, "__aexit__")
            True
        """

        await self.close()

    async def read(self, size: int = -1) -> Any:
        """Read from the synchronous file in a worker thread.

        Example:
            >>> import io
            >>> asyncio.run(AsyncFileFromSync(io.BytesIO(b"ok")).read())
            b'ok'
        """

        return await call_in_thread(self._file.read, size)

    async def write(self, data: Any) -> int:
        """Write to the synchronous file in a worker thread.

        Example:
            >>> import io
            >>> asyncio.run(AsyncFileFromSync(io.BytesIO()).write(b"ok"))
            2
        """

        return await call_in_thread(self._file.write, data)

    async def flush(self) -> None:
        """Flush the synchronous file in a worker thread.

        Example:
            >>> import io
            >>> asyncio.run(AsyncFileFromSync(io.BytesIO()).flush())
        """

        await call_in_thread(self._file.flush)

    async def close(self) -> None:
        """Close the synchronous file once; repeated calls are safe.

        Example:
            >>> import io
            >>> wrapped = AsyncFileFromSync(io.BytesIO())
            >>> asyncio.run(wrapped.close())
            >>> asyncio.run(wrapped.close())
        """

        if self._closed:
            return
        self._closed = True
        await call_in_thread(self._file.close)


class AsyncOpenFromSync:
    """Open a synchronous file lazily and expose it through ``async with``.

    Example:
        >>> import io
        >>> async def read() -> bytes:
        ...     async with AsyncOpenFromSync(lambda: io.BytesIO(b"data")) as source:
        ...         return await source.read()
        >>> asyncio.run(read())
        b'data'
    """

    def __init__(self, opener: Callable[[], Any]) -> None:
        """Store a synchronous opener for execution in a worker thread.

        Example:
            >>> import io
            >>> adapter = AsyncOpenFromSync(lambda: io.BytesIO())
        """

        self._context = AsyncContextFromSync[Any](opener)
        self._file: Any | None = None
        self._wrapper: AsyncFileFromSync | None = None

    async def __aenter__(self) -> AsyncFileFromSync:
        """Open the file and return its asynchronous wrapper.

        Example:
            >>> hasattr(AsyncOpenFromSync, "__aenter__")
            True
        """

        self._file = await self._context.__aenter__()
        self._wrapper = AsyncFileFromSync(self._file)
        return self._wrapper

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool | None:
        """Exit the original synchronous file context exactly once.

        Example:
            >>> hasattr(AsyncOpenFromSync, "__aexit__")
            True
        """

        if self._wrapper is not None:
            self._wrapper._closed = True
        return await self._context.__aexit__(exc_type, exc, traceback)


class SyncFileFromAsync:
    """Expose an entered asynchronous file context as a sync file object.

    Example:
        A driver facade normally receives this wrapper from
        ``SyncOpenFromAsync`` and uses it with an ordinary ``with`` statement.

        >>> hasattr(SyncFileFromAsync, "read")
        True
    """

    def __init__(
        self,
        runner: BackgroundEventLoop,
        async_context: AsyncContextManager[Any],
        async_file: Any,
    ) -> None:
        """Bind the asynchronous context and file to a background runner.

        Example:
            >>> hasattr(SyncFileFromAsync, "close")
            True
        """

        self._runner = runner
        self._context = async_context
        self._file = async_file
        self._closed = False

    def __enter__(self) -> "SyncFileFromAsync":
        """Return this synchronous file wrapper.

        Example:
            >>> hasattr(SyncFileFromAsync, "__enter__")
            True
        """

        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool | None:
        """Exit the asynchronous context exactly once.

        Example:
            >>> hasattr(SyncFileFromAsync, "__exit__")
            True
        """

        if self._closed:
            return None
        self._closed = True
        return self._runner.run(self._context.__aexit__(exc_type, exc, traceback))

    def close(self) -> None:
        """Close the asynchronous context; repeated calls are safe.

        Example:
            >>> hasattr(SyncFileFromAsync, "close")
            True
        """

        self.__exit__(None, None, None)

    def flush(self) -> None:
        """Synchronously flush the asynchronous file object.

        Example:
            >>> hasattr(SyncFileFromAsync, "flush")
            True
        """

        self._runner.run(self._file.flush())

    def read(self, size: int = -1) -> Any:
        """Synchronously read from the asynchronous file object.

        Example:
            >>> hasattr(SyncFileFromAsync, "read")
            True
        """

        return self._runner.run(self._file.read(size))

    def write(self, data: Any) -> int:
        """Synchronously write to the asynchronous file object.

        Example:
            >>> hasattr(SyncFileFromAsync, "write")
            True
        """

        return self._runner.run(self._file.write(data))


class SyncOpenFromAsync:
    """Open an asynchronous file context through a synchronous facade.

    Example:
        >>> hasattr(SyncOpenFromAsync, "__enter__")
        True
    """

    def __init__(
        self,
        opener: Callable[[], AsyncContextManager[Any]],
        *,
        runner: BackgroundEventLoop,
    ) -> None:
        """Store an asynchronous opener and the runner that will own it.

        Example:
            >>> runner = BackgroundEventLoop()
            >>> adapter = SyncOpenFromAsync  # an async opener is supplied by a driver
            >>> runner.close()
        """

        self._opener = opener
        self._runner = runner
        self._file: SyncFileFromAsync | None = None

    def __enter__(self) -> SyncFileFromAsync:
        """Enter the async context and return a synchronous file wrapper.

        Example:
            >>> hasattr(SyncOpenFromAsync, "__enter__")
            True
        """

        async_context = self._opener()
        async_file = self._runner.run(async_context.__aenter__())
        self._file = SyncFileFromAsync(self._runner, async_context, async_file)
        return self._file

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool | None:
        """Exit the entered asynchronous file context.

        Example:
            >>> hasattr(SyncOpenFromAsync, "__exit__")
            True
        """

        if self._file is None:
            return None
        return self._file.__exit__(exc_type, exc, traceback)


class AsyncNativeSyncFacade:
    """Reusable sync facade for a naturally asynchronous implementation.

    Subclasses keep their domain-specific method names and use ``run_async``,
    ``iterate_async``, and ``open_async`` to expose matching synchronous entry
    points.  The mechanism is useful to storage drivers but has no storage
    dependency.  Async resources used through the facade should be created on
    ``sync_bridge_runner``; a subclass may override that runner when isolation
    is required.

    Example:
        >>> class Service(AsyncNativeSyncFacade):
        ...     async def aanswer(self) -> int:
        ...         return 42
        ...     def answer(self) -> int:
        ...         return self.run_async(self.aanswer())
        >>> Service().answer()
        42
    """

    sync_bridge_runner = BackgroundEventLoop(thread_name="LiuXinAsyncNativeFacade")

    def run_async(
        self,
        coroutine: Coroutine[Any, Any, T],
        *,
        timeout: float | None = None,
    ) -> T:
        """Run one asynchronous operation through the synchronous facade.

        Example:
            >>> class Service(AsyncNativeSyncFacade):
            ...     async def value(self): return "ready"
            >>> Service().run_async(Service().value())
            'ready'
        """

        return self.sync_bridge_runner.run(coroutine, timeout=timeout)

    def iterate_async(
        self,
        iterator_factory: Callable[[], AsyncIterator[T]],
    ) -> Iterator[T]:
        """Expose an asynchronous iterator lazily to synchronous callers.

        Example:
            >>> class Service(AsyncNativeSyncFacade):
            ...     async def values(self):
            ...         yield 1
            ...         yield 2
            >>> list(Service().iterate_async(Service().values))
            [1, 2]
        """

        return iterate_async_synchronously(
            iterator_factory,
            runner=self.sync_bridge_runner,
        )

    def open_async(
        self,
        context_factory: Callable[[], AsyncContextManager[Any]],
    ) -> SyncFileFromAsync:
        """Enter an asynchronous file context and return a sync file wrapper.

        Example:
            Drivers use this at a synchronous ``open`` boundary.

            >>> hasattr(AsyncNativeSyncFacade, "open_async")
            True
        """

        async_context = context_factory()
        async_file = self.run_async(async_context.__aenter__())
        return SyncFileFromAsync(
            self.sync_bridge_runner,
            async_context,
            async_file,
        )


class SyncNativeAsyncFacade:
    """Reusable async facade for a naturally synchronous implementation.

    Example:
        >>> class Service(SyncNativeAsyncFacade):
        ...     def answer(self) -> int:
        ...         return 42
        ...     async def aanswer(self) -> int:
        ...         return await self.call_sync(self.answer)
        >>> asyncio.run(Service().aanswer())
        42
    """

    async def call_sync(
        self,
        function: Callable[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        """Run one synchronous operation through the async facade.

        Example:
            >>> class Service(SyncNativeAsyncFacade):
            ...     def value(self): return "ready"
            >>> asyncio.run(Service().call_sync(Service().value))
            'ready'
        """

        return await call_in_thread(function, *args, **kwargs)

    def iterate_sync(
        self,
        iterator_factory: Callable[[], Iterator[T]],
    ) -> AsyncIterator[T]:
        """Expose a synchronous iterator lazily to asynchronous callers.

        Example:
            >>> class Service(SyncNativeAsyncFacade):
            ...     def values(self): return iter((1, 2))
            >>> async def collect():
            ...     service = Service()
            ...     return [item async for item in service.iterate_sync(service.values)]
            >>> asyncio.run(collect())
            [1, 2]
        """

        return iterate_in_thread(iterator_factory)

    def open_sync(self, opener: Callable[[], Any]) -> AsyncOpenFromSync:
        """Return an async context manager around a synchronous file opener.

        Example:
            >>> import io
            >>> class Service(SyncNativeAsyncFacade): pass
            >>> async def read():
            ...     async with Service().open_sync(lambda: io.BytesIO(b"ok")) as source:
            ...         return await source.read()
            >>> asyncio.run(read())
            b'ok'
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
