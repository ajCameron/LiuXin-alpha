from __future__ import annotations

import asyncio
import os
import pathlib
import threading
from abc import ABC, abstractmethod
from concurrent.futures import Future

from LiuXin_alpha.storage.api.storage_api import StorageBackendAPI

from os import PathLike
from typing import TypeAlias, Any, Iterator, Self, AsyncIterator, overload, TextIO, BinaryIO, IO, cast, Callable, TypeVar

from LiuXin_alpha.storage.api.modes_api import OpenTextMode, OpenBinaryMode, AsyncTextFile, AsyncBinaryFile

T = TypeVar("T")

StrOrBytesPath: TypeAlias = str | bytes | PathLike[str] | PathLike[bytes]
FileDescriptorOrPath: TypeAlias = int | str | bytes | PathLike[str] | PathLike[bytes]


class StoreLocationMixinAPI(ABC):
    """
    ABC mixin for a Path-like object backed by a "store" (local pack, S3, HTTP, etc.).

    Intended usage:
        class MyStorePath(StorePathMixin, pathlib.PurePosixPath): ...
    """

    _tokens: list[str]

    _store: StorageBackendAPI

    def __init__(self, *args: str, store: StorageBackendAPI) -> None:
        """
        Startup the class - including any tokens for the location.

        :param args:
        """
        # Tokenize like pathlib: allow users to pass 'a/b' or ('a','b') etc.
        # Store Locations are always *relative* to the store root; absolute paths are refused.
        tokens: list[str] = []
        for ar in args:
            if ar is None:
                continue
            s = str(ar).replace('\\', '/')
            if s.startswith('/'):
                raise ValueError('Location arguments must be store-relative (no leading /).')
            for seg in s.split('/'):
                if seg in ('', '.'):
                    continue
                if seg == '..':
                    raise ValueError("Location cannot contain '..' segments (store escape risk).")
                tokens.append(seg)

        self._tokens = tokens
        self._store = store

    # ---- Core backend plumbing ----

    @property
    def store(self) -> StorageBackendAPI:
        """Backend handle (client/session/repo/etc.)."""
        return self._store

    @store.setter
    def store(self, store: StorageBackendAPI) -> None:
        """
        Refuses to update the store.

        Every location is bound to a store - you cannot change this once it's set.
        :param store:
        :return:
        """
        raise AttributeError("You cannot change the store.")

    @abstractmethod
    def as_store_key(self) -> str:
        """Canonical key used by the backend (often a POSIX-ish path string)."""


    # ---- Path-like semantics (PurePosix-ish) ----

    def _pure(self) -> pathlib.PurePosixPath:
        """Pure, store-relative path view over this Location (POSIX separators)."""
        return pathlib.PurePosixPath(*self._tokens)

    # ---- pathlib-esque structural fields (store-relative) ----

    @property
    def drive(self) -> str:
        """Always empty: store-relative Locations have no drive."""
        return ""

    @property
    def root(self) -> str:
        """Always empty: store-relative Locations have no root."""
        return ""

    @property
    def anchor(self) -> str:
        """Always empty: store-relative Locations have no anchor."""
        return ""

    def is_absolute(self) -> bool:
        return False

    def is_reserved(self) -> bool:
        # "Reserved" is a Windows filesystem notion; a store-relative Location
        # doesn't have this concept.
        return False

    @property
    def parts(self) -> tuple[str, ...]:
        return tuple(self._tokens)

    @property
    def name(self) -> str:
        return self._pure().name

    @property
    def suffix(self) -> str:
        return self._pure().suffix

    @property
    def suffixes(self) -> list[str]:
        return list(self._pure().suffixes)

    @property
    def stem(self) -> str:
        return self._pure().stem

    @property
    def parent(self) -> Self:
        if not self._tokens:
            return self
        return self.__class__(*self._tokens[:-1], store=self._store)

    @property
    def parents(self) -> tuple[Self, ...]:
        out: list[Self] = []
        toks = self._tokens
        for i in range(len(toks) - 1, -1, -1):
            out.append(self.__class__(*toks[:i], store=self._store))
        return tuple(out)

    def joinpath(self, *other: StrOrBytesPath) -> Self:
        tokens: list[str] = list(self._tokens)
        for o in other:
            s = os.fspath(o).decode() if isinstance(o, (bytes, bytearray)) else str(os.fspath(o))
            s = s.replace('\\', '/')
            if s.startswith('/'):
                raise ValueError('Location.joinpath() arguments must be store-relative (no leading /).')
            for seg in s.split('/'):
                if seg in ('', '.'):
                    continue
                if seg == '..':
                    raise ValueError("Location cannot contain '..' segments (store escape risk).") 
                tokens.append(seg)
        return self.__class__(*tokens, store=self._store)

    def __truediv__(self, key: StrOrBytesPath) -> Self:
        return self.joinpath(key)

    def __rtruediv__(self, key: StrOrBytesPath) -> Self:
        """Allow `'a/b' / loc` style composition (pathlib-like).

        The left-hand side must be store-relative (no leading `/`).
        """
        s = os.fspath(key).decode() if isinstance(key, (bytes, bytearray)) else str(os.fspath(key))
        s = s.replace('\\', '/')
        if s.startswith('/'):
            raise ValueError("Left-hand operand must be store-relative (no leading /).")

        toks: list[str] = []
        for seg in s.split('/'):
            if seg in ('', '.'):
                continue
            if seg == '..':
                raise ValueError("Location cannot contain '..' segments (store escape risk).")
            toks.append(seg)
        toks.extend(self._tokens)
        return self.__class__(*toks, store=self._store)

    def __bytes__(self) -> bytes:
        # Mirror pathlib: bytes(path) is the filesystem-encoded string form.
        return os.fsencode(self.as_posix())

    def with_stem(self, stem: str) -> Self:
        # Python >=3.9: PurePath.with_stem exists.
        p = self._pure()
        if hasattr(p, "with_stem"):
            newp = p.with_stem(stem)  # type: ignore[attr-defined]
        else:  # pragma: no cover
            if not p.name:
                raise ValueError("Can't change the stem of a path with no name")
            newp = p.with_name(stem + p.suffix)
        return self.__class__(*newp.parts, store=self._store)

    def as_uri(self) -> str:
        # We cannot define a meaningful, portable URI for an abstract store path.
        raise ValueError("Store-relative Locations do not have a stable URI.")

    def with_name(self, name: str) -> Self:
        p = self._pure().with_name(name)
        return self.__class__(*p.parts, store=self._store)

    def with_suffix(self, suffix: str) -> Self:
        p = self._pure().with_suffix(suffix)
        return self.__class__(*p.parts, store=self._store)

    def relative_to(self, other: StrOrBytesPath | "StoreLocationMixinAPI") -> Self:
        if isinstance(other, StoreLocationMixinAPI):
            if other.store is not self.store:
                raise ValueError("Cannot compute relative path across different stores.")
            base = pathlib.PurePosixPath(*other._tokens)
        else:
            s = os.fspath(other).decode() if isinstance(other, (bytes, bytearray)) else str(os.fspath(other))
            s = s.replace('\\', '/')
            if s.startswith('/'):
                raise ValueError("Base must be store-relative (no leading /).")
            base = pathlib.PurePosixPath(*[seg for seg in s.split('/') if seg not in ('', '.')])
        rel = self._pure().relative_to(base)
        return self.__class__(*rel.parts, store=self._store)

    def is_relative_to(self, other: StrOrBytesPath | "StoreLocationMixinAPI") -> bool:
        try:
            self.relative_to(other)
            return True
        except Exception:
            return False

    def match(self, pattern: str) -> bool:
        return self._pure().match(pattern)

    def as_posix(self) -> str:
        return self._pure().as_posix()

    def __str__(self) -> str:
        return self.as_posix()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.as_posix()!r})"

    def __fspath__(self) -> str:
        return self.as_store_key()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, StoreLocationMixinAPI):
            return NotImplemented
        return (self.__class__ is other.__class__) and (self.store is other.store) and (self._tokens == other._tokens)

    def __hash__(self) -> int:
        return hash((id(self._store), tuple(self._tokens), self.__class__))

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, StoreLocationMixinAPI):
            return NotImplemented
        if self.__class__ is not other.__class__ or self.store is not other.store:
            raise TypeError("Cannot order Locations from different stores or different classes.")
        return tuple(self._tokens) < tuple(other._tokens)

    # ---- Existence / type checks ----

    @abstractmethod
    def exists(self) -> bool: ...

    @abstractmethod
    async def aexists(self) -> bool: ...

    @abstractmethod
    def is_file(self) -> bool: ...

    @abstractmethod
    async def ais_file(self) -> bool: ...

    @abstractmethod
    def is_dir(self) -> bool: ...

    @abstractmethod
    async def ais_dir(self) -> bool: ...

    # ---- Directory traversal ----

    @abstractmethod
    def iterdir(self) -> Iterator[Self]: ...

    @abstractmethod
    async def aiterdir(self) -> AsyncIterator[Self]: ...

    @abstractmethod
    def glob(self, pattern: str) -> Iterator[Self]: ...

    @abstractmethod
    async def aglob(self, pattern: str) -> AsyncIterator[Self]: ...

    @abstractmethod
    def rglob(self, pattern: str) -> Iterator[Self]: ...

    @abstractmethod
    async def arglob(self, pattern: str) -> AsyncIterator[Self]: ...

    # ---- Metadata ----

    @abstractmethod
    def stat(self) -> os.stat_result: ...

    @abstractmethod
    async def astat(self) -> os.stat_result: ...

    # ---- Mutations ----

    @abstractmethod
    def mkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None: ...

    @abstractmethod
    async def amkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None: ...

    @abstractmethod
    def unlink(self, missing_ok: bool = False) -> None: ...

    @abstractmethod
    async def aunlink(self, missing_ok: bool = False) -> None: ...

    @abstractmethod
    def rmdir(self) -> None: ...

    @abstractmethod
    async def armdir(self) -> None: ...

    @abstractmethod
    def rename(self, target: str | os.PathLike[str]) -> Self: ...

    @abstractmethod
    async def arename(self, target: str | os.PathLike[str]) -> Self: ...

    @abstractmethod
    def replace(self, target: str | os.PathLike[str]) -> Self: ...

    @abstractmethod
    async def areplace(self, target: str | os.PathLike[str]) -> Self: ...

    @abstractmethod
    def touch(self, mode: int = 0o666, exist_ok: bool = True) -> None: ...

    @abstractmethod
    async def atouch(self, mode: int = 0o666, exist_ok: bool = True) -> None: ...

    # ---- I/O (sync) ----

    @overload
    def open(
        self,
        mode: OpenTextMode = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> TextIO: ...

    @overload
    def open(
        self,
        mode: OpenBinaryMode,
        buffering: int = -1,
        encoding: None = None,
        errors: None = None,
        newline: None = None,
    ) -> BinaryIO: ...

    @abstractmethod
    def open(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> IO[Any]:
        """Return a *sync* file-like object."""

    # ---- I/O (async) ----

    @overload
    def aopen(
        self,
        mode: OpenTextMode = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> AsyncTextFile: ...

    @overload
    def aopen(
        self,
        mode: OpenBinaryMode,
        buffering: int = -1,
        encoding: None = None,
        errors: None = None,
        newline: None = None,
    ) -> AsyncBinaryFile: ...

    @abstractmethod
    def aopen(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> AsyncTextFile | AsyncBinaryFile:
        """Return an *async* file object supporting `async with` and async read/write."""

    # ---- Convenience helpers (sync) ----

    def read_bytes(self) -> bytes:
        with self.open("rb") as f:
            return f.read()

    def read_text(self, encoding: str | None = None, errors: str | None = None) -> str:
        with self.open("r", encoding=encoding, errors=errors) as f:
            return f.read()

    def write_bytes(self, data: bytes) -> int:
        with self.open("wb") as f:
            return f.write(data)

    def write_text(
        self,
        data: str,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> int:
        with self.open("w", encoding=encoding, errors=errors, newline=newline) as f:
            return f.write(data)

    # ---- Convenience helpers (async) ----

    async def aread_bytes(self) -> bytes:
        async with self.aopen("rb") as f:
            return await f.read()

    async def aread_text(self, encoding: str | None = None, errors: str | None = None) -> str:
        async with self.aopen("r", encoding=encoding, errors=errors) as f:
            return await f.read()

    async def awrite_bytes(self, data: bytes) -> int:
        async with self.aopen("wb") as f:
            return await f.write(data)

    async def awrite_text(
        self,
        data: str,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> int:
        async with self.aopen("w", encoding=encoding, errors=errors, newline=newline) as f:
            return await f.write(data)


class _AsyncLoopThread:
    """
    A dedicated event loop running in a background thread.
    Used to synchronously wait on coroutines from sync code without
    nesting event loops.
    """
    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._started = threading.Event()

    def _thread_main(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._started.set()
        loop.run_forever()
        loop.close()

    def ensure_started(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._thread_main, name="StorePathAsyncBridge", daemon=True)
        self._thread.start()
        self._started.wait()

    def run(self, coro: "asyncio.Future[T] | asyncio.coroutines.Coroutine[Any, Any, T]") -> T:
        self.ensure_started()
        assert self._loop is not None
        fut: Future[T] = asyncio.run_coroutine_threadsafe(cast(Any, coro), self._loop)
        return fut.result()

    def stop(self) -> None:
        if self._loop is None:
            return
        self._loop.call_soon_threadsafe(self._loop.stop)


class _SyncFileFromAsync:
    """
    A sync file-like wrapper over an async file object + its async context manager.
    """
    def __init__(self, runner: _AsyncLoopThread, async_cm: Any, afile: Any) -> None:
        self._runner = runner
        self._cm = async_cm
        self._afile = afile
        self._closed = False

    def __enter__(self) -> "_SyncFileFromAsync":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool | None:
        if self._closed:
            return None
        self._closed = True
        return self._runner.run(self._cm.__aexit__(exc_type, exc, tb))

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._runner.run(self._cm.__aexit__(None, None, None))

    def flush(self) -> None:
        self._runner.run(self._afile.flush())

    def read(self, n: int = -1) -> Any:
        return self._runner.run(self._afile.read(n))

    def write(self, data: Any) -> int:
        return self._runner.run(self._afile.write(data))


async def _to_thread(fn: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
    # stdlib first; easy to swap to anyio.to_thread.run_sync later if you prefer.
    return await asyncio.to_thread(fn, *args, **kwargs)


async def _aiter_from_sync_iter(iter_fn: Callable[[], Iterator[T]]) -> AsyncIterator[T]:
    """
    Stream a sync iterator into async without materializing the whole list.
    """
    loop = asyncio.get_running_loop()
    q: asyncio.Queue[object] = asyncio.Queue()
    SENTINEL = object()
    EXC = object()

    def worker() -> None:
        try:
            for item in iter_fn():
                loop.call_soon_threadsafe(q.put_nowait, item)
        except BaseException as e:  # propagate into async generator
            loop.call_soon_threadsafe(q.put_nowait, (EXC, e))
        finally:
            loop.call_soon_threadsafe(q.put_nowait, SENTINEL)

    task = asyncio.create_task(asyncio.to_thread(worker))

    try:
        while True:
            item = await q.get()
            if item is SENTINEL:
                break
            if isinstance(item, tuple) and len(item) == 2 and item[0] is EXC:
                raise cast(BaseException, item[1])
            yield cast(T, item)
    finally:
        await task


class _AsyncFileFromSync:
    """
    Async file wrapper over a sync file object using to_thread for operations.
    Implements your AsyncTextFile/AsyncBinaryFile Protocol shape.
    """
    def __init__(self, f: Any) -> None:
        self._f = f

    async def __aenter__(self) -> "_AsyncFileFromSync":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool | None:
        await self.close()
        return None

    async def read(self, n: int = -1) -> Any:
        return await _to_thread(self._f.read, n)

    async def write(self, data: Any) -> int:
        return await _to_thread(self._f.write, data)

    async def flush(self) -> None:
        await _to_thread(self._f.flush)

    async def close(self) -> None:
        await _to_thread(self._f.close)


class _AsyncOpenFromSync:
    """
    Async context manager that opens a sync file in a thread, then wraps it.
    """
    def __init__(self, opener: Callable[[], Any]) -> None:
        self._opener = opener
        self._f: Any | None = None

    async def __aenter__(self) -> _AsyncFileFromSync:
        self._f = await _to_thread(self._opener)
        return _AsyncFileFromSync(self._f)

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool | None:
        if self._f is not None:
            await _to_thread(self._f.close)
        return None


# =========================================================
# 1) Async-native: implement async; get sync "pretend" free
# =========================================================
class AsyncNativePretendSyncLocation(StoreLocationMixinAPI, ABC):
    """
    Implement the async methods (aexists/astat/aopen/aiterdir/...) natively.

    Sync methods are derived by running the async methods on a background loop.

    This is the cleanest “async-first but pathlib-ish” bridge that doesn’t rely on
    nested loops or fragile `asyncio.run()` calls.
    """
    _runner = _AsyncLoopThread()

    # --- you implement these natively ---
    @abstractmethod
    async def aexists(self) -> bool: ...
    @abstractmethod
    async def ais_file(self) -> bool: ...
    @abstractmethod
    async def ais_dir(self) -> bool: ...
    @abstractmethod
    async def astat(self) -> os.stat_result: ...
    @abstractmethod
    async def amkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None: ...
    @abstractmethod
    async def aunlink(self, missing_ok: bool = False) -> None: ...
    @abstractmethod
    async def armdir(self) -> None: ...
    @abstractmethod
    async def arename(self, target: str | os.PathLike[str]) -> Self: ...
    @abstractmethod
    async def areplace(self, target: str | os.PathLike[str]) -> Self: ...
    @abstractmethod
    async def atouch(self, mode: int = 0o666, exist_ok: bool = True) -> None: ...
    @abstractmethod
    async def aiterdir(self) -> AsyncIterator[Self]: ...
    @abstractmethod
    async def aglob(self, pattern: str) -> AsyncIterator[Self]: ...
    @abstractmethod
    async def arglob(self, pattern: str) -> AsyncIterator[Self]: ...
    @abstractmethod
    def aopen(self, mode: str = "r", buffering: int = -1,
              encoding: str | None = None, errors: str | None = None, newline: str | None = None) -> Any: ...

    # --- derived sync facade ---
    def exists(self) -> bool:
        return self._runner.run(self.aexists())

    def is_file(self) -> bool:
        return self._runner.run(self.ais_file())

    def is_dir(self) -> bool:
        return self._runner.run(self.ais_dir())

    def stat(self) -> os.stat_result:
        return self._runner.run(self.astat())

    def mkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None:
        self._runner.run(self.amkdir(mode=mode, parents=parents, exist_ok=exist_ok))

    def unlink(self, missing_ok: bool = False) -> None:
        self._runner.run(self.aunlink(missing_ok=missing_ok))

    def rmdir(self) -> None:
        self._runner.run(self.armdir())

    def rename(self, target: str | os.PathLike[str]) -> Self:
        return self._runner.run(self.arename(target))

    def replace(self, target: str | os.PathLike[str]) -> Self:
        return self._runner.run(self.areplace(target))

    def touch(self, mode: int = 0o666, exist_ok: bool = True) -> None:
        self._runner.run(self.atouch(mode=mode, exist_ok=exist_ok))

    def iterdir(self) -> Iterator[Self]:
        async def collect() -> list[Self]:
            out: list[Self] = []
            async for p in self.aiterdir():
                out.append(p)
            return out
        return iter(self._runner.run(collect()))

    def glob(self, pattern: str) -> Iterator[Self]:
        async def collect() -> list[Self]:
            out: list[Self] = []
            async for p in self.aglob(pattern):
                out.append(p)
            return out
        return iter(self._runner.run(collect()))

    def rglob(self, pattern: str) -> Iterator[Self]:
        async def collect() -> list[Self]:
            out: list[Self] = []
            async for p in self.arglob(pattern):
                out.append(p)
            return out
        return iter(self._runner.run(collect()))

    def open(self, mode: str = "r", buffering: int = -1,
             encoding: str | None = None, errors: str | None = None, newline: str | None = None) -> Any:
        # Open immediately (pathlib-like), returning a sync wrapper.
        async_cm = self.aopen(mode=mode, buffering=buffering, encoding=encoding, errors=errors, newline=newline)
        afile = self._runner.run(async_cm.__aenter__())
        return _SyncFileFromAsync(self._runner, async_cm, afile)


# =========================================================
# 2) Sync-native: implement sync; get async "pretend" free
# =========================================================
class SyncNativePretendAsyncLocation(StoreLocationMixinAPI):
    """
    Implement the sync methods (exists/stat/open/iterdir/...) natively.

    Async methods are derived via asyncio.to_thread + streaming iterator bridge.
    """

    # --- you implement these natively ---
    @abstractmethod
    def exists(self) -> bool: ...

    @abstractmethod
    def is_file(self) -> bool: ...

    @abstractmethod
    def is_dir(self) -> bool: ...

    @abstractmethod
    def stat(self) -> os.stat_result: ...

    @abstractmethod
    def mkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None: ...

    @abstractmethod
    def unlink(self, missing_ok: bool = False) -> None: ...

    @abstractmethod
    def rmdir(self) -> None: ...

    @abstractmethod
    def rename(self, target: str | os.PathLike[str]) -> Self: ...

    @abstractmethod
    def replace(self, target: str | os.PathLike[str]) -> Self: ...

    @abstractmethod
    def touch(self, mode: int = 0o666, exist_ok: bool = True) -> None: ...

    @abstractmethod
    def iterdir(self) -> Iterator[Self]: ...

    @abstractmethod
    def glob(self, pattern: str) -> Iterator[Self]: ...

    @abstractmethod
    def rglob(self, pattern: str) -> Iterator[Self]: ...

    @abstractmethod
    def open(self,
             mode: str = "r",
             buffering: int = -1,
             encoding: str | None = None, errors: str | None = None, newline: str | None = None) -> Any: ...

    # --- derived async facade ---
    async def aexists(self) -> bool:
        return await _to_thread(self.exists)

    async def ais_file(self) -> bool:
        return await _to_thread(self.is_file)

    async def ais_dir(self) -> bool:
        return await _to_thread(self.is_dir)

    async def astat(self) -> os.stat_result:
        return await _to_thread(self.stat)

    async def amkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None:
        await _to_thread(self.mkdir, mode, parents, exist_ok)

    async def aunlink(self, missing_ok: bool = False) -> None:
        await _to_thread(self.unlink, missing_ok)

    async def armdir(self) -> None:
        await _to_thread(self.rmdir)

    async def arename(self, target: str | os.PathLike[str]) -> Self:
        return await _to_thread(self.rename, target)

    async def areplace(self, target: str | os.PathLike[str]) -> Self:
        return await _to_thread(self.replace, target)

    async def atouch(self, mode: int = 0o666, exist_ok: bool = True) -> None:
        await _to_thread(self.touch, mode, exist_ok)

    async def aiterdir(self) -> AsyncIterator[Self]:
        async for item in _aiter_from_sync_iter(self.iterdir):
            yield item

    async def aglob(self, pattern: str) -> AsyncIterator[Self]:
        async for item in _aiter_from_sync_iter(lambda: self.glob(pattern)):
            yield item

    async def arglob(self, pattern: str) -> AsyncIterator[Self]:
        async for item in _aiter_from_sync_iter(lambda: self.rglob(pattern)):
            yield item

    def aopen(self, mode: str = "r", buffering: int = -1,
              encoding: str | None = None, errors: str | None = None, newline: str | None = None) -> Any:
        # Return an async context manager that opens the sync file in a thread.
        return _AsyncOpenFromSync(
            lambda: self.open(mode=mode, buffering=buffering, encoding=encoding, errors=errors, newline=newline)
        )
