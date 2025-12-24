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

        self._tokens = [ar for ar in args]
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

    # ---- Pure path / pathlib-compat helpers ----

    @property
    def tokens(self) -> tuple[str, ...]:
        """Tokenized, backend-relative path parts (no leading store root)."""
        return tuple(self._tokens)

    def _normalized_tokens(self) -> list[str]:
        # NOTE: Mirrors pathlib's behaviour loosely: '.' is treated as a no-op.
        out: list[str] = []
        for t in self._tokens:
            if t in ("", "."):
                continue
            out.append(t)
        return out

    def _clone(self, tokens: list[str]) -> Self:
        """Create a new location of the same concrete type with the same store."""
        return self.__class__(*tokens, store=self._store)  # type: ignore[misc]

    def _split_pathish(self, part: str | os.PathLike[str]) -> list[str]:
        s = os.fspath(part)
        # Treat both separators as separators (store keys are typically POSIX-ish).
        s = s.replace("\\", "/")
        pieces = [p for p in s.split("/") if p not in ("", ".")]
        return pieces

    def __str__(self) -> str:
        # Prefer backend key when available; fall back to POSIX-ish rendering.
        try:
            return self.as_store_key()
        except Exception:
            return self.as_posix()

    def __repr__(self) -> str:
        store_name = getattr(self._store, "name", None)
        store_uuid = getattr(self._store, "uuid", None)
        store_bits = ", ".join([b for b in [store_name, store_uuid] if b])
        if store_bits:
            store_bits = f" store={store_bits!r}"
        return f"{self.__class__.__name__}({self.as_posix()!r}{store_bits})"

    def __hash__(self) -> int:
        store_uuid = getattr(self._store, "uuid", None) or id(self._store)
        return hash((self.__class__, store_uuid, tuple(self._normalized_tokens())))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, StoreLocationMixinAPI):
            return False
        # Avoid cross-store equality by default.
        su = getattr(self._store, "uuid", None) or id(self._store)
        ou = getattr(other._store, "uuid", None) or id(other._store)
        return (su == ou) and (tuple(self._normalized_tokens()) == tuple(other._normalized_tokens()))

    # --- pathlib-like properties ---

    @property
    def parts(self) -> tuple[str, ...]:
        """Path components, like pathlib.PurePath.parts (store-relative)."""
        toks = self._normalized_tokens()
        return tuple(toks)

    @property
    def drive(self) -> str:
        # Store keys generally do not have drives.
        return ""

    @property
    def root(self) -> str:
        # Store keys are treated as relative by default.
        return ""

    @property
    def anchor(self) -> str:
        # pathlib: drive + root
        return self.drive + self.root

    @property
    def name(self) -> str:
        p = pathlib.PurePosixPath(self.as_posix())
        return p.name

    @property
    def suffix(self) -> str:
        p = pathlib.PurePosixPath(self.as_posix())
        return p.suffix

    @property
    def suffixes(self) -> list[str]:
        p = pathlib.PurePosixPath(self.as_posix())
        return list(p.suffixes)

    @property
    def stem(self) -> str:
        p = pathlib.PurePosixPath(self.as_posix())
        return p.stem

    @property
    def parent(self) -> Self:
        toks = self._normalized_tokens()
        if not toks:
            # pathlib.Path('.').parent is itself
            return self._clone([])
        return self._clone(toks[:-1])

    @property
    def parents(self) -> tuple[Self, ...]:
        toks = self._normalized_tokens()
        # pathlib.Path('a/b').parents -> ('a', '.'); pathlib.Path('.').parents is empty.
        out: list[Self] = []
        while toks:
            toks = toks[:-1]
            out.append(self._clone(list(toks)))
        return tuple(out)

    # --- pathlib-like pure methods ---

    def as_posix(self) -> str:
        toks = self._normalized_tokens()
        if not toks:
            return "."
        return "/".join(toks)

    def is_absolute(self) -> bool:
        # Backend locations are relative-to-store by default.
        # Backends that support absolute keys can override.
        return False

    def joinpath(self, *other: str | os.PathLike[str]) -> Self:
        toks = self._normalized_tokens()
        for part in other:
            toks.extend(self._split_pathish(part))
        return self._clone(toks)

    def __truediv__(self, key: str | os.PathLike[str]) -> Self:
        return self.joinpath(key)

    def __rtruediv__(self, key: str | os.PathLike[str]) -> Self:
        left = self._split_pathish(key)
        return self._clone(left + self._normalized_tokens())

    def with_name(self, name: str) -> Self:
        if not name or "/" in name or "\\" in name:
            raise ValueError(f"Invalid name: {name!r}")
        toks = self._normalized_tokens()
        if not toks:
            return self._clone([name])
        toks[-1] = name
        return self._clone(toks)

    def with_suffix(self, suffix: str) -> Self:
        if suffix and not suffix.startswith("."):
            raise ValueError(f"Invalid suffix: {suffix!r}")
        nm = self.name
        if not nm:
            raise ValueError("Cannot set suffix on an empty name.")
        base = pathlib.PurePosixPath(nm).stem
        return self.with_name(base + suffix)

    def relative_to(self, other: Self | str | os.PathLike[str]) -> Self:
        if isinstance(other, StoreLocationMixinAPI):
            su = getattr(self._store, "uuid", None) or id(self._store)
            ou = getattr(other._store, "uuid", None) or id(other._store)
            if su != ou:
                raise ValueError("Cannot compute relative_to across different stores.")
            base = list(other._normalized_tokens())
        else:
            base = self._split_pathish(other)

        toks = self._normalized_tokens()
        if toks[:len(base)] != base:
            raise ValueError(f"{self!s} is not in the subpath of {other!s}")
        rel = toks[len(base):]
        return self._clone(rel)

    def is_relative_to(self, other: Self | str | os.PathLike[str]) -> bool:
        try:
            self.relative_to(other)
            return True
        except Exception:
            return False

    def match(self, pattern: str) -> bool:
        return pathlib.PurePosixPath(self.as_posix()).match(pattern)

    # --- IO-ish pathlib methods that many callers expect, but stores may not support ---

    def __fspath__(self) -> str:
        # Avoid accidentally passing non-local keys into os.*.
        raise TypeError(f"{self.__class__.__name__} is not a local filesystem path")

    def as_uri(self) -> str:
        raise NotImplementedError("URI rendering is backend-specific (override in the store plugin).")

    def resolve(self, strict: bool = False) -> Self:
        # Store-safe 'resolve': purely normalize '.' and '..' without touching any filesystem.
        toks = self._normalized_tokens()
        out: list[str] = []
        for t in toks:
            if t == "..":
                if out:
                    out.pop()
                else:
                    out.append("..")
            else:
                out.append(t)
        return self._clone(out)

    def absolute(self) -> Self:
        return self

    def expanduser(self) -> Self:
        return self

    def is_symlink(self) -> bool:
        return False

    async def ais_symlink(self) -> bool:
        return False

    def is_mount(self) -> bool:
        return False

    async def ais_mount(self) -> bool:
        return False

    def owner(self) -> str:
        raise NotImplementedError

    async def aowner(self) -> str:
        raise NotImplementedError

    def group(self) -> str:
        raise NotImplementedError

    async def agroup(self) -> str:
        raise NotImplementedError

    def chmod(self, mode: int) -> None:
        raise NotImplementedError

    async def achmod(self, mode: int) -> None:
        raise NotImplementedError

    def lstat(self) -> os.stat_result:
        raise NotImplementedError

    async def alstat(self) -> os.stat_result:
        raise NotImplementedError

    def samefile(self, other_path: str | os.PathLike[str]) -> bool:
        raise NotImplementedError

    async def asamefile(self, other_path: str | os.PathLike[str]) -> bool:
        raise NotImplementedError

    def symlink_to(self, target: str | os.PathLike[str], target_is_directory: bool = False) -> None:
        raise NotImplementedError

    async def asymlink_to(self, target: str | os.PathLike[str], target_is_directory: bool = False) -> None:
        raise NotImplementedError

    def hardlink_to(self, target: str | os.PathLike[str]) -> None:
        raise NotImplementedError

    async def ahardlink_to(self, target: str | os.PathLike[str]) -> None:
        raise NotImplementedError

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

        self._tokens = [ar for ar in args]
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
