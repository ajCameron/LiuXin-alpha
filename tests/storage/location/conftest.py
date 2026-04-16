from __future__ import annotations

import asyncio
import os
import pathlib
import subprocess
import sys
import textwrap
import threading
from typing import Any, AsyncIterator, Iterator, Self

import pytest

from LiuXin_alpha.storage.api.location_api import AsyncNativePretendSyncLocation
from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_managed_drive.on_disk_existing_managed_drive_location import (
    OnDiskExistingManagedStoreLocation,
)
from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_managed_drive.on_disk_existing_managed_drive_storage_backend import (
    OnDiskExistingManagedStorageBackend,
)


@pytest.fixture(params=[OnDiskExistingManagedStoreLocation, None], name="loc_cls")
def _loc_cls(request, store):
    """Parametrized Location class under test.

    We include both the sync-native and the async-native (sync façade) Location.
    """
    if request.param is None:
        if not _probe_async_native_sync_bridge(store):
            pytest.skip("async-native sync facade is unavailable in this runtime")
        return AsyncOnDiskLocation
    return request.param


@pytest.fixture()
def store(tmp_path: pathlib.Path) -> OnDiskExistingManagedStorageBackend:
    """A real on-disk store rooted in a unique temp directory."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    return OnDiskExistingManagedStorageBackend(url=str(tmp_path))


@pytest.fixture()
def root_loc(store: OnDiskExistingManagedStorageBackend) -> OnDiskExistingManagedStoreLocation:
    """Root Location for the temp store."""
    return OnDiskExistingManagedStoreLocation(store=store)


def fs_path(store: OnDiskExistingManagedStorageBackend, *tokens: str) -> pathlib.Path:
    """Absolute filesystem path for a tokenized Location within the store."""
    return pathlib.Path(store.url).joinpath(*tokens)


class _AsyncFileWrapper:
    """Async wrapper around a sync file object using asyncio.to_thread."""

    def __init__(self, f: Any) -> None:
        self._f = f

    async def read(self, n: int = -1) -> Any:
        return await asyncio.to_thread(self._f.read, n)

    async def write(self, data: Any) -> int:
        return await asyncio.to_thread(self._f.write, data)

    async def flush(self) -> None:
        await asyncio.to_thread(self._f.flush)

    async def close(self) -> None:
        await asyncio.to_thread(self._f.close)


class _AsyncOpen:
    """Async context manager that opens a sync file object in a thread."""

    def __init__(
        self,
        path: pathlib.Path,
        *,
        mode: str,
        buffering: int,
        encoding: str | None,
        errors: str | None,
        newline: str | None,
    ) -> None:
        self._path = path
        self._mode = mode
        self._buffering = buffering
        self._encoding = encoding
        self._errors = errors
        self._newline = newline
        self._f: Any | None = None

    async def __aenter__(self) -> _AsyncFileWrapper:
        self._f = await asyncio.to_thread(
            self._path.open,
            self._mode,
            self._buffering,
            self._encoding,
            self._errors,
            self._newline,
        )
        return _AsyncFileWrapper(self._f)

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool | None:
        if self._f is not None:
            await asyncio.to_thread(self._f.close)
        return None


class AsyncOnDiskLocation(AsyncNativePretendSyncLocation):
    """Test-only async-native Location for validating the async->sync bridge."""

    _loc_path: pathlib.Path

    def __init__(self, *args: str, store: Any) -> None:
        super().__init__(*args, store=store)

        store_root = pathlib.Path(self.store.url).resolve()
        candidate = store_root.joinpath(*self._tokens)

        # Validate that any existing prefix stays inside the store (symlink-safe).
        probe = store_root
        for seg in self._tokens:
            nxt = probe / seg
            if nxt.exists() or nxt.is_symlink():
                try:
                    probe = nxt.resolve(strict=True)
                except FileNotFoundError:
                    probe = nxt
            else:
                probe = nxt

        if not probe.is_relative_to(store_root):
            raise ValueError("Location escapes store root (refusing '..' / traversal).")

        self._loc_path = candidate

    def as_store_key(self) -> str:
        return str(self._loc_path)

    async def aexists(self) -> bool:
        return await asyncio.to_thread(self._loc_path.exists)

    async def ais_file(self) -> bool:
        return await asyncio.to_thread(self._loc_path.is_file)

    async def ais_dir(self) -> bool:
        return await asyncio.to_thread(self._loc_path.is_dir)

    async def astat(self) -> os.stat_result:
        return await asyncio.to_thread(self._loc_path.stat)

    async def amkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None:
        await asyncio.to_thread(self._loc_path.mkdir, mode=mode, parents=parents, exist_ok=exist_ok)

    async def aunlink(self, missing_ok: bool = False) -> None:
        await asyncio.to_thread(self._loc_path.unlink, missing_ok=missing_ok)

    async def armdir(self) -> None:
        await asyncio.to_thread(self._loc_path.rmdir)

    async def arename(self, target: str | os.PathLike[str]) -> Self:
        store_root = pathlib.Path(self.store.url).resolve()
        target_p = pathlib.Path(target)

        if not target_p.is_absolute() and len(target_p.parts) == 1:
            target_p = self._loc_path.with_name(target_p.name)
        elif not target_p.is_absolute():
            target_p = store_root.joinpath(target_p)

        # convenience: create parent directories when doing store-relative moves
        target_p.parent.mkdir(parents=True, exist_ok=True)

        new_path = await asyncio.to_thread(self._loc_path.rename, target_p)
        rel = new_path.relative_to(store_root)
        return self.__class__(*rel.parts, store=self._store)

    async def areplace(self, target: str | os.PathLike[str]) -> Self:
        store_root = pathlib.Path(self.store.url).resolve()
        target_p = pathlib.Path(target)

        if not target_p.is_absolute() and len(target_p.parts) == 1:
            target_p = self._loc_path.with_name(target_p.name)
        elif not target_p.is_absolute():
            target_p = store_root.joinpath(target_p)

        target_p.parent.mkdir(parents=True, exist_ok=True)

        new_path = await asyncio.to_thread(self._loc_path.replace, target_p)
        rel = new_path.relative_to(store_root)
        return self.__class__(*rel.parts, store=self._store)

    async def atouch(self, mode: int = 0o666, exist_ok: bool = True) -> None:
        await asyncio.to_thread(self._loc_path.touch, mode=mode, exist_ok=exist_ok)

    async def aiterdir(self) -> AsyncIterator[Self]:
        store_root = pathlib.Path(self.store.url).resolve()
        paths = await asyncio.to_thread(lambda: list(self._loc_path.iterdir()))
        for path in paths:
            rel = path.relative_to(store_root)
            yield self.__class__(*rel.parts, store=self._store)

    async def aglob(self, pattern: str) -> AsyncIterator[Self]:
        store_root = pathlib.Path(self.store.url).resolve()
        paths = await asyncio.to_thread(lambda: list(self._loc_path.glob(pattern)))
        for path in paths:
            rel = path.relative_to(store_root)
            yield self.__class__(*rel.parts, store=self._store)

    async def arglob(self, pattern: str) -> AsyncIterator[Self]:
        store_root = pathlib.Path(self.store.url).resolve()
        paths = await asyncio.to_thread(lambda: list(self._loc_path.rglob(pattern)))
        for path in paths:
            rel = path.relative_to(store_root)
            yield self.__class__(*rel.parts, store=self._store)

    def aopen(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> Any:
        # aopen must return an *async* context manager for AsyncNativePretendSyncLocation.open()
        return _AsyncOpen(
            self._loc_path,
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )

@pytest.fixture()
def async_root_loc(store: OnDiskExistingManagedStorageBackend) -> AsyncOnDiskLocation:
    return AsyncOnDiskLocation(store=store)


def _probe_asyncio_thread_bridge(timeout_seconds: float = 3.0) -> bool:
    """
    Probe whether this runtime can complete repeated asyncio thread handoffs.

    We run the probe in a subprocess so test collection never hangs if the
    runtime has a broken asyncio<->thread bridge.
    """
    probe_code = textwrap.dedent(
        """
        import asyncio

        async def _go() -> None:
            await asyncio.wait_for(asyncio.to_thread(lambda: 1), timeout=0.5)
            await asyncio.wait_for(asyncio.to_thread(lambda: 2), timeout=0.5)

        asyncio.run(_go())
        """
    )
    try:
        completed = subprocess.run(
            [sys.executable, "-c", probe_code],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=timeout_seconds,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return completed.returncode == 0


ASYNCIO_THREAD_BRIDGE_OK = _probe_asyncio_thread_bridge()


def _probe_async_native_sync_bridge(
    store: OnDiskExistingManagedStorageBackend,
    timeout_seconds: float = 2.0,
) -> bool:
    """
    Probe AsyncNativePretendSyncLocation sync facade health.

    We execute `exists()` in a daemon thread and bound the wait, so a broken
    bridge causes a skip rather than hanging the whole suite.
    """
    done = threading.Event()
    status = {"ok": False}

    def worker() -> None:
        try:
            status["ok"] = bool(AsyncOnDiskLocation(store=store).exists())
        except Exception:
            status["ok"] = False
        finally:
            done.set()

    threading.Thread(target=worker, name="async-native-sync-probe", daemon=True).start()
    return done.wait(timeout_seconds) and status["ok"]


@pytest.fixture()
def require_asyncio_thread_bridge() -> None:
    if not ASYNCIO_THREAD_BRIDGE_OK:
        pytest.skip("asyncio thread bridge is unavailable in this runtime")


@pytest.fixture()
def require_async_native_sync_bridge(
    store: OnDiskExistingManagedStorageBackend,
    require_asyncio_thread_bridge: None,
) -> None:
    if not _probe_async_native_sync_bridge(store):
        pytest.skip("async-native sync facade is unavailable in this runtime")
