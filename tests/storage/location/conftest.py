from __future__ import annotations

import asyncio
import os
import pathlib
from typing import Any, AsyncIterator, Iterator, Self

import pytest

from LiuXin_alpha.storage.api.location_api import AsyncNativePretendSyncLocation
from LiuXin_alpha.storage.store_backend_plugins.on_disk_unmanaged_drive.on_disk_unmanaged_location import (
    OnDiskUnmanagedStoreLocation,
)
from LiuXin_alpha.storage.store_backend_plugins.on_disk_unmanaged_drive.on_disk_unmanaged_storage_backend import (
    OnDiskUnmanagedStorageBackend,
)


@pytest.fixture(params=[OnDiskUnmanagedStoreLocation, None], name="loc_cls")
def _loc_cls(request):
    """Parametrized Location class under test.

    We include both the sync-native and the async-native (sync façade) Location.
    """
    if request.param is None:
        return AsyncOnDiskLocation
    return request.param


@pytest.fixture()
def store(tmp_path: pathlib.Path) -> OnDiskUnmanagedStorageBackend:
    """A real on-disk store rooted in a unique temp directory."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    return OnDiskUnmanagedStorageBackend(url=str(tmp_path))


@pytest.fixture()
def root_loc(store: OnDiskUnmanagedStorageBackend) -> OnDiskUnmanagedStoreLocation:
    """Root Location for the temp store."""
    return OnDiskUnmanagedStoreLocation(store=store)


def fs_path(store: OnDiskUnmanagedStorageBackend, *tokens: str) -> pathlib.Path:
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
def async_root_loc(store: OnDiskUnmanagedStorageBackend) -> AsyncOnDiskLocation:
    return AsyncOnDiskLocation(store=store)
