"""Local-disk Location implementations used by on-disk storage plugins.

This module now exposes a small hierarchy:
- ``OnDiskLocalStoreLocation``: generic writable local-disk location
- ``OnDiskReadOnlyStoreLocation``: same path semantics, but read-only
- ``OnDiskUnmanagedStoreLocation``: compatibility name for the read-only variant

The key separation-of-concerns rule is that read-only plugins must return
read-only Location subclasses as well, so callers and tests can trust the
advertised capability surface instead of stumbling into accidental mutation.
"""

from __future__ import annotations

import os
import pathlib
from typing import Any, Iterator, Self

from LiuXin_alpha.storage.api.location_api import (
    ReadOnlySyncNativePretendAsyncLocation,
    SyncNativePretendAsyncLocation,
)


class _LocalDiskLocationMixin:
    """Shared local-filesystem behaviour for store-relative disk locations."""

    _loc_path: pathlib.Path

    def __init__(self, *args: str, store: Any) -> None:
        super().__init__(*args, store=store)

        store_root = pathlib.Path(self.store.url).resolve()
        candidate = store_root.joinpath(*self._tokens)

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

    def exists(self) -> bool:
        return self._loc_path.exists()

    def is_file(self) -> bool:
        return self._loc_path.is_file()

    def is_dir(self) -> bool:
        return self._loc_path.is_dir()

    def stat(self) -> os.stat_result:
        return self._loc_path.stat()

    def iterdir(self) -> Iterator[Self]:
        store_root = pathlib.Path(self.store.url).resolve()
        for path in self._loc_path.iterdir():
            rel = path.relative_to(store_root)
            yield self.__class__(*rel.parts, store=self._store)

    def glob(self, pattern: str) -> Iterator[Self]:
        if not pattern:
            raise ValueError(f"Unacceptable pattern: {pattern!r}")
        if pattern.startswith(("/", "\\")):
            raise ValueError("Non-relative glob patterns are unsupported")
        win = pathlib.PureWindowsPath(pattern)
        if win.drive or win.root:
            raise ValueError("Non-relative glob patterns are unsupported")
        store_root = pathlib.Path(self.store.url).resolve()
        for path in self._loc_path.glob(pattern):
            rel = path.relative_to(store_root)
            yield self.__class__(*rel.parts, store=self._store)

    def rglob(self, pattern: str) -> Iterator[Self]:
        if not pattern:
            raise ValueError(f"Unacceptable pattern: {pattern!r}")
        if pattern.startswith(("/", "\\")):
            raise ValueError("Non-relative glob patterns are unsupported")
        win = pathlib.PureWindowsPath(pattern)
        if win.drive or win.root:
            raise ValueError("Non-relative glob patterns are unsupported")
        store_root = pathlib.Path(self.store.url).resolve()
        for path in self._loc_path.rglob(pattern):
            rel = path.relative_to(store_root)
            yield self.__class__(*rel.parts, store=self._store)

    def _resolve_target_within_store(self, target: str | os.PathLike[str], *, verb: str) -> pathlib.Path:
        store_root = pathlib.Path(self.store.url).resolve()
        target_p = pathlib.Path(target)

        if not target_p.is_absolute() and any(p == ".." for p in target_p.parts):
            raise ValueError(f"Refusing {verb} with '..' segments (store escape risk).")

        if not target_p.is_absolute() and len(target_p.parts) == 1:
            target_p = self._loc_path.with_name(target_p.name)
        elif not target_p.is_absolute():
            target_p = store_root.joinpath(target_p)
            target_p.parent.mkdir(parents=True, exist_ok=True)
        else:
            try:
                target_p = target_p.resolve()
            except FileNotFoundError:
                target_p = target_p.parent.resolve() / target_p.name
            if not target_p.is_relative_to(store_root):
                raise ValueError(f"Refusing {verb} outside store root.")
            target_p.parent.mkdir(parents=True, exist_ok=True)

        return target_p

    def _open_local_path(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> Any:
        return self._loc_path.open(
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )

    def as_store_key(self) -> str:
        return str(self._loc_path)


class OnDiskLocalStoreLocation(_LocalDiskLocationMixin, SyncNativePretendAsyncLocation):
    """Writable local-disk Location rooted inside one configured store."""

    def mkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None:
        self._loc_path.mkdir(mode=mode, parents=parents, exist_ok=exist_ok)

    def unlink(self, missing_ok: bool = False) -> None:
        self._loc_path.unlink(missing_ok=missing_ok)

    def rmdir(self) -> None:
        self._loc_path.rmdir()

    def rename(self, target: str | os.PathLike[str]) -> Self:
        store_root = pathlib.Path(self.store.url).resolve()
        target_p = self._resolve_target_within_store(target, verb="rename")
        new_path = self._loc_path.rename(target_p)
        rel = new_path.relative_to(store_root)
        return self.__class__(*rel.parts, store=self._store)

    def replace(self, target: str | os.PathLike[str]) -> Self:
        store_root = pathlib.Path(self.store.url).resolve()
        target_p = self._resolve_target_within_store(target, verb="replace")
        new_path = self._loc_path.replace(target_p)
        rel = new_path.relative_to(store_root)
        return self.__class__(*rel.parts, store=self._store)

    def touch(self, mode: int = 0o666, exist_ok: bool = True) -> None:
        self._loc_path.touch(mode=mode, exist_ok=exist_ok)

    def open(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> Any:
        return self._open_local_path(
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )


class OnDiskReadOnlyStoreLocation(_LocalDiskLocationMixin, ReadOnlySyncNativePretendAsyncLocation):
    """Read-only local-disk Location rooted inside one configured store."""

    def open(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> Any:
        self._assert_read_mode(mode)
        return self._open_local_path(
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )


class OnDiskUnmanagedStoreLocation(OnDiskReadOnlyStoreLocation):
    """Compatibility name for the read-only local-disk Location class."""


__all__ = [
    "OnDiskLocalStoreLocation",
    "OnDiskReadOnlyStoreLocation",
    "OnDiskUnmanagedStoreLocation",
]
