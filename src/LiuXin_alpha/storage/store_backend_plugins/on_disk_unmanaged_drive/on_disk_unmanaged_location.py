"""LiuXin on-disk Location implementation.

Reference concrete Location for a plain directory-backed store.

Notes
- The store provides a root directory via ``store.url``.
- A Location is always interpreted relative to that store root.
- We actively refuse traversal outside the store root (including via symlinks).
"""

from __future__ import annotations

import os
import pathlib
from typing import Any, Iterator, Self

from LiuXin_alpha.storage.api.location_api import SyncNativePretendAsyncLocation


class OnDiskUnmanagedStoreLocation(SyncNativePretendAsyncLocation):
    """On-disk Store Location (directory-backed)."""

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

    # ---- Existence / type checks ----

    def exists(self) -> bool:
        return self._loc_path.exists()

    def is_file(self) -> bool:
        return self._loc_path.is_file()

    def is_dir(self) -> bool:
        return self._loc_path.is_dir()

    # ---- Metadata ----

    def stat(self) -> os.stat_result:
        return self._loc_path.stat()

    # ---- Mutations ----

    def mkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None:
        self._loc_path.mkdir(mode=mode, parents=parents, exist_ok=exist_ok)

    def unlink(self, missing_ok: bool = False) -> None:
        self._loc_path.unlink(missing_ok=missing_ok)

    def rmdir(self) -> None:
        self._loc_path.rmdir()

    def rename(self, target: str | os.PathLike[str]) -> Self:
        store_root = pathlib.Path(self.store.url).resolve()

        target_p = pathlib.Path(target)

        # Refuse traversal tokens in relative targets.
        if not target_p.is_absolute() and any(p == ".." for p in target_p.parts):
            raise ValueError("Refusing rename with '..' segments (store escape risk).")

        # A bare name means "rename within the same directory".
        if not target_p.is_absolute() and len(target_p.parts) == 1:
            target_p = self._loc_path.with_name(target_p.name)
        elif not target_p.is_absolute():
            # Otherwise interpret as store-root relative.
            target_p = store_root.joinpath(target_p)
            target_p.parent.mkdir(parents=True, exist_ok=True)
        else:
            # Absolute targets are allowed only if they remain inside the store root.
            try:
                target_p = target_p.resolve()
            except FileNotFoundError:
                # If the path doesn't exist yet, resolve its parent.
                target_p = target_p.parent.resolve() / target_p.name
            if not target_p.is_relative_to(store_root):
                raise ValueError("Refusing rename outside store root.")
            target_p.parent.mkdir(parents=True, exist_ok=True)

        new_path = self._loc_path.rename(target_p)
        rel = new_path.relative_to(store_root)
        return self.__class__(*rel.parts, store=self._store)

    def replace(self, target: str | os.PathLike[str]) -> Self:
        store_root = pathlib.Path(self.store.url).resolve()

        target_p = pathlib.Path(target)

        if not target_p.is_absolute() and any(p == ".." for p in target_p.parts):
            raise ValueError("Refusing replace with '..' segments (store escape risk).")

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
                raise ValueError("Refusing replace outside store root.")
            target_p.parent.mkdir(parents=True, exist_ok=True)

        new_path = self._loc_path.replace(target_p)
        rel = new_path.relative_to(store_root)
        return self.__class__(*rel.parts, store=self._store)

    def touch(self, mode: int = 0o666, exist_ok: bool = True) -> None:
        self._loc_path.touch(mode=mode, exist_ok=exist_ok)

    # ---- Directory traversal ----

    def iterdir(self) -> Iterator[Self]:
        store_root = pathlib.Path(self.store.url).resolve()
        for path in self._loc_path.iterdir():
            rel = path.relative_to(store_root)
            yield self.__class__(*rel.parts, store=self._store)

    def glob(self, pattern: str) -> Iterator[Self]:
        # Enforce pathlib-like semantics but ...
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

    # ---- I/O ----

    def open(
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

    # ---- Backend key ----

    def as_store_key(self) -> str:
        return str(self._loc_path)
