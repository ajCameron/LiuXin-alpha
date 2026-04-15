"""Writable staging Location for SquashFS build plugins."""

from __future__ import annotations

import os
import pathlib
from typing import Any, Iterator, Self

from LiuXin_alpha.storage.api.location_api import SyncNativePretendAsyncLocation


class SquashfsBuildStoreLocation(SyncNativePretendAsyncLocation):
    """Path-like handle to one staged file inside a SquashFS build plugin.

    The public URL/key is the *future archive* path rooted at the output archive
    URL. The underlying bytes live in the plugin's staging directory until the
    archive is sealed.
    """

    _loc_path: pathlib.Path

    def __init__(self, *args: str, store: Any) -> None:
        super().__init__(*args, store=store)
        self._loc_path = self.store.staging_root.joinpath(*self._tokens)

    def as_store_key(self) -> str:
        if not self.parts:
            return self.store.url.rstrip("/")
        return self.store.url.rstrip("/") + "/" + self.as_posix()

    def exists(self) -> bool:
        return self._loc_path.exists()

    def is_file(self) -> bool:
        return self._loc_path.is_file()

    def is_dir(self) -> bool:
        return self._loc_path.is_dir()

    def stat(self) -> os.stat_result:
        return self._loc_path.stat()

    def iterdir(self) -> Iterator[Self]:
        for path in self._loc_path.iterdir():
            rel = path.relative_to(self.store.staging_root)
            yield self.__class__(*rel.parts, store=self._store)

    def glob(self, pattern: str) -> Iterator[Self]:
        if not pattern:
            raise ValueError(f"Unacceptable pattern: {pattern!r}")
        if pattern.startswith(("/", "\\")):
            raise ValueError("Non-relative glob patterns are unsupported")
        for path in self._loc_path.glob(pattern):
            rel = path.relative_to(self.store.staging_root)
            yield self.__class__(*rel.parts, store=self._store)

    def rglob(self, pattern: str) -> Iterator[Self]:
        if not pattern:
            raise ValueError(f"Unacceptable pattern: {pattern!r}")
        if pattern.startswith(("/", "\\")):
            raise ValueError("Non-relative glob patterns are unsupported")
        for path in self._loc_path.rglob(pattern):
            rel = path.relative_to(self.store.staging_root)
            yield self.__class__(*rel.parts, store=self._store)

    def mkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None:
        self._loc_path.mkdir(mode=mode, parents=parents, exist_ok=exist_ok)

    def unlink(self, missing_ok: bool = False) -> None:
        self._loc_path.unlink(missing_ok=missing_ok)

    def rmdir(self) -> None:
        self._loc_path.rmdir()

    def _resolve_target_within_stage(self, target: str | os.PathLike[str], *, verb: str) -> pathlib.Path:
        target_p = pathlib.Path(target)
        stage_root = self.store.staging_root
        if not target_p.is_absolute() and any(p == ".." for p in target_p.parts):
            raise ValueError(f"Refusing {verb} with '..' segments (store escape risk).")
        if not target_p.is_absolute() and len(target_p.parts) == 1:
            target_p = self._loc_path.with_name(target_p.name)
        elif not target_p.is_absolute():
            target_p = stage_root.joinpath(target_p)
            target_p.parent.mkdir(parents=True, exist_ok=True)
        else:
            try:
                target_p = target_p.resolve()
            except FileNotFoundError:
                target_p = target_p.parent.resolve() / target_p.name
            if not target_p.is_relative_to(stage_root):
                raise ValueError(f"Refusing {verb} outside staging root.")
            target_p.parent.mkdir(parents=True, exist_ok=True)
        return target_p

    def rename(self, target: str | os.PathLike[str]) -> Self:
        target_p = self._resolve_target_within_stage(target, verb="rename")
        new_path = self._loc_path.rename(target_p)
        rel = new_path.relative_to(self.store.staging_root)
        return self.__class__(*rel.parts, store=self._store)

    def replace(self, target: str | os.PathLike[str]) -> Self:
        target_p = self._resolve_target_within_stage(target, verb="replace")
        new_path = self._loc_path.replace(target_p)
        rel = new_path.relative_to(self.store.staging_root)
        return self.__class__(*rel.parts, store=self._store)

    def touch(self, mode: int = 0o666, exist_ok: bool = True) -> None:
        self._loc_path.parent.mkdir(parents=True, exist_ok=True)
        self._loc_path.touch(mode=mode, exist_ok=exist_ok)

    def open(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ):
        if any(flag in mode for flag in ("w", "a", "+", "x")):
            self._loc_path.parent.mkdir(parents=True, exist_ok=True)
        return self._loc_path.open(
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )
