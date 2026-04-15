"""Location implementation for files inside a SquashFS archive store."""

from __future__ import annotations

import fnmatch
import io
import os
import stat as statmod
from typing import Iterator, Self

from LiuXin_alpha.storage.api.location_api import ReadOnlySyncNativePretendAsyncLocation


class SquashfsReadOnlyStoreLocation(ReadOnlySyncNativePretendAsyncLocation):
    """Path-like location inside one read-only SquashFS archive."""

    def _internal_path(self) -> str:
        return "/".join(self.parts)

    def as_store_key(self) -> str:
        internal = self._internal_path()
        if not internal:
            return self.store.url.rstrip("/")
        return f"{self.store.url.rstrip('/')}/{internal}"

    def _known_paths(self) -> set[str]:
        return set(self.store._get_index().keys())

    def exists(self) -> bool:
        internal = self._internal_path()
        if not internal:
            return True
        known = self._known_paths()
        if internal in known:
            return True
        prefix = internal.rstrip("/") + "/"
        return any(path.startswith(prefix) for path in known)

    def is_file(self) -> bool:
        internal = self._internal_path()
        return bool(internal) and internal in self._known_paths()

    def is_dir(self) -> bool:
        if not self.parts:
            return True
        if self.is_file():
            return False
        prefix = self._internal_path().rstrip("/") + "/"
        return any(path.startswith(prefix) for path in self._known_paths())

    def stat(self) -> os.stat_result:
        archive_stat = self.store.db_path.stat()
        if self.is_dir():
            return os.stat_result(
                (statmod.S_IFDIR | 0o555, 0, 0, 1, 0, 0, 0, archive_stat.st_atime, archive_stat.st_mtime, archive_stat.st_ctime)
            )
        size = int(self.store.file_size(self) or 0)
        return os.stat_result(
            (statmod.S_IFREG | 0o444, 0, 0, 1, 0, 0, size, archive_stat.st_atime, archive_stat.st_mtime, archive_stat.st_ctime)
        )

    def mkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None:
        raise PermissionError("SquashFS locations are read-only.")

    def unlink(self, missing_ok: bool = False) -> None:
        raise PermissionError("SquashFS locations are read-only.")

    def rmdir(self) -> None:
        raise PermissionError("SquashFS locations are read-only.")

    def rename(self, target: str | os.PathLike[str]) -> Self:
        raise PermissionError("SquashFS locations are read-only.")

    def replace(self, target: str | os.PathLike[str]) -> Self:
        raise PermissionError("SquashFS locations are read-only.")

    def touch(self, mode: int = 0o666, exist_ok: bool = True) -> None:
        raise PermissionError("SquashFS locations are read-only.")

    def iterdir(self) -> Iterator[Self]:
        if not self.is_dir():
            return iter(())
        known = self._known_paths()
        prefix = self._internal_path().rstrip("/")
        if prefix:
            prefix += "/"
        children: set[str] = set()
        for path in known:
            if not path.startswith(prefix):
                continue
            remainder = path[len(prefix):]
            if not remainder:
                continue
            child = remainder.split("/", 1)[0]
            children.add(child)
        return (self.joinpath(child) for child in sorted(children))

    def glob(self, pattern: str) -> Iterator[Self]:
        return (loc for loc in self.rglob("*") if fnmatch.fnmatch(loc.as_posix(), pattern))

    def rglob(self, pattern: str) -> Iterator[Self]:
        known = sorted(self._known_paths())
        prefix = self._internal_path().rstrip("/")
        if prefix:
            prefix += "/"
        matches: list[Self] = []
        for path in known:
            if prefix and not path.startswith(prefix):
                continue
            rel = path[len(prefix):] if prefix else path
            if fnmatch.fnmatch(rel, pattern) or fnmatch.fnmatch(path, pattern):
                matches.append(self.store.locate(path))
        return iter(matches)

    def open(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ):
        self._assert_read_mode(mode)
        if any(flag in mode for flag in ("w", "a", "+", "x")):
            raise PermissionError("SquashFS locations are read-only.")
        payload = self.store.read_file_bytes(self)
        raw = io.BytesIO(payload)
        if "b" in mode:
            return raw
        return io.TextIOWrapper(raw, encoding=encoding or "utf-8", errors=errors or "strict", newline=newline)
