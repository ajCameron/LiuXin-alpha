"""Read-only Location implementation for FTP/FTPS-backed stores."""

from __future__ import annotations

import fnmatch
import io
import os
import stat as statmod
from typing import Iterator, Self
from urllib.parse import urlsplit

from LiuXin_alpha.storage.api.location_api import ReadOnlySyncNativePretendAsyncLocation


class FtpReadOnlyStoreLocation(ReadOnlySyncNativePretendAsyncLocation):
    """Path-like Location for one file or directory inside an FTP-family store."""

    def _relative_path(self) -> str:
        return "/".join(self.parts)

    def as_store_key(self) -> str:
        rel = self._relative_path()
        root = str(self.store.url).rstrip("/")
        if not rel:
            return root
        return root + "/" + rel

    def _entry(self):
        return self.store._entry_for(self)

    def exists(self) -> bool:
        if not self.parts:
            return True
        return self._entry() is not None

    def is_file(self) -> bool:
        entry = self._entry()
        return bool(entry and entry.get("type") == "file")

    def is_dir(self) -> bool:
        if not self.parts:
            return True
        entry = self._entry()
        return bool(entry and entry.get("type") == "dir")

    def stat(self) -> os.stat_result:
        if not self.parts:
            mode = statmod.S_IFDIR | 0o555
            return os.stat_result((mode, 0, 0, 1, 0, 0, 0, 0.0, 0.0, 0.0))
        entry = self._entry()
        if entry is None:
            raise FileNotFoundError(self.file_url)
        is_dir = entry.get("type") == "dir"
        size = int(entry.get("size") or 0)
        mode = (statmod.S_IFDIR if is_dir else statmod.S_IFREG) | (0o555 if is_dir else 0o444)
        return os.stat_result((mode, 0, 0, 1, 0, 0, size, 0.0, 0.0, 0.0))

    def iterdir(self) -> Iterator[Self]:
        if not self.is_dir():
            return iter(())
        return iter(self.store._iter_children(self))

    def glob(self, pattern: str) -> Iterator[Self]:
        return (loc for loc in self.rglob("*") if fnmatch.fnmatch(loc.as_posix(), pattern))

    def rglob(self, pattern: str) -> Iterator[Self]:
        prefix = "" if not self.parts else self.as_posix().rstrip("/")
        if prefix:
            prefix += "/"
        matches: list[Self] = []
        for loc in self.store.iter_locations():
            rel = loc.as_posix()
            if prefix and not rel.startswith(prefix):
                continue
            rel_tail = rel[len(prefix):] if prefix else rel
            if fnmatch.fnmatch(rel_tail, pattern) or fnmatch.fnmatch(rel, pattern):
                matches.append(loc)
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
        payload = self.store.read_file_bytes(self)
        raw = io.BytesIO(payload)
        if "b" in mode:
            return raw
        return io.TextIOWrapper(raw, encoding=encoding or "utf-8", errors=errors or "strict", newline=newline)
