"""Read-only location wrapper for URLs discovered via the native HTML crawler."""

from __future__ import annotations

import fnmatch
import io
import os
import stat as statmod
from typing import Iterator, Self
from urllib.parse import urljoin

from LiuXin_alpha.storage.api.location_api import SyncNativePretendAsyncLocation


class NativeHtmlReadOnlyStoreLocation(SyncNativePretendAsyncLocation):
    def as_store_key(self) -> str:
        rel = self.as_posix()
        if not rel:
            return str(self.store.url).rstrip("/")
        return urljoin(str(self.store.url).rstrip("/") + "/", rel)

    def exists(self) -> bool:
        if not self.parts:
            return True
        return bool(self.store.file_exists(self.as_store_key()))

    def is_file(self) -> bool:
        return bool(self.parts) and self.exists()

    def is_dir(self) -> bool:
        if not self.parts:
            return True
        prefix = self.as_posix().rstrip("/") + "/"
        return any(url.rstrip("/").startswith(self.store.url.rstrip("/") + "/" + prefix) for url in list(self.store._crawl_cache_urls or ()))

    def stat(self) -> os.stat_result:
        mode = statmod.S_IFDIR | 0o555 if self.is_dir() else statmod.S_IFREG | 0o444
        return os.stat_result((mode, 0, 0, 1, 0, 0, 0, 0.0, 0.0, 0.0))

    def mkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None:
        raise PermissionError("Native HTML locations are read-only.")

    def unlink(self, missing_ok: bool = False) -> None:
        raise PermissionError("Native HTML locations are read-only.")

    def rmdir(self) -> None:
        raise PermissionError("Native HTML locations are read-only.")

    def rename(self, target: str | os.PathLike[str]) -> Self:
        raise PermissionError("Native HTML locations are read-only.")

    def replace(self, target: str | os.PathLike[str]) -> Self:
        raise PermissionError("Native HTML locations are read-only.")

    def touch(self, mode: int = 0o666, exist_ok: bool = True) -> None:
        raise PermissionError("Native HTML locations are read-only.")

    def iterdir(self) -> Iterator[Self]:
        if not self.is_dir():
            return iter(())
        children: set[str] = set()
        prefix = self.as_posix().rstrip("/")
        if prefix:
            prefix += "/"
        base = self.store.url.rstrip("/") + "/"
        for url in list(self.store._crawl_cache_urls or ()):  # noqa: SLF001 - backend cache
            if not url.startswith(base):
                continue
            rel = url[len(base):]
            if prefix and not rel.startswith(prefix):
                continue
            remainder = rel[len(prefix):] if prefix else rel
            if not remainder:
                continue
            children.add(remainder.split("/", 1)[0])
        return (self.joinpath(child) for child in sorted(children))

    def glob(self, pattern: str) -> Iterator[Self]:
        return (loc for loc in self.rglob("*") if fnmatch.fnmatch(loc.as_posix(), pattern))

    def rglob(self, pattern: str) -> Iterator[Self]:
        base = self.store.url.rstrip("/") + "/"
        prefix = self.as_posix().rstrip("/")
        if prefix:
            prefix += "/"
        matches = []
        for url in list(self.store._crawl_cache_urls or ()):  # noqa: SLF001 - backend cache
            if not url.startswith(base):
                continue
            rel = url[len(base):]
            if prefix and not rel.startswith(prefix):
                continue
            rel_tail = rel[len(prefix):] if prefix else rel
            if fnmatch.fnmatch(rel_tail, pattern) or fnmatch.fnmatch(rel, pattern):
                matches.append(self.store.get_file(url))
        return iter(matches)

    def open(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ):
        raise NotImplementedError("Native HTML crawler locations do not download payloads.")
