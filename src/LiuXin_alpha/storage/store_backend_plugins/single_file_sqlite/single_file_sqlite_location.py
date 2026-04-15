"""Location implementation for content-addressed SQLite blob storage."""

from __future__ import annotations

import fnmatch
import io
import os
import stat as statmod
from typing import Iterator, Self

from LiuXin_alpha.storage.api.location_api import SyncNativePretendAsyncLocation


class SingleFileSqliteStoreLocation(SyncNativePretendAsyncLocation):
    """Path-like location over a content-addressed SQLite blob store.

    The store root behaves like a synthetic directory listing all known blob
    hashes. Individual child locations identify one canonical blob.
    """

    def _file_hash(self) -> str | None:
        if len(self.parts) != 1:
            return None
        return self.parts[0]

    def as_store_key(self) -> str:
        file_hash = self._file_hash()
        if file_hash is None:
            return self.store.url.rstrip("/")
        return self.store._hash_file_url(file_hash)

    def exists(self) -> bool:
        if not self.parts:
            return True
        return self.store.exists(self)

    def is_file(self) -> bool:
        return bool(self.parts) and self.exists()

    def is_dir(self) -> bool:
        return not self.parts

    def stat(self) -> os.stat_result:
        if not self.parts:
            st = self.store.db_path.stat()
            return os.stat_result((statmod.S_IFDIR | 0o555, 0, 0, 1, 0, 0, 0, st.st_atime, st.st_mtime, st.st_ctime))
        size = int(self.store.file_size(self) or 0)
        st = self.store.db_path.stat()
        return os.stat_result((statmod.S_IFREG | 0o444, 0, 0, 1, 0, 0, size, st.st_atime, st.st_mtime, st.st_ctime))

    def mkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None:
        raise PermissionError("Single-file SQLite locations do not support directories.")

    def unlink(self, missing_ok: bool = False) -> None:
        if not self.store.delete(self):
            if not missing_ok:
                raise FileNotFoundError(self.file_url)

    def rmdir(self) -> None:
        raise PermissionError("Single-file SQLite locations do not support directories.")

    def rename(self, target: str | os.PathLike[str]) -> Self:
        raise PermissionError("Content-addressed SQLite locations cannot be renamed.")

    def replace(self, target: str | os.PathLike[str]) -> Self:
        raise PermissionError("Content-addressed SQLite locations cannot be replaced in place.")

    def touch(self, mode: int = 0o666, exist_ok: bool = True) -> None:
        raise PermissionError("Use store.write_bytes() for content-addressed SQLite locations.")

    def iterdir(self) -> Iterator[Self]:
        if self.parts:
            return iter(())
        return iter(self.store.iter_locations())

    def glob(self, pattern: str) -> Iterator[Self]:
        if self.parts:
            return iter(())
        return (loc for loc in self.store.iter_locations() if fnmatch.fnmatch(loc.as_posix(), pattern))

    def rglob(self, pattern: str) -> Iterator[Self]:
        return self.glob(pattern)

    def open(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ):
        if any(flag in mode for flag in ("w", "a", "+", "x")):
            raise PermissionError("Content-addressed SQLite locations are read-only once materialized.")
        payload = self.store.read_file_bytes(self)
        raw = io.BytesIO(payload)
        if "b" in mode:
            return raw
        return io.TextIOWrapper(raw, encoding=encoding or "utf-8", errors=errors or "strict", newline=newline)
