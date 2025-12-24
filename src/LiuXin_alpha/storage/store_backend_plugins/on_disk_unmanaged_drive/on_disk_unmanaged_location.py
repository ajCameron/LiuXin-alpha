
"""LiuXin on-disk Location implementation.

This is the reference concrete Location for a plain directory-backed store.

Design intent:
 - the *store* provides a root directory via ``store.url``
 - a Location is always interpreted relative to that store root
 - Location methods mirror a small, carefully-chosen subset of ``pathlib.Path``
   while remaining backend-agnostic.
"""

from __future__ import annotations

import os
import pathlib
from typing import Any, Iterator, Self

from LiuXin_alpha.storage.api.location_api import SyncNativePretendAsyncLocation



class OnDiskUnmanagedStoreLocation(SyncNativePretendAsyncLocation):
    """
    On Disk Unmanaged Store Location.
    """

    _loc_path: pathlib.Path

    def __init__(self, *args: str, store: Any) -> None:
        """
        Startup the class - including any tokens for the location.

        :param args:
        """
        super().__init__(*args, store=store)

        # A Location is always resolved under the store root.
        # Root Location: OnDiskUnmanagedStoreLocation(store=...) -> store.url
        store_root = pathlib.Path(self.store.url)
        self._loc_path = store_root.joinpath(*args)

    def exists(self) -> bool:
        """
        Check if the location exists.

        :return:
        """
        return self._loc_path.exists()

    def is_file(self) -> bool:
        """
        Check if the location points to a file.

        :return:
        """
        return self._loc_path.is_file()

    def is_dir(self) -> bool:
        """
        Check if the location points to a directory.

        :return:
        """
        return self._loc_path.is_dir()

    def stat(self) -> os.stat_result:
        """
        Stat the location.

        :return:
        """
        return self._loc_path.stat()

    def mkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None:
        """
        Make a dir at the location.

        :param mode:
        :param parents:
        :param exist_ok:
        :return:
        """
        return self._loc_path.mkdir(mode=mode, parents=parents, exist_ok=exist_ok)

    def unlink(self, missing_ok: bool = False) -> None:
        """
        Unlink a file from the system at this location.

        :param missing_ok:
        :return:
        """
        return self._loc_path.unlink(missing_ok=missing_ok)

    def rmdir(self) -> None:
        """
        rm a dir at this location.

        :return:
        """
        return self._loc_path.rmdir()

    def rename(self, target: str | os.PathLike[str]) -> Self:
        """
        Preform a rename operation and return a new path.

        :param target:
        :return:
        """
        store_root = pathlib.Path(self.store.url)

        # pathlib.Path.rename accepts either a full path or just a name.
        # If it's just a name, we treat it as "rename within the same directory".
        target_p = pathlib.Path(target)
        if not target_p.is_absolute() and len(target_p.parts) == 1:
            target_p = self._loc_path.with_name(target_p.name)

        new_path = self._loc_path.rename(target_p)
        rel = new_path.relative_to(store_root)
        return self.__class__(*rel.parts, store=self._store)

    def replace(self, target: str | os.PathLike[str]) -> Self:
        """
        Preform a replace operation and return the new path.

        :param target:
        :return:
        """
        store_root = pathlib.Path(self.store.url)

        target_p = pathlib.Path(target)
        # Mirror our rename convenience: a bare name means "replace within the same directory".
        if not target_p.is_absolute() and len(target_p.parts) == 1:
            target_p = self._loc_path.with_name(target_p.name)
        elif not target_p.is_absolute():
            # Otherwise interpret as store-root relative.
            target_p = store_root.joinpath(target_p)

        new_path = self._loc_path.replace(target_p)
        rel = new_path.relative_to(store_root)
        return self.__class__(*rel.parts, store=self._store)

    def touch(self, mode: int = 0o666, exist_ok: bool = True) -> None:
        """
        Touch a file at the location.

        :param mode:
        :param exist_ok:
        :return:
        """
        self._loc_path.touch(mode=mode, exist_ok=exist_ok)

    def iterdir(self) -> Iterator[Self]:
        """
        Iterate over the directory at this location.

        :return:
        """
        store_root = pathlib.Path(self.store.url)
        for path in self._loc_path.iterdir():
            rel = path.relative_to(store_root)
            yield self.__class__(*rel.parts, store=self._store)

    def glob(self, pattern: str) -> Iterator[Self]:
        """
        Create an iterator over the directory at this location matching a pattern.

        :param pattern:
        :return:
        """
        store_root = pathlib.Path(self.store.url)
        for path in self._loc_path.glob(pattern):
            rel = path.relative_to(store_root)
            yield self.__class__(*rel.parts, store=self._store)

    def rglob(self, pattern: str) -> Iterator[Self]:
        """
        Create an iterator over the directory at this location matching a pattern.

        :param pattern:
        :return:
        """
        store_root = pathlib.Path(self.store.url)
        for path in self._loc_path.rglob(pattern):
            rel = path.relative_to(store_root)
            yield self.__class__(*rel.parts, store=self._store)

    def open(self,
             mode: str = "r",
             buffering: int = -1,
             encoding: str | None = None, errors: str | None = None, newline: str | None = None) -> Any:
        """
        Open and return a file at this location.

        :param mode:
        :param buffering:
        :param encoding:
        :param errors:
        :param newline:
        :return:
        """
        return self._loc_path.open(mode=mode, buffering=buffering, encoding=encoding, errors=errors, newline=newline)

    def as_store_key(self) -> str:
        """
        Return the exact path the backend will use.

        :return:
        """
        # For a plain on-disk backend, the canonical key is the absolute path
        # under the store root.
        return str(self._loc_path)
