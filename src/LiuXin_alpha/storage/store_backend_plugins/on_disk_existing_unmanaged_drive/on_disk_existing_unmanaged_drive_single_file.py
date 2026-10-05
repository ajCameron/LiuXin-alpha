"""
Keep the legacy single-file interface as a read-only Store/Location wrapper.

The resolved file URI supplies a stable UUID, while the containing directory
becomes the Store root. Reads and stat remain delegated to the current Store API.
"""

from __future__ import annotations

import pathlib

from typing import BinaryIO
from uuid import NAMESPACE_URL, uuid5

from LiuXin_alpha.storage.api import FileInfo

from .on_disk_existing_unmanaged_drive_storage_backend import (
    OnDiskUnmanagedStorageBackend,
)


class OnDiskUnmanagedSingleFile:
    """
    Bind a local pathname to a read-only Store and owned Location for compatibility callers.

    New callers can retain Store and Location separately. This facade adds neither mutation methods
    nor a context-manager protocol. It does not freeze file contents, and its public binding
    attributes remain mutable.

    Example:
        >>> file = OnDiskUnmanagedSingleFile("book.txt")  # doctest: +SKIP
        >>> file.read_text()  # doctest: +SKIP
        'book'


    :ivar path: Expanded, resolved local pathname captured at construction.
    :ivar store: Read-only Store rooted at the resolved file's parent directory.
    :ivar location: Owned Location for the resolved pathname's final component.
    """

    def __init__(self, file_url: str | pathlib.Path) -> None:
        """
        Resolve a local filename, create its parent-rooted Store, and locate its final component.

        The Store UUID is UUID5 in NAMESPACE_URL over the resolved file URI. Symlinks are resolved
        before identity is derived. Neither the parent nor this file must exist at binding time;
        later reads/startup report availability, while invalid empty member keys can reject here.

        Example:
            >>> file = OnDiskUnmanagedSingleFile(pathlib.Path("book.txt"))  # doctest: +SKIP


        :param file_url: Local pathname despite the legacy name; parsed directly by pathlib rather than as a file URI.
        :return: None after assigning path, store, and location; this wrapper does not start the Store.
        """
        path = pathlib.Path(file_url).expanduser().resolve(strict=False)
        self.path = path
        self.store = OnDiskUnmanagedStorageBackend(
            path.parent,
            name=f"unmanaged-file:{path.name}",
            uuid=uuid5(NAMESPACE_URL, path.as_uri()),
        )
        self.location = self.store.locate(path.name)

    @property
    def store_ref(self):
        """
        Expose the identity of the currently wrapped Store.

        Example:
            >>> file.store_ref == file.location.store_ref  # doctest: +SKIP
            True


        :return: StoreUUID from self.store, initially derived from the resolved file URI.
        """

        return self.store.store_ref

    def stat(self) -> FileInfo:
        """
        Delegate current metadata lookup for the bound Location to its Store.

        Stat reports filesystem facts and a metadata version without reading or hashing the payload.

        Example:
            >>> file.stat().size  # doctest: +SKIP
            4


        :return: FileInfo for the current file; missing, invalid, or inaccessible targets raise through the Store.
        """

        return self.store.stat(self.location)

    def open_read(
        self,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> BinaryIO:
        """
        Open the current file or a selected byte range through the bound Store.

        The returned stream owns its file handle. Range and optional version validation are
        delegated; this facade does not take a full-file snapshot or hash the bytes.

        Example:
            >>> with file.open_read(offset=1, length=2) as stream:  # doctest: +SKIP
            ...     stream.read()
            b'oo'


        :param offset: Nonnegative byte offset into the current file.
        :param length: Nonnegative maximum byte count, or None for the remaining contents.
        :param if_version: Optional required filesystem metadata version, checked against the opened file rather than a payload hash.
        :return: Caller-owned readable binary stream; close it independently of this facade.
        """

        return self.store.open_read(
            self.location,
            offset=offset,
            length=length,
            if_version=if_version,
        )

    def read_bytes(
        self,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> bytes:
        """
        Read the selected file range fully into memory through the Store convenience method.

        The convenience method closes its temporary read stream. This facade adds no independent
        memory budget.

        Example:
            >>> file.read_bytes(length=2)  # doctest: +SKIP
            b'bo'


        :param offset: Nonnegative byte offset into the current file.
        :param length: Nonnegative maximum byte count, or None for the remaining contents.
        :param if_version: Optional required filesystem metadata version, checked against the opened file rather than a payload hash.
        :return: Bytes read from the current file or range, including empty bytes at EOF.
        """

        return self.store.read_bytes(
            self.location,
            offset=offset,
            length=length,
            if_version=if_version,
        )

    def read_text(self, *, encoding: str = "utf-8") -> str:
        """
        Read the complete file and decode it with the supplied codec using strict error handling.

        Example:
            >>> file.read_text(encoding="utf-8")  # doctest: +SKIP
            'book'


        :param encoding: Codec name passed to bytes.decode; defaults to UTF-8.
        :return: Decoded text; read, codec lookup, and decoding errors propagate.
        """

        return self.read_bytes().decode(encoding)

    def close(self) -> None:
        """
        Delegate lifecycle closure to the currently wrapped Store.

        This does not close streams previously returned to callers or prevent external filesystem
        access.

        Example:
            >>> file.close()  # doctest: +SKIP


        :return: None after Store.close returns; no file is deleted.
        """

        self.store.close()


__all__ = ["OnDiskUnmanagedSingleFile"]
