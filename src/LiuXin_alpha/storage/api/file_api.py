"""
API contracts for file containers exposed by storage backends.
"""

from __future__ import annotations

import abc
import dataclasses

from io import BufferedRandom, BufferedReader, BufferedWriter, FileIO, TextIOWrapper
from typing import Any, BinaryIO, IO, Literal, Optional, overload

from LiuXin_alpha.storage.api.location_api import FileDescriptorOrPath
from LiuXin_alpha.storage.api.modes_api import (
    OpenBinaryMode,
    OpenBinaryModeReading,
    OpenBinaryModeUpdating,
    OpenBinaryModeWriting,
    OpenTextMode,
    _Opener,
)
from LiuXin_alpha.storage.single_file import SingleFileStatus


class FileOpenerTypeMixin(abc.ABC):
    """
    Typed mirror of Python's built-in `open` signatures.
    """

    @overload
    def open(
        self,
        file: FileDescriptorOrPath,
        mode: OpenTextMode = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
        closefd: bool = True,
        opener: _Opener | None = None,
    ) -> TextIOWrapper: ...

    @overload
    def open(
        self,
        file: FileDescriptorOrPath,
        mode: OpenBinaryMode,
        buffering: Literal[0],
        encoding: None = None,
        errors: None = None,
        newline: None = None,
        closefd: bool = True,
        opener: _Opener | None = None,
    ) -> FileIO: ...

    @overload
    def open(
        self,
        file: FileDescriptorOrPath,
        mode: OpenBinaryModeUpdating,
        buffering: Literal[-1, 1] = -1,
        encoding: None = None,
        errors: None = None,
        newline: None = None,
        closefd: bool = True,
        opener: _Opener | None = None,
    ) -> BufferedRandom: ...

    @overload
    def open(
        self,
        file: FileDescriptorOrPath,
        mode: OpenBinaryModeWriting,
        buffering: Literal[-1, 1] = -1,
        encoding: None = None,
        errors: None = None,
        newline: None = None,
        closefd: bool = True,
        opener: _Opener | None = None,
    ) -> BufferedWriter: ...

    @overload
    def open(
        self,
        file: FileDescriptorOrPath,
        mode: OpenBinaryModeReading,
        buffering: Literal[-1, 1] = -1,
        encoding: None = None,
        errors: None = None,
        newline: None = None,
        closefd: bool = True,
        opener: _Opener | None = None,
    ) -> BufferedReader: ...

    @overload
    def open(
        self,
        file: FileDescriptorOrPath,
        mode: OpenBinaryMode,
        buffering: int = -1,
        encoding: None = None,
        errors: None = None,
        newline: None = None,
        closefd: bool = True,
        opener: _Opener | None = None,
    ) -> BinaryIO: ...

    @overload
    def open(
        self,
        file: FileDescriptorOrPath,
        mode: str,
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
        closefd: bool = True,
        opener: _Opener | None = None,
    ) -> IO[Any]: ...

    def open(self, file: FileDescriptorOrPath, mode: str = "r", **kwargs: Any) -> IO[Any]:
        import builtins

        return builtins.open(file, mode, **kwargs)


class SingleFileAPI(FileOpenerTypeMixin, abc.ABC):
    """
    Container representing one file in one backend store.
    """

    file_status: Optional[SingleFileStatus]
    store: Optional[str]
    file_url: str

    binary: Optional[bytes] = None
    loaded: bool = False

    def __init__(
        self,
        file_url: str,
        file_status: Optional[SingleFileStatus] = None,
        *,
        store: Optional[str] = None,
    ) -> None:
        self.file_url = file_url
        self.file_status = file_status
        self.store = store

    def _required_status(self, *, refresh: bool = False) -> SingleFileStatus:
        if refresh or self.file_status is None:
            self.file_status = self.recheck_status()
        if self.file_status is None:
            raise AttributeError("SingleFileAPI has no available status for {!r}".format(self.file_url))
        return self.file_status

    @property
    def status(self) -> Optional[SingleFileStatus]:
        """Alias retained for older call sites."""
        return self.file_status

    @property
    def uuid(self) -> Optional[str]:
        status = self.file_status
        return None if status is None else status.uuid

    @property
    def cached_size(self) -> Optional[int]:
        status = self.file_status
        return None if status is None else status.size

    @property
    def cached_hash(self) -> Optional[str]:
        status = self.file_status
        return None if status is None else status.hash

    @property
    def size(self) -> int:
        return self._required_status(refresh=True).size

    @property
    def hash(self) -> str:
        return self._required_status(refresh=True).hash

    @property
    def url(self) -> str:
        status = self.file_status
        if status is not None:
            return status.url
        return self.file_url

    @abc.abstractmethod
    def recheck_status(self) -> SingleFileStatus:
        """
        Refresh and return file status.
        """

    @abc.abstractmethod
    def as_string(self) -> str:
        """
        Return the file payload as text.
        """

    @abc.abstractmethod
    def as_bytes(self) -> bytes:
        """
        Return the file payload as bytes.
        """


@dataclasses.dataclass(frozen=True)
class FileStatus:
    """
    LiuXin-level status overlay for one logical file.
    """

    copies: int = 0
    protected: bool = False

