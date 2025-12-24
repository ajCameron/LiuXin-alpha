from __future__ import annotations

import abc
import dataclasses
from io import TextIOWrapper, FileIO, BufferedRandom, BufferedWriter, BufferedReader

from typing import Literal, overload, Optional, BinaryIO, IO, Any, TypeVar

from LiuXin_alpha.storage.api.modes_api import OpenTextMode, OpenBinaryModeUpdating, OpenBinaryModeWriting, \
    OpenBinaryModeReading, OpenBinaryMode, _Opener
from LiuXin_alpha.storage.api.location_api import FileDescriptorOrPath
from LiuXin_alpha.storage.single_file import SingleFileStatus

T = TypeVar("T")


class FileOpenerTypeMixin(abc.ABC):
    """
    When mixed in, provides a
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

    # Unbuffered binary mode: returns FileIO
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

    # Buffered binary mode (buffering -1 or 1): narrower returns
    @overload
    def open(
        self,
        file: FileDescriptorOrPath,
        mode: OpenBinaryModeUpdating,
        buffering: Literal[-1, 1] = -1,
        encoding: Optional[None] = None,
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

    # Buffering can’t be determined precisely: fall back to BinaryIO
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

    # “Anything else” fallback
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


class SingleFileAPI(FileOpenerTypeMixin):
    """
    Container representing a single file in a single store.
    """
    file_status: SingleFileStatus  # - Status for the file on the system

    store: str                      # - Which store is the file in?
    file_url: str                   # - url to this instance of the file

    binary: Optional[bytes] = None      # - Binary bits of the file

    loaded: bool = False        # - Has the file actually been loaded into this dataclass?

    def __init__(self, file_url: str, file_status: Optional[SingleFileStatus]) -> None:
        """
        Initializes a single file.

        :param file_url:
        """
        self.file_url = file_url
        self.file_status = file_status

    @abc.abstractmethod
    def recheck_status(self) -> SingleFileStatus:
        """
        If we suspect something has changed, then we must regenerate status.

        :return:
        """

    @property
    def uuid(self) -> Optional[str]:
        """
        Return the uuid for the file stored in the status.

        This should be (mostly) static for the file.
        :return:
        """
        return self.status.uuid

    @property
    def cached_size(self) -> int:
        """
        Return the cached size for the individual file.

        :return:
        """
        return self.status.size

    @property
    def cached_hash(self) -> str:
        """
        Return the cached hash of the file.
        :return:
        """
        return self.status.hash

    @property
    def size(self) -> int:
        """
        Go and check the actual size of the file.

        :return:
        """
        return self.status.size

    @property
    def hash(self) -> str:
        """
        Go and check the actual hash of the file.

        :return:
        """
        return self.status.hash

    @property
    def url(self) -> str:
        """
        Return the URL of the file.

        :return:
        """
        return self.status.url

    def as_string(self) -> str:
        """
        Return the file as a string - this can be a memory and time intensive operation.

        :return:
        """

    def as_bytes(self) -> bytes:
        """
        Return the file as bytes.

        :return:
        """


@dataclasses.dataclass
class FileStatus:
    """
    Status for a file on the system - includes LiuXin wide metadata
    """

    copies: str         # - Number of copies the system has access to?
    protected: bool     # - Does the system consider the file to be protected?






