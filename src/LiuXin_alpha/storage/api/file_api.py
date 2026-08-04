"""API contracts for file containers exposed by storage backends.

Examples:
    Use the opener mixin anywhere a built-in-compatible ``open`` is needed::

        with opener.open("book.txt", encoding="utf-8") as file:
            text = file.read()
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
    """Typed mirror of Python's built-in ``open`` signatures.

    Examples:
        Binary modes receive an appropriately typed stream::

            with opener.open("cover.jpg", "rb") as file:
                header = file.read(16)
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
        """Open a descriptor or path using Python's built-in implementation.

        Examples:
            Read UTF-8 text::

                with opener.open("book.txt", encoding="utf-8") as file:
                    text = file.read()
        """
        import builtins

        return builtins.open(file, mode, **kwargs)


@dataclasses.dataclass(frozen=True)
class FileStatus:
    """LiuXin-level status overlay for one logical file.

    Examples:
        Represent a file with two protected copies::

            status = FileStatus(copies=2, protected=True)
    """

    copies: int = 0
    protected: bool = False
