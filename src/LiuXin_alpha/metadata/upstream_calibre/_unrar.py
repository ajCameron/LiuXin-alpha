# Stands in for calibre 9.18's src/calibre/utils/unrar.py, which wraps the unrardll
# extension (calibre's binding to RARLAB's library). This module gives the same
# functions on rarfile. License: GPL v3. Copyright: Kovid Goyal.
# SPDX-License-Identifier: GPL-3.0-only
"""
Read RAR archives as calibre's ``unrar`` module does, on ``rarfile``.

calibre reads RAR archives through ``unrardll``, its binding to RARLAB's library,
which LiuXin does not ship. These functions give the same answers through the
``rarfile`` package (the ``archives`` extra), or the copy LiuXin vendors where it is
not installed. ``rarfile`` reads names and comments itself, but needs an extraction
tool on the PATH (``unrar``, ``unar``, ``7z`` or ``bsdtar``) for compressed
contents; ``available`` says whether there is one.

Members larger than LiuXin's archive limits are refused before they are read.

Example:
    >>> available()[0] in (True, False)
    True
"""

from __future__ import annotations

import os
import re
import shutil
import tempfile
from collections.abc import Iterator
from types import ModuleType
from typing import IO, Any

DEFAULT_MATCH = re.compile(r"\.(jpg|jpeg|gif|png)\s*$", re.I)


def _rarfile() -> ModuleType:
    """
    The ``rarfile`` module: the installed package, else LiuXin's vendored copy.

    Example:
        >>> hasattr(_rarfile(), "RarFile")
        True


    :return: The module.
    """
    try:
        import rarfile  # type: ignore[import-not-found, import-untyped, unused-ignore]
    except ImportError:
        from LiuXin_alpha.utils.decompression.rarfile import (
            rarfile,  # type: ignore[no-redef, unused-ignore]
        )
    return rarfile


def available() -> tuple[bool, str]:
    """
    Whether RAR contents can be read: ``rarfile`` has an extraction tool to run.

    Example:
        >>> isinstance(available()[1], str)
        True


    :return: Whether, and if not, why not.
    """
    rarfile = _rarfile()
    setup = getattr(rarfile, "tool_setup", None)
    if setup is not None:
        try:
            setup()
        except Exception as err:
            return False, f"rarfile found no tool to extract RAR contents with: {err}"
        return True, ""
    tool = getattr(rarfile, "UNRAR_TOOL", "unrar")
    if shutil.which(tool) is None:
        return (
            False,
            f"no {tool!r} on the PATH, which rarfile needs to extract RAR contents",
        )
    return True, ""


class StreamAsPath:
    """
    A path for an archive given as a path or a stream, as calibre's ``StreamAsPath`` makes.

    The extraction tools read files, so a stream with no readable file behind it is
    copied to a temporary file, removed again on exit.

    Example:
        >>> with StreamAsPath("/tmp/book.rar") as path:
        ...     path
        '/tmp/book.rar'
    """

    def __init__(self, stream: str | bytes | IO[bytes]) -> None:
        """
        Remember the archive.

        Example:
            >>> StreamAsPath("/tmp/a.rar").stream
            '/tmp/a.rar'


        :param stream: A path or a binary stream.
        """
        self.stream = stream
        self.temppath: str | None = None

    def __enter__(self) -> str:
        """
        Give a path to read the archive at.

        Example:
            See the class.


        :return: The path.
        """
        if isinstance(self.stream, bytes):
            return os.fsdecode(self.stream)
        if isinstance(self.stream, str):
            return self.stream
        name = getattr(self.stream, "name", None)
        if isinstance(name, str) and os.access(name, os.R_OK):
            return name
        pos = self.stream.tell()
        with tempfile.NamedTemporaryFile("wb", prefix="for-unar", delete=False) as f:
            shutil.copyfileobj(self.stream, f)
        self.stream.seek(pos)
        self.temppath = f.name
        return f.name

    def __exit__(self, *args: object) -> None:
        """
        Remove the temporary copy, if one was made.

        Example:
            See the class.


        :param args: The exception, if any; not suppressed.
        """
        if self.temppath is not None:
            try:
                os.remove(self.temppath)
            except OSError:
                pass
        self.temppath = None


def _useful(info: Any) -> bool:
    """
    Whether a header is a file, as ``unrardll``'s ``is_useful`` asks: not a folder or a link.

    Example:
        >>> from types import SimpleNamespace
        >>> _useful(SimpleNamespace(isdir=lambda: True))
        False


    :param info: A ``rarfile`` info object.
    :return: True for a file.
    """
    if info.isdir():
        return False
    is_symlink = getattr(info, "is_symlink", None)
    return not (callable(is_symlink) and is_symlink())


def _infos(path: str) -> list[Any]:
    """
    The archive's file headers, in archive order.

    Example:
        >>> _infos("/no/such.rar")  # doctest: +SKIP


    :param path: The archive.
    :return: Its files' ``rarfile`` info objects.
    """
    with _rarfile().RarFile(path) as archive:
        return [info for info in archive.infolist() if _useful(info)]


def names(path_or_stream: str | bytes | IO[bytes]) -> Iterator[str]:
    """
    The names of the archive's files, as calibre's ``unrar.names`` gives them.

    Example:
        >>> list(names("/no/such.rar"))  # doctest: +SKIP


    :param path_or_stream: The archive.
    :return: The names, in archive order.
    """
    with StreamAsPath(path_or_stream) as path:
        infos = _infos(path)
    for info in infos:
        yield info.filename


def comment(path_or_stream: str | bytes | IO[bytes]) -> str:
    """
    The archive's comment, or an empty string.

    Example:
        >>> comment("/no/such.rar")  # doctest: +SKIP


    :param path_or_stream: The archive.
    :return: The comment.
    """
    with StreamAsPath(path_or_stream) as path, _rarfile().RarFile(path) as archive:
        text = archive.comment
    if isinstance(text, bytes):
        text = text.decode("utf-8", "replace")
    return text or ""


def _check_member(info: Any) -> None:
    """
    Refuse a member larger, or more compressed, than LiuXin's archive limits allow.

    Example:
        >>> from types import SimpleNamespace
        >>> _check_member(SimpleNamespace(filename="a", file_size=10, compress_size=10))


    :param info: The member's header.
    :raises ArchivePreflightError: If it is too large.
    """
    # LiuXin: calibre reads the member whole, whatever its size.
    from LiuXin_alpha.file_formats.archive_preflight import validate_zip_member_infos

    validate_zip_member_infos(
        [info],
        container_label="RAR file",
        member_label="RAR archive",
        allow_unsafe_paths=True,
    )


def extract_member(
    path_or_stream: str | bytes | IO[bytes],
    match: re.Pattern[str] | None = DEFAULT_MATCH,
    name: str | None = None,
) -> tuple[str, bytes] | None:
    """
    Read the first file named ``name``, or whose name ``match`` finds, as calibre does.

    Example:
        >>> extract_member("/no/such.rar", name="a.txt")  # doctest: +SKIP


    :param path_or_stream: The archive.
    :param match: A pattern for names; ``None`` to go by ``name`` alone.
    :param name: The name of the file wanted.
    :return: Its name and contents, or ``None``.
    """
    if os.sep != "/" and name is not None:
        name = name.replace(os.sep, "/")

    def is_match(fname: str) -> bool:
        """
        Whether a member is the one wanted.

        Example:
            >>> is_match("cover.jpg")  # doctest: +SKIP


        :param fname: Its name.
        :return: True if wanted.
        """
        if os.sep != "/":
            fname = fname.replace(os.sep, "/")
        return (name is not None and fname == name) or (
            match is not None and match.search(fname) is not None
        )

    with StreamAsPath(path_or_stream) as path, _rarfile().RarFile(path) as archive:
        for info in archive.infolist():
            if _useful(info) and is_match(info.filename):
                _check_member(info)
                return info.filename, archive.read(info)
    return None


def extract_cover_image(stream: str | bytes | IO[bytes]) -> tuple[str, bytes] | None:
    """
    Read the first image in natural name order, as calibre's comic reader takes for the cover.

    Example:
        >>> extract_cover_image("/no/such.rar")  # doctest: +SKIP


    :param stream: The archive.
    :return: The image's name and data, or ``None``.
    """
    from LiuXin_alpha.metadata.upstream_calibre.libunzip import name_ok, sort_key

    for name in sorted(names(stream), key=sort_key):
        if name_ok(name):
            return extract_member(stream, name=name, match=None)
    return None
