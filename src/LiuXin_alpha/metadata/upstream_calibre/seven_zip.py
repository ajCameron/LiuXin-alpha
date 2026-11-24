#!/usr/bin/env python
# License: GPL v3 Copyright: 2021, Kovid Goyal <kovid at kovidgoyal.net>
# Ported from calibre (https://calibre-ebook.com) 9.18: src/calibre/utils/seven_zip.py
# Changes from upstream: calibre imports point at upstream_calibre and its _compat.
# Only open_archive, names, DataSavingWriter, Writer, read_file, extract_member, extract_cover_image and extract are ported, with the imports they use;
# the rest is conversion code the metadata readers do not use.
# py7zr, which it uses, is optional (the archives extra).
# SPDX-License-Identifier: GPL-3.0-only

import io
import os
import re
from typing import IO, TYPE_CHECKING, Self

from LiuXin_alpha.metadata.upstream_calibre._compat import iswindows

if TYPE_CHECKING:
    from py7zr import SevenZipFile


def open_archive(path_or_stream: str | IO[bytes], mode: str = 'r') -> SevenZipFile:
    from py7zr import SevenZipFile

    return SevenZipFile(path_or_stream, mode=mode)


def names(path_or_stream: str | IO[bytes]) -> tuple[str, ...]:
    with open_archive(path_or_stream) as zf:
        return tuple(zf.getnames())


class DataSavingWriter(io.BytesIO):
    def close(self) -> None:
        return  # make this a no-op as we need to call getvalue() after close


class Writer:
    def __init__(self) -> None:
        self.outputs: dict[str, DataSavingWriter] = {}

    def create(self, filename: str) -> DataSavingWriter:
        b = self.outputs[filename] = DataSavingWriter()
        return b

    def asdatadict(self) -> dict[str, bytes]:
        return {k: v.getvalue() for k, v in self.outputs.items()}

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *a: object) -> None:
        for v in self.outputs.values():
            io.BytesIO.close(v)
        self.outputs.clear()


def read_file(archive: SevenZipFile, name: str) -> bytes:
    with Writer() as w:
        archive.extract(targets=[name], factory=w)  # ty: ignore[invalid-argument-type]
        for v in w.outputs.values():
            return v.getvalue()
    raise KeyError(f'No file named {name} in archive')


def extract_member(path_or_stream: str | IO[bytes], match: re.Pattern[str] | None = None, name: str | None = None) -> tuple[str, bytes] | None:
    if iswindows and name is not None:
        name = name.replace(os.sep, '/')
    if match is None:
        match = re.compile(r'\.(jpg|jpeg|gif|png)\s*$', re.I)

    def is_match(fname: str) -> bool:
        if iswindows:
            fname = fname.replace(os.sep, '/')
        return (name is not None and fname == name) or (match is not None and match.search(fname) is not None)

    with open_archive(path_or_stream) as ar:
        all_names = list(filter(is_match, ar.getnames()))
        if all_names:
            return all_names[0], read_file(ar, all_names[0])


def extract_cover_image(stream: IO[bytes]) -> tuple[str, bytes] | None:
    pos = stream.tell()
    from LiuXin_alpha.metadata.upstream_calibre.libunzip import name_ok
    from LiuXin_alpha.metadata.upstream_calibre.libunzip import sort_key

    all_names = sorted(names(stream), key=sort_key)
    stream.seek(pos)
    for name in all_names:
        if name_ok(name):
            return extract_member(stream, name=name, match=None)


def extract(path_or_stream: str | IO[bytes], location: str) -> None:
    with open_archive(path_or_stream) as f:
        f.extract(location)
