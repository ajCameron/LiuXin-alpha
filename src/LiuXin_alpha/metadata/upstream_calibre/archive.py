#!/usr/bin/env python
# License: GPLv3 Copyright: 2010, Kovid Goyal <kovid@kovidgoyal.net>
# Ported from calibre (https://calibre-ebook.com) 9.18: src/calibre/ebooks/metadata/archive.py
# Changes from upstream: calibre imports point at upstream_calibre and its _compat.
# Only is_comic, archive_type, RAR, SevenZip, fname_ok, get_comic_book_info, parse_comic_comment and get_comic_metadata are ported, with the imports they use;
# the rest is conversion code the metadata readers do not use.
# calibre.utils.unrar, which wraps the unrardll extension, is _unrar, on rarfile.
# SPDX-License-Identifier: GPL-3.0-only

import io
from posixpath import basename
from typing import IO, TYPE_CHECKING

from LiuXin_alpha.metadata.upstream_calibre._compat import canonicalize_lang

if TYPE_CHECKING:
    from collections.abc import Iterable
    from LiuXin_alpha.metadata.upstream_calibre.book_base import Metadata


def is_comic(list_of_names: Iterable[str]) -> bool:
    extensions = {x.rpartition('.')[-1].lower() for x in list_of_names if '.' in x and x.lower().rpartition('/')[-1] != 'thumbs.db'}
    comic_extensions = {'jpg', 'jpeg', 'png'}
    return len(extensions - comic_extensions) == 0


def archive_type(stream: IO[bytes]) -> str | None:
    from LiuXin_alpha.metadata.upstream_calibre._compat import stringFileHeader

    try:
        pos = stream.tell()
    except Exception:
        pos = 0
    id_ = stream.read(4)
    ans = None
    if id_ == stringFileHeader:
        ans = 'zip'
    elif id_.startswith(b'Rar'):
        ans = 'rar'
    try:
        stream.seek(pos)
    except Exception:
        pass
    return ans


class RAR:
    def __init__(self, archive: str | IO[bytes]) -> None:
        self.archive = archive

    def close(self) -> None:
        pass

    def namelist(self) -> list[str]:
        from LiuXin_alpha.metadata.upstream_calibre._unrar import names

        return list(names(self.archive))

    def read(self, fname: str) -> bytes:
        from LiuXin_alpha.metadata.upstream_calibre._unrar import extract_member

        res = extract_member(self.archive, match=None, name=fname)
        if res is None:
            raise KeyError(f'There is no file named {fname} in the archive')
        return res[1]


class SevenZip:
    def __init__(self, archive: str | IO[bytes]) -> None:
        from py7zr import SevenZipFile

        self.zf = SevenZipFile(archive, 'r')

    def namelist(self) -> list[str]:
        return list(self.zf.getnames())

    def close(self) -> None:
        self.zf.close()

    def read(self, fname: str) -> bytes:
        from py7zr import WriterFactory

        class MemoryFactory(WriterFactory):
            def __init__(self) -> None:
                self.buffers: dict[str, io.BytesIO] = {}

            def create(self, filename: str) -> io.BytesIO:  # ty: ignore[incompatible-method-override] BytesIO is duck type compatible with Py7zIO
                # Create an in-memory BytesIO stream for the file
                self.buffers[filename] = io.BytesIO()
                return self.buffers[filename]

        factory = MemoryFactory()
        self.zf.extract(targets=[fname], factory=factory)
        target_buffer: io.BytesIO = factory.buffers[fname]
        target_buffer.seek(0)
        return target_buffer.getvalue()


def fname_ok(fname: str) -> bool:
    fname = fname.replace('\\', '/')
    bn = basename(fname).lower()
    if bn == 'thumbs.db':
        return False
    if '.' not in bn:
        return False
    if bn.rpartition('.')[-1] in {'diz', 'nfo'}:
        return False
    if '__MACOSX' in fname.split('/'):
        return False
    return True


def get_comic_book_info(d: ComicBookInfo, mi: Metadata, series_index: str = 'volume') -> None:
    # See http://code.google.com/p/comicbookinfo/wiki/Example
    series = d.get('series', '')
    if series.strip():
        mi.series = series
        si = d.get(series_index, None)
        if si is None:
            si = d.get('issue' if series_index == 'volume' else 'volume', None)
        if si is not None:
            try:
                mi.series_index = float(si)
            except Exception:
                mi.series_index = 1
    if d.get('language', None):
        lang = canonicalize_lang(d.get('language'))
        if lang:
            mi.languages = [lang]
    if d.get('rating', -1) > -1:
        mi.rating = d['rating']
    for x in ('title', 'publisher'):
        y = d.get(x, '').strip()
        if y:
            setattr(mi, x, y)
    tags = d.get('tags', [])
    if tags:
        mi.tags = tags
    authors: list[str] = []
    for credit in d.get('credits', []):
        if credit.get('role', '') in ('Writer', 'Artist', 'Cartoonist', 'Creator'):
            x = credit.get('person', '')
            if x:
                x = ' '.join(reversed(x.split(', ')))
                authors.append(x)
    if authors:
        mi.authors = authors
    comments = d.get('comments', '')
    if comments and comments.strip():
        mi.comments = comments.strip()
    pubm, puby = d.get('publicationMonth', None), d.get('publicationYear', None)
    if puby is not None:
        from datetime import date

        from LiuXin_alpha.metadata.upstream_calibre.date import parse_only_date

        try:
            dt = date(puby, 6 if pubm is None else pubm, 15)
            dt = parse_only_date(str(dt))
            mi.pubdate = dt
        except Exception:
            pass


def parse_comic_comment(comment: str | bytes, series_index: str = 'volume') -> Metadata:
    # See http://code.google.com/p/comicbookinfo/wiki/Example
    import json

    from LiuXin_alpha.metadata.upstream_calibre.metadata_base import MetaInformation

    mi = MetaInformation(None, None)
    m = json.loads(comment)
    if isinstance(m, dict):
        for cat in m:
            if cat.startswith('ComicBookInfo'):
                get_comic_book_info(m[cat], mi, series_index=series_index)
                break
    return mi


def get_comic_metadata(stream: IO[bytes], stream_type: str, series_index: str = 'volume') -> Metadata:
    comment = None
    if stream_type == 'cbz':
        from LiuXin_alpha.metadata.upstream_calibre._compat import ZipFile

        zf = ZipFile(stream)
        comment = zf.comment
    elif stream_type == 'cbr':
        from LiuXin_alpha.metadata.upstream_calibre._unrar import comment as get_comment

        comment = get_comment(stream)

    return parse_comic_comment(comment or b'{}', series_index=series_index)
