# License: GPLv3 Copyright: 2008, Kovid Goyal <kovid at kovidgoyal.net>
# Ported from calibre (https://calibre-ebook.com) 9.18: src/calibre/ebooks/metadata/meta.py
# Changes from upstream: calibre imports point at upstream_calibre and its _compat.
# Only get_metadata, _get_metadata, metadata_from_filename and opf_metadata are ported, with the imports they use;
# the rest is conversion code the metadata readers do not use.
# calibre's plugin dispatch (customize.ui) is _readers.get_file_type_metadata, which
# runs the ported readers; its preferences are calibre's defaults (_compat.prefs);
# the regex package is used where installed, else _compat's stand-in on re.
# SPDX-License-Identifier: GPL-3.0-only

import os
from typing import IO, TYPE_CHECKING

from LiuXin_alpha.metadata.upstream_calibre._compat import regex

from LiuXin_alpha.metadata.upstream_calibre._compat import filesystem_encoding
from LiuXin_alpha.metadata.upstream_calibre._readers import get_file_type_metadata
from LiuXin_alpha.metadata.upstream_calibre.metadata_base import MetaInformation
from LiuXin_alpha.metadata.upstream_calibre.metadata_base import string_to_authors
from LiuXin_alpha.metadata.upstream_calibre.opf2 import OPF
from LiuXin_alpha.metadata.upstream_calibre._compat import StreamOrPath
from LiuXin_alpha.metadata.upstream_calibre._compat import prefs
from LiuXin_alpha.metadata.upstream_calibre._compat import is_existing_subpath
from LiuXin_alpha.metadata.upstream_calibre._compat import _

if TYPE_CHECKING:
    import re
    from LiuXin_alpha.metadata.upstream_calibre.book_base import Metadata


def get_metadata(
    stream: IO[bytes],
    stream_type: str | None = 'lrf',
    use_libprs_metadata: bool = False,
    force_read_metadata: bool = False,
    pattern: re.Pattern[str] | None = None,
) -> Metadata:
    pos = 0
    if hasattr(stream, 'tell'):
        pos = stream.tell()
    try:
        return _get_metadata(stream, stream_type, use_libprs_metadata, force_read_metadata, pattern)
    finally:
        if hasattr(stream, 'seek'):
            stream.seek(pos)


def _get_metadata(
    stream: IO[bytes],
    stream_type: str | None,
    use_libprs_metadata: bool,
    force_read_metadata: bool = False,
    pattern: re.Pattern[str] | None = None,
) -> Metadata:
    if stream_type:
        stream_type = stream_type.lower()
    if stream_type in ('html', 'html', 'xhtml', 'xhtm', 'xml'):
        stream_type = 'html'
    if stream_type in ('mobi', 'prc', 'azw'):
        stream_type = 'mobi'
    if stream_type in ('odt', 'ods', 'odp', 'odg', 'odf'):
        stream_type = 'odt'

    opf = None
    if hasattr(stream, 'name'):
        c = os.path.splitext(stream.name)[0] + '.opf'
        if os.access(c, os.R_OK):
            opf = opf_metadata(os.path.abspath(c))

    if use_libprs_metadata and getattr(opf, 'application_id', None) is not None:
        assert opf is not None  # getattr(None, 'application_id', None) is None
        return opf

    name = os.path.basename(getattr(stream, 'name', ''))
    # The fallback pattern matches the default filename format produced by calibre
    base = metadata_from_filename(
        name,
        pat=pattern,
        fallback_pat=regex.compile(r'^(?P<title>.+) - (?P<author>[^-]+)$', flags=regex.UNICODE | regex.VERSION1 | regex.FULLCASE),
    )
    if not base.authors:
        base.authors = [_('Unknown')]
    if not base.title:
        base.title = _('Unknown')
    mi = MetaInformation(None, None)
    if force_read_metadata or prefs['read_file_metadata']:
        mi = get_file_type_metadata(stream, stream_type or '')
    base.smart_update(mi)
    if opf is not None:
        base.smart_update(opf)

    return base


def metadata_from_filename(name: str | bytes, pat: re.Pattern[str] | None = None, fallback_pat: re.Pattern[str] | None = None) -> Metadata:
    if isinstance(name, bytes):
        name = name.decode(filesystem_encoding, 'replace')
    name = name.rpartition('.')[0]
    mi = MetaInformation(None, None)
    if pat is None:
        try:
            pat = regex.compile(prefs.get('filename_pattern'), flags=regex.UNICODE | regex.VERSION1 | regex.FULLCASE)
        except Exception:
            try:
                pat = regex.compile(prefs.get('filename_pattern'), flags=regex.UNICODE | regex.VERSION0 | regex.FULLCASE)
            except Exception:
                pat = regex.compile(r'(?P<title>.+) - (?P<author>[^_]+)', flags=regex.UNICODE | regex.VERSION0 | regex.FULLCASE)

    name = name.replace('_', ' ')
    match = pat.search(name)
    if match is None and fallback_pat is not None:
        match = fallback_pat.search(name)
    if match is not None:
        try:
            mi.title = match.group('title')
        except IndexError:
            pass
        try:
            au = match.group('author')
            aus = string_to_authors(au)
            if aus:
                mi.authors = aus
                if prefs['swap_author_names'] and mi.authors:

                    def swap(a: str) -> str:
                        if ',' in a:
                            parts = a.split(',', 1)
                        else:
                            parts = a.split(None, 1)
                        if len(parts) > 1:
                            t = parts[-1]
                            parts = parts[:-1]
                            parts.insert(0, t)
                        return ' '.join(parts)

                    mi.authors = [swap(x) for x in mi.authors]
        except IndexError, ValueError:
            pass
        try:
            mi.series = match.group('series')
        except IndexError:
            pass
        try:
            si = match.group('series_index')
            mi.series_index = float(si)
        except IndexError, ValueError, TypeError:
            pass
        try:
            si = match.group('isbn')
            mi.isbn = si
        except IndexError, ValueError:
            pass
        try:
            publisher = match.group('publisher')
            mi.publisher = publisher
        except IndexError, ValueError:
            pass
        try:
            pubdate = match.group('published')
            if pubdate:
                from LiuXin_alpha.metadata.upstream_calibre.date import parse_only_date

                mi.pubdate = parse_only_date(pubdate)
        except Exception:
            pass
        try:
            comments = match.group('comments')
            mi.comments = comments
        except IndexError, ValueError:
            pass

    if mi.is_null('title'):
        mi.title = name
    return mi


def opf_metadata(opfpath: str | IO[bytes]) -> Metadata | None:
    try:
        with StreamOrPath(opfpath, 'rb') as f:
            opfpath = getattr(f, 'name', os.getcwd())
            opf = OPF(f, os.path.dirname(opfpath))
            if opf.application_id is not None:
                mi = opf.to_book_metadata()
                if hasattr(opf, 'cover') and opf.cover:
                    base = os.path.dirname(opfpath)
                    cpath = os.path.join(base, opf.cover)
                    # Only read covers inside the OPF's directory, to prevent
                    # a malicious OPF from reading arbitrary files
                    if is_existing_subpath(cpath, base) and os.access(cpath, os.R_OK):
                        fmt = cpath.rpartition('.')[-1]
                        with open(cpath, 'rb') as f:
                            data = f.read()
                        mi.cover_data = (fmt, data)
                return mi
    except Exception:
        import traceback

        traceback.print_exc()
