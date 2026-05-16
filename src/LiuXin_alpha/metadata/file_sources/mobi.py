#!/usr/bin/env python
# License: GPLv3 Copyright: 2009, Kovid Goyal <kovid at kovidgoyal.net>


import io
import numbers
import os
from struct import pack, unpack

from LiuXin_alpha.file_formats import normalize
from LiuXin_alpha.file_formats.mobi import MAX_THUMB_DIMEN, MobiError
from LiuXin_alpha.file_formats.mobi.langcodes import iana2mobi
from LiuXin_alpha.file_formats.mobi.utils import rescale_image
from LiuXin_alpha.metadata.utils import calibreMetaInformation
from LiuXin_alpha.utils.date import now as nowf
from LiuXin_alpha.utils.image_tools.imghdr import what
from LiuXin_alpha.utils.libraries.cleantext import clean_xml_chars
from LiuXin_alpha.utils.localization import canonicalize_lang, lang_as_iso639_1, trans as _
from LiuXin_alpha.utils.logging import default_log

'''
Retrieve and modify in-place Mobipocket book metadata.
'''


def is_image(ss):
    if ss is None:
        return False
    return what(None, ss[:200]) is not None


def _clean_mobi_text(value):
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray, memoryview)):
        value = bytes(value).decode("utf-8", "replace")
    return normalize(clean_xml_chars(str(value))).strip()


def _clean_mobi_list(values):
    if values is None:
        return []
    if isinstance(values, str):
        values = [values]
    elif isinstance(values, dict):
        values = values.keys()
    try:
        iterable = list(values)
    except TypeError:
        iterable = [values]
    return [text for text in (_clean_mobi_text(value) for value in iterable) if text]


def _metadata_is_null(mi, field):
    try:
        return bool(mi.is_null(field))
    except Exception:
        return not bool(getattr(mi, field, None))


class StreamSlicer:

    def __init__(self, stream, start=0, stop=None):
        self._stream = stream
        self.start = start
        if stop is None:
            stream.seek(0, 2)
            stop = stream.tell()
        self.stop = stop
        self._len = stop - start

    def __len__(self):
        return self._len

    def __getitem__(self, key):
        stream = self._stream
        base = self.start
        if isinstance(key, numbers.Integral):
            stream.seek(base + key)
            return stream.read(1)
        if isinstance(key, slice):
            start, stop, stride = key.indices(self._len)
            if stride < 0:
                start, stop = stop, start
            size = stop - start
            if size <= 0:
                return b''
            stream.seek(base + start)
            data = stream.read(size)
            if stride != 1:
                data = data[::stride]
            return data
        raise TypeError('stream indices must be integers')

    def __setitem__(self, key, value):
        stream = self._stream
        base = self.start
        if isinstance(key, numbers.Integral):
            if len(value) != 1:
                raise ValueError('key and value lengths must match')
            stream.seek(base + key)
            return stream.write(value)
        if isinstance(key, slice):
            start, stop, stride = key.indices(self._len)
            if stride < 0:
                start, stop = stop, start
            size = stop - start
            if stride != 1:
                value = value[::stride]
            if len(value) != size:
                raise ValueError('key and value lengths must match')
            stream.seek(base + start)
            return stream.write(value)
        raise TypeError('stream indices must be integers')

    def update(self, data_blocks):
        # Rewrite the stream
        stream = self._stream
        base = self.start
        stream.seek(base)
        stream.truncate()
        for block in data_blocks:
            stream.write(block)

    def truncate(self, value):
        self._stream.truncate(value)


class MetadataUpdater:
    DRM_KEY_SIZE = 48

    def __init__(self, stream):
        self.stream = stream
        data = self.data = StreamSlicer(stream)
        self.type = data[60:68]

        if self.type != b'BOOKMOBI':
            return

        self.nrecs, = unpack('>H', data[76:78])
        record0 = self.record0 = self.record(0)
        mobi_header_length, = unpack('>I', record0[0x14:0x18])
        if not mobi_header_length:
            raise MobiError("Non-standard file format.  Try 'Convert E-Books' with MOBI as Input and Output formats.")

        self.encryption_type, = unpack('>H', record0[12:14])
        codepage, = unpack('>I', record0[28:32])
        self.codec = 'utf-8' if codepage == 65001 else 'cp1252'

        image_base, = unpack('>I', record0[108:112])
        flags, = self.flags, = unpack('>I', record0[128:132])
        have_exth = self.have_exth = (flags & 0x40) != 0
        self.cover_record = self.thumbnail_record = None
        self.timestamp = None
        self.pdbrecords = self.get_pdbrecords()

        self.drm_block = None
        if self.encryption_type != 0:
            if self.have_exth:
                self.drm_block = self.fetchDRMdata()
            else:
                raise MobiError('Unable to set metadata on DRM file without EXTH header')

        self.original_exth_records = {}
        if not have_exth:
            self.create_exth()
            self.have_exth = True
        # Fetch timestamp, cover_record, thumbnail_record
        self.fetchEXTHFields()

    def fetchDRMdata(self):
        ''' Fetch the DRM keys '''
        drm_offset = int(unpack('>I', self.record0[0xa8:0xac])[0])
        self.drm_key_count = int(unpack('>I', self.record0[0xac:0xb0])[0])
        drm_keys = b''
        for x in range(self.drm_key_count):
            base_addr = drm_offset + (x * self.DRM_KEY_SIZE)
            drm_keys += self.record0[base_addr:base_addr + self.DRM_KEY_SIZE]
        return drm_keys

    def fetchEXTHFields(self):
        stream = self.stream
        record0 = self.record0

        # 20:24 = mobiHeaderLength, 16=PDBHeader size
        exth_off = unpack('>I', record0[20:24])[0] + 16 + record0.start
        image_base, = unpack('>I', record0[108:112])

        # Fetch EXTH block
        exth = self.exth = StreamSlicer(stream, exth_off, record0.stop)
        nitems, = unpack('>I', exth[8:12])
        pos = 12
        # Store any EXTH fields not specifiable in GUI
        for i in range(nitems):
            id, size = unpack('>II', exth[pos:pos + 8])
            content = exth[pos + 8: pos + size]
            pos += size

            self.original_exth_records[id] = content

            if id == 106:
                self.timestamp = content
            elif id == 201:
                rindex, = self.cover_rindex, = unpack('>I', content)
                if rindex != 0xffffffff:
                    self.cover_record = self.record(rindex + image_base)
            elif id == 202:
                rindex, = self.thumbnail_rindex, = unpack('>I', content)
                if rindex > 0 and rindex != 0xffffffff:
                    self.thumbnail_record = self.record(rindex + image_base)

    def patch(self, off, new_record0):
        # Save the current size of each record
        record_sizes = [len(new_record0)]
        for i in range(1, self.nrecs-1):
            record_sizes.append(self.pdbrecords[i+1][0]-self.pdbrecords[i][0])
        # And the last one
        record_sizes.append(self.data.stop - self.pdbrecords[self.nrecs-1][0])

        # pdbrecord[0] is the offset of record0.  It will not change
        # record1 offset will be offset of record0 + len(new_record0)
        updated_pdbrecords = [self.pdbrecords[0][0]]
        record0_offset = self.pdbrecords[0][0]
        updated_offset = record0_offset + len(new_record0)

        for i in range(1, self.nrecs-1):
            updated_pdbrecords.append(updated_offset)
            updated_offset += record_sizes[i]
        # Update the last pdbrecord
        updated_pdbrecords.append(updated_offset)

        # Read in current records 1 to last
        data_blocks = [new_record0]
        for i in range(1, self.nrecs):
            data_blocks.append(self.data[self.pdbrecords[i][0]:self.pdbrecords[i][0] + record_sizes[i]])

        # Rewrite the stream
        self.record0.update(data_blocks)

        # Rewrite the pdbrecords
        self.update_pdbrecords(updated_pdbrecords)

        # Truncate if necessary
        if (updated_pdbrecords[-1] + record_sizes[-1]) < self.data.stop:
            self.data.truncate(updated_pdbrecords[-1] + record_sizes[-1])
        else:
            self.data.stop = updated_pdbrecords[-1] + record_sizes[-1]

    def patchSection(self, section, new):
        off = self.pdbrecords[section][0]
        self.patch(off, new)

    def create_exth(self, new_title=None, exth=None):
        # Add an EXTH block to record 0, rewrite the stream
        if isinstance(new_title, str):
            new_title = new_title.encode(self.codec, 'replace')

        # Fetch the existing title
        title_offset, = unpack('>L', self.record0[0x54:0x58])
        title_length, = unpack('>L', self.record0[0x58:0x5c])
        title_in_file, = unpack(f'{title_length}s', self.record0[title_offset:title_offset + title_length])

        # Adjust length to accommodate PrimaryINDX if necessary
        mobi_header_length, = unpack('>L', self.record0[0x14:0x18])
        if mobi_header_length == 0xe4:
            # Patch mobi_header_length to 0xE8
            self.record0[0x17] = b'\xe8'
            self.record0[0xf4:0xf8] = pack('>L', 0xFFFFFFFF)
            mobi_header_length = 0xe8

        # Set EXTH flag (0x40)
        self.record0[0x80:0x84] = pack('>L', self.flags|0x40)

        if not exth:
            # Construct an empty EXTH block
            pad = b'\0' * 4
            exth = [b'EXTH', pack('>II', 12, 0), pad]
            exth = b''.join(exth)

        # Update drm_offset(0xa8), title_offset(0x54)
        if self.encryption_type != 0:
            self.record0[0xa8:0xac] = pack('>L', 0x10 + mobi_header_length + len(exth))
            self.record0[0xb0:0xb4] = pack('>L', len(self.drm_block))
            self.record0[0x54:0x58] = pack('>L', 0x10 + mobi_header_length + len(exth) + len(self.drm_block))
        else:
            self.record0[0x54:0x58] = pack('>L', 0x10 + mobi_header_length + len(exth))

        if new_title:
            self.record0[0x58:0x5c] = pack('>L', len(new_title))

        # Create an updated Record0
        new_record0 = io.BytesIO()
        new_record0.write(self.record0[:0x10 + mobi_header_length])
        new_record0.write(exth)
        if self.encryption_type != 0:
            new_record0.write(self.drm_block)
        new_record0.write(new_title if new_title else title_in_file)

        # Pad to a 4-byte boundary
        trail = len(new_record0.getvalue()) % 4
        pad = b'\0' * (4 - trail)  # Always pad w/ at least 1 byte
        new_record0.write(pad)
        new_record0.write(b'\0'*(1024*8))

        # Rebuild the stream, update the pdbrecords pointers
        self.patchSection(0, new_record0.getvalue())

        # Update record0
        self.record0 = self.record(0)

    def hexdump(self, src, length=16):
        # Diagnostic
        FILTER=''.join([((len(repr(chr(x)))==3) and chr(x)) or '.' for x in range(256)])
        N=0
        result=''
        while src:
            s, src = src[:length],src[length:]
            hexa = ' '.join([f'{ord(x):02X}' for x in s])
            s = s.translate(FILTER)
            result += '%04X   %-*s   %s\n' % (N, length*3, hexa, s)  # noqa: UP031
            N+=length
        print(result)

    def get_pdbrecords(self):
        pdbrecords = []
        for i in range(self.nrecs):
            offset, a1,a2,a3,a4 = unpack('>LBBBB', self.data[78+i*8:78+i*8+8])
            flags, val = a1, a2<<16|a3<<8|a4
            pdbrecords.append([offset, flags, val])
        return pdbrecords

    def update_pdbrecords(self, updated_pdbrecords):
        for i,pdbrecord in enumerate(updated_pdbrecords):
            self.data[78+i*8:78+i*8 + 4] = pack('>L',pdbrecord)

        # Refresh local copy
        self.pdbrecords = self.get_pdbrecords()

    def dump_pdbrecords(self):
        # Diagnostic
        print('MetadataUpdater.dump_pdbrecords()')
        print(f"{'offset':>10} {'flags':>10} {'val':>10}")
        for i in range(len(self.pdbrecords)):
            pdbrecord = self.pdbrecords[i]
            print(f'{pdbrecord[0]:10X} {pdbrecord[1]:10X} {pdbrecord[2]:10X}')

    def record(self, n):
        if n >= self.nrecs:
            raise ValueError(f'non-existent record {n!r}')
        offoff = 78 + (8 * n)
        start, = unpack('>I', self.data[offoff + 0:offoff + 4])
        stop = None
        if n < (self.nrecs - 1):
            stop, = unpack('>I', self.data[offoff + 8:offoff + 12])
        return StreamSlicer(self.stream, start, stop)

    def update(self, mi, asin=None):
        if hasattr(mi, "to_calibre"):
            mi = mi.to_calibre()
        title = _clean_mobi_text(getattr(mi, "title", None)) or _("Unknown")

        def update_exth_record(rec):
            recs.append(rec)
            if rec[0] in self.original_exth_records:
                self.original_exth_records.pop(rec[0])

        if self.type != b'BOOKMOBI':
            raise MobiError("Setting metadata only supported for MOBI files of type 'BOOK'.\n"
                            f"\tThis is a {self.type[0:4]!r} file of type {self.type[4:8]!r}")

        recs = []
        added_501 = False
        try:
            from LiuXin_alpha.file_formats.conversion.config import load_defaults
            prefs = load_defaults('mobi_output')
            pas = prefs.get('prefer_author_sort', False)
            kindle_pdoc = prefs.get('personal_doc', None)
            share_not_sync = prefs.get('share_not_sync', False)
        except Exception:
            pas = False
            kindle_pdoc = None
            share_not_sync = False
        author_sort = _clean_mobi_text(getattr(mi, "author_sort", None))
        authors = _clean_mobi_list(getattr(mi, "authors", None))
        if author_sort and pas:
            # We want an EXTH field per author...
            authors = [author for author in author_sort.split(' & ') if author]
            for author in authors:
                update_exth_record((100, author.encode(self.codec, 'replace')))
        elif authors:
            for author in authors:
                update_exth_record((100, author.encode(self.codec, 'replace')))
        publisher = _clean_mobi_text(getattr(mi, "publisher", None))
        if publisher:
            update_exth_record((101, publisher.encode(self.codec, 'replace')))
        comments = _clean_mobi_text(getattr(mi, "comments", None))
        if comments:
            # Strip user annotations
            a_offset = comments.find('<div class="user_annotations">')
            ad_offset = comments.find('<hr class="annotations_divider" />')
            if a_offset >= 0:
                comments = comments[:a_offset]
            if ad_offset >= 0:
                comments = comments[:ad_offset]
            update_exth_record((103, comments.encode(self.codec, 'replace')))
        isbn = _clean_mobi_text(getattr(mi, "isbn", None))
        if isbn:
            update_exth_record((104, isbn.encode(self.codec, 'replace')))
        tags = _clean_mobi_list(getattr(mi, "tags", None))
        if tags:
            # FIXME: Keep a single subject per EXTH field?
            subjects = '; '.join(tags)
            update_exth_record((105, subjects.encode(self.codec, 'replace')))

            if kindle_pdoc and (kindle_pdoc == '*' or kindle_pdoc in tags):
                added_501 = True
                update_exth_record((501, b'PDOC'))

        pubdate = _clean_mobi_text(getattr(mi, "pubdate", None))
        timestamp = _clean_mobi_text(getattr(mi, "timestamp", None))
        if pubdate:
            update_exth_record((106, pubdate.encode(self.codec, 'replace')))
        elif timestamp:
            update_exth_record((106, timestamp.encode(self.codec, 'replace')))
        elif self.timestamp:
            update_exth_record((106, self.timestamp))
        else:
            update_exth_record((106, nowf().isoformat().encode(self.codec, 'replace')))
        if self.cover_record is not None:
            update_exth_record((201, pack('>I', self.cover_rindex)))
            update_exth_record((203, pack('>I', 0)))
        if self.thumbnail_record is not None:
            update_exth_record((202, pack('>I', self.thumbnail_rindex)))
        # Add a 113 record if not present to allow Amazon syncing
        if (113 not in self.original_exth_records and
                self.original_exth_records.get(501, None) == b'EBOK' and
                not added_501 and not share_not_sync):
            from uuid import uuid4
            update_exth_record((113, str(uuid4()).encode(self.codec)))

        if asin is not None:
            clean_asin = _clean_mobi_text(asin)
            update_exth_record((113, clean_asin.encode(self.codec)))
            update_exth_record((501, b'EBOK'))
            update_exth_record((504, clean_asin.encode(self.codec)))

        # Add a 112 record with actual UUID
        uuid = _clean_mobi_text(getattr(mi, 'uuid', None))
        if uuid:
            update_exth_record((112,
                    (f'calibre:{uuid}').encode(self.codec, 'replace')))
        if 503 in self.original_exth_records:
            update_exth_record((503, title.encode(self.codec, 'replace')))

        # Update book producer
        book_producer = _clean_mobi_text(getattr(mi, 'book_producer', None))
        if book_producer:
            update_exth_record((108, book_producer.encode(self.codec, 'replace')))

        # Set langcode in EXTH header
        if not _metadata_is_null(mi, 'language'):
            lang = canonicalize_lang(_clean_mobi_text(getattr(mi, 'language', None)))
            lang = lang_as_iso639_1(lang) or lang
            if lang:
                update_exth_record((524, lang.encode(self.codec, 'replace')))

        # Include remaining original EXTH fields
        for id in sorted(self.original_exth_records):
            recs.append((id, self.original_exth_records[id]))
        recs = sorted(recs, key=lambda x:(x[0],x[0]))

        exth = io.BytesIO()
        for code, data in recs:
            exth.write(pack('>II', code, len(data) + 8))
            exth.write(data)
        exth = exth.getvalue()
        trail = len(exth) % 4
        pad = b'\0' * (4 - trail)  # Always pad w/ at least 1 byte
        exth = [b'EXTH', pack('>II', len(exth) + 12, len(recs)), exth, pad]
        exth = b''.join(exth)

        if getattr(self, 'exth', None) is None:
            raise MobiError('No existing EXTH record. Cannot update metadata.')

        if not _metadata_is_null(mi, 'language'):
            self.record0[92:96] = iana2mobi(_clean_mobi_text(getattr(mi, 'language', None)))
        self.create_exth(exth=exth, new_title=title)

        # Fetch updated timestamp, cover_record, thumbnail_record
        self.fetchEXTHFields()

        cover_data = getattr(mi, "cover_data", None)
        cover_payload = None
        if isinstance(cover_data, (tuple, list)) and len(cover_data) > 1:
            cover_payload = cover_data[1]
        cover_path = getattr(mi, "cover", None)
        if cover_payload or cover_path:
            try:
                data = cover_payload
                if not data:
                    with open(cover_path, 'rb') as f:
                        data = f.read()
            except Exception:
                pass
            else:
                if is_image(self.cover_record):
                    size = len(self.cover_record)
                    cover = rescale_image(data, size)
                    if len(cover) <= size:
                        cover += b'\0' * (size - len(cover))
                        self.cover_record[:] = cover
                if is_image(self.thumbnail_record):
                    size = len(self.thumbnail_record)
                    thumbnail = rescale_image(data, size, dimen=MAX_THUMB_DIMEN)
                    if len(thumbnail) <= size:
                        thumbnail += b'\0' * (size - len(thumbnail))
                        self.thumbnail_record[:] = thumbnail
                return


def set_metadata(stream, mi):
    mu = MetadataUpdater(stream)
    mu.update(mi)


VALID_FOR = ["MOBI", "PRC", "AZW", "AZW3", "AZW4", "POBI"]
PRIORITY_FOR = ["MOBI", "PRC", "AZW", "AZW3", "AZW4", "POBI"]
RUN_COST = ["LOW"]


def _default_metadata(source_name: str = ""):
    title = _("Unknown")
    if source_name:
        stem = os.path.splitext(os.path.basename(source_name))[0].strip()
        if stem:
            title = stem
    return calibreMetaInformation(title, [_("Unknown")])


def _stream_size(stream, fallback: int = 1024 ** 3) -> int:
    if not (hasattr(stream, "seek") and hasattr(stream, "tell")):
        return fallback
    pos = stream.tell()
    try:
        stream.seek(0, 2)
        return stream.tell()
    finally:
        stream.seek(pos)


def _read_cover_from_header(mh):
    if hasattr(mh.exth, "cover_offset"):
        cover_index = mh.first_image_index + mh.exth.cover_offset
        return mh.section_data(int(cover_index))
    try:
        return mh.section_data(mh.first_image_index)
    except Exception:
        return b""


def read_metadata_from_stream(stream, source_name: str = "", extract_cover: bool = True):
    from LiuXin_alpha.file_formats.mobi.reader.headers import MetadataHeader
    from LiuXin_alpha.file_formats.mobi.reader.mobi6 import MobiReader
    from LiuXin_alpha.utils.ptempfiles import TemporaryDirectory
    from LiuXin_alpha.utils.storage.local import CurrentDir

    log = default_log
    mi = _default_metadata(source_name)

    stream.seek(0)
    try:
        raw = stream.read(3)
    except Exception:
        raw = b""
    stream.seek(0)

    if raw == b"TPZ":
        try:
            from LiuXin_alpha.metadata.file_sources.topaz import get_metadata as topaz_get_metadata
        except Exception as err:
            default_log.log_exception(
                "Topaz metadata reader is unavailable.",
                err,
                "WARNING",
                ("source", source_name or "<stream>"),
            )
            return mi
        try:
            return topaz_get_metadata(stream)
        except Exception as err:
            default_log.log_exception(
                "Failed to read metadata from embedded Topaz stream.",
                err,
                "ERROR",
                ("source", source_name or "<stream>"),
            )
            return mi

    try:
        mh = MetadataHeader(stream, log)
        if getattr(mh, "title", None) and mh.title != _("Unknown"):
            mi.title = mh.title

        if mh.exth is not None and getattr(mh.exth, "mi", None) is not None:
            mi = mh.exth.mi
        else:
            if _stream_size(stream) < 4 * 1024 * 1024:
                with TemporaryDirectory("_mobi_meta_reader") as tdir:
                    with CurrentDir(tdir):
                        mr = MobiReader(stream, log)
                        parse_cache = {}
                        mr.extract_content(tdir, parse_cache)
                        if mr.embedded_mi is not None:
                            mi = mr.embedded_mi

        if not extract_cover:
            return mi

        data = _read_cover_from_header(mh)
        fmt = what(None, data) if data else None
        if fmt in {"jpg", "jpeg", "gif", "png", "bmp", "webp"}:
            mi.cover_data = ("jpg" if fmt == "jpeg" else fmt, data)
    except Exception as err:
        default_log.log_exception(
            "Failed to read MOBI metadata.",
            err,
            "ERROR",
            ("source", source_name or "<stream>"),
        )
    return mi


def get_metadata(target_file, extract_cover: bool = True):
    """
    Read metadata from a MOBI path/pathlike or readable binary stream.
    """
    stream_needs_close = False
    source_name = ""

    if isinstance(target_file, (str, bytes, os.PathLike)):
        source_name = os.fspath(target_file)
        stream_needs_close = True
        stream = open(source_name, "rb")
    elif hasattr(target_file, "read"):
        stream = target_file
        source_name = getattr(stream, "name", "") or ""
    else:
        raise TypeError("target_file must be a filesystem path or a binary stream")

    pos = None
    if hasattr(stream, "tell"):
        try:
            pos = stream.tell()
        except Exception:
            pos = None

    try:
        return read_metadata_from_stream(stream, source_name=source_name, extract_cover=extract_cover)
    finally:
        if stream_needs_close:
            stream.close()
        elif pos is not None and hasattr(stream, "seek"):
            try:
                stream.seek(pos)
            except Exception:
                pass


def get_metadata_inplace(target_file):
    """
    Path-oriented metadata read optimized for in-place plugin calls.
    """
    return get_metadata(target_file, extract_cover=False)


__all__ = [
    "VALID_FOR",
    "PRIORITY_FOR",
    "RUN_COST",
    "StreamSlicer",
    "MetadataUpdater",
    "get_metadata",
    "get_metadata_inplace",
    "read_metadata_from_stream",
    "set_metadata",
]
