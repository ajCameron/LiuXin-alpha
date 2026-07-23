#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

from __future__ import absolute_import, print_function
from __future__ import annotations

import typing as _typing

import os
import re
import struct

from LiuXin_alpha.file_formats.mobi import MobiError
from LiuXin_alpha.file_formats.mobi.langcodes import main_language, sub_language, mobi2iana

from LiuXin_alpha.metadata.utils import calibreMetaInformation
from LiuXin_alpha.metadata.ebook_metadata_tools import check_isbn

from LiuXin_alpha.utils.text.xml_utils import replace_entities
from LiuXin_alpha.utils.date import parse_date
from LiuXin_alpha.utils.config.config_base import tweaks
from LiuXin_alpha.utils.libraries.cleantext import clean_ascii_chars, clean_xml_chars
from LiuXin_alpha.utils.libraries.iso639.iso639_tools import canonicalize_lang

from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL v3"
__copyright__ = "2012, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"

NULL_INDEX = 0xFFFFFFFF
PALMDB_HEADER_SIZE = 78
PALMDB_RECORD_TABLE_ENTRY_SIZE = 8
MAX_PALMDB_RECORDS = 4096
MAX_MOBI_HEADER_LENGTH = 500


def _require_bytes(raw: _typing.Any, length: _typing.Any, context: _typing.Any) -> None:
    if len(raw) < length:
        raise MobiError("Truncated MOBI data while reading %s" % context)


def _unpack(fmt: _typing.Any, raw: _typing.Any, offset: _typing.Any, context: _typing.Any) -> _typing.Any:
    length = struct.calcsize(fmt)
    _require_bytes(raw, offset + length, context)
    return struct.unpack_from(fmt, raw, offset)


def _read_exact(stream: _typing.Any, length: _typing.Any, context: _typing.Any) -> _typing.Any:
    raw = stream.read(length)
    if len(raw) != length:
        raise MobiError("Truncated MOBI data while reading %s" % context)
    return raw


def _stream_length(stream: _typing.Any) -> _typing.Any:
    if not (hasattr(stream, "seek") and hasattr(stream, "tell")):
        return None
    pos = stream.tell()
    try:
        stream.seek(0, os.SEEK_END)
        return stream.tell()
    finally:
        stream.seek(pos)


def _validate_record_count(count: _typing.Any) -> None:
    if count > MAX_PALMDB_RECORDS:
        raise MobiError("PalmDB record count %d exceeds limit %d" % (count, MAX_PALMDB_RECORDS))


def _validate_record_offsets(offsets: _typing.Any, *, data_size: _typing.Any, table_end: _typing.Any) -> None:
    previous = None
    for index, offset in enumerate(offsets):
        if offset < table_end:
            raise MobiError("PalmDB record %d starts inside the record table" % index)
        if data_size is not None and offset > data_size:
            raise MobiError("PalmDB record %d starts beyond end of file" % index)
        if previous is not None and offset <= previous:
            raise MobiError("PalmDB record offsets are not strictly increasing")
        previous = offset


def read_palmdb_record_table(raw: _typing.Any) -> tuple[_typing.Any, ...]:
    _require_bytes(raw, PALMDB_HEADER_SIZE, "PalmDB header")
    count = _unpack(">H", raw, 76, "PalmDB record count")[0]
    if count < 1:
        raise MobiError("PalmDB file has no records")
    _validate_record_count(count)

    table_end = PALMDB_HEADER_SIZE + (count * PALMDB_RECORD_TABLE_ENTRY_SIZE) + 2
    _require_bytes(raw, table_end, "PalmDB record table")

    records = []
    offsets = []
    for index in range(count):
        entry_offset = PALMDB_HEADER_SIZE + (index * PALMDB_RECORD_TABLE_ENTRY_SIZE)
        offset, a1, a2, a3, a4 = _unpack(">LBBBB", raw, entry_offset, "PalmDB record table entry")
        flags, val = a1, a2 << 16 | a3 << 8 | a4
        records.append((offset, flags, val))
        offsets.append(offset)
    _validate_record_offsets(offsets, data_size=len(raw), table_end=table_end)
    return count, records


class EXTHHeader(object):  # {{{
    def __init__(self: _typing.Self, raw: _typing.Any, codec: _typing.Any, title: _typing.Any) -> None:
        _require_bytes(raw, 12, "EXTH header")
        self.doctype = raw[:4]
        if self.doctype != b"EXTH":
            raise MobiError("Invalid EXTH header signature")
        self.length, self.num_items = _unpack(">LL", raw, 4, "EXTH length and item count")
        if self.length < 12:
            raise MobiError("Invalid EXTH header length")
        if self.length > len(raw):
            raise MobiError("EXTH header length exceeds available data")
        raw = raw[12:self.length]
        pos = 0
        self.mi = calibreMetaInformation(_("Unknown"), [_("Unknown")])
        self.has_fake_cover = True
        self.start_offset = None
        left = self.num_items
        self.kf8_header = None
        self.uuid = self.cdetype = None
        self.page_progression_direction = None

        self.decode = lambda x: clean_ascii_chars(x.decode(codec, "replace"))

        while left > 0:
            left -= 1
            if pos + 8 > len(raw):
                raise MobiError("Truncated EXTH item header")
            idx, size = struct.unpack(">LL", raw[pos : pos + 8])
            if size < 8:
                raise MobiError("Invalid EXTH item size")
            if pos + size > len(raw):
                raise MobiError("EXTH item extends beyond header block")
            content = raw[pos + 8 : pos + size]
            pos += size
            if 200 > idx >= 100:
                self.process_metadata(idx, content, codec)
            elif idx == 203:
                _require_bytes(content, 4, "EXTH fake-cover flag")
                self.has_fake_cover = bool(struct.unpack(">L", content)[0])
            elif idx == 201:
                _require_bytes(content, 4, "EXTH cover offset")
                (co,) = struct.unpack(">L", content)
                if co < NULL_INDEX:
                    self.cover_offset = co
            elif idx == 202:
                _require_bytes(content, 4, "EXTH thumbnail offset")
                (self.thumbnail_offset,) = struct.unpack(">L", content)
            elif idx == 501:
                try:
                    self.cdetype = content.decode("ascii")
                except UnicodeDecodeError:
                    self.cdetype = None
                # cdetype
                if content == b"EBSP":
                    if not self.mi.tags:
                        self.mi.tags = []
                    self.mi.tags.append(_("Sample Book"))
            elif idx == 502:
                # last update time
                pass
            elif idx == 503:  # Long title
                # Amazon seems to regard this as the definitive book title
                # rather than the title from the PDB header. In fact when
                # sending MOBI files through Amazon's email service if the
                # title contains non ASCII chars or non filename safe chars
                # they are messed up in the PDB header
                try:
                    title = self.decode(content)
                except:
                    pass
            elif idx == 524:  # Lang code
                try:
                    lang = content.decode(codec)
                    lang = canonicalize_lang(lang)
                    if lang:
                        self.mi.language = lang
                except:
                    pass
            elif idx == 527:
                try:
                    ppd = content.decode(codec)
                    if ppd:
                        self.page_progression_direction = ppd
                except Exception:
                    pass
            # else:
            #    print 'unknown record', idx, repr(content)
        if title:
            self.mi.title = replace_entities(clean_xml_chars(clean_ascii_chars(title)))

    def process_metadata(self: _typing.Self, idx: _typing.Any, content: _typing.Any, codec: _typing.Any) -> None:
        if idx == 100:
            if self.mi.is_null("authors"):
                self.mi.authors = []
            au = clean_xml_chars(self.decode(content).strip())
            # Author names in Amazon  MOBI files are usually in LN, FN format,
            # try to detect and auto-correct that.
            m = re.match(r"([^,]+?)\s*,\s+([^,]+)$", au.strip())
            if m is not None:
                if tweaks["author_sort_copy_method"] != "copy":
                    self.mi.authors.append(m.group(2) + " " + m.group(1))
                else:
                    self.mi.authors.append(m.group())
                if self.mi.is_null("author_sort"):
                    self.mi.author_sort = m.group()
            else:
                self.mi.authors.append(au)
        elif idx == 101:
            self.mi.publisher = clean_xml_chars(self.decode(content).strip())
            if self.mi.publisher in {"Unknown", _("Unknown")}:
                self.mi.publisher = None
        elif idx == 103:
            self.mi.comments = clean_xml_chars(self.decode(content).strip())
        elif idx == 104:
            raw = check_isbn(self.decode(content).strip().replace("-", ""))
            if raw:
                self.mi.isbn = raw
        elif idx == 105:
            if not self.mi.tags:
                self.mi.tags = []
            self.mi.tags.extend([x.strip() for x in clean_xml_chars(self.decode(content)).split(";")])
            self.mi.tags = list(set(self.mi.tags))
        elif idx == 106:
            try:
                self.mi.pubdate = parse_date(content, as_utc=False)
            except:
                pass
        elif idx == 108:
            self.mi.book_producer = clean_xml_chars(self.decode(content).strip())
        elif idx == 109:
            self.mi.rights = clean_xml_chars(self.decode(content).strip())
        elif idx == 112:  # dc:source set in some EBSP amazon samples
            try:
                content = content.decode(codec).strip()
                isig = "urn:isbn:"
                if content.lower().startswith(isig):
                    raw = check_isbn(content[len(isig) :])
                    if raw and not self.mi.isbn:
                        self.mi.isbn = raw
                elif content.startswith("calibre:"):
                    # calibre book uuid is stored here by recent calibre
                    # releases
                    cid = content[len("calibre:") :]
                    if cid:
                        self.mi.application_id = self.mi.uuid = cid
            except:
                pass
        elif idx == 113:  # ASIN or other id
            try:
                self.uuid = content.decode("ascii")
                self.mi.set_identifier("mobi-asin", self.uuid)
            except:
                self.uuid = None
        elif idx == 116:
            _require_bytes(content, 4, "EXTH start offset")
            (self.start_offset,) = struct.unpack(b">L", content)
        elif idx == 121:
            _require_bytes(content, 4, "EXTH KF8 header offset")
            (self.kf8_header,) = struct.unpack(b">L", content)
            if self.kf8_header == NULL_INDEX:
                self.kf8_header = None
        # else:
        #    print 'unhandled metadata record', idx, repr(content)


# }}}


class BookHeader(object):
    def __init__(self: _typing.Self, raw: _typing.Any, ident: _typing.Any, user_encoding: _typing.Any, log: _typing.Any, try_extra_data_fix: bool = False) -> None:
        self.log = log
        ident_text = ident.decode("ascii", "ignore") if isinstance(ident, (bytes, bytearray)) else str(ident)
        self.compression_type = raw[:2]
        _require_bytes(raw, 14, "MOBI record 0 header")
        self.records, self.records_size = _unpack(">HH", raw, 8, "MOBI text record metadata")
        (self.encryption_type,) = _unpack(">H", raw, 12, "MOBI encryption type")
        if ident_text == "TEXTREAD":
            self.codepage = 1252
        if len(raw) <= 16:
            self.codec = "cp1252"
            self.extra_flags = 0
            self.title = _("Unknown")
            self.language = "ENGLISH"
            self.sublanguage = "NEUTRAL"
            self.exth_flag, self.exth = 0, None
            self.ancient = True
            self.first_image_index = -1
            self.mobi_version = 1
        else:
            _require_bytes(raw, 0x84, "MOBI header")
            self.ancient = False
            self.doctype = raw[16:20]
            if ident_text != "TEXTREAD" and self.doctype != b"MOBI":
                raise MobiError("Invalid MOBI record 0 signature")
            (
                self.length,
                self.type,
                self.codepage,
                self.unique_id,
                self.version,
            ) = _unpack(">LLLLL", raw, 20, "MOBI header fields")
            if self.length > MAX_MOBI_HEADER_LENGTH:
                raise MobiError("MOBI header length %d exceeds limit %d" % (self.length, MAX_MOBI_HEADER_LENGTH))
            if 16 + self.length > len(raw):
                raise MobiError("MOBI header length exceeds record 0")

            try:
                self.codec = {
                    1252: "cp1252",
                    65001: "utf-8",
                }[self.codepage]
            except (IndexError, KeyError):
                self.codec = "cp1252" if not user_encoding else user_encoding
                log.warn("Unknown codepage %d. Assuming %s" % (self.codepage, self.codec))
            # Some KF8 files have header length == 264 (generated by kindlegen
            # 2.9?). See https://bugs.launchpad.net/bugs/1179144
            max_header_length = 500  # We choose 500 for future versions of kindlegen

            if (
                ident_text == "TEXTREAD"
                or self.length < 0xE4
                or self.length > max_header_length
                or (try_extra_data_fix and self.length == 0xE4)
            ):
                self.extra_flags = 0
            else:
                (self.extra_flags,) = _unpack(">H", raw, 0xF2, "MOBI extra data flags")

            if self.compression_type == b"DH":
                self.huff_offset, self.huff_number = _unpack(">LL", raw, 0x70, "HUFF/CDIC offsets")

            toff, tlen = _unpack(">II", raw, 0x54, "MOBI title offset and length")
            tend = toff + tlen
            if tlen and (toff >= len(raw) or tend > len(raw)):
                raise MobiError("MOBI title extends beyond record 0")
            self.title = raw[toff:tend] if tend < len(raw) else _("Unknown")
            langcode = _unpack("!L", raw, 0x5C, "MOBI language code")[0]
            langid = langcode & 0xFF
            sublangid = (langcode >> 10) & 0xFF
            self.language = main_language.get(langid, "ENGLISH")
            self.sublanguage = sub_language.get(sublangid, "NEUTRAL")
            self.mobi_version = _unpack(">I", raw, 0x68, "MOBI version")[0]
            self.first_image_index = _unpack(">L", raw, 0x6C, "MOBI first image index")[0]

            (self.exth_flag,) = _unpack(">L", raw, 0x80, "MOBI EXTH flag")
            self.exth = None

            if not isinstance(self.title, str):
                self.title = self.title.decode(self.codec, "replace")

            if self.exth_flag & 0x40:
                try:
                    self.exth = EXTHHeader(raw[16 + self.length :], self.codec, self.title)
                    self.exth.mi.uid = self.unique_id
                    if self.exth.mi.is_null("language"):
                        try:
                            self.exth.mi.language = mobi2iana(langid, sublangid)
                        except:
                            self.log.exception("Unknown language code")
                except MobiError:
                    raise
                except:
                    self.log.exception("Invalid EXTH header")
                    self.exth_flag = 0

            self.ncxidx = NULL_INDEX
            if len(raw) >= 0xF8:
                (self.ncxidx,) = _unpack(b">L", raw, 0xF4, "MOBI NCX index")

            # Ancient PRC files from Baen can have random values for
            # mobi_version, so be conservative
            if self.mobi_version == 8 and len(raw) >= (0xF8 + 16):
                (
                    self.dividx,
                    self.skelidx,
                    self.datpidx,
                    self.othidx,
                ) = _unpack(b">4L", raw, 0xF8, "KF8 index pointers")

                # need to use the FDST record to find out how to properly
                # unpack the raw_ml into pieces it is simply a table of start
                # and end locations for each flow piece
                self.fdstidx, self.fdstcnt = _unpack(b">2L", raw, 0xC0, "KF8 FDST metadata")
                # if cnt is 1 or less, fdst section number can be garbage
                if self.fdstcnt <= 1:
                    self.fdstidx = NULL_INDEX
            else:  # Null values
                self.skelidx = self.dividx = self.othidx = self.fdstidx = NULL_INDEX


class MetadataHeader(BookHeader):
    def __init__(self: _typing.Self, stream: _typing.Any, log: _typing.Any) -> None:
        self.stream = stream
        self._stream_size = _stream_length(stream)
        self.ident = self.identity()
        self.num_sections = self.section_count()
        _validate_record_count(self.num_sections)
        self._record_offsets = self._read_record_offsets() if self.num_sections else []
        if self.num_sections >= 2:
            header = self.header()
            BookHeader.__init__(self, header, self.ident, None, log)
        else:
            self.exth = None

    @property
    def kf8_type(self: _typing.Self) -> str | None:
        if self.mobi_version == 8 and getattr(self, "skelidx", NULL_INDEX) != NULL_INDEX:
            return "standalone"

        kf8_header_index = getattr(self.exth, "kf8_header", None)
        if kf8_header_index is None:
            return None
        try:
            if self.section_data(kf8_header_index - 1) == b"BOUNDARY":
                return "joint"
        except:
            pass
        return None

    def identity(self: _typing.Self) -> _typing.Any:
        self.stream.seek(60)
        ident = _read_exact(self.stream, 8, "PalmDB identity").upper()
        if ident not in (b"BOOKMOBI", b"TEXTREAD"):
            raise MobiError("Unknown book type: %s" % ident)
        return ident

    def section_count(self: _typing.Self) -> _typing.Any:
        self.stream.seek(76)
        return struct.unpack(">H", _read_exact(self.stream, 2, "PalmDB record count"))[0]

    def _read_record_offsets(self: _typing.Self) -> _typing.Any:
        table_end = PALMDB_HEADER_SIZE + (self.num_sections * PALMDB_RECORD_TABLE_ENTRY_SIZE) + 2
        if self._stream_size is not None and table_end > self._stream_size:
            raise MobiError("Truncated MOBI data while reading PalmDB record table")

        offsets = []
        for number in range(self.num_sections):
            self.stream.seek(PALMDB_HEADER_SIZE + number * PALMDB_RECORD_TABLE_ENTRY_SIZE)
            data = _read_exact(self.stream, PALMDB_RECORD_TABLE_ENTRY_SIZE, "PalmDB record table entry")
            offsets.append(struct.unpack(">LBBBB", data)[0])
        _validate_record_offsets(offsets, data_size=self._stream_size, table_end=table_end)
        return offsets

    def section_offset(self: _typing.Self, number: _typing.Any) -> _typing.Any:
        if number < 0 or number >= self.num_sections:
            raise MobiError("non-existent MOBI section %r" % number)
        if hasattr(self, "_record_offsets"):
            return self._record_offsets[number]
        self.stream.seek(78 + number * 8)
        return struct.unpack(">LBBBB", _read_exact(self.stream, 8, "PalmDB record table entry"))[0]

    def header(self: _typing.Self) -> _typing.Any:
        section_headers = list()
        # First section with the metadata
        section_headers.append(self.section_offset(0))
        # Second section used to get the length of the first
        section_headers.append(self.section_offset(1))

        end_off = section_headers[1]
        off = section_headers[0]
        if end_off <= off:
            raise MobiError("Invalid MOBI record 0 bounds")
        self.stream.seek(off)
        return _read_exact(self.stream, end_off - off, "MOBI record 0")

    def section_data(self: _typing.Self, number: _typing.Any) -> _typing.Any:
        if number < 0 or number >= self.num_sections:
            raise MobiError("non-existent MOBI section %r" % number)
        start = self.section_offset(number)
        if number == self.num_sections - 1:
            stream_size = getattr(self, "_stream_size", None)
            if stream_size is not None:
                end = stream_size
            elif hasattr(self.stream, "name") and self.stream.name:
                end = os.stat(self.stream.name).st_size
            else:
                pos = self.stream.tell()
                try:
                    self.stream.seek(0, os.SEEK_END)
                    end = self.stream.tell()
                finally:
                    self.stream.seek(pos)
        else:
            end = self.section_offset(number + 1)
        if end < start:
            raise MobiError("Invalid MOBI section bounds")
        self.stream.seek(start)
        try:
            return self.stream.read(end - start)
        except OverflowError:
            self.stream.seek(start)
            return self.stream.read()
