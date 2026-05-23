#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

from __future__ import unicode_literals, division, absolute_import, print_function

import logging
import struct
from collections import OrderedDict, namedtuple

from LiuXin_alpha.file_formats.mobi.utils import decint, count_set_bits, decode_string

# Py2/Py3 compatibility layer
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems
from LiuXin_alpha.utils.libraries.liuxin_six import memory_range

__license__ = "GPL v3"
__copyright__ = "2012, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"

logger = logging.getLogger(__name__)

TagX = namedtuple("TagX", "tag num_of_values bitmask eof")
PTagX = namedtuple("PTagX", "tag value_count value_bytes num_of_values")
INDEX_HEADER_FIELDS = (
    (
        "len",
        "nul1",
        "type",
        "gen",
        "start",
        "count",
        "code",
        "lng",
        "total",
        "ordt",
        "ligt",
        "nligt",
        "ncncx",
    )
    + tuple("unknown%d" % i for i in memory_range(27))
    + ("ocnt", "oentries", "ordt1", "ordt2", "tagx")
)


class InvalidFile(ValueError):
    pass


def _require_bytes(data, length, context):
    if len(data) < length:
        raise InvalidFile("Truncated %s" % context)


def _decode_index_int(data, context):
    value, consumed = decint(data)
    if consumed <= 0:
        raise InvalidFile("Malformed variable-width integer in %s" % context)
    return value, consumed


def _section_data(sections, index, context):
    if index < 0 or index >= len(sections):
        raise InvalidFile("%s index outside section table: %d" % (context, index))
    try:
        return sections[index][0]
    except (IndexError, TypeError):
        raise InvalidFile("Malformed %s section entry" % context)


def check_signature(data, signature):
    if data[: len(signature)] != signature:
        raise InvalidFile("Not a valid %r section" % signature)


class NotAnINDXRecord(InvalidFile):
    pass


class NotATAGXSection(InvalidFile):
    pass


def format_bytes(byts):
    byts = bytearray(byts)
    byts = [hex(b)[2:] for b in byts]
    return " ".join(byts)


def parse_indx_header(data):
    check_signature(data, b"INDX")
    words = INDEX_HEADER_FIELDS
    num = len(words)
    _require_bytes(data, 4 * (num + 1), "INDX header")
    values = struct.unpack(">%dL" % num, data[4 : 4 * (num + 1)])
    ans = dict(zip(words, values))
    ordt1, ordt2 = ans["ordt1"], ans["ordt2"]
    ans["ordt1_raw"], ans["ordt2_raw"] = [], []
    ans["ordt_map"] = ""

    if ordt1 > 0 and data[ordt1 : ordt1 + 4] == b"ORDT":
        # I dont know what this is, but using it seems to be unnecessary, so
        # just leave it as the raw bytestring
        ans["ordt1_raw"] = data[ordt1 + 4 : ordt1 + 4 + ans["oentries"]]
    if ordt2 > 0 and data[ordt2 : ordt2 + 4] == b"ORDT":
        ans["ordt2_raw"] = raw = bytearray(data[ordt2 + 4 : ordt2 + 4 + 2 * ans["oentries"]])
        if ans["code"] == 65002:
            # This appears to be EBCDIC-UTF (65002) encoded. I can't be
            # bothered to write a decoder for this (see
            # http://www.unicode.org/reports/tr16/) Just how stupid is Amazon?
            # Instead, we use a weird hack that seems to do the trick for all
            # the books with this type of ORDT record that I have come across.
            # Some EBSP book samples in KF8 format from Amazon have this type
            # of encoding.
            # Basically we try to interpret every second byte as a printable
            # ascii character. If we cannot, we map to the ? char.

            parsed = bytearray(ans["oentries"])
            for i in memory_range(0, 2 * ans["oentries"], 2):
                parsed[i // 2] = raw[i + 1] if 0x20 < raw[i + 1] < 0x7F else 0x3F
            ans["ordt_map"] = bytes(parsed).decode("ascii")
        else:
            ans["ordt_map"] = "?" * ans["oentries"]

    return ans


class CNCX(object):  # {{{

    """
    Parses the records that contain the compiled NCX (all strings from the
    NCX). Presents a simple offset : string mapping interface to access the
    data.
    """

    def __init__(self, records, codec):
        self.records = OrderedDict()
        record_offset = 0
        for raw in records:
            pos = 0
            while pos < len(raw):
                length, consumed = decint(raw[pos:])
                if length > 0:
                    try:
                        self.records[pos + record_offset] = raw[pos + consumed : pos + consumed + length].decode(codec)
                    except:
                        byts = raw[pos:]
                        r = format_bytes(byts)
                        logger.warning("CNCX entry at offset %d has unknown format %s", pos + record_offset, r)
                        self.records[pos + record_offset] = r
                        pos = len(raw)
                pos += consumed + length
            record_offset += 0x10000

    def __getitem__(self, offset):
        return self.records.get(offset)

    def get(self, offset, default=None):
        return self.records.get(offset, default)

    def __bool__(self):
        return bool(self.records)

    __nonzero__ = __bool__

    def iteritems(self):
        return iteritems(self.records)


# }}}


def parse_tagx_section(data):
    check_signature(data, b"TAGX")
    _require_bytes(data, 12, "TAGX header")

    tags = []
    (first_entry_offset,) = struct.unpack_from(b">L", data, 4)
    (control_byte_count,) = struct.unpack_from(b">L", data, 8)
    if first_entry_offset < 12 or first_entry_offset > len(data):
        raise InvalidFile("TAGX first entry offset outside section")
    if (first_entry_offset - 12) % 4:
        raise InvalidFile("TAGX entries are not 4-byte aligned")

    for i in memory_range(12, first_entry_offset, 4):
        vals = list(bytearray(data[i : i + 4]))
        tags.append(TagX(*vals))
    return control_byte_count, tags


def get_tag_map(control_byte_count, tagx, data, strict=False):
    ptags = []
    ans = {}
    if control_byte_count > len(data):
        raise InvalidFile("TAGX control bytes exceed index entry")
    control_bytes = list(bytearray(data[:control_byte_count]))
    data = data[control_byte_count:]

    for x in tagx:
        if x.eof == 0x01:
            if not control_bytes:
                raise InvalidFile("TAGX control byte underflow")
            control_bytes = control_bytes[1:]
            continue
        if not control_bytes:
            raise InvalidFile("TAGX control byte underflow")
        value = control_bytes[0] & x.bitmask
        if value != 0:
            value_count = value_bytes = None
            if value == x.bitmask:
                if count_set_bits(x.bitmask) > 1:
                    # If all bits of masked value are set and the mask has more
                    # than one bit, a variable width value will follow after
                    # the control bytes which defines the length of bytes (NOT
                    # the value count!) which will contain the corresponding
                    # variable width values.
                    value_bytes, consumed = _decode_index_int(data, "TAGX variable-width length")
                    data = data[consumed:]
                    if value_bytes > len(data):
                        raise InvalidFile("TAGX variable-width value exceeds index entry")
                else:
                    value_count = 1
            else:
                # Shift bits to get the masked value.
                mask = x.bitmask
                while mask & 0b1 == 0:
                    mask >>= 1
                    value >>= 1
                value_count = value
            ptags.append(PTagX(x.tag, value_count, value_bytes, x.num_of_values))

    for x in ptags:
        values = []
        if x.value_count is not None:
            # Read value_count * values_per_entry variable width values.
            for _ in memory_range(x.value_count * x.num_of_values):
                byts, consumed = _decode_index_int(data, "TAGX value")
                data = data[consumed:]
                values.append(byts)
        else:  # value_bytes is not None
            # Convert value_bytes to variable width values.
            total_consumed = 0
            while total_consumed < x.value_bytes:
                # Does this work for values_per_entry != 1?
                byts, consumed = _decode_index_int(data, "TAGX variable-width value")
                data = data[consumed:]
                total_consumed += consumed
                values.append(byts)
            if total_consumed != x.value_bytes:
                err = "Error: Should consume %s bytes, but consumed %s" % (
                    x.value_bytes,
                    total_consumed,
                )
                if strict:
                    raise InvalidFile(err)
                else:
                    logger.warning("%s", err)
        ans[x.tag] = values
    # Test that all bytes have been processed
    if data.replace(b"\0", b""):
        err = "Warning: There are unprocessed index bytes left: %s" % format_bytes(data)
        if strict:
            raise InvalidFile(err)
        else:
            logger.warning("%s", err)

    return ans


def parse_index_record(table, data, control_byte_count, tags, codec, ordt_map, strict=False):
    header = parse_indx_header(data)
    idxt_pos = header["start"]
    _require_bytes(data, idxt_pos + 4, "INDX IDXT table")
    if data[idxt_pos : idxt_pos + 4] != b"IDXT":
        logger.warning("Invalid INDX record")
    entry_count = header["count"]
    _require_bytes(data, idxt_pos + 4 + (2 * entry_count), "INDX IDXT offsets")

    # loop through to build up the IDXT position starts
    idx_positions = []
    for j in memory_range(entry_count):
        (pos,) = struct.unpack_from(b">H", data, idxt_pos + 4 + (2 * j))
        if pos > idxt_pos:
            raise InvalidFile("INDX entry offset outside IDXT table")
        if idx_positions and pos < idx_positions[-1]:
            raise InvalidFile("INDX entry offsets are not ordered")
        idx_positions.append(pos)
    # The last entry ends before the IDXT tag (but there might be zero fill
    # bytes we need to ignore!)
    idx_positions.append(idxt_pos)

    # For each entry in the IDXT build up the tag map and any associated
    # text
    for j in memory_range(entry_count):
        start, end = idx_positions[j : j + 2]
        rec = data[start:end]
        # Sometimes (in the guide table if the type attribute has non ascii
        # values) the ident is UTF-16 encoded. Try to handle that.
        try:
            ident, consumed = decode_string(rec, codec=codec, ordt_map=ordt_map)
        except UnicodeDecodeError:
            ident, consumed = decode_string(rec, codec="utf-16", ordt_map=ordt_map)
        if "\x00" in ident:
            try:
                ident, consumed = decode_string(rec, codec="utf-16", ordt_map=ordt_map)
            except UnicodeDecodeError:
                ident = ident.replace("u\x00", "")
        rec = rec[consumed:]
        tag_map = get_tag_map(control_byte_count, tags, rec, strict=strict)
        table[ident] = tag_map
    return header


def read_index(sections, idx, codec):
    table, cncx = OrderedDict(), CNCX([], codec)

    data = _section_data(sections, idx, "INDX")

    indx_header = parse_indx_header(data)
    indx_count = indx_header["count"]
    if indx_count and idx + indx_count >= len(sections):
        raise InvalidFile("INDX records are outside section table")

    if indx_header["ncncx"] > 0:
        off = idx + indx_count + 1
        if off + indx_header["ncncx"] > len(sections):
            raise InvalidFile("CNCX records are outside section table")
        cncx_records = [x[0] for x in sections[off : off + indx_header["ncncx"]]]
        cncx = CNCX(cncx_records, codec)

    tag_section_start = indx_header["tagx"]
    if tag_section_start >= len(data):
        raise InvalidFile("TAGX section is outside INDX record")
    control_byte_count, tags = parse_tagx_section(data[tag_section_start:])

    for i in memory_range(idx + 1, idx + 1 + indx_count):
        # Index record
        data = _section_data(sections, i, "INDX")
        parse_index_record(table, data, control_byte_count, tags, codec, indx_header["ordt_map"])
    return table, cncx
