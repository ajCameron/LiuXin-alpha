"""
Read metadata from Plucker PDB files.
"""

from __future__ import annotations

import struct
from datetime import datetime, timezone

from LiuXin_alpha.file_formats.pdb.header import PdbHeaderReader
from LiuXin_alpha.file_formats.pdb.plucker.reader import DATATYPE_METADATA, MIBNUM_TO_NAME, SectionHeader
from LiuXin_alpha.metadata.utils import calibreMetaInformation
from LiuXin_alpha.metadata.utils import string_to_authors
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log

__license__ = "GPL v3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


def _decode_text(raw: bytes | None, encoding: str) -> str:
    if not raw:
        return ""
    try:
        decoded = raw.decode(encoding, "replace")
    except Exception:
        decoded = raw.decode("latin-1", "replace")
    cleaned = decoded.replace("\x00", "")
    return " ".join(cleaned.split()).strip()


def _iter_records(section_data: bytes):
    if len(section_data) < 2:
        return

    (record_count,) = struct.unpack(">H", section_data[0:2])
    adv = 0
    for _ in range(record_count):
        start = 2 + adv
        if start + 4 > len(section_data):
            break

        (rtype,) = struct.unpack(">H", section_data[start : start + 2])
        (length_words,) = struct.unpack(">H", section_data[start + 2 : start + 4])
        record_size = 2 * length_words
        if record_size < 4:
            break

        end = start + record_size
        if end > len(section_data):
            break

        payload = section_data[start + 4 : end]
        yield rtype, payload
        adv += record_size


def get_metadata(stream, extract_cover: bool = True):
    """
    Return metadata from a Plucker stream.
    """
    del extract_cover  # Plucker metadata reader does not expose cover bytes.
    mi = calibreMetaInformation(_("Unknown"), [_("Unknown")])
    start_pos = None
    try:
        if hasattr(stream, "tell"):
            try:
                start_pos = stream.tell()
            except Exception:
                start_pos = None

        stream.seek(0)
        pheader = PdbHeaderReader(stream)
        if getattr(pheader, "title", ""):
            mi.title = pheader.title

        section_data = None
        for i in range(1, pheader.num_sections):
            try:
                raw_data = pheader.section_data(i)
            except Exception:
                continue
            if len(raw_data) < 8:
                continue
            try:
                section_header = SectionHeader(raw_data)
            except Exception:
                continue
            if section_header.type == DATATYPE_METADATA:
                # Only parse the declared metadata payload for this section.
                section_data = raw_data[8 : 8 + max(section_header.size, 0)]
                break

        if not section_data:
            return mi

        default_encoding = "latin-1"
        title = ""
        author = ""
        pubdate = None

        for rtype, payload in _iter_records(section_data):
            if rtype == 1 and len(payload) >= 2:
                (mibnum,) = struct.unpack(">H", payload[0:2])
                default_encoding = MIBNUM_TO_NAME.get(mibnum, "latin-1")
            elif rtype == 4:
                author = _decode_text(payload, default_encoding)
            elif rtype == 5:
                title = _decode_text(payload, default_encoding)
            elif rtype == 6 and len(payload) >= 4:
                (pubdate_raw,) = struct.unpack(">I", payload[0:4])
                try:
                    if pubdate_raw > 0:
                        pubdate = datetime.fromtimestamp(pubdate_raw, tz=timezone.utc)
                except Exception as err:
                    default_log.log_exception("Invalid Plucker publication timestamp encountered.", err, "DEBUG")

        if title:
            mi.title = title
        if author:
            authors = [x.strip() for x in string_to_authors(author) if x and x.strip()]
            if len(authors) <= 1 and "," in author:
                authors = [x.strip() for x in author.split(",") if x.strip()]
            if authors:
                mi.authors = authors
        if pubdate is not None:
            mi.pubdate = pubdate

        return mi
    finally:
        if start_pos is not None and hasattr(stream, "seek"):
            try:
                stream.seek(start_pos)
            except Exception:
                pass
