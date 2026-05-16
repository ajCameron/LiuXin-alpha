"""
Read and write metadata from eReader PDB files.
"""

from __future__ import annotations

import struct
from collections.abc import Iterable

from LiuXin_alpha.file_formats.pdb.ereader.reader132 import HeaderRecord
from LiuXin_alpha.file_formats.pdb.header import PdbHeaderBuilder, PdbHeaderReader
from LiuXin_alpha.metadata.ebook_metadata_tools import authors_to_string
from LiuXin_alpha.metadata.utils import calibreMetaInformation
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log

__license__ = "GPL v3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


def _clean_text(value: str | None) -> str:
    if not value:
        return ""
    return value.replace("\x00", "").strip()


def _decode_field(raw: bytes) -> str:
    if not raw:
        return ""
    try:
        return _clean_text(raw.decode("utf-8"))
    except Exception:
        return _clean_text(raw.decode("cp1252", "replace"))


def _safe_section(pheader: PdbHeaderReader, index: int) -> bytes:
    try:
        return pheader.section_data(index)
    except Exception:
        return b""


def _normalize_authors(value) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, dict):
        return [str(author) for author in value.keys() if author]
    if isinstance(value, Iterable):
        return [str(author) for author in value if author]
    return [str(value)]


def get_cover(pheader: PdbHeaderReader, eheader: HeaderRecord):
    for i in range(eheader.image_count):
        raw = _safe_section(pheader, eheader.image_data_offset + i)
        if len(raw) < 63:
            continue
        image_name = raw[4:36].split(b"\x00", 1)[0].decode("ascii", "ignore").lower()
        if image_name == "cover.png":
            return "png", raw[62:]
    return None


def get_metadata(stream, extract_cover: bool = True):
    """
    Return metadata from an eReader PDB stream.
    """
    mi = calibreMetaInformation(None, [_("Unknown")])
    stream.seek(0)
    pheader = PdbHeaderReader(stream)

    section0 = _safe_section(pheader, 0)
    if len(section0) == 132:
        hr = HeaderRecord(section0)
        if hr.compression in (2, 10) and hr.has_metadata == 1:
            try:
                raw_metadata = _safe_section(pheader, hr.metadata_offset)
                parts = raw_metadata.split(b"\x00")
                if len(parts) > 0 and parts[0]:
                    mi.title = _decode_field(parts[0])
                if len(parts) > 1 and parts[1]:
                    author = _decode_field(parts[1])
                    mi.authors = [author] if author else [_("Unknown")]
                if len(parts) > 3 and parts[3]:
                    mi.publisher = _decode_field(parts[3])
                if len(parts) > 4 and parts[4]:
                    mi.isbn = _decode_field(parts[4])
            except Exception as err:
                default_log.log_exception("Unable to read metadata in eReader PDB.", err, "WARNING")

            if extract_cover:
                cover_data = get_cover(pheader, hr)
                if cover_data is not None:
                    mi.cover_data = cover_data

    # calibre-style metadata objects default title to _("Unknown"), so we need to
    # treat that value as "unset" for fallback purposes as well.
    current_title = getattr(mi, "title", None)
    if not current_title or current_title == _("Unknown"):
        mi.title = pheader.title if pheader.title else _("Unknown")

    return mi


def _metadata_record_bytes(mi) -> bytes:
    title = _clean_text(getattr(mi, "title", "")) or _("Unknown")
    authors = _normalize_authors(getattr(mi, "authors", None))
    author = authors_to_string(authors)
    publisher = _clean_text(getattr(mi, "publisher", ""))
    isbn = _clean_text(getattr(mi, "isbn", ""))

    payload = f"{title}\x00{author}\x00\x00{publisher}\x00{isbn}\x00"
    return payload.encode("cp1252", "replace")


def set_metadata(stream, mi) -> None:
    """
    Write metadata into an eReader PDB stream.
    """
    stream.seek(0)
    pheader = PdbHeaderReader(stream)

    section0 = _safe_section(pheader, 0)
    # Only Dropbook produced 132-byte record0 files are supported.
    if len(section0) != 132:
        return

    sections = [pheader.section_data(x) for x in range(pheader.num_sections)]
    hr = HeaderRecord(sections[0])
    if hr.compression not in (2, 10):
        return

    # Create a metadata record for the file if one does not already exist.
    if not hr.has_metadata:
        mutable_header = bytearray(sections[0])
        sections.extend([b"", b"MeTaInFo\x00"])
        last_data = len(sections) - 1

        offset_fields = (12, 32, 36, 38, 40, 42, 48, 50)
        for i in offset_fields:
            (val,) = struct.unpack(">H", mutable_header[i : i + 2])
            if val >= hr.last_data_offset:
                mutable_header[i : i + 2] = struct.pack(">H", last_data)

        mutable_header[24:26] = struct.pack(">H", 1)  # has metadata
        mutable_header[44:46] = struct.pack(">H", last_data - 1)  # metadata offset
        mutable_header[52:54] = struct.pack(">H", last_data)  # last data offset
        sections[0] = bytes(mutable_header)
        hr = HeaderRecord(sections[0])

    file_mi = get_metadata(stream, extract_cover=False)
    if hasattr(file_mi, "smart_update"):
        file_mi.smart_update(mi)
    else:
        file_mi = mi

    if 0 <= hr.metadata_offset < len(sections):
        sections[hr.metadata_offset] = _metadata_record_bytes(file_mi)
    else:
        default_log.warning("Skipping eReader metadata write due to invalid metadata_offset=%s", hr.metadata_offset)
        return

    # Rebuild the PDB wrapper because section sizes may have changed.
    pheader_builder = PdbHeaderBuilder(pheader.ident, pheader.title)
    stream.seek(0)
    stream.truncate(0)
    pheader_builder.build_header([len(x) for x in sections], stream)

    for item in sections:
        stream.write(item)
    stream.seek(0)
