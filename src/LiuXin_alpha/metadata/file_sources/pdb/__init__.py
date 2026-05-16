"""
Metadata read/write entry points for PDB files.
"""

from __future__ import annotations

import os
import re
from contextlib import contextmanager
from typing import Iterator

from LiuXin_alpha.file_formats.pdb.header import PdbHeaderReader
from LiuXin_alpha.metadata.file_sources.pdb.ereader import get_metadata as get_ereader
from LiuXin_alpha.metadata.file_sources.pdb.ereader import set_metadata as set_ereader
from LiuXin_alpha.metadata.file_sources.pdb.haodoo import get_metadata as get_haodoo
from LiuXin_alpha.metadata.file_sources.pdb.plucker import get_metadata as get_plucker
from LiuXin_alpha.metadata.utils import calibreMetaInformation
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.libraries.cleantext import clean_xml_chars

__license__ = "GPL v3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"

_TITLE_SANITIZE_RE = re.compile(r"[^-A-Za-z0-9 ]+")

# Keyed with the pheader ident and valued with the reader needed to get metadata from the file.
MREADER = {
    "PNPdPPrs": get_ereader,
    "PNRdPPrs": get_ereader,
    "DataPlkr": get_plucker,
    "BOOKMTIT": get_haodoo,
    "BOOKMTIU": get_haodoo,
}

# Keyed with the pheader ident and valued with the writer used to write metadata.
MWRITER = {
    "PNPdPPrs": set_ereader,
    "PNRdPPrs": set_ereader,
}


class PdbFormatError(Exception):
    pass


def _is_path_like(stream_or_path) -> bool:
    return isinstance(stream_or_path, (str, bytes, os.PathLike))


@contextmanager
def _open_stream_for_reading(stream_or_path) -> Iterator:
    if _is_path_like(stream_or_path):
        with open(stream_or_path, "rb") as stream:
            yield stream
        return
    if not hasattr(stream_or_path, "read"):
        raise TypeError("PDB metadata reader expects a binary stream or filesystem path.")
    yield stream_or_path


@contextmanager
def _open_stream_for_writing(stream_or_path) -> Iterator:
    if _is_path_like(stream_or_path):
        with open(stream_or_path, "r+b") as stream:
            yield stream
        return
    if not hasattr(stream_or_path, "read") or not hasattr(stream_or_path, "write"):
        raise TypeError("PDB metadata writer expects a read/write binary stream or filesystem path.")
    yield stream_or_path


def _fallback_metadata(title: str | None):
    fallback_title = title or _("Unknown")
    return calibreMetaInformation(fallback_title, [_("Unknown")])


def _source_title_hint(stream_or_path) -> str | None:
    if _is_path_like(stream_or_path):
        try:
            stem = os.path.splitext(os.path.basename(os.fspath(stream_or_path)))[0]
            return stem or None
        except Exception:
            return None
    name = getattr(stream_or_path, "name", None)
    if isinstance(name, str) and name:
        return os.path.splitext(os.path.basename(name))[0] or None
    return None


def _normalize_title_bytes(title: object) -> bytes:
    text = _TITLE_SANITIZE_RE.sub("_", clean_xml_chars(str(title or _("Unknown"))))
    return text.encode("ascii", "replace")[:31].ljust(31, b"\x00") + b"\x00"


def get_metadata(stream_or_path, extract_cover: bool = True, *, fallback_on_parse_error: bool = False):
    """
    Return metadata for a PDB stream/path.

    If a specialized reader is not available (or fails), this falls back to
    title-from-header + unknown author.
    """
    with _open_stream_for_reading(stream_or_path) as stream:
        try:
            stream.seek(0)
            pheader = PdbHeaderReader(stream)
        except Exception as err:
            default_log.log_exception(
                "Unable to parse PDB header. Returning minimal fallback metadata.",
                err,
                "WARNING",
            )
            if not fallback_on_parse_error:
                raise PdbFormatError("Unable to parse PDB header.") from err
            return _fallback_metadata(_source_title_hint(stream_or_path))

        reader = MREADER.get(pheader.ident)
        if reader is None:
            return _fallback_metadata(pheader.title)

        try:
            return reader(stream, extract_cover=extract_cover)
        except Exception as err:
            default_log.log_exception(
                "Falling back to PDB header-only metadata after reader failure.",
                err,
                "WARNING",
                ("ident", pheader.ident),
            )
            return _fallback_metadata(pheader.title)


def get_pheader_ident(stream_or_path) -> str:
    """
    Return the PDB header ident for the given stream/path.
    """
    with _open_stream_for_reading(stream_or_path) as stream:
        try:
            stream.seek(0)
            return PdbHeaderReader(stream).ident
        except Exception as err:
            raise ValueError("Unable to parse PDB header identity from stream/path.") from err


def set_metadata(stream_or_path, mi) -> None:
    """
    Write metadata into the given PDB stream/path when supported.

    Full metadata writes are only supported for eReader PDB variants. For all
    PDB files, the title in the wrapper header is updated.
    """
    with _open_stream_for_writing(stream_or_path) as stream:
        try:
            stream.seek(0)
            pheader = PdbHeaderReader(stream)
        except Exception as err:
            raise ValueError("Cannot set metadata: invalid or corrupt PDB header.") from err

        writer = MWRITER.get(pheader.ident)
        if writer is not None:
            writer(stream, mi)

        stream.seek(0)
        stream.write(_normalize_title_bytes(getattr(mi, "title", None)))
