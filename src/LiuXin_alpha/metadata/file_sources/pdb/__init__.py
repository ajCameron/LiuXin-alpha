"""
Dispatch PDB metadata reads and writes by wrapper identity with explicit malformed-header fallback and stream ownership.

The module makes ordering, fallback, ownership and optional-integration behavior
explicit for callers.

Example:
    Exercise   init   with the owning regression module::

        python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py
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
    """
    Signal a PDB wrapper whose header cannot be parsed under strict reader policy.

    Example:
        Exercise PdbFormatError with the owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py
    """
    pass


def _is_path_like(stream_or_path) -> bool:
    """
    Return whether the PDB input should be opened as a filesystem path.

    Example:
        Exercise  is path like with the owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param stream_or_path: Binary stream or filesystem path accepted by the PDB wrapper.
    :return: True when the described condition is satisfied; otherwise False.
    """
    return isinstance(stream_or_path, (str, bytes, os.PathLike))


@contextmanager
def _open_stream_for_reading(stream_or_path) -> Iterator:
    """
    Yield a readable binary stream, opening and closing path inputs while leaving caller streams owned by the caller.

    Example:
        Exercise  open stream for reading with the owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param stream_or_path: Binary stream or filesystem path accepted by the PDB wrapper.
    :return: A context manager yielding the readable binary stream.
    """
    if _is_path_like(stream_or_path):
        with open(stream_or_path, "rb") as stream:
            yield stream
        return
    if not hasattr(stream_or_path, "read"):
        raise TypeError("PDB metadata reader expects a binary stream or filesystem path.")
    yield stream_or_path


@contextmanager
def _open_stream_for_writing(stream_or_path) -> Iterator:
    """
    Yield a readable/writable binary stream, opening path inputs in place and validating caller streams.

    Example:
        Exercise  open stream for writing with the owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param stream_or_path: Binary stream or filesystem path accepted by the PDB wrapper.
    :return: A context manager yielding the readable/writable binary stream.
    """
    if _is_path_like(stream_or_path):
        with open(stream_or_path, "r+b") as stream:
            yield stream
        return
    if not hasattr(stream_or_path, "read") or not hasattr(stream_or_path, "write"):
        raise TypeError("PDB metadata writer expects a read/write binary stream or filesystem path.")
    yield stream_or_path


def _fallback_metadata(title: str | None):
    """
    Build header- or filename-derived PDB metadata with an explicit unknown author.

    Example:
        Exercise  fallback metadata with the owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param title: Book title used for lookup, ranking or result comparison.
    :return: The normalized row, metadata object or value described above.
    """
    fallback_title = title or _("Unknown")
    return calibreMetaInformation(fallback_title, [_("Unknown")])


def _source_title_hint(stream_or_path) -> str | None:
    """
    Derive a fallback title from a path or named stream without consuming its data.

    Example:
        Exercise  source title hint with the owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param stream_or_path: Binary stream or filesystem path accepted by the PDB wrapper.
    :return: The normalized row, metadata object or value described above.
    """
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
    """
    Sanitize a PDB wrapper title and encode its fixed 32-byte ASCII field.

    Example:
        Exercise  normalize title bytes with the owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param title: Book title used for lookup, ranking or result comparison.
    :return: The normalized row, metadata object or value described above.
    """
    text = _TITLE_SANITIZE_RE.sub("_", clean_xml_chars(str(title or _("Unknown"))))
    return text.encode("ascii", "replace")[:31].ljust(31, b"\x00") + b"\x00"


def get_metadata(stream_or_path, extract_cover: bool = True, *, fallback_on_parse_error: bool = False):
    """
    Read normalized metadata using this module's format-specific parser and fallback policy.

    Example:
        Exercise get metadata with the owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param stream_or_path: Binary stream or filesystem path accepted by the PDB wrapper.
    :param extract_cover: Request cover extraction when the underlying format supports
        it.
    :param fallback_on_parse_error: Return minimal fallback metadata for an invalid
        header when true; otherwise raise.
    :return: The normalized row, metadata object or value described above.
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
    Read and return the eight-byte PDB application identity as normalized text.

    Example:
        Exercise get pheader ident with the owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param stream_or_path: Binary stream or filesystem path accepted by the PDB wrapper.
    :return: The normalized row, metadata object or value described above.
    """
    with _open_stream_for_reading(stream_or_path) as stream:
        try:
            stream.seek(0)
            return PdbHeaderReader(stream).ident
        except Exception as err:
            raise ValueError("Unable to parse PDB header identity from stream/path.") from err


def set_metadata(stream_or_path, mi) -> None:
    """
    Rewrite supported metadata while preserving unrelated PDB sections and caller ownership.

    Example:
        Exercise set metadata with the owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param stream_or_path: Binary stream or filesystem path accepted by the PDB wrapper.
    :param mi: Metadata object supplying fields to clean, cache or write.
    :return: None.
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
