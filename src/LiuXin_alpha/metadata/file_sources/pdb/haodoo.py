"""
Read metadata from Haodoo.net PDB files.
"""

from __future__ import annotations

from LiuXin_alpha.file_formats.pdb.haodoo.reader import Reader
from LiuXin_alpha.file_formats.pdb.header import PdbHeaderReader
from LiuXin_alpha.metadata.utils import calibreMetaInformation
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log

__license__ = "GPL v3"
__copyright__ = "2012, Kan-Ru Chen <kanru@kanru.info>"
__docformat__ = "restructuredtext en"


def _normalize_authors(raw_authors) -> list[str]:
    if isinstance(raw_authors, list):
        return [str(x) for x in raw_authors if x]
    if isinstance(raw_authors, dict):
        return [str(x) for x in raw_authors.keys() if x]
    if isinstance(raw_authors, str):
        return [raw_authors] if raw_authors else []
    if raw_authors:
        return [str(raw_authors)]
    return []


def get_metadata(stream, extract_cover: bool = True):
    """
    Return metadata as a metadata object.
    """
    del extract_cover  # Haodoo metadata does not carry cover bytes.
    stream.seek(0)
    pheader = PdbHeaderReader(stream)

    try:
        # The legacy haodoo reader compares against byte-valued idents.
        if isinstance(pheader.ident, str):
            pheader.ident = pheader.ident.encode("ascii", "ignore")

        reader = Reader(pheader, stream, default_log, None)
        src = reader.get_metadata()

        title = getattr(src, "title", None) or pheader.title or _("Unknown")
        authors = _normalize_authors(getattr(src, "authors", None)) or [_("Unknown")]
        mi = calibreMetaInformation(title, authors)
        language = getattr(src, "language", None)
        if language:
            mi.language = language
        return mi
    except Exception as err:
        default_log.log_exception("Unable to read Haodoo metadata; returning header fallback.", err, "WARNING")
        return calibreMetaInformation(pheader.title or _("Unknown"), [_("Unknown")])
