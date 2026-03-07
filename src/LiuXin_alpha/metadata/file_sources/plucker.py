"""
Legacy module path for Plucker PDB metadata.

Historically this reader lived at `metadata.file_sources.plucker`, but the
format-specific implementation now lives under
`metadata.file_sources.pdb.plucker`. Keep this module as a thin forwarder so
old imports continue to work without duplicating parser logic.
"""

from __future__ import annotations

from LiuXin_alpha.metadata.file_sources.pdb.plucker import get_metadata as _get_pdb_plucker_metadata

__license__ = "GPL v3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"

__all__ = ["get_metadata"]


def get_metadata(stream, extract_cover: bool = True):
    return _get_pdb_plucker_metadata(stream, extract_cover=extract_cover)
