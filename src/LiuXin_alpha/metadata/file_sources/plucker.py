"""
Expose the Plucker metadata reader through the metadata-source registry contract.

The module keeps malformed-input, optional dependency and resource ownership
behavior explicit for registry callers.

Example:
    Exercise plucker with pytest::

        python -m pytest -q tests/metadata/file_sources/test_plucker_metadata_source.py
"""

from __future__ import annotations

from LiuXin_alpha.metadata.file_sources.pdb.plucker import get_metadata as _get_pdb_plucker_metadata

__license__ = "GPL v3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"

__all__ = ["get_metadata"]


def get_metadata(stream, extract_cover: bool = True):
    """
    Read metadata from the supported path, bytes or stream input while applying module ownership and fallback policy.

    Example:
        Exercise get metadata with pytest::

            python -m pytest -q tests/metadata/file_sources/test_plucker_metadata_source.py


    :param stream: Caller-supplied path, path-like object or stream described by this
        operation.
    :param extract_cover: Request cover discovery or cover payload extraction when true.
    :return: Parsed, normalized or serialized value described above.
    """
    return _get_pdb_plucker_metadata(stream, extract_cover=extract_cover)
