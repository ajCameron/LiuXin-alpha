"""
Select the first supported ebook in a RAR archive and delegate metadata extraction to its registered reader.

The module keeps malformed-input, optional dependency and resource ownership
behavior explicit for registry callers.

Example:
    Exercise rar with pytest::

        python -m pytest -q tests/metadata/file_sources/test_rar_metadata_source.py
"""

from __future__ import annotations

import os
from io import BytesIO

from LiuXin_alpha.metadata.file_sources.archive import is_comic
from LiuXin_alpha.utils.decompression.unrar import extract_member, names
from LiuXin_alpha.utils.logging import default_log

__license__ = "GPL v3"
__copyright__ = "2009, Kovid Goyal kovid@kovidgoyal.net"
__docformat__ = "restructuredtext en"

VALID_FOR = ["RAR"]
PRIORITY_FOR = ["RAR"]
RUN_COST = ["LOW"]

_SUPPORTED_MEMBER_EXTENSIONS = {
    "azw",
    "azw1",
    "azw3",
    "azw4",
    "epub",
    "fb2",
    "fbz",
    "imp",
    "lit",
    "lrf",
    "mobi",
    "opf",
    "pdb",
    "pdf",
    "pml",
    "pmlz",
    "prc",
    "rb",
    "rtf",
}


def _member_type(member_name: str) -> str:
    """
    Return the normalized lowercase extension used to choose an archive member reader.

    Example:
        Exercise  member type with pytest::

            python -m pytest -q tests/metadata/file_sources/test_rar_metadata_source.py


    :param member_name: Source or member label used for lookup, fallback titles or
        diagnostics.
    :return: Parsed, normalized or serialized value described above.
    """
    ext = os.path.splitext(member_name.replace("\\", "/"))[1].lower()
    return ext[1:] if ext.startswith(".") else ext


def _dispatch_metadata(target, *, force_type: str):
    """
    Delegate a member or stream to the shared metadata dispatcher with an explicit type.

    Example:
        Exercise  dispatch metadata with pytest::

            python -m pytest -q tests/metadata/file_sources/test_rar_metadata_source.py


    :param target: Caller-supplied path, path-like object or stream described by this
        operation.
    :param force_type: Name, type or encoding selector used for lookup or
        interpretation.
    :return: Parsed, normalized or serialized value described above.
    """
    from LiuXin_alpha.metadata.file_sources import get_metadata as dispatch_get_metadata

    return dispatch_get_metadata(target, force_type=force_type)


def _set_timestamp_none(mi) -> None:
    """
    Set timestamp none while preserving unrelated metadata state.

    Example:
        Exercise  set timestamp none with pytest::

            python -m pytest -q tests/metadata/file_sources/test_rar_metadata_source.py


    :param mi: Metadata object supplying or receiving the supported fields.
    :return: None.
    """
    try:
        mi.timestamp = None
    except Exception:
        pass


def _find_first_supported_member(file_names: list[str]) -> tuple[str, str] | None:
    """
    Return the first archive member supported by the delegated metadata registry.

    Example:
        Exercise  find first supported member with pytest::

            python -m pytest -q tests/metadata/file_sources/test_rar_metadata_source.py


    :param file_names: Ordered input values processed by this operation.
    :return: Parsed, normalized or serialized value described above.
    """
    for file_name in file_names:
        stream_type = _member_type(file_name)
        if stream_type in _SUPPORTED_MEMBER_EXTENSIONS:
            return file_name, stream_type
    return None


def _source_label(stream) -> str:
    """
    Perform the format-specific source label operation used by this metadata source.

    Example:
        Exercise  source label with pytest::

            python -m pytest -q tests/metadata/file_sources/test_rar_metadata_source.py


    :param stream: Caller-supplied path, path-like object or stream described by this
        operation.
    :return: Parsed, normalized or serialized value described above.
    """
    name = getattr(stream, "name", "") or ""
    if not name:
        return "<stream>"
    return os.path.basename(name)


def _read_metadata_from_rar_stream(stream):
    """
    Read comic metadata or the first supported ebook member from an open RAR stream.

    Example:
        Exercise  read metadata from rar stream with pytest::

            python -m pytest -q tests/metadata/file_sources/test_rar_metadata_source.py


    :param stream: Caller-supplied path, path-like object or stream described by this
        operation.
    :return: Parsed, normalized or serialized value described above.
    """
    file_names = list(names(stream))
    if is_comic(file_names):
        mi = _dispatch_metadata(stream, force_type="cbr")
        _set_timestamp_none(mi)
        return mi

    chosen = _find_first_supported_member(file_names)
    if chosen is None:
        raise ValueError(f"No ebook found in RAR archive ({_source_label(stream)})")
    member_name, stream_type = chosen

    extracted = extract_member(stream, match=None, name=member_name)
    if extracted is None:
        raise ValueError(f"Unable to extract selected archive member: {member_name}")

    extracted_name, extracted_data = extracted
    payload_stream = BytesIO(extracted_data)
    payload_stream.name = os.path.basename(extracted_name)
    mi = _dispatch_metadata(payload_stream, force_type=stream_type)
    _set_timestamp_none(mi)
    return mi


def get_metadata(target_file):
    """
    Read metadata from the supported path, bytes or stream input while applying module ownership and fallback policy.

    Example:
        Exercise get metadata with pytest::

            python -m pytest -q tests/metadata/file_sources/test_rar_metadata_source.py


    :param target_file: Caller-supplied path, path-like object or stream described by
        this operation.
    :return: Parsed, normalized or serialized value described above.
    """
    stream_needs_close = False
    if isinstance(target_file, os.PathLike):
        target_file = os.fspath(target_file)
    if isinstance(target_file, str):
        stream = open(target_file, "rb")
        stream_needs_close = True
    elif hasattr(target_file, "read"):
        stream = target_file
    else:
        raise TypeError("RAR metadata reader expects a filesystem path or binary stream.")

    pos = None
    if hasattr(stream, "tell"):
        try:
            pos = stream.tell()
        except Exception:
            pos = None

    try:
        return _read_metadata_from_rar_stream(stream)
    except Exception as err:
        default_log.log_exception(
            "Failed reading metadata from RAR archive.",
            err,
            "ERROR",
            ("source", _source_label(stream)),
        )
        raise
    finally:
        if stream_needs_close:
            stream.close()
        elif pos is not None and hasattr(stream, "seek"):
            try:
                stream.seek(pos)
            except Exception:
                pass


__all__ = [
    "VALID_FOR",
    "PRIORITY_FOR",
    "RUN_COST",
    "get_metadata",
]
