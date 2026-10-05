"""
Read and update DOCX package properties, sanitizing XML text and optionally extracting a plausible cover image.

The module keeps binary parsing, optional dependency and stream ownership policy
explicit for registry callers.

Example:
    Exercise docx with pytest::

        python -m pytest -q tests/metadata/file_sources/test_docx_metadata_source.py
"""

from __future__ import annotations

import os
from copy import deepcopy
from io import BytesIO
from typing import Any

from LiuXin_alpha.file_formats.docx.container import DOCX
from LiuXin_alpha.file_formats.docx.writer.container import update_doc_props, xml2str
from LiuXin_alpha.utils.image_tools.imghdr import identify
from LiuXin_alpha.utils.libraries.calibre_zipfile import safe_replace
from LiuXin_alpha.utils.libraries.cleantext import clean_xml_chars
from LiuXin_alpha.utils.libraries.liuxin_etree import etree
from LiuXin_alpha.utils.logging import default_log

__license__ = "GPL v3"
__copyright__ = "2012, Kovid Goyal <kovid at kovidgoyal.net>"
__docformat__ = "restructuredtext en"


def _is_path_like(target: Any) -> bool:
    """
    Return whether the supplied value satisfies the path like condition.

    Example:
        Exercise  is path like with pytest::

            python -m pytest -q tests/metadata/file_sources/test_docx_metadata_source.py


    :param target: Caller-supplied path, path-like object or stream described by this
        operation.
    :return: True when the condition is satisfied; otherwise False.
    """
    return isinstance(target, (str, bytes, os.PathLike))


def _clean_xml_text(value: Any) -> Any:
    """
    Normalize xml text into the representation expected by later parsing or serialization steps.

    Example:
        Exercise  clean xml text with pytest::

            python -m pytest -q tests/metadata/file_sources/test_docx_metadata_source.py


    :param value: Raw value or payload to normalize, parse or serialize.
    :return: Parsed, normalized or updated value described above.
    """
    if value is None:
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        value = bytes(value).decode("utf-8", "replace")
    if isinstance(value, str):
        return clean_xml_chars(value)
    return value


def _clean_xml_list(values: Any) -> list[Any]:
    """
    Normalize xml list into the representation expected by later parsing or serialization steps.

    Example:
        Exercise  clean xml list with pytest::

            python -m pytest -q tests/metadata/file_sources/test_docx_metadata_source.py


    :param values: Raw value or payload to normalize, parse or serialize.
    :return: Parsed, normalized or updated value described above.
    """
    if values is None:
        return []
    if isinstance(values, str):
        values = [values]
    try:
        iterable = list(values)
    except Exception:
        iterable = [values]

    cleaned = []
    for item in iterable:
        clean_item = _clean_xml_text(item)
        if isinstance(clean_item, str) and not clean_item.strip():
            continue
        if clean_item is not None:
            cleaned.append(clean_item)
    return cleaned


def _sanitize_metadata_for_xml(mi: Any) -> Any:
    """
    Perform the format-specific sanitize metadata for xml operation used by the metadata reader or writer.

    Example:
        Exercise  sanitize metadata for xml with pytest::

            python -m pytest -q tests/metadata/file_sources/test_docx_metadata_source.py


    :param mi: Metadata object supplying or receiving supported fields.
    :return: Parsed, normalized or updated value described above.
    """
    safe_mi = None
    for clone_method in ("deepcopy_metadata", "deepcopy"):
        fn = getattr(mi, clone_method, None)
        if callable(fn):
            try:
                safe_mi = fn()
                break
            except Exception:
                safe_mi = None
    if safe_mi is None:
        try:
            safe_mi = deepcopy(mi)
        except Exception:
            safe_mi = mi

    for attr in ("title", "comments", "publisher"):
        if hasattr(safe_mi, attr):
            setattr(safe_mi, attr, _clean_xml_text(getattr(safe_mi, attr, None)))

    for attr in ("authors", "tags", "languages"):
        if hasattr(safe_mi, attr):
            setattr(safe_mi, attr, _clean_xml_list(getattr(safe_mi, attr, None)))

    return safe_mi


def get_cover(docx: DOCX):
    """
    Return cover bytes selected by the format-specific cover discovery policy, or None when no usable cover exists.

    Example:
        Exercise get cover with pytest::

            python -m pytest -q tests/metadata/file_sources/test_docx_metadata_source.py


    :param docx: Value supplied for docx.
    :return: Parsed, normalized or updated value described above.
    """
    document = docx.document
    get = docx.namespace.get
    images = docx.namespace.XPath(
        '//*[name()="w:drawing" or name()="w:pict"]/descendant::*[(name()="a:blip" and @r:embed) or '
        '(name()="v:imagedata" and @r:id)][1]'
    )
    rid_map = docx.document_relationships[0]

    for image in images(document):
        rid = get(image, "r:embed") or get(image, "r:id")
        if rid not in rid_map:
            continue
        try:
            raw = docx.read(rid_map[rid])
            fmt, width, height = identify(bytes(raw))
        except Exception:
            continue

        if width <= 0 or height <= 0:
            continue

        ratio = height / float(width)
        if 0.8 <= ratio <= 1.8 and height * width >= 160000:
            return fmt, raw
    return None


def get_metadata(stream_or_path, extract_cover: bool = True):
    """
    Read metadata from the supported path or stream input while applying the module's ownership and fallback policy.

    Example:
        Exercise get metadata with pytest::

            python -m pytest -q tests/metadata/file_sources/test_docx_metadata_source.py


    :param stream_or_path: Caller-supplied path, path-like object or stream described by
        this operation.
    :param extract_cover: Attempt cover discovery and extraction when true.
    :return: Parsed, normalized or updated value described above.
    """
    if _is_path_like(stream_or_path):
        with open(stream_or_path, "rb") as stream:
            return get_metadata_from_stream(stream, extract_cover=extract_cover)
    if not hasattr(stream_or_path, "read"):
        raise TypeError("DOCX metadata reader expects a filesystem path or readable binary stream.")
    return get_metadata_from_stream(stream_or_path, extract_cover=extract_cover)


def get_metadata_from_stream(stream, extract_cover: bool = True):
    """
    Read metadata from a caller-owned stream and restore or reset its position according to the format contract.

    Example:
        Exercise get metadata from stream with pytest::

            python -m pytest -q tests/metadata/file_sources/test_docx_metadata_source.py


    :param stream: Caller-supplied path, path-like object or stream described by this
        operation.
    :param extract_cover: Attempt cover discovery and extraction when true.
    :return: Parsed, normalized or updated value described above.
    """
    container = DOCX(stream, extract=False)
    try:
        metadata = container.metadata
        cover_data = get_cover(container) if extract_cover else None
        if cover_data is not None:
            metadata.cover_data = cover_data
        return metadata
    except Exception as err:
        default_log.log_exception(
            "Failed to read DOCX metadata.",
            err,
            "ERROR",
            ("stream_name", getattr(stream, "name", "<stream>")),
        )
        raise
    finally:
        try:
            container.close()
        finally:
            if hasattr(stream, "seek"):
                try:
                    stream.seek(0)
                except Exception:
                    pass


def _set_metadata_on_stream(stream, mi) -> None:
    """
    Set metadata on stream under the supplied null and replacement policy.

    Example:
        Exercise  set metadata on stream with pytest::

            python -m pytest -q tests/metadata/file_sources/test_docx_metadata_source.py


    :param stream: Caller-supplied path, path-like object or stream described by this
        operation.
    :param mi: Metadata object supplying or receiving supported fields.
    :return: None.
    """
    mi = _sanitize_metadata_for_xml(mi)
    container = DOCX(stream, extract=False)
    try:
        dp_name, ap_name = container.get_document_properties_names()
        if not dp_name:
            raise ValueError("DOCX metadata cannot be updated: missing core document properties file.")

        core_props = etree.fromstring(container.read(dp_name))
        update_doc_props(core_props, mi, container.namespace)
        replacements: dict[str, BytesIO] = {}

        if ap_name:
            try:
                app_props = etree.fromstring(container.read(ap_name))
            except Exception:
                app_props = None

            if app_props is not None:
                company_tag = "{%s}Company" % container.namespace.namespaces["ep"]
                for child in tuple(app_props):
                    if child.tag == company_tag:
                        app_props.remove(child)
                company = app_props.makeelement(company_tag)
                company.text = getattr(mi, "publisher", None)
                app_props.append(company)
                replacements[ap_name] = BytesIO(xml2str(app_props))

        stream.seek(0)
        safe_replace(stream, dp_name, BytesIO(xml2str(core_props)), extra_replacements=replacements)
        stream.seek(0)
    finally:
        container.close()


def set_metadata(stream_or_path, mi):
    """
    Write supported metadata fields to a path or mutable binary stream without taking ownership of caller-supplied streams.

    Example:
        Exercise set metadata with pytest::

            python -m pytest -q tests/metadata/file_sources/test_docx_metadata_source.py


    :param stream_or_path: Caller-supplied path, path-like object or stream described by
        this operation.
    :param mi: Metadata object supplying or receiving supported fields.
    :return: None.
    """
    if _is_path_like(stream_or_path):
        with open(stream_or_path, "r+b") as stream:
            _set_metadata_on_stream(stream, mi)
        return

    if not hasattr(stream_or_path, "read") or not hasattr(stream_or_path, "write"):
        raise TypeError("DOCX metadata writer expects a path or read/write binary stream.")

    _set_metadata_on_stream(stream_or_path, mi)


__all__ = [
    "get_cover",
    "get_metadata",
    "get_metadata_from_stream",
    "set_metadata",
]
