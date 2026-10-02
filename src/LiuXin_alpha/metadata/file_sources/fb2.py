"""
Parse and update FictionBook metadata in plain or archived FB2 payloads with namespace-tolerant XML helpers.

The module keeps binary parsing, optional dependency and stream ownership policy
explicit for registry callers.

Example:
    Exercise fb2 with pytest::

        python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py
"""

from __future__ import annotations

import base64
import datetime
import html
import os
import random
import re
from collections.abc import Iterable, Mapping
from io import BytesIO
from pathlib import Path
from string import ascii_letters, digits
from typing import Any

from LiuXin_alpha.file_formats.fb2 import base64_decode
from LiuXin_alpha.file_formats.fb2.archive import (
    DEFAULT_MAX_ARCHIVE_MEMBERS,
    DEFAULT_MAX_COMPRESSION_RATIO,
    DEFAULT_MAX_MEMBER_UNCOMPRESSED_SIZE,
    DEFAULT_MAX_TOTAL_UNCOMPRESSED_SIZE,
    DEFAULT_MIN_COMPRESSION_RATIO_CHECK_SIZE,
    FB2ZipError,
    extract_fb2_payload_from_bytes,
)
from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import (
    CalibreLikeLiuXinBookMetaData as MetaInformation,
)
from LiuXin_alpha.metadata.utils import check_isbn
from LiuXin_alpha.utils.libraries.calibre_chardet import xml_to_unicode
from LiuXin_alpha.utils.libraries.calibre_zipfile import safe_replace
from LiuXin_alpha.utils.libraries.cleantext import clean_xml_chars
from LiuXin_alpha.utils.libraries.liuxin_etree import etree
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.mine_types import guess_all_extensions, guess_type

try:
    from LiuXin_alpha.utils.image_tools.img import save_cover_data_to
except Exception:
    try:
        from LiuXin_alpha.utils.image_tools.img_fallback import save_cover_data_to
    except Exception:
        save_cover_data_to = None

try:
    from LiuXin_alpha.utils.image_tools.imghdr import identify
except Exception:
    identify = None


__license__ = "GPL v3"
__copyright__ = "2011, Roman Mukhin <ramses_ru at hotmail.com>, 2008, Anatoly Shipitsin <norguhtar at gmail.com>"

VALID_FOR = ["FB2"]
PRIORITY_FOR = ["NONE"]
RUN_COST = ["LOW"]


NAMESPACES = {
    "fb2": "http://www.gribuser.ru/xml/fictionbook/2.0",
    "fb21": "http://www.gribuser.ru/xml/fictionbook/2.1",
    "xlink": "http://www.w3.org/1999/xlink",
}

_HTML_TAG_RE = re.compile(r"<[^>]+>")


class FB2ParseError(Exception):
    """
    Signal malformed, unsupported or unreadable FictionBook input.

    Example:
        Exercise FB2ParseError with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py
    """
    pass


FB2_ZIP_MAX_ARCHIVE_MEMBERS = DEFAULT_MAX_ARCHIVE_MEMBERS
FB2_ZIP_MAX_MEMBER_UNCOMPRESSED_SIZE = DEFAULT_MAX_MEMBER_UNCOMPRESSED_SIZE
FB2_ZIP_MAX_TOTAL_UNCOMPRESSED_SIZE = DEFAULT_MAX_TOTAL_UNCOMPRESSED_SIZE
FB2_ZIP_MAX_COMPRESSION_RATIO = DEFAULT_MAX_COMPRESSION_RATIO
FB2_ZIP_MIN_COMPRESSION_RATIO_CHECK_SIZE = DEFAULT_MIN_COMPRESSION_RATIO_CHECK_SIZE


def _is_path_like(target: Any) -> bool:
    """
    Return whether the supplied value satisfies the path like condition.

    Example:
        Exercise  is path like with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param target: Caller-supplied path, path-like object or stream described by this
        operation.
    :return: True when the condition is satisfied; otherwise False.
    """
    return isinstance(target, (str, bytes, os.PathLike))


def _source_name(target: Any) -> str:
    """
    Derive the name used for fallback metadata and diagnostics.

    Example:
        Exercise  source name with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param target: Caller-supplied path, path-like object or stream described by this
        operation.
    :return: Parsed, normalized or updated value described above.
    """
    if _is_path_like(target):
        return os.fspath(target)
    return getattr(target, "name", "<stream>")


def _source_title(target: Any) -> str:
    """
    Derive the title used for fallback metadata and diagnostics.

    Example:
        Exercise  source title with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param target: Caller-supplied path, path-like object or stream described by this
        operation.
    :return: Parsed, normalized or updated value described above.
    """
    name = os.path.basename(_source_name(target))
    title = os.path.splitext(name)[0].strip()
    return title or _("Unknown")


def _ensure_bytes(raw: Any) -> bytes:
    """
    Coerce bytes to the required representation or supply its documented fallback.

    Example:
        Exercise  ensure bytes with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param raw: Raw value or payload to normalize, parse or serialize.
    :return: Parsed, normalized or updated value described above.
    """
    if isinstance(raw, bytes):
        return raw
    if isinstance(raw, bytearray):
        return bytes(raw)
    if isinstance(raw, str):
        return raw.encode("utf-8", "replace")
    return bytes(raw)


def _localname(tag: Any) -> str:
    """
    Perform the format-specific localname operation used by the metadata reader or writer.

    Example:
        Exercise  localname with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param tag: Value supplied for tag.
    :return: Parsed, normalized or updated value described above.
    """
    return str(tag).rsplit("}", 1)[-1]


def _namespace_from_tag(tag: Any) -> str | None:
    """
    Perform the format-specific namespace from tag operation used by the metadata reader or writer.

    Example:
        Exercise  namespace from tag with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param tag: Value supplied for tag.
    :return: Parsed, normalized or updated value described above.
    """
    text = str(tag)
    if text.startswith("{") and "}" in text:
        return text[1:].split("}", 1)[0]
    return None


def _iter_children_local(parent, local_name: str):
    """
    Iterate children local in source order using local-name tolerant matching.

    Example:
        Exercise  iter children local with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param parent: XML or metadata node used as the operation context.
    :param local_name: Value supplied for local name.
    :return: Parsed, normalized or updated value described above.
    """
    for child in parent:
        if _localname(getattr(child, "tag", "")) == local_name:
            yield child


def _first_child_local(parent, local_name: str):
    """
    Return the first usable child local under the format's fallback rules.

    Example:
        Exercise  first child local with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param parent: XML or metadata node used as the operation context.
    :param local_name: Value supplied for local name.
    :return: Parsed, normalized or updated value described above.
    """
    for child in _iter_children_local(parent, local_name):
        return child
    return None


def _iter_descendants_local(root, local_name: str):
    """
    Iterate descendants local in source order using local-name tolerant matching.

    Example:
        Exercise  iter descendants local with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param root: XML or metadata node used as the operation context.
    :param local_name: Value supplied for local name.
    :return: Parsed, normalized or updated value described above.
    """
    for elem in root.iter():
        if _localname(getattr(elem, "tag", "")) == local_name:
            yield elem


def _normalize_text(raw: Any) -> str:
    """
    Normalize text into the representation expected by later parsing or serialization steps.

    Example:
        Exercise  normalize text with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param raw: Raw value or payload to normalize, parse or serialize.
    :return: Parsed, normalized or updated value described above.
    """
    if raw is None:
        return ""
    text = clean_xml_chars(str(raw))
    return " ".join(text.split()).strip()


def _metadata_values(raw: Any) -> list[Any]:
    """
    Perform the format-specific metadata values operation used by the metadata reader or writer.

    Example:
        Exercise  metadata values with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param raw: Raw value or payload to normalize, parse or serialize.
    :return: Parsed, normalized or updated value described above.
    """
    if raw is None:
        return []
    if isinstance(raw, Mapping):
        return list(raw.keys())
    if isinstance(raw, str):
        return [raw]
    if isinstance(raw, tuple) and len(raw) == 2 and isinstance(raw[0], str):
        return [raw]
    if isinstance(raw, Iterable):
        return list(raw)
    return [raw]


def _first_metadata_text(raw: Any) -> str:
    """
    Return the first usable metadata text under the format's fallback rules.

    Example:
        Exercise  first metadata text with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param raw: Raw value or payload to normalize, parse or serialize.
    :return: Parsed, normalized or updated value described above.
    """
    values = _metadata_values(raw)
    for value in values:
        text = _normalize_text(value)
        if text:
            return text
    return ""


def _is_null_field(mi, field: str) -> bool:
    """
    Return whether the supplied value satisfies the null field condition.

    Example:
        Exercise  is null field with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param mi: Metadata object supplying or receiving supported fields.
    :param field: Value supplied for field.
    :return: True when the condition is satisfied; otherwise False.
    """
    try:
        return bool(mi.is_null(field))
    except Exception:
        value = getattr(mi, field, None)
        return not bool(value)


def _iter_sections(root, section_name: str):
    """
    Iterate sections in source order using local-name tolerant matching.

    Example:
        Exercise  iter sections with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param root: XML or metadata node used as the operation context.
    :param section_name: Value supplied for section name.
    :return: Parsed, normalized or updated value described above.
    """
    for section in _iter_descendants_local(root, section_name):
        yield section


def _first_text_from_section(root, section_name: str, child_name: str) -> str | None:
    """
    Return the first usable text from section under the format's fallback rules.

    Example:
        Exercise  first text from section with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param root: XML or metadata node used as the operation context.
    :param section_name: Value supplied for section name.
    :param child_name: Value supplied for child name.
    :return: Parsed, normalized or updated value described above.
    """
    for section in _iter_sections(root, section_name):
        child = _first_child_local(section, child_name)
        if child is None:
            continue
        text = _normalize_text("".join(child.itertext()))
        if text:
            return text
    return None


def _get_xlink_href(elem) -> str | None:
    """
    Perform the format-specific get xlink href operation used by the metadata reader or writer.

    Example:
        Exercise  get xlink href with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param elem: XML or metadata node used as the operation context.
    :return: Parsed, normalized or updated value described above.
    """
    if elem is None:
        return None
    for key in (f"{{{NAMESPACES['xlink']}}}href", "xlink:href", "href"):
        val = elem.attrib.get(key)
        if val:
            return str(val)
    return None


def _safe_float(raw: Any) -> float | None:
    """
    Convert the supplied value using float semantics without propagating conversion failures.

    Example:
        Exercise  safe float with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param raw: Raw value or payload to normalize, parse or serialize.
    :return: Parsed, normalized or updated value described above.
    """
    if raw is None:
        return None
    text = str(raw).strip().replace(",", ".")
    if not text:
        return None
    try:
        return float(text)
    except Exception:
        parts = text.split()
        if not parts:
            return None
        try:
            return float(".".join(parts[:2]))
        except Exception:
            return None


def _safe_int(raw: Any) -> int | None:
    """
    Convert the supplied value using int semantics without propagating conversion failures.

    Example:
        Exercise  safe int with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param raw: Raw value or payload to normalize, parse or serialize.
    :return: Parsed, normalized or updated value described above.
    """
    if raw is None:
        return None
    try:
        return int(str(raw).strip())
    except Exception:
        return None


def _annotation_to_text(annotation) -> str:
    """
    Perform the format-specific annotation to text operation used by the metadata reader or writer.

    Example:
        Exercise  annotation to text with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param annotation: XML or metadata node used as the operation context.
    :return: Parsed, normalized or updated value described above.
    """
    if annotation is None:
        return ""

    lines: list[str] = []
    children = list(annotation)
    if not children:
        return _normalize_text("".join(annotation.itertext()))

    for child in children:
        local = _localname(getattr(child, "tag", ""))
        if local == "empty-line":
            lines.append("")
            continue
        text = _normalize_text("".join(child.itertext()))
        if local == "p":
            lines.append(text)
        elif text:
            lines.append(text)

    # Keep intentional blank lines between paragraphs.
    while lines and lines[-1] == "":
        lines.pop()
    return "\n".join(lines).strip()


def _htmlish_to_text(raw: Any) -> str:
    """
    Perform the format-specific htmlish to text operation used by the metadata reader or writer.

    Example:
        Exercise  htmlish to text with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param raw: Raw value or payload to normalize, parse or serialize.
    :return: Parsed, normalized or updated value described above.
    """
    text = str(raw or "")
    if not text:
        return ""

    if "<" not in text or ">" not in text:
        return text

    wrapped = f"<root>{text}</root>"
    try:
        root = etree.fromstring(wrapped.encode("utf-8", "replace"))
        text = "".join(root.itertext())
    except Exception:
        text = _HTML_TAG_RE.sub("", text)
    return html.unescape(text)


def _extract_cover_payload(mi) -> tuple[str, bytes] | None:
    """
    Extract cover payload using the format-specific ordering and validation rules.

    Example:
        Exercise  extract cover payload with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param mi: Metadata object supplying or receiving supported fields.
    :return: Parsed, normalized or updated value described above.
    """
    cover_data = getattr(mi, "cover_data", None)

    if isinstance(cover_data, tuple) and len(cover_data) == 2 and cover_data[1]:
        fmt = _normalize_text(cover_data[0]).lower() or "jpeg"
        return fmt, _ensure_bytes(cover_data[1])

    if isinstance(cover_data, Mapping):
        for key in cover_data.keys():
            if isinstance(key, tuple) and len(key) == 2 and key[1]:
                fmt = _normalize_text(key[0]).lower() or "jpeg"
                return fmt, _ensure_bytes(key[1])

    cover_path = getattr(mi, "cover", None)
    if isinstance(cover_path, str) and cover_path:
        try:
            payload = Path(cover_path).read_bytes()
        except Exception:
            return None
        fmt = os.path.splitext(cover_path)[1].lstrip(".").lower() or "jpeg"
        return fmt, payload

    return None


def _coerce_cover_to_jpeg_bytes(payload: bytes) -> bytes:
    """
    Normalize cover to jpeg bytes into the representation expected by later parsing or serialization steps.

    Example:
        Exercise  coerce cover to jpeg bytes with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param payload: Raw value or payload to normalize, parse or serialize.
    :return: Parsed, normalized or updated value described above.
    """
    if save_cover_data_to is None:
        return payload
    try:
        converted = save_cover_data_to(payload, path=None, data_fmt="jpeg")
    except Exception:
        return payload
    return _ensure_bytes(converted)


def _cover_format_from_payload(default_fmt: str, payload: bytes) -> str:
    """
    Perform the format-specific cover format from payload operation used by the metadata reader or writer.

    Example:
        Exercise  cover format from payload with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param default_fmt: Format or encoding hint used for interpretation.
    :param payload: Raw value or payload to normalize, parse or serialize.
    :return: Parsed, normalized or updated value described above.
    """
    fmt = _normalize_text(default_fmt).lower()
    if fmt == "jpg":
        fmt = "jpeg"
    if identify is not None:
        try:
            detected = identify(payload)[0]
            if detected:
                fmt = str(detected).lower()
        except Exception:
            pass
    return fmt or "jpeg"


def _extract_fb2_payload(raw_container_bytes: bytes, *, source_name: str = "<stream>") -> tuple[bytes, str | None]:
    """
    Extract fb2 payload using the format-specific ordering and validation rules.

    Example:
        Exercise  extract fb2 payload with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param raw_container_bytes: Raw value or payload to normalize, parse or serialize.
    :param source_name: Source label used for fallback titles and diagnostics.
    :return: Parsed, normalized or updated value described above.
    """
    try:
        return extract_fb2_payload_from_bytes(
            raw_container_bytes,
            label="FB2 archive",
            force_zip=str(source_name).lower().endswith(".fbz"),
            max_archive_members=FB2_ZIP_MAX_ARCHIVE_MEMBERS,
            max_member_uncompressed_size=FB2_ZIP_MAX_MEMBER_UNCOMPRESSED_SIZE,
            max_total_uncompressed_size=FB2_ZIP_MAX_TOTAL_UNCOMPRESSED_SIZE,
            max_compression_ratio=FB2_ZIP_MAX_COMPRESSION_RATIO,
            min_compression_ratio_check_size=FB2_ZIP_MIN_COMPRESSION_RATIO_CHECK_SIZE,
        )
    except FB2ZipError as err:
        raise FB2ParseError(str(err)) from err


def _parse_fb2_root(raw_payload: bytes):
    """
    Parse fb2 root and apply supported values without inventing absent metadata.

    Example:
        Exercise  parse fb2 root with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param raw_payload: Raw value or payload to normalize, parse or serialize.
    :return: Parsed, normalized or updated value described above.
    """
    if not raw_payload:
        raise FB2ParseError("Empty FB2 XML payload.")

    text, _enc = xml_to_unicode(raw_payload, strip_encoding_pats=True)
    if not isinstance(text, str):
        text = _ensure_bytes(text).decode("utf-8", "replace")
    text = text[text.find("<") :] if "<" in text else text

    parser = None
    try:
        parser = etree.XMLParser(recover=True, no_network=True)
    except TypeError:
        parser = etree.XMLParser()

    if parser is not None:
        try:
            root = etree.fromstring(text.encode("utf-8", "replace"), parser=parser)
            _validate_fb2_root(root)
            return root
        except Exception:
            pass

    try:
        root = etree.fromstring(text.encode("utf-8", "replace"))
    except Exception as err:
        raise FB2ParseError("Failed to parse FB2 XML payload.") from err
    _validate_fb2_root(root)
    return root


def _validate_fb2_root(root) -> None:
    """
    Perform the format-specific validate fb2 root operation used by the metadata reader or writer.

    Example:
        Exercise  validate fb2 root with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param root: XML or metadata node used as the operation context.
    :return: Parsed, normalized or updated value described above.
    """
    if root is None or _localname(getattr(root, "tag", "")).lower() != "fictionbook":
        raise FB2ParseError("FB2 XML payload does not look like a FictionBook document.")


def _serialize_root(root) -> bytes:
    """
    Serialize root into the binary or XML representation required by the container.

    Example:
        Exercise  serialize root with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param root: XML or metadata node used as the operation context.
    :return: Parsed, normalized or updated value described above.
    """
    xml_body = etree.tostring(root, method="xml", encoding="utf-8", xml_declaration=False)
    return b'<?xml version="1.0" encoding="UTF-8"?>\n' + _ensure_bytes(xml_body)


def XLINK(tag: str) -> str:
    """
    Perform the format-specific XLINK operation used by the metadata reader or writer.

    Example:
        Exercise XLINK with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param tag: Value supplied for tag.
    :return: Parsed, normalized or updated value described above.
    """
    return f"{{{NAMESPACES['xlink']}}}{tag}"


class Context:
    """
    Bind namespace-aware FictionBook XML creation and cleanup helpers to one root element.

    Example:
        Exercise Context with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py
    """

    def __init__(self, root):
        """
        Implement init for the format helper while retaining caller-owned underlying objects.

        Example:
            Exercise Context.  init   with pytest::

                python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


        :param root: XML or metadata node used as the operation context.
        :return: None.
        """
        self.fb_ns = _namespace_from_tag(getattr(root, "tag", "")) or NAMESPACES["fb2"]
        self.namespaces = {
            "fb": self.fb_ns,
            "fb2": self.fb_ns,
            "xlink": NAMESPACES["xlink"],
        }

    def tag(self, local_name: str) -> str:
        """
        Perform the namespace-aware FictionBook tag operation for the bound XML root.

        Example:
            Exercise Context.tag with pytest::

                python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


        :param local_name: Value supplied for local name.
        :return: Parsed, normalized or updated value described above.
        """
        return f"{{{self.fb_ns}}}{local_name}"

    def create_tag(self, parent, tag: str, attribs: dict[str, str] | None = None, at_start: bool = True):
        """
        Create tag in the container's required binary or XML form.

        Example:
            Exercise Context.create tag with pytest::

                python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


        :param parent: XML or metadata node used as the operation context.
        :param tag: Value supplied for tag.
        :param attribs: Value supplied for attribs.
        :param at_start: Value supplied for at start.
        :return: Parsed, normalized or updated value described above.
        """
        elem = etree.Element(self.tag(tag))
        if attribs:
            elem.attrib.update(attribs)
        if at_start:
            parent.insert(0, elem)
        else:
            parent.append(elem)
        return elem

    def get_or_create(self, parent, tag: str, attribs: dict[str, str] | None = None, at_start: bool = True):
        """
        Return or create derived from the current parser or container state.

        Example:
            Exercise Context.get or create with pytest::

                python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


        :param parent: XML or metadata node used as the operation context.
        :param tag: Value supplied for tag.
        :param attribs: Value supplied for attribs.
        :param at_start: Value supplied for at start.
        :return: Parsed, normalized or updated value described above.
        """
        attribs = dict(attribs or {})
        for child in _iter_children_local(parent, tag):
            if all(child.attrib.get(k) == v for k, v in attribs.items()):
                return child
        return self.create_tag(parent, tag, attribs=attribs, at_start=at_start)

    def clear_meta_tags(self, root, tag: str):
        """
        Perform the namespace-aware FictionBook clear meta tags operation for the bound XML root.

        Example:
            Exercise Context.clear meta tags with pytest::

                python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


        :param root: XML or metadata node used as the operation context.
        :param tag: Value supplied for tag.
        :return: Parsed, normalized or updated value described above.
        """
        for parent_name in ("title-info", "src-title-info", "publish-info"):
            for parent in _iter_descendants_local(root, parent_name):
                for child in list(parent):
                    if _localname(getattr(child, "tag", "")) == tag:
                        parent.remove(child)

    def text2fb2(self, parent, text: str):
        """
        Perform the namespace-aware FictionBook text2fb2 operation for the bound XML root.

        Example:
            Exercise Context.text2fb2 with pytest::

                python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


        :param parent: XML or metadata node used as the operation context.
        :param text: Value supplied for text.
        :return: Parsed, normalized or updated value described above.
        """
        for line in clean_xml_chars(str(text or "")).splitlines():
            clean = line.strip()
            if clean:
                p = self.create_tag(parent, "p", at_start=False)
                p.text = clean
            else:
                self.create_tag(parent, "empty-line", at_start=False)


def _parse_author(author_elem) -> str:
    """
    Parse author and apply supported values without inventing absent metadata.

    Example:
        Exercise  parse author with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param author_elem: XML or metadata node used as the operation context.
    :return: Parsed, normalized or updated value described above.
    """
    def _child_text(local_name: str) -> str:
        """
        Perform the format-specific child text operation used by the metadata reader or writer.

        Example:
            Exercise  parse author. child text with pytest::

                python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


        :param local_name: Value supplied for local name.
        :return: Parsed, normalized or updated value described above.
        """
        child = _first_child_local(author_elem, local_name)
        if child is None:
            return ""
        return _normalize_text("".join(child.itertext()))

    first_name = _child_text("first-name")
    middle_name = _child_text("middle-name")
    last_name = _child_text("last-name")

    parts = [p for p in (first_name, middle_name, last_name) if p]
    if parts:
        return " ".join(parts)

    nickname = _first_child_local(author_elem, "nickname")
    if nickname is not None:
        ntext = _normalize_text("".join(nickname.itertext()))
        if ntext:
            return ntext

    return ""


def _parse_authors(root) -> list[str]:
    """
    Parse authors and apply supported values without inventing absent metadata.

    Example:
        Exercise  parse authors with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param root: XML or metadata node used as the operation context.
    :return: Parsed, normalized or updated value described above.
    """
    for section_name in ("title-info", "src-title-info", "document-info"):
        authors: list[str] = []
        for section in _iter_sections(root, section_name):
            for au in _iter_children_local(section, "author"):
                author = _parse_author(au)
                if author:
                    authors.append(author)
        if authors:
            return authors
    return [_("Unknown")]


def _parse_book_title(root) -> str | None:
    """
    Parse book title and apply supported values without inventing absent metadata.

    Example:
        Exercise  parse book title with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param root: XML or metadata node used as the operation context.
    :return: Parsed, normalized or updated value described above.
    """
    for section_name in ("title-info", "publish-info", "src-title-info"):
        title = _first_text_from_section(root, section_name, "book-title")
        if title:
            return title
    return None


def _parse_cover(root, mi) -> None:
    """
    Parse cover and apply supported values without inventing absent metadata.

    Example:
        Exercise  parse cover with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param root: XML or metadata node used as the operation context.
    :param mi: Metadata object supplying or receiving supported fields.
    :return: Parsed, normalized or updated value described above.
    """
    img_id = None
    for cover in _iter_descendants_local(root, "coverpage"):
        image = _first_child_local(cover, "image")
        href = _get_xlink_href(image)
        if not href:
            continue
        img_id = href[1:] if href.startswith("#") else href
        if img_id:
            break

    if not img_id:
        return

    binary_elem = None
    for candidate in _iter_descendants_local(root, "binary"):
        if str(candidate.attrib.get("id", "")) == img_id:
            binary_elem = candidate
            break

    if binary_elem is None:
        return

    encoded = binary_elem.text
    if not encoded:
        return

    try:
        payload = base64_decode(encoded.strip())
    except Exception:
        try:
            payload = base64.b64decode(encoded.strip())
        except Exception:
            return

    mime_type = str(binary_elem.attrib.get("content-type", "") or "").strip().lower()
    fmt = ""
    if identify is not None:
        try:
            detected = identify(payload)[0]
            if detected:
                fmt = str(detected).lower()
        except Exception:
            pass

    if not fmt and mime_type:
        guessed = guess_all_extensions(mime_type)
        if guessed:
            fmt = guessed[0].lstrip(".").lower()

    if not fmt:
        guessed_mime = guess_type(img_id)[0]
        if guessed_mime:
            guessed_exts = guess_all_extensions(guessed_mime)
            if guessed_exts:
                fmt = guessed_exts[0].lstrip(".").lower()

    mi.cover_data = (fmt or "jpeg", payload)


def _parse_tags(root, mi) -> None:
    """
    Parse tags and apply supported values without inventing absent metadata.

    Example:
        Exercise  parse tags with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param root: XML or metadata node used as the operation context.
    :param mi: Metadata object supplying or receiving supported fields.
    :return: Parsed, normalized or updated value described above.
    """
    for section_name in ("title-info", "src-title-info"):
        tags: list[str] = []
        for section in _iter_sections(root, section_name):
            for genre in _iter_children_local(section, "genre"):
                text = _normalize_text("".join(genre.itertext()))
                if text:
                    tags.append(text)
        if tags:
            # Keep deterministic order while removing duplicates.
            seen = set()
            uniq = []
            for tag in tags:
                if tag not in seen:
                    seen.add(tag)
                    uniq.append(tag)
            mi.tags = uniq
            return


def _parse_series(root, mi) -> None:
    """
    Parse series and apply supported values without inventing absent metadata.

    Example:
        Exercise  parse series with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param root: XML or metadata node used as the operation context.
    :param mi: Metadata object supplying or receiving supported fields.
    :return: Parsed, normalized or updated value described above.
    """
    for section_name in ("title-info", "publish-info"):
        for section in _iter_sections(root, section_name):
            sequence = _first_child_local(section, "sequence")
            if sequence is None:
                continue
            name = _normalize_text(sequence.attrib.get("name"))
            if not name:
                continue
            mi.series = name
            si = _safe_float(sequence.attrib.get("number"))
            if si is not None:
                mi.series_index = si
            return


def _parse_isbn(root, mi) -> None:
    """
    Parse isbn and apply supported values without inventing absent metadata.

    Example:
        Exercise  parse isbn with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param root: XML or metadata node used as the operation context.
    :param mi: Metadata object supplying or receiving supported fields.
    :return: Parsed, normalized or updated value described above.
    """
    isbn = _first_text_from_section(root, "publish-info", "isbn")
    if not isbn:
        return
    isbn = isbn.split(",", 1)[0].strip()
    checked = check_isbn(isbn)
    if checked:
        mi.isbn = checked


def _parse_comments(root, mi) -> None:
    """
    Parse comments and apply supported values without inventing absent metadata.

    Example:
        Exercise  parse comments with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param root: XML or metadata node used as the operation context.
    :param mi: Metadata object supplying or receiving supported fields.
    :return: Parsed, normalized or updated value described above.
    """
    for section_name in ("title-info", "src-title-info"):
        for section in _iter_sections(root, section_name):
            annotation = _first_child_local(section, "annotation")
            if annotation is None:
                continue
            text = _annotation_to_text(annotation)
            if text:
                mi.comments = text
            return


def _parse_publisher(root, mi) -> None:
    """
    Parse publisher and apply supported values without inventing absent metadata.

    Example:
        Exercise  parse publisher with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param root: XML or metadata node used as the operation context.
    :param mi: Metadata object supplying or receiving supported fields.
    :return: Parsed, normalized or updated value described above.
    """
    publisher = _first_text_from_section(root, "publish-info", "publisher")
    if publisher:
        mi.publisher = publisher


def _parse_pubdate(root, mi) -> None:
    """
    Parse pubdate and apply supported values without inventing absent metadata.

    Example:
        Exercise  parse pubdate with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param root: XML or metadata node used as the operation context.
    :param mi: Metadata object supplying or receiving supported fields.
    :return: Parsed, normalized or updated value described above.
    """
    year = _safe_int(_first_text_from_section(root, "publish-info", "year"))
    if year is None:
        return
    # FB2 usually stores only publication year.
    try:
        mi.pubdate = datetime.date(year, 6, 2)
    except Exception:
        pass


def _parse_language(root, mi) -> None:
    """
    Parse language and apply supported values without inventing absent metadata.

    Example:
        Exercise  parse language with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param root: XML or metadata node used as the operation context.
    :param mi: Metadata object supplying or receiving supported fields.
    :return: Parsed, normalized or updated value described above.
    """
    language = _first_text_from_section(root, "title-info", "lang")
    if not language:
        return
    mi.language = language
    mi.languages = [language]


def _set_title(root, title_info, mi, ctx: Context, apply_null: bool = False) -> None:
    """
    Set title under the supplied null and replacement policy.

    Example:
        Exercise  set title with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param root: XML or metadata node used as the operation context.
    :param title_info: XML or metadata node used as the operation context.
    :param mi: Metadata object supplying or receiving supported fields.
    :param ctx: Value supplied for ctx.
    :param apply_null: Clear supported destination fields when the source metadata marks
        them null.
    :return: None.
    """
    should_clear = apply_null or not _is_null_field(mi, "title")
    if not should_clear:
        return
    ctx.clear_meta_tags(root, "book-title")

    if _is_null_field(mi, "title"):
        return

    title = ctx.get_or_create(title_info, "book-title")
    title.text = _normalize_text(getattr(mi, "title", ""))


def _set_comments(root, title_info, mi, ctx: Context, apply_null: bool = False) -> None:
    """
    Set comments under the supplied null and replacement policy.

    Example:
        Exercise  set comments with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param root: XML or metadata node used as the operation context.
    :param title_info: XML or metadata node used as the operation context.
    :param mi: Metadata object supplying or receiving supported fields.
    :param ctx: Value supplied for ctx.
    :param apply_null: Clear supported destination fields when the source metadata marks
        them null.
    :return: None.
    """
    should_clear = apply_null or not _is_null_field(mi, "comments")
    if not should_clear:
        return
    ctx.clear_meta_tags(root, "annotation")

    if _is_null_field(mi, "comments"):
        return

    annotation = ctx.get_or_create(title_info, "annotation")
    plain = _htmlish_to_text(_first_metadata_text(getattr(mi, "comments", "")))
    ctx.text2fb2(annotation, plain)


def _set_authors(root, title_info, mi, ctx: Context, apply_null: bool = False) -> None:
    """
    Set authors under the supplied null and replacement policy.

    Example:
        Exercise  set authors with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param root: XML or metadata node used as the operation context.
    :param title_info: XML or metadata node used as the operation context.
    :param mi: Metadata object supplying or receiving supported fields.
    :param ctx: Value supplied for ctx.
    :param apply_null: Clear supported destination fields when the source metadata marks
        them null.
    :return: None.
    """
    should_clear = apply_null or not _is_null_field(mi, "authors")
    if not should_clear:
        return
    ctx.clear_meta_tags(root, "author")

    if _is_null_field(mi, "authors"):
        return

    authors = [
        _normalize_text(author)
        for author in _metadata_values(getattr(mi, "authors", None))
        if _normalize_text(author)
    ]

    for author in reversed(authors):
        parts = author.split()
        if not parts:
            continue
        author_tag = ctx.create_tag(title_info, "author")
        if len(parts) == 1:
            ctx.create_tag(author_tag, "nickname").text = author
            continue
        ctx.create_tag(author_tag, "first-name").text = parts[0]
        tail = parts[1:]
        if len(tail) > 1:
            ctx.create_tag(author_tag, "middle-name", at_start=False).text = tail[0]
            tail = tail[1:]
        if tail:
            ctx.create_tag(author_tag, "last-name", at_start=False).text = " ".join(tail)


def _set_publisher(root, publish_info, mi, ctx: Context, apply_null: bool = False) -> None:
    """
    Set publisher under the supplied null and replacement policy.

    Example:
        Exercise  set publisher with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param root: XML or metadata node used as the operation context.
    :param publish_info: XML or metadata node used as the operation context.
    :param mi: Metadata object supplying or receiving supported fields.
    :param ctx: Value supplied for ctx.
    :param apply_null: Clear supported destination fields when the source metadata marks
        them null.
    :return: None.
    """
    should_clear = apply_null or not _is_null_field(mi, "publisher")
    if not should_clear:
        return
    ctx.clear_meta_tags(root, "publisher")

    if _is_null_field(mi, "publisher"):
        return

    publisher = _first_metadata_text(getattr(mi, "publisher", ""))
    if not publisher:
        return
    tag = ctx.create_tag(publish_info, "publisher")
    tag.text = publisher


def _set_pubdate(root, publish_info, mi, ctx: Context, apply_null: bool = False) -> None:
    """
    Set pubdate under the supplied null and replacement policy.

    Example:
        Exercise  set pubdate with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param root: XML or metadata node used as the operation context.
    :param publish_info: XML or metadata node used as the operation context.
    :param mi: Metadata object supplying or receiving supported fields.
    :param ctx: Value supplied for ctx.
    :param apply_null: Clear supported destination fields when the source metadata marks
        them null.
    :return: None.
    """
    should_clear = apply_null or not _is_null_field(mi, "pubdate")
    if not should_clear:
        return
    ctx.clear_meta_tags(root, "year")

    if _is_null_field(mi, "pubdate"):
        return

    year = None
    pubdate = getattr(mi, "pubdate", None)
    if pubdate is not None:
        year = getattr(pubdate, "year", None)
    if year is None:
        year = _safe_int(_first_metadata_text(pubdate))
    if year is None:
        return

    tag = ctx.create_tag(publish_info, "year")
    tag.text = str(year)


def _set_tags(root, title_info, mi, ctx: Context, apply_null: bool = False) -> None:
    """
    Set tags under the supplied null and replacement policy.

    Example:
        Exercise  set tags with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param root: XML or metadata node used as the operation context.
    :param title_info: XML or metadata node used as the operation context.
    :param mi: Metadata object supplying or receiving supported fields.
    :param ctx: Value supplied for ctx.
    :param apply_null: Clear supported destination fields when the source metadata marks
        them null.
    :return: None.
    """
    should_clear = apply_null or not _is_null_field(mi, "tags")
    if not should_clear:
        return
    ctx.clear_meta_tags(root, "genre")

    if _is_null_field(mi, "tags"):
        return

    tags = [
        _normalize_text(tag)
        for tag in _metadata_values(getattr(mi, "tags", None))
        if _normalize_text(tag)
    ]
    for tag in tags:
        tag_elem = ctx.create_tag(title_info, "genre")
        tag_elem.text = tag


def _set_series(root, title_info, mi, ctx: Context, apply_null: bool = False) -> None:
    """
    Set series under the supplied null and replacement policy.

    Example:
        Exercise  set series with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param root: XML or metadata node used as the operation context.
    :param title_info: XML or metadata node used as the operation context.
    :param mi: Metadata object supplying or receiving supported fields.
    :param ctx: Value supplied for ctx.
    :param apply_null: Clear supported destination fields when the source metadata marks
        them null.
    :return: None.
    """
    should_clear = apply_null or not _is_null_field(mi, "series")
    if not should_clear:
        return
    ctx.clear_meta_tags(root, "sequence")

    if _is_null_field(mi, "series"):
        return

    seq = ctx.get_or_create(title_info, "sequence")
    seq.set("name", _first_metadata_text(getattr(mi, "series", "")))

    series_index = getattr(mi, "series_index", None)
    value = _safe_float(series_index)
    seq.set("number", "1" if value is None else f"{value:g}")


def _rnd_name(size: int = 8, chars: str = ascii_letters + digits) -> str:
    """
    Perform the format-specific rnd name operation used by the metadata reader or writer.

    Example:
        Exercise  rnd name with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param size: Value supplied for size.
    :param chars: Value supplied for chars.
    :return: Parsed, normalized or updated value described above.
    """
    return "".join(random.choice(chars) for _ in range(size))


def _rnd_pic_file_name(prefix: str = "calibre_cover_", size: int = 32, ext: str = "jpg") -> str:
    """
    Perform the format-specific rnd pic file name operation used by the metadata reader or writer.

    Example:
        Exercise  rnd pic file name with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param prefix: Value supplied for prefix.
    :param size: Value supplied for size.
    :param ext: Value supplied for ext.
    :return: Parsed, normalized or updated value described above.
    """
    return prefix + _rnd_name(size=size) + "." + ext


def _set_cover(root, title_info, mi, ctx: Context) -> None:
    """
    Set cover under the supplied null and replacement policy.

    Example:
        Exercise  set cover with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param root: XML or metadata node used as the operation context.
    :param title_info: XML or metadata node used as the operation context.
    :param mi: Metadata object supplying or receiving supported fields.
    :param ctx: Value supplied for ctx.
    :return: None.
    """
    cover = _extract_cover_payload(mi)
    if cover is None:
        return

    _input_fmt, payload = cover
    payload = _coerce_cover_to_jpeg_bytes(payload)
    fmt = _cover_format_from_payload("jpeg", payload)

    coverpage = ctx.get_or_create(title_info, "coverpage")
    image_tag = ctx.get_or_create(coverpage, "image")

    if XLINK("href") in image_tag.attrib:
        filename = str(image_tag.attrib[XLINK("href")]).lstrip("#")
    else:
        ext = "jpg" if fmt in {"jpeg", "jpg"} else (fmt or "jpg")
        filename = _rnd_pic_file_name("cover", ext=ext)
        image_tag.attrib[XLINK("href")] = f"#{filename}"

    binary_tag = ctx.get_or_create(root, "binary", attribs={"id": filename}, at_start=False)
    binary_tag.attrib["content-type"] = f"image/{'jpeg' if fmt == 'jpg' else fmt}"
    binary_tag.text = base64.b64encode(payload).decode("ascii")


def _build_metadata_shell(target: Any) -> MetaInformation:
    """
    Build a minimally usable metadata object for missing or explicitly tolerated malformed input.

    Example:
        Exercise  build metadata shell with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param target: Caller-supplied path, path-like object or stream described by this
        operation.
    :return: Parsed, normalized or updated value described above.
    """
    return MetaInformation(_source_title(target), [_("Unknown")])


def get_metadata(stream_or_path, *, fallback_on_parse_error: bool = False):
    """
    Read metadata from the supported path or stream input while applying the module's ownership and fallback policy.

    Example:
        Exercise get metadata with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param stream_or_path: Caller-supplied path, path-like object or stream described by
        this operation.
    :param fallback_on_parse_error: Return default metadata after parse errors when
        true; otherwise raise the format error.
    :return: Parsed, normalized or updated value described above.
    """
    if _is_path_like(stream_or_path):
        with open(stream_or_path, "rb") as stream:
            return get_metadata(stream, fallback_on_parse_error=fallback_on_parse_error)

    stream = stream_or_path
    if not hasattr(stream, "read"):
        raise TypeError("FB2 metadata reader expects a filesystem path or readable binary stream.")

    source = _source_name(stream)
    pos = None
    if hasattr(stream, "tell"):
        try:
            pos = stream.tell()
        except Exception:
            pos = None

    mi = _build_metadata_shell(stream)
    try:
        if hasattr(stream, "seek"):
            try:
                stream.seek(0)
            except Exception:
                pass

        container_bytes = _ensure_bytes(stream.read())
        payload, _zip_member = _extract_fb2_payload(container_bytes, source_name=source)
        root = _parse_fb2_root(payload)

        title = _parse_book_title(root)
        authors = _parse_authors(root)
        if title:
            mi = MetaInformation(title, authors or [_("Unknown")])

        for parser in (
            _parse_cover,
            _parse_comments,
            _parse_tags,
            _parse_series,
            _parse_isbn,
            _parse_publisher,
            _parse_pubdate,
            _parse_language,
        ):
            try:
                parser(root, mi)
            except Exception as err:
                default_log.log_exception(
                    "FB2 metadata parser step failed.",
                    err,
                    "DEBUG",
                    ("source", source),
                    ("parser", parser.__name__),
                )

        return mi
    except Exception as err:
        default_log.log_exception(
            "Failed to extract metadata from FB2 source.",
            err,
            "DEBUG",
            ("source", source),
        )
        if not fallback_on_parse_error:
            if isinstance(err, FB2ParseError):
                raise
            raise FB2ParseError("Failed to extract metadata from FB2 source.") from err
        return mi
    finally:
        if pos is not None and hasattr(stream, "seek"):
            try:
                stream.seek(pos)
            except Exception:
                pass


def get_metadata_inplace(target_fb2_path):
    """
    Read metadata through the path-oriented adapter used by registry plugins that support in-place access.

    Example:
        Exercise get metadata inplace with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param target_fb2_path: Caller-supplied path, path-like object or stream described
        by this operation.
    :return: Parsed, normalized or updated value described above.
    """
    return get_metadata(target_fb2_path)


def set_metadata(stream_or_path, mi, apply_null: bool = False, update_timestamp: bool = False):
    """
    Write supported metadata fields to a path or mutable binary stream without taking ownership of caller-supplied streams.

    Example:
        Exercise set metadata with pytest::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param stream_or_path: Caller-supplied path, path-like object or stream described by
        this operation.
    :param mi: Metadata object supplying or receiving supported fields.
    :param apply_null: Clear supported destination fields when the source metadata marks
        them null.
    :param update_timestamp: Refresh the package modification timestamp when true.
    :return: None.
    """
    del update_timestamp

    if _is_path_like(stream_or_path):
        with open(stream_or_path, "r+b") as stream:
            return set_metadata(stream, mi, apply_null=apply_null, update_timestamp=False)

    stream = stream_or_path
    if not hasattr(stream, "read") or not hasattr(stream, "write"):
        raise TypeError("FB2 metadata writer expects a path or read/write binary stream.")

    source = _source_name(stream)

    try:
        if hasattr(stream, "seek"):
            stream.seek(0)

        raw_container = _ensure_bytes(stream.read())
        payload, zip_member = _extract_fb2_payload(raw_container, source_name=source)
        root = _parse_fb2_root(payload)

        ctx = Context(root)
        desc = ctx.get_or_create(root, "description")
        title_info = ctx.get_or_create(desc, "title-info")
        publish_info = ctx.get_or_create(desc, "publish-info")

        indent = title_info.text

        _set_comments(root, title_info, mi, ctx, apply_null=apply_null)
        _set_series(root, title_info, mi, ctx, apply_null=apply_null)
        _set_tags(root, title_info, mi, ctx, apply_null=apply_null)
        _set_authors(root, title_info, mi, ctx, apply_null=apply_null)
        _set_title(root, title_info, mi, ctx, apply_null=apply_null)
        _set_publisher(root, publish_info, mi, ctx, apply_null=apply_null)
        _set_pubdate(root, publish_info, mi, ctx, apply_null=apply_null)
        _set_cover(root, title_info, mi, ctx)

        if indent is not None:
            for child in title_info:
                child.tail = indent

        serialized = _serialize_root(root)

        if zip_member:
            safe_replace(stream, zip_member, BytesIO(serialized), add_missing=True)
        else:
            stream.seek(0)
            stream.truncate()
            stream.write(serialized)

        if hasattr(stream, "seek"):
            stream.seek(0)
    except Exception as err:
        default_log.log_exception(
            "Failed to write metadata to FB2 source.",
            err,
            "ERROR",
            ("source", source),
        )
        raise


__all__ = [
    "VALID_FOR",
    "PRIORITY_FOR",
    "RUN_COST",
    "FB2ParseError",
    "get_metadata",
    "get_metadata_inplace",
    "set_metadata",
]
