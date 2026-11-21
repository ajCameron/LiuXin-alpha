"""
Read and write ODT package metadata, user-defined OPF fields and cover candidates while preserving caller-owned stream positions.

The module keeps malformed-input, optional dependency and resource ownership
behavior explicit for registry callers.

Example:
    Exercise odt with pytest::

        python -m pytest -q tests/metadata/file_sources/test_odt_metadata_source.py
"""
from __future__ import annotations

import io
import json
import os
import re
import zipfile
from typing import Any, Iterable

from lxml import etree as _lxml_etree  # type: ignore[import-untyped]

from LiuXin_alpha.file_formats.odf.draw import Frame as ODFFrame
from LiuXin_alpha.file_formats.odf.draw import Image as ODFImage
from LiuXin_alpha.file_formats.odf.namespaces import DCNS, METANS, OFFICENS
from LiuXin_alpha.file_formats.odf.opendocument import load as od_load
from LiuXin_alpha.metadata.book.base import calibreMetaInformation
from LiuXin_alpha.metadata.utils import authors_to_string, check_isbn, string_to_authors
from LiuXin_alpha.utils.image_tools.imghdr import identify
from LiuXin_alpha.utils.libraries.calibre_zipfile import safe_replace
from LiuXin_alpha.utils.libraries.cleantext import clean_xml_chars
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.libraries.liuxin_etree import etree

__all__ = ["OdtFormatError", "get_metadata", "get_metadata_inplace", "set_metadata"]


class OdtFormatError(ValueError):
    """
    Signal unreadable ODT package metadata or malformed ODT XML.

    Example:
        Exercise OdtFormatError with pytest::

            python -m pytest -q tests/metadata/file_sources/test_odt_metadata_source.py
    """
    pass


_WHITESPACE = re.compile(r"\s+")
_SPLIT_TAGS = re.compile(r"[;,]")


def _normalize_text(raw: str | None) -> str:
    """
    Collapse whitespace and trim a possibly absent metadata text value.

    Example:
        Exercise  normalize text with pytest::

            python -m pytest -q tests/metadata/file_sources/test_odt_metadata_source.py


    :param raw: Raw value or payload to normalize, parse or serialize.
    :return: Parsed, normalized or serialized value described above.
    """
    if not raw:
        return ""
    return _WHITESPACE.sub(" ", raw).strip()


def _read_source_bytes(stream_or_path) -> bytes:
    """
    Read the complete source payload from bytes, a path or a stream and restore a caller-owned stream position when available.

    Example:
        Exercise  read source bytes with pytest::

            python -m pytest -q tests/metadata/file_sources/test_odt_metadata_source.py


    :param stream_or_path: Caller-supplied path, path-like object or stream described by
        this operation.
    :return: Parsed, normalized or serialized value described above.
    """
    if hasattr(stream_or_path, "read"):
        stream = stream_or_path
        pos = None
        if hasattr(stream, "tell"):
            try:
                pos = stream.tell()
            except Exception:
                pos = None
        try:
            if hasattr(stream, "seek"):
                stream.seek(0)
        except Exception:
            pass
        data = stream.read()
        if pos is not None and hasattr(stream, "seek"):
            try:
                stream.seek(pos)
            except Exception:
                pass
        if isinstance(data, str):
            data = data.encode("utf-8", "replace")
        return bytes(data)

    if isinstance(stream_or_path, (bytes, bytearray)):
        return bytes(stream_or_path)

    if isinstance(stream_or_path, os.PathLike):
        stream_or_path = os.fspath(stream_or_path)

    if isinstance(stream_or_path, str):
        with open(stream_or_path, "rb") as f:
            return f.read()

    raise TypeError("ODT metadata reader expects a binary stream, bytes, or a filesystem path.")


def _get_source_title(stream_or_path) -> str:
    """
    Derive a filename-based title from a path or named stream without reading content.

    Example:
        Exercise  get source title with pytest::

            python -m pytest -q tests/metadata/file_sources/test_odt_metadata_source.py


    :param stream_or_path: Caller-supplied path, path-like object or stream described by
        this operation.
    :return: Parsed, normalized or serialized value described above.
    """
    name = getattr(stream_or_path, "name", None)
    if isinstance(name, str) and name:
        return os.path.splitext(os.path.basename(name))[0]
    if isinstance(stream_or_path, os.PathLike):
        return os.path.splitext(os.path.basename(os.fspath(stream_or_path)))[0]
    if isinstance(stream_or_path, str):
        return os.path.splitext(os.path.basename(stream_or_path))[0]
    return ""


def _iter_ns_text(root, namespace: str, local_name: str) -> Iterable[str]:
    """
    Return ns text in deterministic source or registry order.

    Example:
        Exercise  iter ns text with pytest::

            python -m pytest -q tests/metadata/file_sources/test_odt_metadata_source.py


    :param root: Parsed XML, PDF or metadata node used as the operation context.
    :param namespace: Name, type or encoding selector used for lookup or interpretation.
    :param local_name: Name, type or encoding selector used for lookup or
        interpretation.
    :return: Parsed, normalized or serialized value described above.
    """
    tag = "{%s}%s" % (namespace, local_name)
    for elem in root.iter(tag):
        text = _normalize_text("".join(elem.itertext()))
        if text:
            yield text


def _first_ns_text(root, namespace: str, local_name: str) -> str | None:
    """
    Return the first usable ns text under fallback policy.

    Example:
        Exercise  first ns text with pytest::

            python -m pytest -q tests/metadata/file_sources/test_odt_metadata_source.py


    :param root: Parsed XML, PDF or metadata node used as the operation context.
    :param namespace: Name, type or encoding selector used for lookup or interpretation.
    :param local_name: Name, type or encoding selector used for lookup or
        interpretation.
    :return: Parsed, normalized or serialized value described above.
    """
    for text in _iter_ns_text(root, namespace, local_name):
        return text
    return None


def _split_tags(raw: str) -> list[str]:
    """
    Perform the format-specific split tags operation used by this metadata source.

    Example:
        Exercise  split tags with pytest::

            python -m pytest -q tests/metadata/file_sources/test_odt_metadata_source.py


    :param raw: Raw value or payload to normalize, parse or serialize.
    :return: Parsed, normalized or serialized value described above.
    """
    return [x for x in (_normalize_text(p) for p in _SPLIT_TAGS.split(raw)) if x]


def _stable_dedupe(items: Iterable[str]) -> list[str]:
    """
    Remove duplicate strings while preserving their first-seen order.

    Example:
        Exercise  stable dedupe with pytest::

            python -m pytest -q tests/metadata/file_sources/test_odt_metadata_source.py


    :param items: Ordered input values processed by this operation.
    :return: Parsed, normalized or serialized value described above.
    """
    seen = set()
    ans: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            ans.append(item)
    return ans


def _parse_series_index(raw: str | None) -> float | None:
    """
    Parse a decimal series index, accepting locale commas and returning None for invalid input.

    Example:
        Exercise  parse series index with pytest::

            python -m pytest -q tests/metadata/file_sources/test_odt_metadata_source.py


    :param raw: Raw value or payload to normalize, parse or serialize.
    :return: Parsed, normalized or serialized value described above.
    """
    if not raw:
        return None
    try:
        return float(raw)
    except Exception:
        # Some ODT producers use locale commas for decimals.
        try:
            return float(raw.replace(",", "."))
        except Exception:
            return None


def _parse_bool(raw: str | None, default: bool = False) -> bool:
    """
    Parse conventional textual Boolean values and retain the supplied default for unknown input.

    Example:
        Exercise  parse bool with pytest::

            python -m pytest -q tests/metadata/file_sources/test_odt_metadata_source.py


    :param raw: Raw value or payload to normalize, parse or serialize.
    :param default: Policy flag controlling the behavior described above.
    :return: Parsed, normalized or serialized value described above.
    """
    if raw is None:
        return default
    val = raw.strip().lower()
    if val in {"1", "true", "yes", "on"}:
        return True
    if val in {"0", "false", "no", "off"}:
        return False
    return default


def _read_user_defined(root) -> dict[str, str]:
    """
    Collect named ODT user-defined properties using normalized lowercase keys.

    Example:
        Exercise  read user defined with pytest::

            python -m pytest -q tests/metadata/file_sources/test_odt_metadata_source.py


    :param root: Parsed XML, PDF or metadata node used as the operation context.
    :return: Parsed, normalized or serialized value described above.
    """
    ans: dict[str, str] = {}
    tag = "{%s}%s" % (METANS, "user-defined")
    name_attr = "{%s}%s" % (METANS, "name")
    for elem in root.iter(tag):
        name = _normalize_text(elem.attrib.get(name_attr) or elem.attrib.get("name"))
        if not name:
            continue
        ans[name.lower()] = _normalize_text("".join(elem.itertext()))
    return ans


def _parse_xml_bytes(raw_xml: bytes):
    """
    Parse xml bytes without inventing absent metadata values.

    Example:
        Exercise  parse xml bytes with pytest::

            python -m pytest -q tests/metadata/file_sources/test_odt_metadata_source.py


    :param raw_xml: Raw value or payload to normalize, parse or serialize.
    :return: Parsed, normalized or serialized value described above.
    """
    try:
        return etree.fromstring(raw_xml)
    except Exception:
        # Keep metadata extraction resilient on mildly malformed XML.
        try:
            parser = etree.XMLParser(recover=True)
            return etree.fromstring(raw_xml, parser=parser)
        except Exception as e:
            raise OdtFormatError("Failed to parse ODT XML metadata") from e


def _default_metadata(source_title: str = ""):
    """
    Build minimally usable metadata for missing or explicitly tolerated malformed input.

    Example:
        Exercise  default metadata with pytest::

            python -m pytest -q tests/metadata/file_sources/test_odt_metadata_source.py


    :param source_title: Source or member label used for lookup, fallback titles or
        diagnostics.
    :return: Parsed, normalized or serialized value described above.
    """
    title = source_title or _("Unknown")
    mi = calibreMetaInformation(title, [_("Unknown")])
    try:
        mi.finalize()
    except Exception:
        pass
    return mi


def _read_meta_xml(raw_odt: bytes) -> bytes:
    """
    Return the meta.xml member from an ODT payload or raise the format-specific error.

    Example:
        Exercise  read meta xml with pytest::

            python -m pytest -q tests/metadata/file_sources/test_odt_metadata_source.py


    :param raw_odt: Value supplied for raw odt.
    :return: Parsed, normalized or serialized value described above.
    """
    try:
        with zipfile.ZipFile(io.BytesIO(raw_odt), "r") as zin:
            return zin.read("meta.xml")
    except Exception as e:
        raise OdtFormatError("Not a valid ODT file (missing readable meta.xml)") from e


def _extract_cover(raw_odt: bytes, mi, opf_meta: bool, extract_cover: bool) -> None:
    """
    Select an explicit OPF cover frame or a plausible first image and attach its data to metadata.

    Example:
        Exercise  extract cover with pytest::

            python -m pytest -q tests/metadata/file_sources/test_odt_metadata_source.py


    :param raw_odt: Value supplied for raw odt.
    :param mi: Metadata object supplying or receiving the supported fields.
    :param opf_meta: Value supplied for opf meta.
    :param extract_cover: Request cover discovery or cover payload extraction when true.
    :return: Parsed, normalized or serialized value described above.
    """
    try:
        zin = zipfile.ZipFile(io.BytesIO(raw_odt), "r")
    except Exception:
        return

    def _iter_frame_images_from_odf():
        """
        Return frame images from odf in deterministic source or registry order.

        Example:
            Exercise  extract cover. iter frame images from odf with pytest::

                python -m pytest -q tests/metadata/file_sources/test_odt_metadata_source.py


        :return: Parsed, normalized or serialized value described above.
        """
        otext = od_load(io.BytesIO(raw_odt))
        for frame in otext.topnode.getElementsByType(ODFFrame):
            imgs = frame.getElementsByType(ODFImage)
            if not imgs:
                continue
            href = imgs[0].getAttribute("href")
            if not href:
                continue
            frame_name = _normalize_text(frame.getAttribute("name") or "")
            yield frame_name, href

    def _iter_frame_images_from_content_xml():
        """
        Return frame images from content xml in deterministic source or registry order.

        Example:
            Exercise  extract cover. iter frame images from content xml with pytest::

                python -m pytest -q tests/metadata/file_sources/test_odt_metadata_source.py


        :return: Parsed, normalized or serialized value described above.
        """
        try:
            content_xml = zin.read("content.xml")
        except Exception:
            return
        try:
            root = etree.fromstring(content_xml)
        except Exception:
            try:
                parser = etree.XMLParser(recover=True)
                root = etree.fromstring(content_xml, parser=parser)
            except Exception:
                return
        for frame in root.iter():
            if not str(getattr(frame, "tag", "")).endswith("}frame"):
                continue
            frame_name = ""
            for k, v in frame.attrib.items():
                ks = str(k)
                if ks.endswith("}name") or ks == "name" or ks.endswith(":name"):
                    frame_name = _normalize_text(v)
                    break
            href = None
            for child in frame.iter():
                if not str(getattr(child, "tag", "")).endswith("}image"):
                    continue
                for k, v in child.attrib.items():
                    ks = str(k)
                    if ks.endswith("}href") or ks == "href" or ks.endswith(":href"):
                        href = _normalize_text(v)
                        break
                if href:
                    break
            if href:
                yield frame_name, href

    try:
        frame_images = list(_iter_frame_images_from_odf())
    except Exception:
        frame_images = []
    if not frame_images:
        frame_images = list(_iter_frame_images_from_content_xml())

    cover_href = None
    cover_data = None
    cover_frame = None
    imgnum = 0

    for frame_name, href in frame_images:
        try:
            raw = zin.read(href)
        except Exception:
            continue

        fmt, width, height = identify(raw)
        if not fmt:
            continue

        imgnum += 1

        if opf_meta and frame_name.lower() == "opf.cover":
            cover_href = href
            cover_data = (fmt, raw)
            cover_frame = frame_name
            break

        if cover_href is None and imgnum == 1 and width > 0 and height > 0:
            ratio = float(height) / float(width)
            if 0.8 <= ratio <= 1.8 and (height * width) >= 12000:
                cover_href = href
                cover_data = (fmt, raw)
                if not opf_meta:
                    break

    if cover_href:
        mi.cover = cover_href
        if cover_frame:
            mi.odf_cover_frame = cover_frame
        if extract_cover and cover_data:
            mi.cover_data = cover_data


def _set_language(mi, lang: str) -> None:
    """
    Canonicalize and assign a non-empty metadata language without failing on optional helpers.

    Example:
        Exercise  set language with pytest::

            python -m pytest -q tests/metadata/file_sources/test_odt_metadata_source.py


    :param mi: Metadata object supplying or receiving the supported fields.
    :param lang: Value supplied for lang.
    :return: None.
    """
    try:
        from LiuXin_alpha.utils.localization import canonicalize_lang

        cl = canonicalize_lang(lang) or lang
    except Exception:
        cl = lang
    if cl:
        mi.language = cl


def _parse_identifiers(raw: str | None) -> dict[str, str] | None:
    """
    Read the ``opf.identifiers`` property calibre writes: a JSON object of scheme and value.

    As in calibre, it replaces the identifiers read from elsewhere in the file.

    Example:
        >>> _parse_identifiers('{"isbn": "9780441013593", "": "x"}'), _parse_identifiers("[1]")
        ({'isbn': '9780441013593'}, None)


    :param raw: The property's text, or ``None`` where the file has none.
    :return: The identifiers, or ``None`` where there are none to read.
    """
    if not raw:
        return None
    try:
        value = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(value, dict):
        return None
    return {str(k): str(v) for k, v in value.items() if k and v not in (None, "")}


def _parse_rating(raw: str | None) -> float | None:
    """
    Read the ``opf.rating`` property calibre writes, held to calibre's range of 0 to 10.

    Example:
        >>> _parse_rating("8"), _parse_rating("1e+01"), _parse_rating("12"), _parse_rating("x")
        (8.0, 10.0, 10.0, None)


    :param raw: The property's text, or ``None`` where the file has none.
    :return: The rating, or ``None`` where there is none to read.
    """
    if not raw:
        return None
    try:
        return max(0.0, min(float(raw), 10.0))
    except ValueError:
        return None


def _read_identifiers_and_rating(mi, user_defined: dict[str, str]) -> None:
    """
    Read the ``opf.identifiers`` and ``opf.rating`` properties calibre writes into a record.

    Example:
        >>> mi = calibreMetaInformation("T", [])
        >>> _read_identifiers_and_rating(mi, {"opf.identifiers": '{"isbn": "9780441013593"}', "opf.rating": "8"})
        >>> mi.identifiers, mi.rating
        ({'isbn': '9780441013593'}, 8.0)


    :param mi: The record being read into.
    :param user_defined: The file's user-defined properties, by lower-case name.
    :return: None.
    """
    identifiers = _parse_identifiers(user_defined.get("opf.identifiers"))
    if identifiers is not None:
        mi.set_identifiers(identifiers)
    rating = _parse_rating(user_defined.get("opf.rating"))
    if rating is not None:
        mi.rating = rating


def get_metadata(stream, extract_cover: bool = True, *, fallback_on_parse_error: bool = False):
    """
    Read metadata from the supported path, bytes or stream input while applying module ownership and fallback policy.

    Example:
        Exercise get metadata with pytest::

            python -m pytest -q tests/metadata/file_sources/test_odt_metadata_source.py


    :param stream: Caller-supplied path, path-like object or stream described by this
        operation.
    :param extract_cover: Request cover discovery or cover payload extraction when true.
    :param fallback_on_parse_error: Return safe default metadata after parse errors when
        true; otherwise raise the format error.
    :return: Parsed, normalized or serialized value described above.
    """
    raw_odt = _read_source_bytes(stream)

    try:
        meta_xml = _read_meta_xml(raw_odt)
        root = _parse_xml_bytes(meta_xml)
    except Exception as e:
        if fallback_on_parse_error:
            default_log.log_exception(
                "Failed to read ODT metadata; returning fallback metadata.",
                e,
                "DEBUG",
                ("source", getattr(stream, "name", "<stream>")),
            )
            return _default_metadata(_get_source_title(stream))
        if isinstance(e, OdtFormatError):
            raise
        raise OdtFormatError("Failed to read ODT metadata") from e

    user_defined = _read_user_defined(root)

    opf_title = user_defined.get("opf.title")
    title = opf_title or _first_ns_text(root, DCNS, "title") or _get_source_title(stream) or _("Unknown")

    author_raw = (
        user_defined.get("opf.authors")
        or _first_ns_text(root, DCNS, "creator")
        or _first_ns_text(root, METANS, "initial-creator")
    )
    authors = string_to_authors(author_raw) if author_raw else [_("Unknown")]
    if not authors:
        authors = [_("Unknown")]

    mi = calibreMetaInformation(title, authors)

    author_sort = user_defined.get("opf.authorsort")
    if author_sort:
        mi.author_sort = author_sort

    title_sort = user_defined.get("opf.titlesort")
    if title_sort:
        mi.title_sort = title_sort

    comments = _first_ns_text(root, DCNS, "description")
    if comments:
        mi.comments = comments

    publisher = user_defined.get("opf.publisher") or _first_ns_text(root, DCNS, "publisher")
    if publisher:
        mi.publisher = publisher

    language = user_defined.get("opf.language") or _first_ns_text(root, DCNS, "language")
    if language:
        _set_language(mi, language)

    pubdate = user_defined.get("opf.pubdate") or _first_ns_text(root, DCNS, "date")
    if pubdate:
        try:
            from LiuXin_alpha.utils.date import parse_date

            mi.pubdate = parse_date(pubdate, assume_utc=True)
        except Exception:
            mi.pubdate = pubdate

    tags: list[str] = []
    opf_subject = user_defined.get("opf.subject")
    if opf_subject:
        tags.extend(_split_tags(opf_subject))
    else:
        for value in _iter_ns_text(root, DCNS, "subject"):
            tags.extend(_split_tags(value))
        for value in _iter_ns_text(root, METANS, "keyword"):
            tags.extend(_split_tags(value))
    tags = _stable_dedupe(tags)
    if tags:
        mi.tags = tags

    series = user_defined.get("opf.series") or user_defined.get("series")
    if series:
        mi.series = series
    series_index = (
        user_defined.get("opf.series_index")
        or user_defined.get("opf.seriesindex")
        or user_defined.get("series_index")
        or user_defined.get("seriesindex")
    )
    parsed_index = _parse_series_index(series_index)
    if parsed_index is not None:
        mi.series_index = parsed_index

    isbn_raw = user_defined.get("opf.isbn")
    if isbn_raw:
        isbn = check_isbn(isbn_raw)
        if isbn:
            mi.isbn = isbn
    generic_identifier = None
    for ident in _iter_ns_text(root, DCNS, "identifier"):
        isbn = check_isbn(ident)
        if isbn:
            mi.isbn = isbn
            break
        if ident and generic_identifier is None:
            generic_identifier = ident
    if generic_identifier:
        try:
            mi.set_identifier("odt", generic_identifier)
        except Exception:
            pass
    _read_identifiers_and_rating(mi, user_defined)

    opf_meta = _parse_bool(user_defined.get("opf.metadata"), default=False)
    opf_nocover = _parse_bool(user_defined.get("opf.nocover"), default=False)
    if extract_cover and not opf_nocover:
        try:
            _extract_cover(raw_odt, mi, opf_meta=opf_meta, extract_cover=extract_cover)
        except Exception as e:
            default_log.log_exception("Failed to extract ODT cover metadata", e, "DEBUG")

    try:
        mi.finalize()
    except Exception as e:
        default_log.log_exception("Failed to finalize ODT metadata", e, "DEBUG")

    return mi


def get_metadata_inplace(path, extract_cover: bool = True, *, fallback_on_parse_error: bool = False):
    """
    Read metadata through the path-oriented adapter exposed to registry plugins.

    Example:
        Exercise get metadata inplace with pytest::

            python -m pytest -q tests/metadata/file_sources/test_odt_metadata_source.py


    :param path: Caller-supplied path, path-like object or stream described by this
        operation.
    :param extract_cover: Request cover discovery or cover payload extraction when true.
    :param fallback_on_parse_error: Return safe default metadata after parse errors when
        true; otherwise raise the format error.
    :return: Parsed, normalized or serialized value described above.
    """
    with open(path, "rb") as stream:
        return get_metadata(stream, extract_cover=extract_cover, fallback_on_parse_error=fallback_on_parse_error)


_USER_DEFINED = f"{{{METANS}}}user-defined"
_NAME = f"{{{METANS}}}name"
_VALUE_TYPE = f"{{{METANS}}}value-type"
#: The ``meta.xml`` elements a write may replace, by the names calibre uses.
_ELEMENTS = {
    "title": f"{{{DCNS}}}title",
    "description": f"{{{DCNS}}}description",
    "creator": f"{{{DCNS}}}creator",
    "language": f"{{{DCNS}}}language",
    "initial-creator": f"{{{METANS}}}initial-creator",
    "keyword": f"{{{METANS}}}keyword",
    "keywords": f"{{{METANS}}}keywords",
}


def _xml_text(value: Any) -> str:
    """
    Make a metadata value into text XML can hold, dropping characters it cannot.

    Example:
        >>> _xml_text("Dune\\x00 \\ud800II")
        'Dune II'


    :param value: The value.
    :return: Its text, without NULs, C0 controls or unpaired surrogates.
    """
    return clean_xml_chars(str(value))


class _MetaXml:
    """
    Change the ``office:meta`` element of an ODT's ``meta.xml``, as calibre's writer does.

    Example:
        >>> root = _lxml_etree.fromstring(
        ...     f'<office:document-meta xmlns:office="{OFFICENS}"><office:meta/></office:document-meta>'
        ... )
        >>> meta = _MetaXml(root[0])
        >>> meta.add_property("opf.publisher", "Ace")
        >>> [(x.get(_NAME), x.text) for x in root.iter(_USER_DEFINED)]
        [('opf.metadata', 'true'), ('opf.publisher', 'Ace')]


    :param meta: The ``office:meta`` element.
    """

    def __init__(self, meta: Any) -> None:
        """
        Hold the element to change.

        Example:
            See the class.


        :param meta: The ``office:meta`` element.
        :return: None.
        """
        self.meta = meta
        self.marked = False

    def replace(self, name: str, text: str, *also: str) -> None:
        """
        Replace an element, and any of the others named, with one holding the text given.

        Example:
            See :func:`set_metadata`.


        :param name: The element's name in :data:`_ELEMENTS`.
        :param text: Its new text.
        :param also: Other elements holding the same field, removed.
        :return: None.
        """
        self.remove(name, *also)
        _lxml_etree.SubElement(self.meta, _ELEMENTS[name]).text = text

    def remove(self, *names: str) -> None:
        """
        Remove every element with one of the names given.

        Example:
            See :func:`set_metadata`.


        :param names: Names in :data:`_ELEMENTS`.
        :return: None.
        """
        for name in names:
            for element in list(self.meta.iter(_ELEMENTS[name])):
                element.getparent().remove(element)

    def remove_properties(self, *names: str) -> None:
        """
        Remove the user-defined properties with the names given, in any case.

        Example:
            See :func:`set_metadata`.


        :param names: Lower-case property names.
        :return: None.
        """
        for element in list(self.meta.iter(_USER_DEFINED)):
            if (element.get(_NAME) or "").lower() in names:
                element.getparent().remove(element)

    def add_property(self, name: str, text: str, value_type: str = "string") -> None:
        """
        Add a user-defined property, marking the file once as holding calibre's ``opf`` fields.

        Example:
            See the class.


        :param name: The property's name.
        :param text: Its value.
        :param value_type: Its ODF value type: ``string``, ``date`` or ``boolean``.
        :return: None.
        """
        if not self.marked:
            self.marked = True
            self.remove_properties("opf.metadata")
            self.add_property("opf.metadata", "true", "boolean")
        element = _lxml_etree.SubElement(self.meta, _USER_DEFINED)
        element.text = text
        element.set(_VALUE_TYPE, value_type)
        element.set(_NAME, name)

    def replace_property(self, name: str, text: str, *also: str, value_type: str = "string") -> None:
        """
        Replace a user-defined property, and any of the others named, with one holding the text given.

        Example:
            See :func:`set_metadata`.


        :param name: The property's name, in lower case.
        :param text: Its new value.
        :param also: Other lower-case property names holding the same field, removed.
        :param value_type: Its ODF value type.
        :return: None.
        """
        self.remove_properties(name, *also)
        self.add_property(name, text, value_type)


def _write_title_and_authors(meta: _MetaXml, mi: Any, changed: frozenset[str]) -> None:
    """
    Write the title, its sort, the authors and their sort, where they change.

    Example:
        See :func:`set_metadata`.


    :param meta: The element being changed.
    :param mi: The metadata.
    :param changed: The fields to write, from :func:`_changed_fields`.
    :return: None.
    """
    if "title" in changed:
        meta.replace("title", _xml_text(mi.title))
        meta.remove_properties("opf.title")
    if "title_sort" in changed:
        meta.replace_property("opf.titlesort", _xml_text(mi.title_sort))
    if "authors" in changed:
        authors = authors_to_string([_xml_text(a) for a in mi.authors if a])
        meta.remove("initial-creator", "creator")
        meta.replace("initial-creator", authors)
        meta.replace("creator", authors)
        meta.replace_property("opf.authors", authors)
    if "author_sort" in changed:
        meta.replace_property("opf.authorsort", _xml_text(mi.author_sort))


def _write_description(meta: _MetaXml, mi: Any, changed: frozenset[str]) -> None:
    """
    Write the comments, the tags and the language, where they change.

    LiuXin's reader counts the document's Subject (``dc:subject``) as tags as well as
    its keywords; calibre's reads the keywords alone. So the keywords get the tags the
    Subject does not already supply, and the Subject is left as it is: each reader
    then reads the tags it read before, unless the record changes them.

    The language is written as ISO 639-1, as calibre writes it; one with no such code
    is not written.

    Example:
        See :func:`set_metadata`.


    :param meta: The element being changed.
    :param mi: The metadata.
    :param changed: The fields to write, from :func:`_changed_fields`.
    :return: None.
    """
    if "comments" in changed:
        meta.replace("description", _xml_text(mi.comments))
    if "tags" in changed:
        in_subject = {
            tag for element in meta.meta.iter(f"{{{DCNS}}}subject") for tag in _split_tags("".join(element.itertext()))
        }
        keywords = [tag for tag in (_xml_text(t) for t in mi.tags if t) if tag and tag not in in_subject]
        meta.remove("keyword", "keywords")
        if keywords:
            meta.replace("keyword", ", ".join(keywords))
        meta.remove_properties("opf.subject")
    if "languages" in changed:
        from LiuXin_alpha.utils.localization import lang_as_iso639_1

        language = lang_as_iso639_1(mi.languages[0])
        if language:
            meta.replace("language", _xml_text(language))
            meta.remove_properties("opf.language")


def _write_properties(meta: _MetaXml, mi: Any, changed: frozenset[str]) -> None:
    """
    Write the publication date, publisher, series, identifiers and rating, where they change.

    Example:
        See :func:`set_metadata`.


    :param meta: The element being changed.
    :param mi: The metadata.
    :param changed: The fields to write, from :func:`_changed_fields`.
    :return: None.
    """
    if "pubdate" in changed:
        if hasattr(mi.pubdate, "strftime"):
            from LiuXin_alpha.utils.date import isoformat

            meta.replace_property("opf.pubdate", isoformat(mi.pubdate, as_utc=True).split("T")[0], value_type="date")
        else:
            meta.replace_property("opf.pubdate", _xml_text(mi.pubdate))
    if "publisher" in changed:
        meta.replace_property("opf.publisher", _xml_text(mi.publisher))
    if "series" in changed:
        index = 1.0 if mi.series_index is None else float(mi.series_index)
        meta.remove_properties("opf.series_index")
        meta.replace_property("opf.series", _xml_text(mi.series), "opf.seriesindex")
        meta.add_property("opf.seriesindex", f"{index}")
    if "identifiers" in changed:
        identifiers = {_xml_text(k): _xml_text(v) for k, v in mi.identifiers.items()}
        meta.replace_property("opf.identifiers", json.dumps(identifiers), "opf.isbn")
    if "rating" in changed:
        meta.replace_property("opf.rating", f"{float(mi.rating):g}")


#: The fields the ODT writer stores, by the record's names.
_WRITTEN_FIELDS = (
    "title",
    "title_sort",
    "authors",
    "author_sort",
    "comments",
    "tags",
    "languages",
    "pubdate",
    "publisher",
    "series",
    "identifiers",
    "rating",
)


def _series_index(mi: Any) -> float | None:
    """
    Take a record's series index as a number.

    Example:
        >>> mi = calibreMetaInformation("T", [])
        >>> mi.series_index = "2"
        >>> _series_index(mi)
        2.0


    :param mi: The record.
    :return: The index, or ``None`` where it has none.
    """
    return None if mi.series_index is None else float(mi.series_index)


def _same(field: str, new: Any, old: Any) -> bool:
    """
    Tell whether two records hold the same value for a field the ODT writer stores.

    Example:
        >>> a, b = calibreMetaInformation("T", []), calibreMetaInformation("T", [])
        >>> a.tags, b.tags = ["x", "y"], ["y", "x"]
        >>> _same("tags", a, b), _same("title", a, calibreMetaInformation("U", []))
        (True, False)


    :param field: The field.
    :param new: The record being written.
    :param old: The record read from the file.
    :return: True when they agree; tags in any order, a series with its index.
    """
    if field == "series":
        return new.series == old.series and _series_index(new) == _series_index(old)
    a, b = getattr(new, field, None), getattr(old, field, None)
    if field == "tags":
        return sorted(a or []) == sorted(b or [])
    if field in ("authors", "languages"):
        return list(a or []) == list(b or [])
    if field == "identifiers":
        return dict(a or {}) == dict(b or {})
    return a == b


def _changed_fields(mi: Any, current: Any | None) -> frozenset[str]:
    """
    Choose the fields to write: those the record holds, less those already read from the file.

    A field whose value is what LiuXin's reader reads from the file is left as the
    file has it. LiuXin's reader takes some values from places calibre's writer does
    not write (the last-saved ``dc:date`` as the publication date, the Subject as
    tags, plain ``series`` properties); copying them into calibre's properties would
    change what calibre reads, though the caller changed nothing.

    Example:
        >>> mi = calibreMetaInformation("T", ["A"])
        >>> sorted(_changed_fields(mi, None)), sorted(_changed_fields(mi, calibreMetaInformation("T", ["B"])))
        (['authors', 'title'], ['authors'])


    :param mi: The record being written.
    :param current: The file's metadata as LiuXin reads it, or ``None`` to write every field the record holds.
    :return: The names of the fields to write.
    """
    return frozenset(
        field
        for field in _WRITTEN_FIELDS
        if not mi.is_null(field) and (current is None or not _same(field, mi, current))
    )


def _set_meta_xml(raw: bytes, mi: Any, current: Any | None = None) -> bytes:
    """
    Write the metadata into the text of a ``meta.xml``.

    Example:
        >>> raw = (
        ...     f'<office:document-meta xmlns:office="{OFFICENS}" xmlns:dc="{DCNS}">'
        ...     "<office:meta/></office:document-meta>"
        ... )
        >>> b"<dc:title>Dune</dc:title>" in _set_meta_xml(raw.encode(), calibreMetaInformation("Dune", []))
        True


    :param raw: The ``meta.xml`` as it is.
    :param mi: The metadata.
    :param current: The file's metadata as LiuXin reads it; fields that match it are
        not written. ``None`` writes every field the record holds.
    :return: The new ``meta.xml``.
    :raises OdtFormatError: When it is not well-formed XML or has no ``office:meta``.
    """
    parser = _lxml_etree.XMLParser(resolve_entities=False, no_network=True, load_dtd=False, huge_tree=False)
    try:
        root = _lxml_etree.fromstring(raw, parser=parser)
    except _lxml_etree.XMLSyntaxError as e:
        raise OdtFormatError(f"The ODT's meta.xml is not well-formed XML: {e}") from e
    element = next(root.iter(f"{{{OFFICENS}}}meta"), None)
    if element is None:
        raise OdtFormatError("The ODT's meta.xml has no office:meta element to write into")
    meta = _MetaXml(element)
    changed = _changed_fields(mi, current)
    _write_title_and_authors(meta, mi, changed)
    _write_description(meta, mi, changed)
    _write_properties(meta, mi, changed)
    return _lxml_etree.tostring(root, encoding="UTF-8", xml_declaration=True, pretty_print=True)


def set_metadata(stream, mi) -> None:
    """
    Write a book's metadata into an ODT's ``meta.xml``, as calibre's writer does.

    Only ``meta.xml`` is replaced; every other member is copied as it was. A field the
    record leaves null, or holds as LiuXin already reads it from the file, is left as
    the file has it (see :func:`_changed_fields`). The title, authors, comments, tags
    and language go into the ODF elements; the rest into calibre's ``opf.*``
    user-defined properties, with ``opf.metadata`` set so calibre's reader reads them.
    Properties a reader would read in place of a written field are replaced with it:
    ``opf.title``, ``opf.subject``, ``opf.language``, ``opf.isbn`` and
    ``opf.series_index``. Tags the document's Subject already supplies (LiuXin's
    reader counts it as tags) are not copied into the keywords. As in calibre, no
    cover is written.

    Example:
        >>> buffer = io.BytesIO()
        >>> with zipfile.ZipFile(buffer, "w") as package:
        ...     package.writestr("mimetype", "application/vnd.oasis.opendocument.text")
        ...     package.writestr(
        ...         "meta.xml",
        ...         f'<office:document-meta xmlns:office="{OFFICENS}"><office:meta/></office:document-meta>',
        ...     )
        >>> set_metadata(buffer, calibreMetaInformation("Dune", ["Frank Herbert"]))
        >>> mi = get_metadata(buffer, extract_cover=False)
        >>> mi.title, mi.authors
        ('Dune', ['Frank Herbert'])


    :param stream: The ODT, open for reading and writing; it is rewritten in place.
    :param mi: The metadata to write.
    :return: None.
    :raises OdtFormatError: When the file is not a ZIP package or has no usable
        ``meta.xml``; the stream is then left as it was.
    """
    stream.seek(0)
    try:
        with zipfile.ZipFile(stream) as package:
            raw = package.read("meta.xml")
    except KeyError as e:
        raise OdtFormatError("The ODT has no meta.xml to write metadata into") from e
    except zipfile.BadZipFile as e:
        raise OdtFormatError("The file is not an ODT package") from e
    stream.seek(0)
    try:
        current = get_metadata(io.BytesIO(stream.read()), extract_cover=False)
    except Exception as e:
        default_log.log_exception("Could not read the ODT's metadata first; writing every field", e, "DEBUG")
        current = None
    updated = _set_meta_xml(raw, mi, current)
    stream.seek(0)
    safe_replace(stream, "meta.xml", io.BytesIO(updated))
