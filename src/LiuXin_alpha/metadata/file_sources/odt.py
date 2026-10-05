"""
Read ODT package metadata, user-defined OPF fields and cover candidates while preserving caller-owned stream positions.

The module keeps malformed-input, optional dependency and resource ownership
behavior explicit for registry callers.

Example:
    Exercise odt with pytest::

        python -m pytest -q tests/metadata/file_sources/test_odt_metadata_source.py
"""
from __future__ import annotations

import io
import os
import re
import zipfile
from typing import Iterable

from LiuXin_alpha.file_formats.odf.draw import Frame as ODFFrame
from LiuXin_alpha.file_formats.odf.draw import Image as ODFImage
from LiuXin_alpha.file_formats.odf.namespaces import DCNS, METANS
from LiuXin_alpha.file_formats.odf.opendocument import load as od_load
from LiuXin_alpha.metadata.utils import calibreMetaInformation, check_isbn, string_to_authors
from LiuXin_alpha.utils.image_tools.imghdr import identify
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.libraries.liuxin_etree import etree

__all__ = ["OdtFormatError", "get_metadata", "get_metadata_inplace"]


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
