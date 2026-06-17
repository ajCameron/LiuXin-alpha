"""
ODT metadata reader (beta/backup path).

This module intentionally keeps an implementation separate from
`metadata.file_sources.odt` so it can act as a fallback reader while sharing the
same public interface.
"""

from __future__ import annotations

import io
import os
import re
import zipfile
from typing import Iterable

import LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.utils
from LiuXin_alpha.file_formats.odf.draw import Frame as ODFFrame
from LiuXin_alpha.file_formats.odf.draw import Image as ODFImage
from LiuXin_alpha.file_formats.odf.namespaces import DCNS, METANS
from LiuXin_alpha.file_formats.odf.opendocument import load as od_load
from LiuXin_alpha.metadata.utils import calibreMetaInformation, check_isbn, string_to_authors
from LiuXin_alpha.utils.image_tools.imghdr import identify
from LiuXin_alpha.utils.libraries.liuxin_etree import etree
from LiuXin_alpha.utils.localization import canonicalize_lang, trans as _
from LiuXin_alpha.utils.logging import default_log

try:
    from LiuXin_alpha.utils.wrappers.magick.draw import identify_data as _identify_data
except Exception:
    _identify_data = None

__all__ = [
    "VALID_FOR",
    "PRIORITY_FOR",
    "RUN_COST",
    "get_metadata",
    "get_metadata_inplace",
    "read_cover",
    "xml_get_bool",
]

VALID_FOR = ["ODT"]
PRIORITY_FOR = ["ODT"]
RUN_COST = ["LOW"]

_WHITESPACE = re.compile(r"\s+")
_SPLIT_TAGS = re.compile(r"[;,]")


def _normalize(raw: str | None) -> str:
    if not raw:
        return ""
    return _WHITESPACE.sub(" ", raw).strip()


def _read_source_bytes(stream_or_path) -> bytes:
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
        with open(stream_or_path, "rb") as stream:
            return stream.read()

    raise TypeError("ODT beta metadata reader expects stream, bytes or path/pathlike input.")


def _source_title(stream_or_path) -> str:
    name = getattr(stream_or_path, "name", None)
    if isinstance(name, str) and name:
        return os.path.splitext(os.path.basename(name))[0]
    if isinstance(stream_or_path, os.PathLike):
        return os.path.splitext(os.path.basename(os.fspath(stream_or_path)))[0]
    if isinstance(stream_or_path, str):
        return os.path.splitext(os.path.basename(stream_or_path))[0]
    return ""


def _iter_ns_text(root, namespace: str, local_name: str) -> Iterable[str]:
    tag = "{%s}%s" % (namespace, local_name)
    for elem in root.iter(tag):
        text = _normalize("".join(elem.itertext()))
        if text:
            yield text


def _first_ns_text(root, namespace: str, local_name: str) -> str | None:
    for text in _iter_ns_text(root, namespace, local_name):
        return text
    return None


def _parse_xml(raw_xml: bytes):
    try:
        return etree.fromstring(raw_xml)
    except Exception:
        parser = etree.XMLParser(recover=True)
        return etree.fromstring(raw_xml, parser=parser)


def _read_user_defined(root) -> dict[str, str]:
    ans: dict[str, str] = {}
    tag = "{%s}user-defined" % METANS
    name_attr = "{%s}name" % METANS
    for elem in root.iter(tag):
        name = _normalize(elem.attrib.get(name_attr) or elem.attrib.get("name"))
        if not name:
            continue
        ans[name.lower()] = _normalize("".join(elem.itertext()))
    return ans


def _split_tags(raw: str) -> list[str]:
    return [x for x in (_normalize(p) for p in _SPLIT_TAGS.split(raw)) if x]


def _stable_dedupe(items: Iterable[str]) -> list[str]:
    seen = set()
    out: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


def _parse_series_index(raw: str | None) -> float | None:
    if not raw:
        return None
    try:
        return float(raw)
    except Exception:
        try:
            return float(raw.replace(",", "."))
        except Exception:
            return None


def _parse_bool(raw: str | None, default: bool = False) -> bool:
    if raw is None:
        return default
    val = raw.strip().lower()
    if val in {"1", "true", "yes", "on"}:
        return True
    if val in {"0", "false", "no", "off"}:
        return False
    return default


def _image_meta(raw: bytes) -> tuple[str | None, int, int]:
    fmt, width, height = identify(raw)
    if fmt and width > 0 and height > 0:
        return fmt, width, height
    if _identify_data is not None:
        try:
            w, h, f = _identify_data(raw)
            return (str(f).lower() if f else fmt), int(w or width), int(h or height)
        except Exception:
            pass
    return fmt, width, height


def _fmt_from_href(href: str | None) -> str | None:
    if not href:
        return None
    ext = os.path.splitext(href)[1].lower().lstrip(".")
    if ext in {"jpg", "jpeg", "png", "gif", "webp", "bmp"}:
        return "jpg" if ext == "jpeg" else ext
    return None


def xml_get_bool(root, name, default=False):
    """
    Compatibility helper: read a boolean custom metadata value by name.
    """
    lname = str(name).lower()
    for elem in root.iter():
        for val in elem.attrib.values():
            if str(val).lower() != lname:
                continue
            text = _normalize(getattr(elem, "text", None))
            if text.lower() == "true":
                return True
            if text.lower() == "false":
                return False
            return default
    return default


def read_cover(stream, zin, mi, opfmeta, extract_cover):
    """
    Try to identify the most plausible cover image from ODT frame/image nodes.
    """
    raw_odt = _read_source_bytes(stream)

    def _iter_frame_images_from_odf():
        otext = od_load(io.BytesIO(raw_odt))
        for frame in otext.topnode.getElementsByType(ODFFrame):
            images = frame.getElementsByType(ODFImage)
            if not images:
                continue
            href = images[0].getAttribute("href")
            if not href:
                continue
            frame_name = _normalize(frame.getAttribute("name") or "")
            yield frame_name, href

    def _iter_frame_images_from_content_xml():
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
            for key, value in frame.attrib.items():
                ks = str(key)
                if ks.endswith("}name") or ks == "name" or ks.endswith(":name"):
                    frame_name = _normalize(value)
                    break
            href = None
            for child in frame.iter():
                if not str(getattr(child, "tag", "")).endswith("}image"):
                    continue
                for key, value in child.attrib.items():
                    ks = str(key)
                    if ks.endswith("}href") or ks == "href" or ks.endswith(":href"):
                        href = _normalize(value)
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

        fmt, width, height = _image_meta(raw)
        if not fmt:
            fmt = _fmt_from_href(href)
        if not fmt:
            fmt = "jpeg"
        imgnum += 1

        if frame_name.lower() == "opf.cover":
            cover_href = href
            cover_data = (fmt, raw)
            cover_frame = frame_name
            break

        if (
            cover_href is None
            and imgnum == 1
            and width > 0
            and height > 0
            and 0.8 <= float(height) / float(width) <= 1.8
            and (height * width) >= 12000
        ):
            cover_href = href
            cover_data = (fmt, raw)
            if not opfmeta:
                break

    if cover_href is None:
        return

    mi.cover = cover_href
    if cover_frame:
        mi.odf_cover_frame = cover_frame
    if extract_cover and cover_data:
        mi.cover_data = cover_data


def get_metadata(stream, extract_cover=True):
    raw_odt = _read_source_bytes(stream)

    with zipfile.ZipFile(io.BytesIO(raw_odt), "r") as zin:
        meta_xml = zin.read("meta.xml")

        root = _parse_xml(meta_xml)
        user_defined = _read_user_defined(root)

        title = (
            user_defined.get("opf.title")
            or _first_ns_text(root, DCNS, "title")
            or _source_title(stream)
            or _("Unknown")
        )
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
            LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.utils.title_sort = title_sort

        comments = _first_ns_text(root, DCNS, "description")
        if comments:
            mi.comments = comments

        publisher = user_defined.get("opf.publisher") or _first_ns_text(root, DCNS, "publisher")
        if publisher:
            mi.publisher = publisher

        language = user_defined.get("opf.language") or _first_ns_text(root, DCNS, "language")
        if language:
            mi.language = canonicalize_lang(language) or language

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

        opf_meta = _parse_bool(user_defined.get("opf.metadata"), default=xml_get_bool(root, "opf.metadata", False))
        opf_nocover = _parse_bool(user_defined.get("opf.nocover"), default=xml_get_bool(root, "opf.nocover", False))
        if extract_cover and not opf_nocover:
            try:
                read_cover(io.BytesIO(raw_odt), zin, mi, opf_meta, extract_cover)
            except Exception as err:
                default_log.log_exception("Failed to extract ODT beta cover metadata", err, "DEBUG")

        try:
            mi.finalize()
        except Exception as err:
            default_log.log_exception("Failed to finalize ODT beta metadata", err, "DEBUG")
        return mi


def get_metadata_inplace(path, extract_cover=True):
    with open(path, "rb") as stream:
        return get_metadata(stream, extract_cover=extract_cover)
