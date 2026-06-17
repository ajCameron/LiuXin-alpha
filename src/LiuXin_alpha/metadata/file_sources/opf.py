"""
Metadata extraction helpers for OPF/XML payloads.

This module replaces the legacy hand-rolled OPF node switch with a robust
parser that prefers the canonical OPF stack and falls back to tolerant XML
field extraction when needed.
"""

from __future__ import annotations

import os
import re
from typing import Iterable

import LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.utils
from LiuXin_alpha.metadata.constants import canonicalize_id_name
from LiuXin_alpha.metadata.metadata import MetaData
from LiuXin_alpha.metadata.utils import calibreMetaInformation, check_isbn, string_to_authors
from LiuXin_alpha.utils.libraries.liuxin_etree import etree
from LiuXin_alpha.utils.localization import canonicalize_lang, trans as _
from LiuXin_alpha.utils.logging import default_log

VALID_FOR = ["OPF"]
PRIORITY_FOR = ["OPF"]
RUN_COST = ["LOW"]

_WHITESPACE = re.compile(r"\s+")
_SPLIT_TAGS = re.compile(r"[;,]")


class OpfParseError(Exception):
    pass


def _local_name(tag) -> str:
    if tag is None:
        return ""
    text = str(tag)
    if "}" in text:
        return text.rsplit("}", 1)[-1].lower()
    if ":" in text:
        return text.rsplit(":", 1)[-1].lower()
    return text.lower()


def _normalize(raw: str | None) -> str:
    if not raw:
        return ""
    return _WHITESPACE.sub(" ", raw).strip()


def _split_tags(raw: str) -> list[str]:
    return [x for x in (_normalize(part) for part in _SPLIT_TAGS.split(raw)) if x]


def _stable_dedupe(items: Iterable[str]) -> list[str]:
    seen = set()
    out: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


def _default_metadata(source_name: str = ""):
    title = _("Unknown")
    if source_name:
        stem = os.path.splitext(os.path.basename(source_name))[0].strip()
        if stem:
            title = stem
    return calibreMetaInformation(title, [_("Unknown")])


def _source_name(target_file) -> str:
    if isinstance(target_file, os.PathLike):
        return os.fspath(target_file)
    if isinstance(target_file, str):
        return target_file
    return getattr(target_file, "name", "") or ""


def _is_xml_element(obj) -> bool:
    return hasattr(obj, "tag") and hasattr(obj, "iter")


def _parse_root_from_payload(payload: bytes):
    if not payload:
        raise OpfParseError("Empty OPF/XML payload.")
    try:
        return etree.fromstring(payload)
    except Exception:
        try:
            parser = etree.XMLParser(recover=True)
            root = etree.fromstring(payload, parser=parser)
        except Exception as err:
            raise OpfParseError("Failed to parse OPF/XML payload.") from err
    if root is None:
        raise OpfParseError("Failed to parse OPF/XML payload.")
    return root


def _read_target_bytes(target_file, *, text: bool, file_is_raw_root: bool) -> bytes:
    if file_is_raw_root and _is_xml_element(target_file):
        return etree.tostring(target_file, encoding="utf-8")

    if text:
        if isinstance(target_file, bytes):
            return target_file
        if isinstance(target_file, bytearray):
            return bytes(target_file)
        if isinstance(target_file, str):
            return target_file.encode("utf-8", "replace")
        raise TypeError("text=True expects an XML string/bytes payload.")

    if isinstance(target_file, os.PathLike):
        target_file = os.fspath(target_file)

    if isinstance(target_file, str):
        with open(target_file, "rb") as stream:
            return stream.read()

    if isinstance(target_file, (bytes, bytearray)):
        return bytes(target_file)

    if hasattr(target_file, "read"):
        stream = target_file
        pos = None
        if hasattr(stream, "tell"):
            try:
                pos = stream.tell()
            except Exception:
                pos = None
        try:
            if hasattr(stream, "seek"):
                try:
                    stream.seek(0)
                except Exception:
                    pass
            data = stream.read()
        finally:
            if pos is not None and hasattr(stream, "seek"):
                try:
                    stream.seek(pos)
                except Exception:
                    pass
        if isinstance(data, str):
            data = data.encode("utf-8", "replace")
        return bytes(data)

    if file_is_raw_root and _is_xml_element(target_file):
        return etree.tostring(target_file, encoding="utf-8")

    raise TypeError("Unsupported OPF target type.")


def _metadata_candidates(root) -> list:
    candidates = []
    for node in root.iter():
        if _local_name(getattr(node, "tag", None)) in {"metadata", "dc-metadata"}:
            candidates.append(node)
    return candidates


def simple_get_metadata_node(root):
    """
    Compatibility helper retained for legacy callers.
    """
    return _metadata_candidates(root)


def _best_metadata_root(root, seek_md_node: bool):
    if not seek_md_node:
        return root
    candidates = _metadata_candidates(root)
    if not candidates:
        return root
    # Prefer exact `metadata` node where available.
    for node in candidates:
        if _local_name(node.tag) == "metadata":
            return node
    return candidates[0]


def _first_text(root, names: set[str]) -> str | None:
    for node in root.iter():
        if _local_name(node.tag) not in names:
            continue
        text = _normalize("".join(node.itertext()))
        if text:
            return text
    return None


def _iter_text(root, names: set[str]) -> Iterable[str]:
    for node in root.iter():
        if _local_name(node.tag) not in names:
            continue
        text = _normalize("".join(node.itertext()))
        if text:
            yield text


def _extract_opf_like_meta_overrides(root, mi) -> None:
    for node in root.iter():
        if _local_name(node.tag) not in {"meta", "user-defined"}:
            continue
        attrib = {str(k): str(v) for k, v in getattr(node, "attrib", {}).items()}
        name = ""
        content = _normalize("".join(node.itertext()))
        for key, value in attrib.items():
            lk = key.lower()
            if lk.endswith("}name") or lk == "name":
                name = value.strip()
            elif lk.endswith("}content") or lk == "content":
                content = value.strip()
        lname = name.lower()
        if not lname or not content:
            continue

        if lname in {"calibre:series", "opf.series", "series"}:
            mi.series = content
            continue
        if lname in {"calibre:series_index", "opf.series_index", "opf.seriesindex", "series_index", "seriesindex"}:
            try:
                mi.series_index = float(content)
            except Exception:
                try:
                    mi.series_index = float(content.replace(",", "."))
                except Exception:
                    pass
            continue
        if lname in {"calibre:title_sort", "opf.titlesort", "opf.title_sort"}:
            LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.utils.title_sort = content
            continue
        if lname in {"opf.publisher"}:
            mi.publisher = content
            continue
        if lname in {"opf.language"}:
            mi.language = canonicalize_lang(content) or content
            continue
        if lname in {"opf.subject"}:
            tags = _stable_dedupe(list(getattr(mi, "tags", []) or []) + _split_tags(content))
            if tags:
                mi.tags = tags
            continue
        if lname in {"opf.pubdate"}:
            try:
                from LiuXin_alpha.utils.date import parse_date

                mi.pubdate = parse_date(content, assume_utc=True)
            except Exception:
                mi.pubdate = content


def _is_blank(value, *, treat_und_as_blank: bool = False) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        normed = _normalize(value)
        if not normed:
            return True
        if treat_und_as_blank and normed.lower() == "und":
            return True
        return False
    return False


def _iter_values(value) -> list:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    try:
        return list(value)
    except Exception:
        return [value]


def _safe_get_identifiers(md) -> dict[str, str]:
    if md is None or not hasattr(md, "get_identifiers"):
        return {}
    try:
        raw = md.get_identifiers() or {}
    except Exception:
        return {}
    if isinstance(raw, dict):
        return {str(k): str(v) for k, v in raw.items() if k and v}
    try:
        return {str(k): str(v) for k, v in dict(raw).items() if k and v}
    except Exception:
        return {}


def _merge_calibre_metadata(preferred, fallback):
    """
    Fill gaps in `preferred` from `fallback`, preserving preferred values where
    present.
    """
    if preferred is None:
        return fallback
    if fallback is None:
        return preferred

    if _is_blank(getattr(preferred, "title", None)):
        title = _normalize(getattr(fallback, "title", None))
        if title:
            preferred.title = title

    preferred_authors = [a for a in (_normalize(x) for x in _iter_values(getattr(preferred, "authors", None))) if a]
    fallback_authors = [a for a in (_normalize(x) for x in _iter_values(getattr(fallback, "authors", None))) if a]
    if not preferred_authors and fallback_authors:
        preferred.authors = _stable_dedupe(fallback_authors)

    preferred_lang = _normalize(getattr(preferred, "language", None))
    fallback_lang = _normalize(getattr(fallback, "language", None))
    if (not preferred_lang or preferred_lang.lower() == "und") and fallback_lang:
        preferred.language = fallback_lang

    if _is_blank(getattr(preferred, "comments", None)):
        comments = _normalize(getattr(fallback, "comments", None))
        if comments:
            preferred.comments = comments

    if _is_blank(getattr(preferred, "publisher", None)):
        publisher = _normalize(getattr(fallback, "publisher", None))
        if publisher:
            preferred.publisher = publisher

    preferred_tags = [t for t in (_normalize(x) for x in _iter_values(getattr(preferred, "tags", None))) if t]
    fallback_tags = [t for t in (_normalize(x) for x in _iter_values(getattr(fallback, "tags", None))) if t]
    merged_tags = _stable_dedupe(preferred_tags + fallback_tags)
    if merged_tags:
        preferred.tags = merged_tags

    if _is_blank(getattr(preferred, "series", None)):
        series = _normalize(getattr(fallback, "series", None))
        if series:
            preferred.series = series

    if getattr(preferred, "series_index", None) in (None, ""):
        fallback_series_index = getattr(fallback, "series_index", None)
        if fallback_series_index not in (None, ""):
            preferred.series_index = fallback_series_index

    if _is_blank(getattr(preferred, "title_sort", None)):
        title_sort = _normalize(getattr(fallback, "title_sort", None))
        if title_sort:
            LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.utils.title_sort = title_sort

    preferred_ids = _safe_get_identifiers(preferred)
    fallback_ids = _safe_get_identifiers(fallback)
    merged_ids = dict(fallback_ids)
    merged_ids.update(preferred_ids)
    if merged_ids and hasattr(preferred, "set_identifiers"):
        try:
            preferred.set_identifiers(merged_ids)
        except Exception:
            pass

    if _is_blank(getattr(preferred, "isbn", None)):
        isbn = _normalize(getattr(fallback, "isbn", None))
        if isbn:
            preferred.isbn = isbn

    return preferred


def _extract_generic_metadata_from_root(root, source_name: str = ""):
    mi = _default_metadata(source_name)

    title = _first_text(root, {"title"})
    if title:
        mi.title = title

    authors: list[str] = []
    for text in _iter_text(root, {"creator", "author"}):
        authors.extend(string_to_authors(text))
    authors = [x for x in authors if _normalize(x)]
    if authors:
        mi.authors = _stable_dedupe(authors)

    language = _first_text(root, {"language"})
    if language:
        mi.language = canonicalize_lang(language) or language

    description = _first_text(root, {"description"})
    if description:
        mi.comments = description

    publisher = _first_text(root, {"publisher"})
    if publisher:
        mi.publisher = publisher

    for field_name in ("subject", "keyword"):
        tags: list[str] = []
        for raw in _iter_text(root, {field_name}):
            tags.extend(_split_tags(raw))
        tags = _stable_dedupe(tags)
        if tags:
            current = list(getattr(mi, "tags", []) or [])
            mi.tags = _stable_dedupe(current + tags)

    # Identifier parsing across OPF2/OPF3-ish forms.
    for node in root.iter():
        if _local_name(node.tag) != "identifier":
            continue
        text = _normalize("".join(node.itertext()))
        if not text:
            continue

        isbn = check_isbn(text)
        if isbn:
            mi.isbn = isbn
            continue

        scheme = None
        for key, value in getattr(node, "attrib", {}).items():
            lkey = str(key).lower()
            if lkey.endswith("}scheme") or lkey.endswith(":scheme") or lkey == "scheme":
                scheme = str(value).strip()
                break
        if scheme is None and ":" in text:
            scheme = text.split(":", 1)[0]
        if scheme:
            try:
                canonical = canonicalize_id_name(scheme)
            except Exception:
                canonical = str(scheme).lower()
            try:
                mi.set_identifier(canonical, text)
            except Exception:
                try:
                    mi.set_identifiers({canonical: text})
                except Exception:
                    pass

    date_text = _first_text(root, {"date", "pubdate"})
    if date_text:
        try:
            from LiuXin_alpha.utils.date import parse_date

            mi.pubdate = parse_date(date_text, assume_utc=True)
        except Exception:
            mi.pubdate = date_text

    _extract_opf_like_meta_overrides(root, mi)

    if not getattr(mi, "title", None):
        mi.title = _default_metadata(source_name).title
    if not getattr(mi, "authors", None):
        mi.authors = [_("Unknown")]

    return mi


def _parse_using_opf_stack(root):
    from LiuXin_alpha.file_formats.opf.opf import get_metadata_from_parsed

    mi, _ver, _cover, _first_spine = get_metadata_from_parsed(root)
    return mi


def _to_liuxin_metadata(calibre_md):
    calibre_authors = [a for a in (_normalize(x) for x in _iter_values(getattr(calibre_md, "authors", None))) if a]
    try:
        md = MetaData.from_calibre(calibre_md)
    except Exception:
        # Last-resort conversion path.
        md = MetaData(getattr(calibre_md, "title", None), getattr(calibre_md, "authors", None))
    try:
        md.finalize()
    except Exception:
        pass

    # The current from_calibre path intentionally skips some fields.
    # Preserve richer OPF-derived information in the returned LiuXin metadata.
    if calibre_authors and not getattr(md, "authors", None):
        for author in calibre_authors:
            md.authors = author

    calibre_language = _normalize(getattr(calibre_md, "language", None))
    if calibre_language and _is_blank(getattr(md, "language", None), treat_und_as_blank=True):
        md.language = calibre_language

    calibre_comments = _normalize(getattr(calibre_md, "comments", None))
    if calibre_comments and not getattr(md, "comments", None):
        md.comments = calibre_comments

    calibre_publisher = _normalize(getattr(calibre_md, "publisher", None))
    if calibre_publisher and not getattr(md, "publisher", None):
        md.publisher = calibre_publisher

    calibre_tags = [t for t in (_normalize(x) for x in _iter_values(getattr(calibre_md, "tags", None))) if t]
    if calibre_tags and not getattr(md, "tags", None):
        for tag in calibre_tags:
            md.tags = tag

    calibre_series = _normalize(getattr(calibre_md, "series", None))
    if calibre_series and not getattr(md, "series", None):
        md.series = calibre_series

    calibre_series_index = getattr(calibre_md, "series_index", None)
    if calibre_series_index not in (None, "") and not getattr(md, "series_index", None):
        series_name = calibre_series
        if not series_name:
            try:
                current_series = getattr(md, "series", None) or {}
                if current_series:
                    series_name = next(iter(current_series.keys()))
            except Exception:
                series_name = ""
        if series_name:
            md.series_index = (series_name, calibre_series_index)
        else:
            # Avoid the legacy scalar-path bug in series_index assignment.
            md.calibre_series_index = calibre_series_index

    calibre_title_sort = _normalize(getattr(calibre_md, "title_sort", None))
    if calibre_title_sort and _is_blank(getattr(md, "title_sort", None)):
        md.title_sort = calibre_title_sort

    if hasattr(calibre_md, "get_identifiers") and hasattr(md, "set_identifiers"):
        try:
            ids = calibre_md.get_identifiers() or {}
            if ids:
                md.set_identifiers(ids)
        except Exception:
            pass

    calibre_isbn = _normalize(getattr(calibre_md, "isbn", None))
    if calibre_isbn:
        try:
            md.isbn = calibre_isbn
        except Exception:
            pass

    return md


def get_metadata(
    target_file,
    calibre=False,
    text=False,
    file_is_raw_root=False,
    seek_md_node=True,
    walk=False,
):
    """
    Read metadata from an OPF/XML source.

    Compatibility args (`seek_md_node`, `walk`) are retained; `walk` no longer
    changes traversal behavior as the new parser always walks metadata nodes.
    """
    del walk  # retained for compatibility only
    source_name = "" if text else _source_name(target_file)

    try:
        if file_is_raw_root and _is_xml_element(target_file):
            root = target_file
        else:
            payload = _read_target_bytes(target_file, text=text, file_is_raw_root=file_is_raw_root)
            root = _parse_root_from_payload(payload)
    except Exception as err:
        default_log.log_exception(
            "Failed to parse OPF metadata source.",
            err,
            "ERROR",
            ("source", source_name or "<stream>"),
        )
        fallback = _default_metadata(source_name)
        return fallback if calibre else _to_liuxin_metadata(fallback)

    metadata_root = _best_metadata_root(root, seek_md_node=seek_md_node)
    generic_md = _extract_generic_metadata_from_root(metadata_root, source_name=source_name)
    calibre_md = generic_md
    if _local_name(getattr(root, "tag", None)) == "package":
        try:
            canonical_md = _parse_using_opf_stack(root)
            calibre_md = _merge_calibre_metadata(canonical_md, generic_md)
        except Exception as err:
            default_log.log_exception(
                "Canonical OPF parser failed; falling back to tolerant XML metadata extraction.",
                err,
                "DEBUG",
                ("source", source_name or "<stream>"),
            )

    if calibre:
        return calibre_md
    return _to_liuxin_metadata(calibre_md)


def get_metadata_inplace(path, calibre=False, **kwargs):
    with open(path, "rb") as stream:
        return get_metadata(stream, calibre=calibre, **kwargs)


__all__ = [
    "VALID_FOR",
    "PRIORITY_FOR",
    "RUN_COST",
    "OpfParseError",
    "get_metadata",
    "get_metadata_inplace",
    "simple_get_metadata_node",
]
