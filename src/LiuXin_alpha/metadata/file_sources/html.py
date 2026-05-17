"""
Read metadata from HTML files.

Supports metadata encoded in `<meta>` tags, special HTML comments, and title
fallback from the document `<title>` element.
"""

from __future__ import annotations

import os
import re
from collections import defaultdict
from datetime import datetime
from html.parser import HTMLParser
from typing import Any

from LiuXin_alpha.file_formats.chardet import detect, xml_to_unicode
from LiuXin_alpha.metadata.metadata import MetaData as Metadata
from LiuXin_alpha.metadata.utils import check_isbn, string_to_authors
from LiuXin_alpha.utils.calibre import replace_entities
from LiuXin_alpha.utils.date import is_date_undefined, parse_date, parse_only_date
from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"

# Extract an HTML attribute value; supports both quote styles.
attr_pat = r"""(?:(?P<sq>')|(?P<dq>"))(?P<content>(?(sq)[^']+|[^"]+))(?(sq)'|")"""

VALID_FOR = ["HTML", "HTM", "XHTML", "XHTM", "XML"]
PRIORITY_FOR = ["NONE"]
RUN_COST = ["LOW"]

META_NAMES = {
    "title": ("dc.title", "dcterms.title", "title"),
    "authors": ("author", "dc.creator.aut", "dcterms.creator.aut", "dc.creator"),
    "publisher": ("publisher", "dc.publisher", "dcterms.publisher"),
    "isbn": ("isbn", "dc.identifier.isbn", "dcterms.identifier.isbn"),
    "language": ("dc.language", "dcterms.language", "language"),
    "pubdate": (
        "pubdate",
        "date of publication",
        "dc.date.published",
        "dc.date.publication",
        "dc.date.issued",
        "dcterms.issued",
    ),
    "timestamp": (
        "timestamp",
        "date of creation",
        "dc.date.created",
        "dc.date.creation",
        "dcterms.created",
    ),
    "series": ("series",),
    "series_index": ("seriesnumber", "series_index", "series.index"),
    "rating": ("rating",),
    "comments": ("comments", "dc.description", "description"),
    "tags": ("tags", "subject"),
}

COMMENT_NAMES = {
    "title": "TITLE",
    "authors": "AUTHOR",
    "publisher": "PUBLISHER",
    "isbn": "ISBN",
    "language": "LANGUAGE",
    "pubdate": "PUBDATE",
    "timestamp": "TIMESTAMP",
    "series": "SERIES",
    "series_index": "SERIESNUMBER",
    "rating": "RATING",
    "comments": "COMMENTS",
    "tags": "TAGS",
}

_COMMENT_PAIR_RE = re.compile(rf"(?P<name>\S+)\s*=\s*{attr_pat}")
_IDENTIFIER_NAME_RE = re.compile(r"(?:dc|dcterms)[.:]identifier(?:\.|$)", flags=re.IGNORECASE)
_IDENTIFIER_EXACT_RE = re.compile(r"(?:dc|dcterms)[.:]identifier$", flags=re.IGNORECASE)
_SERIES_INDEX_IN_SERIES_RE = re.compile(r"\[([.0-9]+)\]$")
_BINARY_SIGNATURES = (
    b"\x89PNG\r\n\x1a\n",
    b"\xff\xd8\xff",
    b"GIF87a",
    b"GIF89a",
    b"%PDF-",
    b"PK\x03\x04",
    b"PK\x05\x06",
    b"Rar!",
)

_RMAP_COMMENT = {v: k for k, v in COMMENT_NAMES.items()}
_RMAP_META = {n: field for field, names in META_NAMES.items() for n in names}


def _coerce_text(raw: Any, encoding: str | None = None) -> str:
    if isinstance(raw, str):
        return raw
    if isinstance(raw, (bytes, bytearray)):
        data = bytes(raw)
        if encoding:
            return data.decode(encoding, "replace")
        try:
            info = detect(data) or {}
        except Exception:
            info = {}
        guessed_encoding = str(info.get("encoding") or "").lower()
        confidence = float(info.get("confidence") or 0.0)
        has_c1_bytes = any(0x80 <= byte <= 0x9F for byte in data)
        has_cp1252_punctuation = any(
            byte in data
            for byte in (
                0x80,
                0x82,
                0x83,
                0x84,
                0x85,
                0x86,
                0x87,
                0x88,
                0x89,
                0x8A,
                0x8B,
                0x8C,
                0x91,
                0x92,
                0x93,
                0x94,
                0x95,
                0x96,
                0x97,
                0x98,
                0x99,
                0x9A,
                0x9B,
                0x9C,
                0x9F,
            )
        )
        if (
            has_c1_bytes
            and has_cp1252_punctuation
            and confidence < 0.2
            and (
                not guessed_encoding
                or guessed_encoding.startswith("iso-8859")
                or guessed_encoding.startswith("windows-125")
                or guessed_encoding.startswith("cp125")
            )
        ):
            try:
                return data.decode("cp1252", "replace")
            except Exception:
                pass
        return xml_to_unicode(data)[0]
    return str(raw)


def _looks_binaryish(raw: bytes) -> bool:
    if not raw:
        return False
    if raw.startswith((b"\xff\xfe", b"\xfe\xff", b"\xef\xbb\xbf")):
        return False
    if raw.startswith(_BINARY_SIGNATURES):
        return True
    sample = raw[:1024]
    control_count = sum(1 for byte in sample if byte < 32 and byte not in (9, 10, 13))
    return bool(sample) and (control_count / len(sample)) > 0.20


def _default_metadata() -> Metadata:
    return Metadata(_("Unknown"), [_("Unknown")])


def _dedupe_stable(values: list[str]) -> list[str]:
    seen = set()
    out: list[str] = []
    for val in values:
        key = val.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(val)
    return out


def _clean_values(values: list[str] | None) -> list[str]:
    if not values:
        return []
    cleaned = [str(v).strip() for v in values if str(v).strip()]
    return _dedupe_stable(cleaned)


def _safe_rating(raw: str | None) -> float | None:
    if raw is None:
        return None
    try:
        rating = float(str(raw).strip())
    except Exception:
        return None

    if rating < 0:
        rating = 0
    if rating > 5:
        rating /= 2.0
    if rating > 5:
        rating = 0
    return rating


def _parse_date_value(raw: str | None):
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None

    normalized = text.replace("/", "-").replace(".", "-")
    if re.fullmatch(r"[12]\d{3}", normalized):
        try:
            return datetime(int(normalized), 6, 2)
        except Exception:
            return None
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y%m%d"):
        try:
            return datetime.strptime(normalized, fmt)
        except Exception:
            continue

    # Prefer parse_date to preserve full timestamp information when present.
    try:
        dt = parse_date(text)
        if not is_date_undefined(dt):
            return dt
    except Exception:
        pass

    # Fallback for date-only noisy fields.
    try:
        dt = parse_only_date(text)
        if not is_date_undefined(dt):
            return dt
    except Exception:
        pass

    return None


def _extract_comment_pairs(comment_text: str) -> dict[str, list[str]]:
    ans: dict[str, list[str]] = defaultdict(list)
    for match in _COMMENT_PAIR_RE.finditer(comment_text or ""):
        raw_name = match.group("name")
        field = _RMAP_COMMENT.get(raw_name.upper())
        if not field:
            continue
        ans[field].append(replace_entities(match.group("content")))
    return ans


class _HTMLMetadataParser(HTMLParser):
    """
    Tolerant parser for metadata-like HTML patterns.
    """

    def __init__(self) -> None:
        super().__init__(convert_charrefs=False)
        self.comment_tags: dict[str, list[str]] = defaultdict(list)
        self.meta_tags: dict[str, list[str]] = defaultdict(list)
        self.meta_identifiers: dict[str, list[str]] = defaultdict(list)
        self._in_title = False
        self._title_chunks: list[str] = []

    @property
    def title_text(self) -> str:
        return "".join(self._title_chunks).strip()

    def handle_starttag(self, tag: str, attrs):
        self._handle_tag(tag, attrs)

    def handle_startendtag(self, tag: str, attrs):
        self._handle_tag(tag, attrs)

    def _handle_tag(self, tag: str, attrs):
        tag = (tag or "").lower()
        if tag == "title":
            self._in_title = True
            return
        if tag != "meta":
            return

        ad = {str(k).lower(): ("" if v is None else str(v)) for k, v in attrs}
        name = ad.get("name", "").strip()
        content = ad.get("content", "")
        if not name or not content:
            return

        lowered_name = name.lower()

        if _IDENTIFIER_NAME_RE.match(lowered_name):
            scheme = None
            if _IDENTIFIER_EXACT_RE.match(lowered_name):
                scheme = ad.get("scheme", "").strip().lower()
            else:
                parts = re.split(r"[.:]", lowered_name)
                if len(parts) == 3 and not ad.get("scheme"):
                    scheme = parts[2].strip().lower()
            if scheme:
                self.meta_identifiers[scheme].append(content)
            return

        field = _RMAP_META.get(lowered_name) or _RMAP_META.get(lowered_name.replace(":", "."))
        if field:
            self.meta_tags[field].append(replace_entities(content))

    def handle_endtag(self, tag: str):
        if (tag or "").lower() == "title":
            self._in_title = False

    def handle_data(self, data: str):
        if self._in_title and data:
            self._title_chunks.append(data)

    def handle_entityref(self, name: str):
        if self._in_title and name:
            # Keep entities for later centralized decode via replace_entities().
            self._title_chunks.append(f"&{name};")

    def handle_charref(self, name: str):
        if self._in_title and name:
            # Keep charrefs for later centralized decode via replace_entities().
            self._title_chunks.append(f"&#{name};")

    def handle_comment(self, data: str):
        for field, values in _extract_comment_pairs(data).items():
            self.comment_tags[field].extend(values)


def parse_metadata(src: str) -> tuple[dict[str, list[str]], dict[str, list[str]], dict[str, list[str]], str]:
    parser = _HTMLMetadataParser()
    try:
        parser.feed(src)
        parser.close()
    except Exception:
        return ({}, {}, {}, "")

    return (
        {k: _clean_values(v) for k, v in parser.comment_tags.items()},
        {k: _clean_values(v) for k, v in parser.meta_tags.items()},
        {k: _clean_values(v) for k, v in parser.meta_identifiers.items()},
        replace_entities(parser.title_text),
    )


def parse_meta_tags(src):
    """
    Parse metadata-like `<meta>` tags.

    Returns a dict keyed by canonical metadata field with first observed value.
    """
    _comment_tags, meta_tags, _meta_ids, _title = parse_metadata(_coerce_text(src))
    return {k: v[0] for k, v in meta_tags.items() if v}


def parse_comment_tags(src):
    """
    Parse calibre-style metadata comments such as `<!-- TITLE="..." -->`.

    Returns a dict keyed by canonical metadata field with first observed value.
    """
    comment_tags, _meta_tags, _meta_ids, _title = parse_metadata(_coerce_text(src))
    return {k: v[0] for k, v in comment_tags.items() if v}


def get_metadata(target_file):
    """
    Read metadata from a filesystem path or readable stream.
    """
    if isinstance(target_file, (bytes, bytearray, memoryview)):
        return get_metadata_(bytes(target_file))

    if isinstance(target_file, (str, os.PathLike)):
        with open(target_file, "rb") as html_stream:
            src = html_stream.read()
        return get_metadata_(src)

    stream = target_file
    if not hasattr(stream, "read"):
        raise TypeError("HTML metadata reader expects a path or readable stream.")

    pos = None
    if hasattr(stream, "tell"):
        try:
            pos = stream.tell()
        except Exception:
            pos = None

    try:
        if hasattr(stream, "seek"):
            stream.seek(0)
        src = stream.read()
        return get_metadata_(src)
    finally:
        if pos is not None and hasattr(stream, "seek"):
            try:
                stream.seek(pos)
            except Exception:
                pass


def get_metadata_(src, encoding=None):
    """
    Parse metadata from HTML content as `bytes` or `str`.
    """
    if isinstance(src, (bytes, bytearray, memoryview)):
        raw = bytes(src)
        if _looks_binaryish(raw):
            return _default_metadata()
        src = _coerce_text(raw, encoding=encoding)

    src = _coerce_text(src)
    src = src[:250000]

    comment_tags, meta_tags, meta_ids, title_tag = parse_metadata(src)

    def get_all(local_field: str) -> list[str]:
        # Preserve legacy precedence: comment tags override meta tags.
        values = comment_tags.get(local_field) or meta_tags.get(local_field) or []
        return _clean_values(values)

    def get(local_field: str) -> str | None:
        values = get_all(local_field)
        return values[0] if values else None

    title = get("title") or (title_tag.strip() if title_tag.strip() else None) or _("Unknown")

    authors: list[str] = []
    for raw_authors in get_all("authors"):
        authors.extend(string_to_authors(raw_authors))
    authors = _dedupe_stable([a.strip() for a in authors if a.strip()])
    if not authors:
        authors = [_("Unknown")]

    mi = Metadata(title, authors)

    # Single value fields.
    for field in ("publisher", "comments"):
        val = get(field)
        if val:
            setattr(mi, field, val)

    # ISBN from dedicated field first.
    isbn = get("isbn")
    if isbn:
        checked = check_isbn(re.sub(r"[^0-9Xx]", "", isbn))
        if checked:
            mi.isbn = checked

    # Language(s).
    languages = get_all("language")
    if languages:
        mi.languages = languages
        mi.language = languages[0]

    # Date-like fields.
    for field in ("pubdate", "timestamp"):
        parsed = _parse_date_value(get(field))
        if parsed is not None:
            setattr(mi, field, parsed)

    # Series and index.
    series = get("series")
    if series:
        series_index = None
        match = _SERIES_INDEX_IN_SERIES_RE.search(series)
        if match is not None:
            try:
                series_index = float(match.group(1))
            except Exception:
                series_index = None
            series = series.replace(match.group(), "").strip()

        mi.series = series

        if series_index is None:
            raw_idx = get("series_index")
            try:
                series_index = float(raw_idx) if raw_idx is not None else None
            except Exception:
                series_index = None

        if series_index is not None:
            mi.series_index = (series, series_index)

    rating = _safe_rating(get("rating"))
    if rating is not None:
        mi.rating = rating

    tags = []
    for block in get_all("tags"):
        tags.extend([x.strip() for x in re.split(r"[,;]", block) if x.strip()])
    tags = _dedupe_stable(tags)
    if tags:
        mi.tags = tags

    # Generic identifier support from dc.identifier.<scheme> tags.
    for scheme, values in meta_ids.items():
        if not values:
            continue
        val = values[0]
        if not val:
            continue
        try:
            mi.set_identifier(scheme, val)
        except Exception:
            pass
        if scheme == "isbn" and mi.is_null("isbn"):
            checked = check_isbn(re.sub(r"[^0-9Xx]", "", val))
            if checked:
                mi.isbn = checked

    return mi


__all__ = [
    "VALID_FOR",
    "PRIORITY_FOR",
    "RUN_COST",
    "META_NAMES",
    "COMMENT_NAMES",
    "parse_meta_tags",
    "parse_comment_tags",
    "parse_metadata",
    "get_metadata",
    "get_metadata_",
]
