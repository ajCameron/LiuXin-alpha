"""
Stage B4 (pre-C): best-effort readers for Calibre sidecar OPF metadata.

Why this exists
---------------
Some Calibre libraries drift into states where ``metadata.db`` is missing,
unreadable, or intentionally excluded from a backup/transfer. In those cases,
Calibre typically still leaves per-book sidecar files such as:

- ``metadata.opf`` (OPF2/OPF3-ish metadata payload)
- ``cover.jpg`` (or similar)

This module provides a conservative, best-effort parser for ``metadata.opf`` and
a filesystem scanner that yields :class:`~LiuXin_alpha.databases.calibre_emulation.types.CalibreBookNormalized`
payloads without requiring SQLite access.

Design goals
------------
- Never write to disk
- Do not require Calibre as a dependency
- Be tolerant of mangled XML/JSON
- Keep the API surface small and streaming-friendly
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import zlib
import xml.etree.ElementTree as ET
from typing import Any, Dict, IO, Iterator, List, Mapping, Optional, Tuple

from LiuXin_alpha.utils.calibre_compat.calibre_database_emulation.errors import CalibreUnsafePathError
from LiuXin_alpha.utils.calibre_compat.calibre_database_emulation.types import CalibreBookNormalized, CalibreFormatRef, CalibreSeriesRef


# ----------------------------
# XML helpers
# ----------------------------

def _localname(tag: str) -> str:
    """
    Extract the tag text from a tag.

    :param tag:
    :return:
    """
    # '{ns}name' -> 'name'
    if not tag:
        return ""
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _strip_text(x: Optional[str]) -> str:
    """
    Strip leading and trailing whitespace from a string.

    :param x:
    :return:
    """
    if x is None:
        return ""
    return str(x).strip()


def _iter_elements_by_localname(root: ET.Element, local: str) -> Iterator[ET.Element]:
    """
    Iterate over all elements within a local-name tag.

    :param root:
    :param local:
    :return:
    """
    want = local.lower()
    for elem in root.iter():
        if _localname(elem.tag).lower() == want:
            yield elem


def _meta_key(elem: ET.Element) -> str:
    """
    OPF2 uses name/content; OPF3 uses property/text.

    :param elem:
    :return:
    """
    return _strip_text(elem.attrib.get("name") or elem.attrib.get("property"))


def _meta_value(elem: ET.Element) -> str:
    """
    Best effort to get the value from the element.

    :param elem:
    :return:
    """
    if "content" in elem.attrib:
        return _strip_text(elem.attrib.get("content"))
    return _strip_text(elem.text)


def _safe_parse_xml(opf_bytes: bytes) -> Tuple[Optional[ET.Element], Optional[str]]:
    """
    Pase an xml as bytes into something with an iterate over.

    :param opf_bytes:
    :return:
    """
    try:
        root = ET.fromstring(opf_bytes)
        return root, None
    except Exception as e1:
        # Second attempt: decode with replacement and re-encode. This helps with
        # real-world "almost UTF-8" garbage.
        try:
            s = opf_bytes.decode("utf-8", errors="replace")
            root = ET.fromstring(s.encode("utf-8"))
            return root, f"opf_xml_reencoded: {e1!r}"
        except Exception as e2:
            return None, f"opf_xml_unparseable: {e2!r}"


# ----------------------------
# OPF parsing
# ----------------------------

@dataclass(frozen=True, slots=True)
class ParsedOPF:
    """
    The results of parsing an OPF.
    """
    title: str = ""
    authors: Tuple[str, ...] = ()
    tags: Tuple[str, ...] = ()
    languages: Tuple[str, ...] = ()
    identifiers: Mapping[str, str] = None  # type: ignore[assignment]
    comments_html: Optional[str] = None
    series: Optional[CalibreSeriesRef] = None
    extras: Mapping[str, Any] = None  # type: ignore[assignment]
    user_metadata: Mapping[str, Any] = None  # type: ignore[assignment]
    warnings: Tuple[str, ...] = ()


def parse_metadata_opf(opf_path: Path) -> ParsedOPF:
    """
    Parse a Calibre ``metadata.opf`` file best-effort.

    Returns a ParsedOPF with warnings filled if parsing was partial.

    :param opf_path:
    :return:
    """
    warnings: List[str] = []
    try:
        opf_bytes = Path(opf_path).read_bytes()
    except Exception as e:
        return ParsedOPF(warnings=(f"opf_read_failed: {e!r}",))

    root, w = _safe_parse_xml(opf_bytes)
    if w:
        warnings.append(w)
    if root is None:
        # Total failure: caller can still fall back to folder names.
        return ParsedOPF(warnings=tuple(warnings))

    # Collect core Dublin Core-ish fields by localname to avoid namespace pain.
    title = ""
    titles = [_strip_text(e.text) for e in _iter_elements_by_localname(root, "title") if _strip_text(e.text)]
    if titles:
        title = titles[0]

    authors = tuple(
        _strip_text(e.text)
        for e in _iter_elements_by_localname(root, "creator")
        if _strip_text(e.text)
    )

    tags = tuple(
        _strip_text(e.text)
        for e in _iter_elements_by_localname(root, "subject")
        if _strip_text(e.text)
    )

    languages = tuple(
        _strip_text(e.text)
        for e in _iter_elements_by_localname(root, "language")
        if _strip_text(e.text)
    )

    comments_html: Optional[str] = None
    descs = [_strip_text(e.text) for e in _iter_elements_by_localname(root, "description") if _strip_text(e.text)]
    if descs:
        comments_html = descs[0]

    # Identifiers: best effort mapping. Prefer opf:scheme if present.
    identifiers: Dict[str, str] = {}
    for e in _iter_elements_by_localname(root, "identifier"):
        val = _strip_text(e.text)
        if not val:
            continue
        scheme = ""
        for k, v in e.attrib.items():
            if k.lower().endswith("scheme"):
                scheme = _strip_text(v).lower()
                break
        if not scheme:
            # Heuristic: urn:isbn, urn:uuid etc.
            m = re.match(r"^\s*urn:([a-z0-9_+-]+):", val, flags=re.IGNORECASE)
            if m:
                scheme = m.group(1).lower()
        if not scheme:
            scheme = "identifier"
        identifiers[scheme] = val

    # Read <meta> tags for calibre:* extras + series.
    meta_entries: List[Tuple[str, str]] = []
    for e in _iter_elements_by_localname(root, "meta"):
        k = _meta_key(e)
        if not k:
            continue
        meta_entries.append((k, _meta_value(e)))

    def _get_meta(name: str) -> Optional[str]:
        # last one wins
        out = None
        for k, v in meta_entries:
            if k == name:
                out = v
        return out

    series_name = _get_meta("calibre:series")
    series_idx = _get_meta("calibre:series_index")
    series: Optional[CalibreSeriesRef] = None
    if series_name:
        try:
            idxf = float(series_idx) if series_idx not in (None, "") else None
        except Exception:
            idxf = None
        series = CalibreSeriesRef(name=str(series_name), index=idxf)

    extras: Dict[str, Any] = {}
    for k, v in meta_entries:
        if k.startswith("calibre:") and k not in {"calibre:user_metadata:#", "calibre:user_metadata"}:
            # Keep simple scalar extras (rating, timestamp, title_sort, etc.)
            if k in {"calibre:rating"}:
                try:
                    extras[k] = int(v)
                except Exception:
                    extras[k] = v
            else:
                extras[k] = v

    # User metadata (custom columns): either per-field meta name
    # 'calibre:user_metadata:#foo' or a combined OPF3 property.
    user_meta_raw: Dict[str, Any] = {}
    for k, v in meta_entries:
        if k.startswith("calibre:user_metadata:") and v:
            field = k.split("calibre:user_metadata:", 1)[1]
            try:
                user_meta_raw[field] = json.loads(v)
            except Exception as e:
                warnings.append(f"user_metadata_json_failed:{field}:{e!r}")
    # OPF3 combined payload: <meta property="calibre:user_metadata">{"#a": {...}}</meta>
    for k, v in meta_entries:
        if k == "calibre:user_metadata" and v:
            try:
                data = json.loads(v)
                if isinstance(data, dict):
                    user_meta_raw = data
            except Exception as e:
                warnings.append(f"user_metadata_json_failed:combined:{e!r}")

    user_meta_values: Dict[str, Any] = {}
    for field, fm in (user_meta_raw or {}).items():
        if not isinstance(fm, dict):
            # Sometimes people hand-edit and store a scalar.
            user_meta_values[field] = fm
            continue

        val = fm.get("#value#")
        extra = fm.get("#extra#", None)
        datatype = str(fm.get("datatype") or fm.get("type") or "text").lower()
        is_mult = fm.get("is_multiple", None)

        # Decode multiplicity for common cases.
        if is_mult in ("|", ","):
            if isinstance(val, str):
                parts = [p.strip() for p in val.split(is_mult) if p.strip()]
                val = parts
            # If list already, keep as-is.

        # Coerce some primitives (best-effort; never raise).
        if datatype in {"int", "rating"}:
            try:
                if isinstance(val, list):
                    val = [int(x) for x in val]
                elif val is not None and val != "":
                    val = int(val)
            except Exception:
                pass
        elif datatype in {"float"}:
            try:
                if isinstance(val, list):
                    val = [float(x) for x in val]
                elif val is not None and val != "":
                    val = float(val)
            except Exception:
                pass
        elif datatype in {"bool"}:
            try:
                if isinstance(val, str):
                    val = val.strip().lower() in {"1", "true", "yes", "y", "t"}
                elif isinstance(val, (int, float)):
                    val = bool(val)
            except Exception:
                pass
        elif datatype == "series":
            # Calibre stores series index as extra.
            try:
                idxf = float(extra) if extra is not None and extra != "" else None
            except Exception:
                idxf = None
            val = {"name": None if val is None else str(val), "index": idxf}

        user_meta_values[field] = val
        # Add a convenience alias without the leading '#', if safe.
        if isinstance(field, str) and field.startswith("#"):
            alias = field[1:]
            if alias and alias not in user_meta_values:
                user_meta_values[alias] = val

    return ParsedOPF(
        title=title,
        authors=tuple(a for a in authors if a),
        tags=tuple(t for t in tags if t),
        languages=tuple(l for l in languages if l),
        identifiers=dict(identifiers),
        comments_html=comments_html,
        series=series,
        extras=dict(extras),
        user_metadata=dict(user_meta_values),
        warnings=tuple(warnings),
    )


# ----------------------------
# File scanning + streaming
# ----------------------------

def _ensure_path_under_root(library_root: Path, p: Path) -> Path:
    """Ensure an absolute path is within the library root."""
    root = library_root.resolve()
    pp = p.resolve()
    try:
        pp.relative_to(root)
    except Exception as e:
        raise CalibreUnsafePathError(f"path {pp} is outside library root {root}: {e!r}")
    return pp


def _synthetic_id_from_relpath(rel: str) -> int:
    """
    Deterministic negative int.

    :param rel:
    :return:
    """
    b = rel.encode("utf-8", errors="ignore")
    crc = zlib.crc32(b) & 0xFFFFFFFF
    if crc == 0:
        crc = 1
    return -int(crc)


def _find_cover_file(book_dir: Path) -> Optional[Path]:
    """
    Calibre default is cover.jpg, but we allow a few variants.

    :param book_dir:
    :return:
    """
    candidates = ["cover.jpg", "cover.jpeg", "cover.png", "Cover.jpg", "Cover.jpeg", "Cover.png"]
    for c in candidates:
        p = book_dir / c
        if p.exists() and p.is_file():
            return p
    # last resort: any file starting with cover.* in dir
    try:
        for p in sorted(book_dir.iterdir()):
            if p.is_file() and p.name.lower().startswith("cover.") and p.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp"}:
                return p
    except Exception:
        return None
    return None


def _list_format_files(book_dir: Path) -> Tuple[CalibreFormatRef, ...]:
    out: List[CalibreFormatRef] = []
    try:
        for p in sorted(book_dir.iterdir()):
            if not p.is_file():
                continue
            name_l = p.name.lower()
            if name_l == "metadata.opf" or name_l.startswith("cover."):
                continue
            if p.suffix:
                fmt = p.suffix.lstrip(".").upper()
            else:
                continue
            try:
                size = p.stat().st_size
            except Exception:
                size = None
            out.append(CalibreFormatRef(fmt=fmt, file_path=p, size_bytes=size))
    except Exception:
        return tuple()
    return tuple(out)


@dataclass(frozen=True, slots=True)
class CalibreSidecarReader:
    """
    Stream Calibre payloads using per-book sidecar files (no metadata.db).
    """

    library_root: Path

    @classmethod
    def from_root(cls, library_root: str | Path) -> "CalibreSidecarReader":
        """
        Populate the sidecar reader from a Calibre library.

        :param library_root:
        :return:
        """
        return cls(library_root=Path(library_root))

    def open_cover(self, cover_path: Path) -> IO[bytes]:
        """
        Open a cover file.

        :param cover_path:
        :return:
        """
        root = Path(self.library_root)
        safe = _ensure_path_under_root(root, Path(cover_path))
        return open(safe, "rb")

    def open_format(self, fmt: CalibreFormatRef) -> IO[bytes]:
        """
        Open a format file.

        :param fmt:
        :return:
        """
        root = Path(self.library_root)
        safe = _ensure_path_under_root(root, Path(fmt.file_path))
        return open(safe, "rb")

    @staticmethod
    def iter_file_chunks(fh: IO[bytes], *, chunk_size: int = 1024 * 1024) -> Iterator[bytes]:
        """
        Iterate over a file in chunks.

        :param fh:
        :param chunk_size:
        :return:
        """
        while True:
            chunk = fh.read(int(chunk_size))
            if not chunk:
                return
            yield chunk

    def iter_book_payloads(
        self,
        *,
        include_formats: bool = True,
        include_cover_path: bool = True,
        strict_paths: bool = False,
        best_effort: bool = True,
        max_books: Optional[int] = None,
    ) -> Iterator[CalibreBookNormalized]:
        """
        Walk the library and yield per-book payloads based on ``metadata.opf``.

        :param include_formats:
        :param include_cover_path:
        :param strict_paths:
        :param best_effort:
        :param max_books:
        :return:
        """
        root = Path(self.library_root)
        yielded = 0

        # Deterministic walk: sort dirs/files at each level.
        for dirpath, dirnames, filenames in os.walk(root, topdown=True, followlinks=False):
            # Skip Calibre internals + hidden dirs to avoid accidental churn.
            dirnames[:] = sorted(
                d for d in dirnames
                if not d.startswith(".") and d not in {"metadata.db", ".caltrash", ".calnotes"}
            )

            # Look for metadata.opf in this directory.
            opf_name = None
            for fn in filenames:
                if fn.lower() == "metadata.opf":
                    opf_name = fn
                    break
            if not opf_name:
                continue

            book_dir = Path(dirpath)
            opf_path = book_dir / opf_name

            rel = str(book_dir.relative_to(root))
            book_id = _synthetic_id_from_relpath(rel)
            warnings: List[str] = []
            warnings.append("sidecar_mode:synthetic_book_id")

            parsed = parse_metadata_opf(opf_path)
            warnings.extend(list(parsed.warnings))

            # Fallback title if OPF had nothing.
            title = parsed.title or book_dir.name

            authors = parsed.authors
            if not authors and best_effort:
                # Common Calibre layout: root/Author/Title (id)
                try:
                    parts = book_dir.relative_to(root).parts
                    if len(parts) >= 2:
                        authors = (str(parts[0]),)
                        warnings.append("authors_fallback_from_path")
                except Exception:
                    pass

            tags = parsed.tags
            languages = parsed.languages
            identifiers = dict(parsed.identifiers or {})

            # Merge extras + user metadata values into custom_values (namespaced).
            custom_values: Dict[str, Any] = {}
            for k, v in (parsed.extras or {}).items():
                custom_values[k] = v
            for k, v in (parsed.user_metadata or {}).items():
                custom_values[k] = v

            series = parsed.series

            cover_path: Optional[Path] = None
            if include_cover_path:
                cp = _find_cover_file(book_dir)
                if cp is None:
                    warnings.append("cover_missing")
                else:
                    if strict_paths:
                        cp = _ensure_path_under_root(root, cp)
                    cover_path = cp

            fmt_refs: Tuple[CalibreFormatRef, ...] = tuple()
            if include_formats:
                fmt_refs = _list_format_files(book_dir)
                if strict_paths:
                    # Validate each format path is under root.
                    safe_refs: List[CalibreFormatRef] = []
                    for fr in fmt_refs:
                        try:
                            _ensure_path_under_root(root, fr.file_path)
                            safe_refs.append(fr)
                        except Exception as e:
                            warnings.append(f"format_path_unsafe:{fr.file_path}:{e!r}")
                    fmt_refs = tuple(safe_refs)

            yield CalibreBookNormalized(
                calibre_book_id=book_id,
                title=title,
                authors=tuple(authors or ()),
                tags=tuple(tags or ()),
                languages=tuple(languages or ()),
                identifiers=identifiers,
                series=series,
                formats=fmt_refs,
                comments_html=parsed.comments_html,
                cover_path=cover_path,
                custom_values=custom_values,
                warnings=tuple(warnings),
            )

            yielded += 1
            if max_books is not None and yielded >= int(max_books):
                return
