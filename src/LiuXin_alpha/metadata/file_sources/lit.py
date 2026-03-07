"""
Support for reading metadata from LIT files.
"""

from __future__ import annotations

import io
import os
from urllib.parse import quote, unquote

from LiuXin_alpha.file_formats.opf.opf2 import OPF
from LiuXin_alpha.metadata.utils import calibreMetaInformation
from LiuXin_alpha.utils.image_tools.imghdr import identify
from LiuXin_alpha.utils.libraries.liuxin_six import six_string_types
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal <kovid at kovidgoyal.net>"

VALID_FOR = ["LIT"]
PRIORITY_FOR = ["LIT"]
RUN_COST = ["LOW"]


class _LitLogProxy:
    """
    Adapter exposing the tiny logger surface LIT reader code expects.
    """

    def _emit(self, level: str, *parts: object) -> None:
        message = " ".join(str(p) for p in parts if p is not None)
        if not message:
            return
        method = getattr(default_log, level, None)
        if method is None and level == "warning":
            method = getattr(default_log, "warn", None)
        if method is None and level == "warn":
            method = getattr(default_log, "warning", None)
        if method is not None:
            method(message)

    def warn(self, *parts: object) -> None:
        self._emit("warning", *parts)

    def warning(self, *parts: object) -> None:
        self._emit("warning", *parts)

    def info(self, *parts: object) -> None:
        self._emit("info", *parts)

    def debug(self, *parts: object) -> None:
        self._emit("debug", *parts)


def _load_lit_container_class():
    from LiuXin_alpha.file_formats.lit.reader import LitContainer

    return LitContainer


def _log_exception(base: str, exc: Exception, source_name: str) -> None:
    if hasattr(default_log, "log_exception"):
        default_log.log_exception(base, exc, "ERROR", ("source", source_name or "<stream>"))
        return
    warn = getattr(default_log, "warning", None) or getattr(default_log, "warn", None)
    if warn is not None:
        warn("%s (source=%s): %s" % (base, source_name or "<stream>", exc))


def _default_metadata(source_name: str = ""):
    title = _("Unknown")
    if source_name:
        stem = os.path.splitext(os.path.basename(source_name))[0].strip()
        if stem:
            title = stem
    return calibreMetaInformation(title, [_("Unknown")])


def _normalize_href(href: str | None) -> str:
    raw = str(href or "").strip().replace("\\", "/")
    if not raw:
        return ""
    raw = raw.split("#", 1)[0]
    while raw.startswith("./"):
        raw = raw[2:]
    raw = raw.lstrip("/")
    try:
        raw = unquote(raw)
    except Exception:
        pass
    return raw


def _href_candidates(href: str | None) -> tuple[str, ...]:
    raw = str(href or "")
    if not raw:
        return ()

    raw_no_fragment = raw.split("#", 1)[0]
    decoded = _normalize_href(raw_no_fragment)
    encoded = _normalize_href(quote(decoded, safe="/:@%+~.-_"))
    amp = _normalize_href(raw_no_fragment.replace("&", "%26"))

    candidates = []
    for candidate in (raw_no_fragment, decoded, encoded, amp):
        candidate = _normalize_href(candidate)
        if candidate and candidate not in candidates:
            candidates.append(candidate)
    return tuple(candidates)


def _guess_cover_format(path: str, data: bytes) -> str:
    try:
        fmt, _w, _h = identify(data)
        if fmt:
            return "jpg" if fmt == "jpeg" else fmt
    except Exception:
        pass
    ext = os.path.splitext(path or "")[1].lower().lstrip(".")
    if ext in {"jpeg", "jpe"}:
        ext = "jpg"
    return ext or "jpg"


def _extract_cover_from_guide(opf: OPF, lit_file) -> tuple[str, bytes] | None:
    manifest = getattr(lit_file, "manifest", {}) or {}
    by_path = {}
    by_path_ci = {}
    for manifest_item in manifest.values():
        path = _normalize_href(getattr(manifest_item, "path", ""))
        if not path:
            continue
        by_path[path] = manifest_item
        by_path_ci[path.casefold()] = manifest_item

    covers: list[tuple[bytes, str, str]] = []
    for guide_item in opf.iterguide() or ():
        guide_type = str(guide_item.get("type", "") or "")
        if "cover" not in guide_type.lower():
            continue
        href = guide_item.get("href", "")
        manifest_item = None
        for candidate in _href_candidates(href):
            manifest_item = by_path.get(candidate)
            if manifest_item is None:
                manifest_item = by_path_ci.get(candidate.casefold())
            if manifest_item is not None:
                break
        if manifest_item is None:
            continue
        internal = getattr(manifest_item, "internal", None)
        if not internal:
            continue
        try:
            payload = lit_file.get_file("/data/" + internal)
        except Exception:
            continue
        if not isinstance(payload, (bytes, bytearray)) or not payload:
            continue
        payload = bytes(payload)
        covers.append((payload, guide_type, _guess_cover_format(getattr(manifest_item, "path", ""), payload)))

    if not covers:
        return None

    covers.sort(key=lambda item: len(item[0]), reverse=True)
    selected_index = 0
    if len(covers) > 1 and covers[1][1] == covers[0][1] + "-standard":
        selected_index = 1
    selected = covers[selected_index]
    return selected[2], selected[0]


def read_metadata_from_stream(stream, source_name: str = ""):
    mi = _default_metadata(source_name)

    if hasattr(stream, "seek"):
        try:
            stream.seek(0)
        except Exception:
            pass

    try:
        lit_container_cls = _load_lit_container_class()
        lit_container = lit_container_cls(stream, _LitLogProxy())
        raw_opf = lit_container.get_metadata()
        if isinstance(raw_opf, bytes):
            opf_bytes = raw_opf
        else:
            opf_bytes = str(raw_opf).encode("utf-8", "replace")
        base_dir = os.getcwd()
        if source_name:
            try:
                base_dir = os.path.dirname(os.path.abspath(source_name)) or base_dir
            except Exception:
                pass

        opf = OPF(io.BytesIO(opf_bytes), base_dir)
        mi = opf.to_book_metadata()
        if not getattr(mi, "title", None):
            mi.title = _default_metadata(source_name).title
        if not getattr(mi, "authors", None):
            mi.authors = [_("Unknown")]

        inner_lit_file = getattr(lit_container, "_litfile", None)
        if inner_lit_file is not None:
            cover_data = _extract_cover_from_guide(opf, inner_lit_file)
            if cover_data is not None:
                mi.cover_data = cover_data
    except Exception as err:
        _log_exception("Failed to read metadata from LIT file.", err, source_name)
        return mi
    return mi


def get_metadata(target_file):
    """
    Read metadata from a LIT filesystem path or a readable binary stream.
    """
    stream_needs_close = False
    source_name = ""

    if isinstance(target_file, six_string_types):
        source_name = target_file
        stream_needs_close = True
        stream = open(target_file, "rb")
    elif isinstance(target_file, os.PathLike):
        source_name = os.fspath(target_file)
        stream_needs_close = True
        stream = open(source_name, "rb")
    elif hasattr(target_file, "read"):
        stream = target_file
        source_name = getattr(stream, "name", "") or ""
    else:
        raise TypeError("target_file must be a filesystem path or a binary stream")

    pos = None
    if hasattr(stream, "tell"):
        try:
            pos = stream.tell()
        except Exception:
            pos = None

    try:
        return read_metadata_from_stream(stream, source_name=source_name)
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
    "read_metadata_from_stream",
]
