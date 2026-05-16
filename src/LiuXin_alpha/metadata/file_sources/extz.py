"""
Read/write metadata in archive-based text formats (TXTZ/HTMLZ/EXTZ).
"""

from __future__ import annotations

import os
import posixpath
from io import BytesIO
from pathlib import Path
from typing import Any

from LiuXin_alpha.file_formats.opf.opf import _sanitize_metadata_for_xml
from LiuXin_alpha.file_formats.opf.opf2 import OPF
from LiuXin_alpha.metadata.metadata import MetaData as MetaInformation
from LiuXin_alpha.utils.calibre_compat.ebooks.metadata.book.base import Metadata as OPFCalibreMetadata
from LiuXin_alpha.utils.libraries.calibre_zipfile import ZipFile, safe_replace
from LiuXin_alpha.utils.libraries.liuxin_etree import etree
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log

__license__ = "GPL v3"
__copyright__ = "2011, John Schember <john@nachtimwald.com>"

_IMAGE_EXTENSIONS = {"jpeg", "jpg", "png", "webp", "gif", "bmp"}


def _is_path_like(target: Any) -> bool:
    return isinstance(target, (str, bytes, os.PathLike))


def _source_name(target: Any) -> str:
    if _is_path_like(target):
        return os.fspath(target)
    return getattr(target, "name", "<stream>")


def _fallback_metadata() -> MetaInformation:
    return MetaInformation(_("Unknown"), [_("Unknown")])


def _as_opf_calibre_metadata(mi: Any) -> OPFCalibreMetadata:
    if isinstance(mi, OPFCalibreMetadata):
        return mi

    if hasattr(mi, "to_calibre"):
        converted = mi.to_calibre()
        if isinstance(converted, OPFCalibreMetadata):
            return converted
        return OPFCalibreMetadata(getattr(converted, "title", None), getattr(converted, "authors", None), other=converted)

    return OPFCalibreMetadata(getattr(mi, "title", None), getattr(mi, "authors", None), other=mi)


def _serialize_cover_data(new_cdata: bytes, cpath: str) -> bytes:
    try:
        from LiuXin_alpha.utils.image_tools.img import save_cover_data_to
    except Exception:
        return new_cdata
    try:
        fmt = os.path.splitext(cpath)[1].lstrip(".") or "jpeg"
        data = save_cover_data_to(new_cdata, path=None, data_fmt=fmt)
        if isinstance(data, bytes):
            return data
        if isinstance(data, bytearray):
            return bytes(data)
        if isinstance(data, str):
            return data.encode("utf-8", "replace")
        return bytes(data)
    except Exception:
        return new_cdata


def _looks_like_cover_path(raw: str | None) -> bool:
    ext = os.path.splitext(raw or "")[1].lower().lstrip(".")
    return ext in _IMAGE_EXTENSIONS


def _manifest_href_by_id(opf: OPF, item_id: str | None) -> str | None:
    if not item_id:
        return None
    for item in opf.itermanifest():
        if item.get("id") == item_id:
            href = item.get("href")
            if href:
                return href
    return None


def _resolve_cover_href(opf: OPF) -> str | None:
    cover_href = opf.raster_cover
    if cover_href:
        return cover_href

    guide_cover = getattr(opf, "guide_raster_cover", None)
    if guide_cover is not None:
        if isinstance(guide_cover, str) and guide_cover:
            return guide_cover
        if hasattr(guide_cover, "get"):
            href = guide_cover.get("href") or guide_cover.get("path")
            if href:
                return href

    for meta in opf.metadata.xpath('//*[local-name()="meta" and @name="cover"]'):
        value = (meta.get("content") or "").strip()
        if not value:
            continue
        if _looks_like_cover_path(value):
            return value
        href = _manifest_href_by_id(opf, value)
        if href:
            return href

    # Calibre compatibility fallback: look up guide cover references even when
    # the cover is not wired in metadata.
    try:
        for href in opf.guide_cover_path(opf.root):
            if _looks_like_cover_path(href):
                return href
    except Exception:
        pass

    # TXTZ may store a dedicated relative cover path element outside OPF
    # metadata conventions.
    try:
        for node in opf.root.xpath('//*[local-name()="cover-relpath-from-base"]'):
            candidate = (getattr(node, "text", None) or "").strip()
            if candidate and _looks_like_cover_path(candidate):
                return candidate
    except Exception:
        pass

    return None


def _cover_member_from_opf(opf: OPF, opf_path: str) -> str | None:
    cover_href = _resolve_cover_href(opf)
    if not cover_href:
        return None
    if cover_href.startswith("/"):
        return cover_href.lstrip("/")
    return posixpath.normpath(posixpath.join(posixpath.dirname(opf_path), cover_href))


def get_first_opf_name(zf) -> str:
    """
    Return the best OPF candidate from an EXTZ archive.
    """
    names = [str(name) for name in zf.namelist()]

    # Prefer EPUB-style container resolution when present.
    if "META-INF/container.xml" in names:
        try:
            root = etree.fromstring(zf.read("META-INF/container.xml"))
            for node in root.iter():
                if str(node.tag).rsplit("}", 1)[-1] != "rootfile":
                    continue
                media_type = node.attrib.get("media-type")
                full_path = node.attrib.get("full-path")
                if media_type == OPF.MIMETYPE and full_path:
                    return full_path.lstrip("/")
            for node in root.iter():
                if str(node.tag).rsplit("}", 1)[-1] != "rootfile":
                    continue
                full_path = node.attrib.get("full-path")
                if full_path and full_path.lower().endswith(".opf"):
                    return full_path.lstrip("/")
        except Exception:
            pass

    opf_names = [name for name in names if name.lower().endswith(".opf")]
    if not opf_names:
        raise FileNotFoundError("No OPF found in EXTZ archive")

    # Historically these formats usually store metadata.opf at root.
    top_level = [name for name in opf_names if "/" not in name.strip("/")]
    if top_level:
        return sorted(top_level)[0]

    return sorted(opf_names)[0]


def get_metadata(stream_or_path, extract_cover: bool = True):
    """
    Return metadata from an EXTZ stream/path as a LiuXin metadata object.
    """
    if _is_path_like(stream_or_path):
        with open(stream_or_path, "rb") as stream:
            return get_metadata(stream, extract_cover=extract_cover)

    stream = stream_or_path
    if not hasattr(stream, "read"):
        raise TypeError("EXTZ metadata reader expects a filesystem path or readable binary stream.")

    source_name = _source_name(stream)
    pos = None
    if hasattr(stream, "tell"):
        try:
            pos = stream.tell()
        except Exception:
            pos = None

    mi = _fallback_metadata()
    try:
        if hasattr(stream, "seek"):
            stream.seek(0)
        with ZipFile(stream) as zf:
            opf_name = get_first_opf_name(zf)
            opf_stream = BytesIO(zf.read(opf_name))
            opf = OPF(opf_stream)
            mi = opf.to_book_metadata(calibre=False)

            if extract_cover:
                cover_member = _cover_member_from_opf(opf, opf_name)
                if cover_member:
                    try:
                        raw = zf.read(cover_member)
                    except Exception as err:
                        default_log.log_exception(
                            "Failed to read cover data from EXTZ archive.",
                            err,
                            "DEBUG",
                            ("archive", source_name),
                            ("cover_member", cover_member),
                        )
                    else:
                        mi.cover_data = (os.path.splitext(cover_member)[1].lstrip(".").lower(), raw)
    except Exception as err:
        default_log.log_exception(
            "Problem extracting metadata from an EXTZ archive.",
            err,
            "DEBUG",
            ("archive", source_name),
            ("extract_cover", extract_cover),
        )
        return mi
    finally:
        if pos is not None and hasattr(stream, "seek"):
            try:
                stream.seek(pos)
            except Exception:
                pass

    return mi


def set_metadata(stream_or_path, mi):
    """
    Write metadata into an EXTZ path or read/write stream.
    """
    if _is_path_like(stream_or_path):
        with open(stream_or_path, "r+b") as stream:
            return set_metadata(stream, mi)

    stream = stream_or_path
    if not hasattr(stream, "read") or not hasattr(stream, "write"):
        raise TypeError("EXTZ metadata writer expects a path or read/write binary stream.")

    source_name = _source_name(stream)

    try:
        if hasattr(stream, "seek"):
            stream.seek(0)
        with ZipFile(stream) as zf:
            opf_path = get_first_opf_name(zf)
            opf = OPF(BytesIO(zf.read(opf_path)))

        md = _sanitize_metadata_for_xml(_as_opf_calibre_metadata(mi))

        cover_data = None
        try:
            cover_info = getattr(md, "cover_data", None)
            if isinstance(cover_info, tuple) and len(cover_info) == 2 and cover_info[1]:
                cover_data = cover_info[1]
        except Exception:
            cover_data = None
        if cover_data is None:
            cover_path = getattr(md, "cover", None)
            if isinstance(cover_path, str) and cover_path:
                try:
                    cover_data = Path(cover_path).read_bytes()
                except Exception:
                    cover_data = None

        replacements: dict[str, BytesIO] = {}
        if cover_data:
            cpath = _cover_member_from_opf(opf, opf_path) or "cover.jpg"
            replacements[cpath] = BytesIO(_serialize_cover_data(cover_data, cpath))
            md.cover = cpath

        opf.smart_update(md, replace_metadata=True)
        new_opf = BytesIO(opf.render())
        safe_replace(stream, opf_path, new_opf, extra_replacements=replacements, add_missing=True)
        if hasattr(stream, "seek"):
            stream.seek(0)
    except Exception as err:
        default_log.log_exception(
            "Failed to write metadata to EXTZ archive.",
            err,
            "ERROR",
            ("archive", source_name),
        )
        raise


__all__ = [
    "get_first_opf_name",
    "get_metadata",
    "set_metadata",
]
