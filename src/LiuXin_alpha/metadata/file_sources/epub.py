"""
Read/write EPUB metadata.
"""

from __future__ import annotations

import os
import posixpath
from io import BytesIO
from pathlib import Path
from typing import Any

from LiuXin_alpha.file_formats.opf.opf import (
    get_metadata as get_metadata_from_opf,
)
from LiuXin_alpha.file_formats.opf.opf import (
    set_metadata as set_metadata_opf,
)
from LiuXin_alpha.file_formats.opf.opf2 import OPF
from LiuXin_alpha.metadata.book.base import calibreMetadata
from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import (
    CalibreLikeLiuXinBookMetaData,
)
from LiuXin_alpha.metadata.utils import normalize_languages as normalize_languages_impl
from LiuXin_alpha.utils.decompression.localunzip import LocalZipFile
from LiuXin_alpha.utils.image_tools.imghdr import identify
from LiuXin_alpha.utils.libraries.calibre_zipfile import BadZipfile, ZipFile, safe_replace
from LiuXin_alpha.utils.libraries.liuxin_etree import etree
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.ptempfiles import TemporaryDirectory

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal <kovid at kovidgoyal.net>"

VALID_FOR = ["EPUB"]
PRIORITY_FOR = ["EPUB"]
RUN_COST = ["LOW"]


class EpubParseError(Exception):
    pass


class EPubException(Exception):
    pass


class OCFException(EPubException):
    pass


class ContainerException(OCFException):
    pass


def _is_path_like(target: Any) -> bool:
    return isinstance(target, (str, bytes, os.PathLike))


def _source_name(target: Any) -> str:
    if _is_path_like(target):
        return os.fspath(target)
    return getattr(target, "name", "<stream>")


def _localname(tag: Any) -> str:
    text = str(tag)
    return text.rsplit("}", 1)[-1]


def _ensure_bytes(raw: Any) -> bytes:
    if isinstance(raw, bytes):
        return raw
    if isinstance(raw, bytearray):
        return bytes(raw)
    if isinstance(raw, str):
        return raw.encode("utf-8", "replace")
    return bytes(raw)


def _cover_format_from_path(path: str | None) -> str:
    ext = os.path.splitext(path or "")[1].lower().lstrip(".")
    if ext == "jpg":
        return "jpeg"
    if ext:
        return ext
    return "jpeg"


def _serialize_cover_data(new_cdata: bytes, cpath: str) -> bytes:
    try:
        from LiuXin_alpha.utils.image_tools.img import save_cover_data_to
    except Exception:
        return new_cdata
    try:
        return _ensure_bytes(save_cover_data_to(new_cdata, path=None, data_fmt=_cover_format_from_path(cpath)))
    except Exception:
        return new_cdata


def _resolve_member(base_path: str, href: str | None) -> str | None:
    if not href:
        return None
    if href.startswith("/"):
        return href.lstrip("/")
    return posixpath.normpath(posixpath.join(posixpath.dirname(base_path), href))


def _extract_cover_payload(mi: Any) -> bytes | None:
    cover_data = getattr(mi, "cover_data", None)
    if isinstance(cover_data, tuple) and len(cover_data) == 2 and cover_data[1]:
        return _ensure_bytes(cover_data[1])

    if isinstance(cover_data, dict):
        try:
            key = next(iter(cover_data.keys()))
            if isinstance(key, tuple) and len(key) == 2 and key[1]:
                return _ensure_bytes(key[1])
        except Exception:
            pass

    cover_path = getattr(mi, "cover", None)
    if isinstance(cover_path, str) and cover_path:
        try:
            return Path(cover_path).read_bytes()
        except Exception:
            return None
    return None


def _to_liuxin_metadata(calibre_md: calibreMetadata):
    return CalibreLikeLiuXinBookMetaData.from_calibre(calibre_md)


def _as_opf_calibre_metadata(mi: Any):
    """
    Convert metadata to the calibre-compat class expected by OPF helpers.
    """
    from LiuXin_alpha.utils.calibre_compat.ebooks.metadata.book.base import Metadata as OPFCalibreMetadata

    if isinstance(mi, OPFCalibreMetadata):
        return mi

    if isinstance(mi, calibreMetadata):
        return OPFCalibreMetadata(getattr(mi, "title", None), getattr(mi, "authors", None), other=mi)

    if hasattr(mi, "to_calibre"):
        converted = mi.to_calibre()
        if isinstance(converted, OPFCalibreMetadata):
            return converted
        return OPFCalibreMetadata(
            getattr(converted, "title", None),
            getattr(converted, "authors", None),
            other=converted,
        )

    raise TypeError("EPUB metadata writer expects calibreMetadata or an object with to_calibre().")


def get_metadata_inplace(target_epub_path):
    """
    Extract metadata from a filesystem EPUB path.
    """
    return get_metadata(target_epub_path, extract_cover=False, calibre_metadata=False)


def get_metadata(stream_or_path, extract_cover: bool = True, calibre_metadata: bool = True):
    """
    Read metadata from an EPUB stream/path.
    """
    if _is_path_like(stream_or_path):
        with open(stream_or_path, "rb") as stream:
            return get_metadata(stream, extract_cover=extract_cover, calibre_metadata=calibre_metadata)

    if not hasattr(stream_or_path, "read"):
        raise TypeError("EPUB metadata reader expects a filesystem path or readable binary stream.")

    stream = stream_or_path
    pos = None
    source_name = _source_name(stream)
    if hasattr(stream, "tell"):
        try:
            pos = stream.tell()
        except Exception:
            pos = None
    try:
        if hasattr(stream, "seek"):
            stream.seek(0)

        try:
            reader = get_zip_reader(stream)
            opf_bytes = reader.read_bytes(reader.opf_path)
            mi, _ver, raster_cover, first_spine_item = get_metadata_from_opf(opf_bytes)
        except Exception as err:
            default_log.log_exception(
                "Failed to parse EPUB metadata.",
                err,
                "ERROR",
                ("epub_path", source_name),
                ("extract_cover", extract_cover),
                ("calibre_metadata", calibre_metadata),
            )
            if isinstance(err, EPubException):
                raise
            raise EPubException("Failed to parse EPUB metadata.") from err

        if extract_cover:
            raster_cover_path = _resolve_member(reader.opf_path, raster_cover)
            first_spine_item_path = _resolve_member(reader.opf_path, first_spine_item)
            try:
                cdata = get_cover(raster_cover_path, first_spine_item_path, reader)
                if cdata:
                    fmt = _cover_format_from_path(raster_cover_path)
                    if raster_cover_path:
                        try:
                            fmt, _w, _h = identify(cdata)
                        except Exception:
                            pass
                    mi.cover_data = (fmt, cdata)
            except Exception as err:
                default_log.log_exception(
                    "Failed while extracting EPUB cover during metadata read.",
                    err,
                    "DEBUG",
                    ("epub_path", getattr(stream, "name", "<stream>")),
                )

        mi.timestamp = None
        if not calibre_metadata:
            return _to_liuxin_metadata(mi)
        return mi
    finally:
        if pos is not None and hasattr(stream, "seek"):
            try:
                stream.seek(pos)
            except Exception:
                pass


def get_quick_metadata(stream_or_path):
    """
    Read metadata without cover extraction.
    """
    return get_metadata(stream_or_path, extract_cover=False)


class Container(dict):
    """
    Parsed OCF container map (media-type -> full-path).
    """

    def __init__(self, stream=None):
        super().__init__()
        if stream is None:
            return

        raw = stream.read()
        root = etree.fromstring(raw) if raw else None
        if root is None:
            raise OCFException("Invalid container.xml")
        if root.attrib.get("version") not in {"1.0", None}:
            raise EPubException("Unsupported OCF container version")

        rootfiles = [node for node in root.iter() if _localname(node.tag) == "rootfile"]
        if not rootfiles:
            raise EPubException("Missing <rootfile> entry in container.xml")

        for node in rootfiles:
            media_type = node.attrib.get("media-type")
            full_path = node.attrib.get("full-path")
            if not media_type or not full_path:
                continue
            self[media_type] = full_path

        if OPF.MIMETYPE not in self:
            # Keep compatibility with EPUBs that omit media-type but still provide
            # a single .opf rootfile.
            for node in rootfiles:
                full_path = node.attrib.get("full-path")
                if full_path and full_path.lower().endswith(".opf"):
                    self[OPF.MIMETYPE] = full_path
                    break


class OCF:
    MIMETYPE = "application/epub+zip"
    CONTAINER_PATH = "META-INF/container.xml"
    ENCRYPTION_PATH = "META-INF/encryption.xml"

    def __init__(self):
        raise NotImplementedError("Abstract base class")


class Encryption:
    OBFUSCATION_ALGORITHMS = frozenset({"http://ns.adobe.com/pdf/enc#RC", "http://www.idpf.org/2008/embedding"})

    def __init__(self, raw):
        self.entries: dict[str, str] = {}
        if not raw:
            return
        try:
            root = etree.fromstring(raw)
        except Exception:
            return

        for node in root.iter():
            if _localname(node.tag) != "EncryptedData":
                continue
            algorithm = ""
            uri = ""
            for child in node.iter():
                lname = _localname(child.tag)
                if lname == "EncryptionMethod":
                    algorithm = child.attrib.get("Algorithm", "")
                elif lname == "CipherReference":
                    uri = child.attrib.get("URI", "")
            if uri and algorithm:
                self.entries[uri] = algorithm

    def is_encrypted(self, uri: str | None) -> bool:
        if not uri:
            return False
        algorithm = self.entries.get(uri)
        return algorithm is not None and algorithm not in self.OBFUSCATION_ALGORITHMS


class OCFReader(OCF):
    def __init__(self):
        try:
            mimetype = self.open("mimetype").read().rstrip()
            if isinstance(mimetype, bytes):
                mimetype = mimetype.decode("ascii", "replace")
            if mimetype != OCF.MIMETYPE:
                default_log.warning(f"Invalid EPUB mimetype declaration: {mimetype!r}")
        except Exception:
            default_log.warning("EPUB has no readable mimetype declaration")

        try:
            with self.open(OCF.CONTAINER_PATH) as stream:
                self.container = Container(stream)
        except Exception as err:
            raise EPubException("Missing OCF container.xml file") from err

        self.opf_path = self.container.get(OPF.MIMETYPE)
        if not self.opf_path:
            raise EPubException("Missing OPF package file entry in container")
        self._encryption_meta_cached: Encryption | None = None

    @property
    def encryption_meta(self) -> Encryption:
        if self._encryption_meta_cached is None:
            try:
                with self.open(self.ENCRYPTION_PATH) as stream:
                    self._encryption_meta_cached = Encryption(stream.read())
            except Exception:
                self._encryption_meta_cached = Encryption(None)
        return self._encryption_meta_cached

    def read_bytes(self, name: str) -> bytes:
        with self.open(name) as stream:
            return _ensure_bytes(stream.read())


class OCFZipReader(OCFReader):
    def __init__(self, stream, mode: str = "r", root: str | None = None):
        if isinstance(stream, (LocalZipFile, ZipFile)):
            self.archive = stream
        else:
            try:
                self.archive = ZipFile(stream, mode=mode)
            except BadZipfile as err:
                raise EPubException("Not a valid ZIP-based EPUB container") from err
        self.root = root or os.getcwd()
        super().__init__()

    def open(self, name, mode: str = "r"):
        if isinstance(self.archive, LocalZipFile):
            return self.archive.open(name)
        return BytesIO(self.archive.read(name))

    def read_bytes(self, name):
        return _ensure_bytes(self.archive.read(name))


def get_zip_reader(stream, root: str | None = None):
    """
    Open a ZIP reader with fallback to local-header parser for damaged files.
    """
    try:
        zf = ZipFile(stream, mode="r")
    except Exception:
        first_error = None
        try:
            if hasattr(stream, "seek"):
                stream.seek(0)
            zf = LocalZipFile(stream)
        except Exception as second_error:
            first_error = second_error
            default_log.log_exception(
                "Failed to open EPUB as ZIP container.",
                second_error,
                "ERROR",
                ("epub_path", _source_name(stream)),
            )
            raise EPubException("Unable to open EPUB container as ZIP data.") from first_error
    return OCFZipReader(zf, root=root)


class OCFDirReader(OCFReader):
    def __init__(self, path):
        self.root = path
        super().__init__()

    def open(self, path, *args, **kwargs):
        return open(os.path.join(self.root, path), *args, **kwargs)


def _extract_cover_from_member(reader: OCFZipReader, member_name: str | None) -> bytes | None:
    if not member_name or reader.encryption_meta.is_encrypted(member_name):
        return None
    try:
        return reader.read_bytes(member_name)
    except Exception:
        return None


def _render_cover_from_spine(reader: OCFZipReader, first_spine_item: str | None) -> bytes | None:
    if not first_spine_item or reader.encryption_meta.is_encrypted(first_spine_item):
        return None
    try:
        with TemporaryDirectory("_epub_meta") as tdir:
            reader.archive.extractall(path=tdir)
            html_path = os.path.join(tdir, first_spine_item.replace("/", os.sep))
            if not os.path.exists(html_path):
                return None
            from LiuXin_alpha.file_formats import render_html_svg_workaround

            return render_html_svg_workaround(html_path, default_log)
    except Exception as err:
        default_log.log_exception(
            "Failed to render EPUB spine item as cover.",
            err,
            "DEBUG",
            ("spine_item", first_spine_item),
        )
        return None


def get_cover(raster_cover, first_spine_item, reader):
    cdata = _extract_cover_from_member(reader, raster_cover)
    if cdata:
        return cdata
    return _render_cover_from_spine(reader, first_spine_item)


def normalize_languages(opf_languages, mi_languages):
    return normalize_languages_impl(opf_languages, mi_languages)


def update_metadata(opf, mi, apply_null: bool = False, update_timestamp: bool = False, force_identifiers: bool = False):
    """
    Update an OPF2 object in-place using metadata from `mi`.
    """
    mi = _as_opf_calibre_metadata(mi)

    for field_name in ("guide", "toc", "manifest", "spine"):
        setattr(mi, field_name, None)

    if getattr(mi, "languages", None):
        mi.languages = normalize_languages(list(opf.raw_languages) or [], mi.languages)

    opf.smart_update(mi, apply_null=apply_null)

    if getattr(mi, "uuid", None):
        opf.application_id = mi.uuid

    if apply_null or force_identifiers:
        opf.set_identifiers(mi.get_identifiers())
    else:
        identifiers = opf.get_identifiers()
        identifiers.update(mi.get_identifiers())
        opf.set_identifiers({k: v for k, v in identifiers.items() if k and v})

    if update_timestamp and getattr(mi, "timestamp", None) is not None:
        opf.timestamp = mi.timestamp


def set_metadata(
    stream_or_path,
    mi,
    apply_null: bool = False,
    update_timestamp: bool = False,
    force_identifiers: bool = False,
    add_missing_cover: bool = True,
):
    """
    Write metadata into an EPUB path or read/write stream.
    """
    mi = _as_opf_calibre_metadata(mi)

    if _is_path_like(stream_or_path):
        with open(stream_or_path, "r+b") as stream:
            return set_metadata(
                stream,
                mi,
                apply_null=apply_null,
                update_timestamp=update_timestamp,
                force_identifiers=force_identifiers,
                add_missing_cover=add_missing_cover,
            )

    if not hasattr(stream_or_path, "read") or not hasattr(stream_or_path, "write"):
        raise TypeError("EPUB metadata writer expects a path or read/write binary stream.")

    stream = stream_or_path
    source_name = _source_name(stream)
    if hasattr(stream, "seek"):
        stream.seek(0)
    try:
        reader = get_zip_reader(stream, root=os.getcwd())

        new_cdata = _extract_cover_payload(mi)
        opf_bytes, _ver, raster_cover = set_metadata_opf(
            reader.read_bytes(reader.opf_path),
            mi,
            cover_prefix=posixpath.dirname(reader.opf_path),
            cover_data=new_cdata,
            apply_null=apply_null,
            update_timestamp=update_timestamp,
            force_identifiers=force_identifiers,
            add_missing_cover=add_missing_cover,
        )

        replacements: dict[str, BytesIO] = {}
        if new_cdata and raster_cover:
            cover_path = _resolve_member(reader.opf_path, raster_cover)
            if cover_path and not reader.encryption_meta.is_encrypted(cover_path):
                cover_ext = os.path.splitext(cover_path)[1].lower()
                if cover_ext in {".png", ".jpg", ".jpeg", ".webp", ".gif"}:
                    replacements[cover_path] = BytesIO(_serialize_cover_data(new_cdata, cover_path))

        opf_datastream = BytesIO(_ensure_bytes(opf_bytes))
        if isinstance(reader.archive, LocalZipFile):
            reader.archive.safe_replace(
                reader.container[OPF.MIMETYPE],
                opf_datastream,
                extra_replacements=replacements,
                add_missing=True,
            )
        else:
            safe_replace(
                stream,
                reader.container[OPF.MIMETYPE],
                opf_datastream,
                extra_replacements=replacements,
                add_missing=True,
            )
    except Exception as err:
        default_log.log_exception(
            "Failed to write EPUB metadata.",
            err,
            "ERROR",
            ("epub_path", source_name),
            ("apply_null", apply_null),
            ("update_timestamp", update_timestamp),
            ("force_identifiers", force_identifiers),
            ("add_missing_cover", add_missing_cover),
        )
        if isinstance(err, EPubException):
            raise
        raise EPubException("Failed to write EPUB metadata.") from err

    if hasattr(stream, "seek"):
        stream.seek(0)


__all__ = [
    "Container",
    "ContainerException",
    "EPubException",
    "EpubParseError",
    "Encryption",
    "OCF",
    "OCFDirReader",
    "OCFException",
    "OCFReader",
    "OCFZipReader",
    "get_cover",
    "get_metadata",
    "get_metadata_inplace",
    "get_quick_metadata",
    "get_zip_reader",
    "normalize_languages",
    "set_metadata",
    "update_metadata",
]
