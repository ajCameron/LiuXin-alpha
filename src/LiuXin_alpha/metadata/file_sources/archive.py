"""
Archive helpers for metadata extraction and on-import archive flattening.
"""

from __future__ import annotations

import json
import os
from datetime import date
from typing import Any, Iterable, Mapping

from LiuXin_alpha.customize import FileTypePlugin
from LiuXin_alpha.metadata.utils import calibreMetaInformation
from LiuXin_alpha.utils.date import parse_only_date
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log

__license__ = "GPL v3"
__copyright__ = "2010, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"

_COMIC_EXTENSIONS = {"jpg", "jpeg", "png", "gif", "webp"}
_SUPPORTED_SINGLE_MEMBER_TYPES = {
    "lit",
    "epub",
    "mobi",
    "prc",
    "rtf",
    "pdf",
    "mp3",
    "pdb",
    "azw",
    "azw1",
    "azw3",
    "fb2",
    "fbz",
}


def _iter_clean_names(list_of_names: Iterable[str]) -> Iterable[str]:
    for raw_name in list_of_names:
        name = str(raw_name).replace("\\", "/")
        if "." not in name:
            continue
        if os.path.basename(name).lower() == "thumbs.db":
            continue
        yield name


def is_comic(list_of_names: Iterable[str]) -> bool:
    """
    Return True if all relevant files are comic-image formats.
    """
    extensions = {
        name.rpartition(".")[-1].lower()
        for name in _iter_clean_names(list_of_names)
    }
    return bool(extensions) and extensions.issubset(_COMIC_EXTENSIONS)


def archive_type(stream) -> str | None:
    """
    Detect archive type from a binary stream header.
    """
    from LiuXin_alpha.utils.libraries.calibre_zipfile import stringFileHeader

    try:
        pos = stream.tell()
    except Exception:
        pos = None

    id_ = stream.read(4)
    ans = None
    if id_ == stringFileHeader:
        ans = "zip"
    elif isinstance(id_, (bytes, bytearray)) and id_.startswith(b"Rar"):
        ans = "rar"
    elif isinstance(id_, str) and id_.startswith("Rar"):
        ans = "rar"

    if pos is not None:
        try:
            stream.seek(pos)
        except Exception:
            pass
    return ans


class ArchiveExtract(FileTypePlugin):
    name = "Archive Extract"
    author = "Kovid Goyal"
    description = _(
        "Extract common e-book formats from archives "
        "(zip/rar) files. Also try to autodetect if they are actually "
        "cbz/cbr files."
    )
    file_types = {"zip", "rar"}
    supported_platforms = ["windows", "osx", "linux"]
    on_import = True

    def run(self, archive):
        from LiuXin_alpha.utils.libraries.calibre_zipfile import ZipFile

        is_rar = str(archive).lower().endswith(".rar")
        try:
            if is_rar:
                from LiuXin_alpha.utils.decompression.unrar import extract_member, names

                with open(archive, "rb") as rf:
                    fnames = list(names(rf))
            else:
                with ZipFile(archive, "r") as zf:
                    fnames = zf.namelist()
        except Exception as err:
            default_log.log_exception(
                "Unable to inspect archive during import extraction.",
                err,
                "WARNING",
                ("archive", archive),
            )
            return archive

        fnames = list(_iter_clean_names(fnames))

        if is_comic(fnames):
            ext = ".cbr" if is_rar else ".cbz"
            temp_file = self.temporary_file("_archive_extract" + ext)
            try:
                with open(archive, "rb") as src:
                    temp_file.write(src.read())
            finally:
                temp_file.close()
            return temp_file.name

        if len(fnames) != 1:
            return archive

        fname = fnames[0]
        ext = os.path.splitext(fname)[1][1:].lower()
        if ext not in _SUPPORTED_SINGLE_MEMBER_TYPES:
            return archive

        temp_file = self.temporary_file("_archive_extract." + ext)
        try:
            if is_rar:
                from LiuXin_alpha.utils.decompression.unrar import extract_member

                with open(archive, "rb") as src:
                    extracted = extract_member(src, match=None, name=fname)
                if extracted is None:
                    return archive
                _member_name, data = extracted
                temp_file.write(data)
            else:
                with ZipFile(archive, "r") as zf:
                    temp_file.write(zf.read(fname))
        finally:
            temp_file.close()
        return temp_file.name


def _safe_int(raw: Any, default: int | None = None) -> int | None:
    try:
        return int(raw)
    except Exception:
        return default


def _safe_float(raw: Any) -> float | None:
    try:
        return float(raw)
    except Exception:
        return None


def get_comic_book_info(d, mi, series_index: str = "volume"):
    """
    Extract ComicBookInfo fields and apply them to the given metadata object.
    """
    if not isinstance(d, Mapping):
        return

    series = str(d.get("series", "") or "").strip()
    if series:
        mi.series = series
        si = d.get(series_index, None)
        if si is None:
            fallback = "issue" if series_index == "volume" else "volume"
            si = d.get(fallback, None)
        si_val = _safe_float(si)
        if si_val is not None:
            mi.series_index = si_val

    rating = _safe_float(d.get("rating", None))
    if rating is not None and rating > -1:
        mi.rating = rating

    for field_name in ("title", "publisher"):
        value = str(d.get(field_name, "") or "").strip()
        if value:
            setattr(mi, field_name, value)

    raw_tags = d.get("tags", ())
    if isinstance(raw_tags, (list, tuple)):
        tags = [str(x).strip() for x in raw_tags if str(x).strip()]
        if tags:
            mi.tags = tags

    authors = []
    for credit in d.get("credits", ()) or ():
        if not isinstance(credit, Mapping):
            continue
        role = str(credit.get("role", "") or "").strip().lower()
        if role not in {"writer", "artist", "cartoonist", "creator"}:
            continue
        person = str(credit.get("person", "") or "").strip()
        if not person:
            continue
        if ", " in person:
            person = " ".join(reversed(person.split(", ")))
        authors.append(person)
    if authors:
        mi.authors = authors

    comments = str(d.get("comments", "") or "").strip()
    if comments:
        mi.comments = comments

    puby = _safe_int(d.get("publicationYear", None))
    if puby is not None:
        pubm = _safe_int(d.get("publicationMonth", None), default=6)
        if pubm is None or not 1 <= pubm <= 12:
            pubm = 6
        try:
            base_date = date(puby, pubm, 15)
            try:
                mi.pubdate = parse_only_date(str(base_date))
            except Exception:
                # Keep publication date information even when optional date parsing
                # dependencies are unavailable in constrained environments.
                mi.pubdate = base_date
        except Exception:
            pass


def _decode_json_payload(raw_comment: bytes | str) -> Mapping[str, Any] | None:
    text: str
    if isinstance(raw_comment, bytes):
        for encoding in ("utf-8-sig", "utf-8", "cp1252"):
            try:
                text = raw_comment.decode(encoding)
                break
            except Exception:
                continue
        else:
            return None
    else:
        text = str(raw_comment)

    text = text.strip().strip("\x00")
    if not text:
        return None

    payload = None
    try:
        payload = json.loads(text)
    except Exception:
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            try:
                payload = json.loads(text[start : end + 1])
            except Exception:
                payload = None

    if isinstance(payload, Mapping):
        return payload
    return None


def get_comic_metadata(stream, stream_type, series_index: str = "volume"):
    """
    Parse embedded ComicBookInfo metadata from comic archives.
    """
    from LiuXin_alpha.utils.libraries.calibre_zipfile import ZipFile

    mi = calibreMetaInformation(None, [_("Unknown")])
    comment = None
    stream_type = (stream_type or "").lower()

    try:
        pos = stream.tell() if hasattr(stream, "tell") else None
    except Exception:
        pos = None

    try:
        if hasattr(stream, "seek"):
            stream.seek(0)
        if stream_type == "cbz":
            with ZipFile(stream) as zf:
                comment = zf.comment
        elif stream_type == "cbr":
            from LiuXin_alpha.utils.decompression.unrar import RARFile

            comment = RARFile(stream, get_comment=True).comment
    except Exception as err:
        default_log.log_exception(
            "Failed while reading comic archive metadata comment.",
            err,
            "DEBUG",
            ("stream_type", stream_type),
            ("stream_name", getattr(stream, "name", "<stream>")),
        )
    finally:
        if pos is not None and hasattr(stream, "seek"):
            try:
                stream.seek(pos)
            except Exception:
                pass

    payload = _decode_json_payload(comment) if comment else None
    if payload:
        for category, category_data in payload.items():
            if str(category).startswith("ComicBookInfo"):
                get_comic_book_info(category_data, mi, series_index=series_index)
                break
    return mi


__all__ = [
    "ArchiveExtract",
    "archive_type",
    "get_comic_book_info",
    "get_comic_metadata",
    "is_comic",
]
