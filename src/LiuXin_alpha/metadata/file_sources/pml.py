"""
Read PML comments and PMLZ archive metadata, including conventional cover locations and tolerant text decoding.

The module keeps malformed-input, optional dependency and resource ownership
behavior explicit for registry callers.

Example:
    Exercise pml with pytest::

        python -m pytest -q tests/metadata/file_sources/test_pml_metadata_source.py
"""

from __future__ import annotations

import io
import os
import re
import zipfile
from pathlib import Path

from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import (
    CalibreLikeLiuXinBookMetaData as Metadata,
)
from LiuXin_alpha.metadata.utils import check_isbn, string_to_authors
from LiuXin_alpha.utils.calibre import prepare_string_for_xml
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log

__license__ = "GPL v3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"

VALID_FOR = ["PML", "PMLZ"]
PRIORITY_FOR = ["PML", "PMLZ"]
RUN_COST = ["LOW"]

_COMMENT_RE = re.compile(br"\\v(.*?)\\v", flags=re.DOTALL)
_FIELD_RE = re.compile(br'([A-Za-z_][A-Za-z0-9_]*)\s*=\s*"([^"]*)"')
_CONTROL_CHARS_RE = re.compile(r"[\x00-\x1f]")


class PmlFormatError(Exception):
    """
    Signal malformed PMLZ archive structure or unreadable PML metadata input.

    Example:
        Exercise PmlFormatError with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pml_metadata_source.py
    """
    pass


def _source_name(target_file) -> str:
    """
    Return the best available source label for fallback titles and diagnostics.

    Example:
        Exercise  source name with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pml_metadata_source.py


    :param target_file: Caller-supplied path, path-like object or stream described by
        this operation.
    :return: Parsed, normalized or serialized value described above.
    """
    if isinstance(target_file, os.PathLike):
        return os.fspath(target_file)
    if isinstance(target_file, str):
        return target_file
    return getattr(target_file, "name", "") or ""


def _read_source_bytes(target_file) -> tuple[bytes, str]:
    """
    Read the complete source payload from bytes, a path or a stream and restore a caller-owned stream position when available.

    Example:
        Exercise  read source bytes with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pml_metadata_source.py


    :param target_file: Caller-supplied path, path-like object or stream described by
        this operation.
    :return: Parsed, normalized or serialized value described above.
    """
    source_name = _source_name(target_file)
    if isinstance(target_file, os.PathLike):
        target_file = os.fspath(target_file)

    if isinstance(target_file, str):
        with open(target_file, "rb") as stream:
            return stream.read(), source_name

    if isinstance(target_file, (bytes, bytearray)):
        return bytes(target_file), source_name

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
        return bytes(data), source_name

    raise TypeError("PML metadata reader expects a filesystem path or readable binary stream.")


def _default_metadata() -> Metadata:
    """
    Build minimally usable metadata for missing or explicitly tolerated malformed input.

    Example:
        Exercise  default metadata with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pml_metadata_source.py


    :return: Parsed, normalized or serialized value described above.
    """
    return Metadata(_("Unknown"), [_("Unknown")])


def _decode_field(raw: bytes) -> str:
    """
    Decode field using the format's ordered fallback policy.

    Example:
        Exercise  decode field with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pml_metadata_source.py


    :param raw: Raw value or payload to normalize, parse or serialize.
    :return: Parsed, normalized or serialized value described above.
    """
    if not raw:
        return ""
    for enc in ("utf-8", "cp1252", "latin-1"):
        try:
            return raw.decode(enc)
        except Exception:
            continue
    return raw.decode("utf-8", "replace")


def _sanitize_field(raw: bytes, *, escape_xml: bool = True) -> str:
    """
    Perform the format-specific sanitize field operation used by this metadata source.

    Example:
        Exercise  sanitize field with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pml_metadata_source.py


    :param raw: Raw value or payload to normalize, parse or serialize.
    :param escape_xml: Policy flag controlling the behavior described above.
    :return: Parsed, normalized or serialized value described above.
    """
    text = _decode_field(raw)
    if escape_xml:
        text = prepare_string_for_xml(text)
    text = _CONTROL_CHARS_RE.sub("", text)
    return text.strip()


def _normalize_zip_name(name: str) -> str:
    """
    Perform the format-specific normalize zip name operation used by this metadata source.

    Example:
        Exercise  normalize zip name with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pml_metadata_source.py


    :param name: Name, type or encoding selector used for lookup or interpretation.
    :return: Parsed, normalized or serialized value described above.
    """
    return name.replace("\\", "/").lstrip("./")


def _is_probable_pmlz(source_name: str, payload: bytes) -> bool:
    """
    Return whether the source name or ZIP contents identify a PMLZ archive.

    Example:
        Exercise  is probable pmlz with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pml_metadata_source.py


    :param source_name: Source or member label used for lookup, fallback titles or
        diagnostics.
    :param payload: Raw value or payload to normalize, parse or serialize.
    :return: True when the described condition is satisfied; otherwise False.
    """
    if source_name.lower().endswith(".pmlz"):
        return True
    if len(payload) < 4 or payload[:2] != b"PK":
        return False
    try:
        with zipfile.ZipFile(io.BytesIO(payload)) as zf:
            return any(_normalize_zip_name(name).lower().endswith(".pml") for name in zf.namelist())
    except Exception:
        return False


def _zip_lookup(zf: zipfile.ZipFile) -> dict[str, str]:
    """
    Perform the format-specific zip lookup operation used by this metadata source.

    Example:
        Exercise  zip lookup with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pml_metadata_source.py


    :param zf: Open container used for member lookup and reads; ownership remains with
        the caller.
    :return: Parsed, normalized or serialized value described above.
    """
    lookup: dict[str, str] = {}
    for name in zf.namelist():
        if name.endswith("/"):
            continue
        norm = _normalize_zip_name(name).lower()
        lookup.setdefault(norm, name)
    return lookup


def _read_cover_from_zip(
    zf: zipfile.ZipFile,
    *,
    source_name: str,
    pml_entries: list[str],
) -> bytes | None:
    """
    Return cover bytes from the first conventional PMLZ cover location that exists.

    Example:
        Exercise  read cover from zip with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pml_metadata_source.py


    :param zf: Open container used for member lookup and reads; ownership remains with
        the caller.
    :param source_name: Source or member label used for lookup, fallback titles or
        diagnostics.
    :param pml_entries: Value supplied for pml entries.
    :return: Parsed, normalized or serialized value described above.
    """
    lookup = _zip_lookup(zf)
    source_stem = Path(source_name).stem if source_name else ""
    candidates: list[str] = []
    candidates.append("cover.png")
    if source_stem:
        candidates.append(f"{source_stem}_img/cover.png")
    candidates.append("images/cover.png")

    for pml_name in pml_entries:
        norm = _normalize_zip_name(pml_name)
        parent = Path(norm).parent.as_posix()
        stem = Path(norm).stem
        if parent and parent != ".":
            candidates.append(f"{parent}/{stem}_img/cover.png")
        candidates.append(f"{stem}_img/cover.png")

    for candidate in candidates:
        member = lookup.get(candidate.lower())
        if member is None:
            continue
        try:
            return zf.read(member)
        except Exception as err:
            default_log.log_exception(
                "Failed reading candidate PMLZ cover from archive.",
                err,
                "DEBUG",
                ("member", member),
            )

    # Final fallback: first *cover.png member encountered.
    for norm_name in sorted(lookup):
        if norm_name == "cover.png" or norm_name.endswith("/cover.png"):
            try:
                return zf.read(lookup[norm_name])
            except Exception:
                continue
    return None


def _extract_pmlz_payload(
    payload: bytes,
    *,
    source_name: str,
    extract_cover: bool,
    fallback_on_parse_error: bool = False,
) -> tuple[bytes, bytes | None]:
    """
    Concatenate PML members and optionally read a cover from a PMLZ archive.

    Example:
        Exercise  extract pmlz payload with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pml_metadata_source.py


    :param payload: Raw value or payload to normalize, parse or serialize.
    :param source_name: Source or member label used for lookup, fallback titles or
        diagnostics.
    :param extract_cover: Request cover discovery or cover payload extraction when true.
    :param fallback_on_parse_error: Return safe default metadata after parse errors when
        true; otherwise raise the format error.
    :return: Parsed, normalized or serialized value described above.
    """
    pml_data = bytearray()
    cover_data = None
    try:
        with zipfile.ZipFile(io.BytesIO(payload)) as zf:
            pml_entries = sorted(
                [name for name in zf.namelist() if _normalize_zip_name(name).lower().endswith(".pml")],
                key=str.casefold,
            )
            if not pml_entries and not fallback_on_parse_error:
                raise PmlFormatError("PMLZ archive does not contain a PML member.")
            for name in pml_entries:
                try:
                    pml_data.extend(zf.read(name))
                except Exception as err:
                    default_log.log_exception(
                        "Failed reading PML entry from PMLZ archive.",
                        err,
                        "DEBUG",
                        ("entry", name),
                    )
            if extract_cover:
                cover_data = _read_cover_from_zip(zf, source_name=source_name, pml_entries=pml_entries)
    except Exception as err:
        default_log.log_exception(
            "Failed parsing PMLZ archive; falling back to empty metadata payload.",
            err,
            "DEBUG",
            ("source", source_name or "<stream>"),
        )
        if not fallback_on_parse_error:
            if isinstance(err, PmlFormatError):
                raise
            raise PmlFormatError("Failed parsing PMLZ archive.") from err
    return bytes(pml_data), cover_data


def _clear_default_authors(mi) -> None:
    """
    Clear default authors while keeping shared state coherent.

    Example:
        Exercise  clear default authors with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pml_metadata_source.py


    :param mi: Metadata object supplying or receiving the supported fields.
    :return: None.
    """
    try:
        raw_data = object.__getattribute__(mi, "_data")
    except Exception:
        raw_data = None
    if isinstance(raw_data, dict) and isinstance(raw_data.get("authors"), dict):
        raw_data["authors"].clear()
        return
    try:
        mi.authors = []
    except Exception:
        pass


def _set_authors(mi, authors: list[str]) -> None:
    """
    Set authors while preserving unrelated metadata state.

    Example:
        Exercise  set authors with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pml_metadata_source.py


    :param mi: Metadata object supplying or receiving the supported fields.
    :param authors: Ordered input values processed by this operation.
    :return: None.
    """
    if not authors:
        return
    try:
        current = getattr(mi, "authors", None)
        if isinstance(current, list):
            mi.authors = list(authors)
            return
    except Exception:
        pass

    _clear_default_authors(mi)
    for author in authors:
        try:
            mi.authors = author
        except Exception:
            break


def _extract_comment_fields(comment: bytes) -> dict[str, list[str]]:
    """
    Parse key/value fields from one PML metadata comment.

    Example:
        Exercise  extract comment fields with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pml_metadata_source.py


    :param comment: Value supplied for comment.
    :return: Parsed, normalized or serialized value described above.
    """
    fields: dict[str, list[str]] = {}
    for match in _FIELD_RE.finditer(comment):
        key = match.group(1).decode("ascii", "ignore").upper()
        value = _sanitize_field(match.group(2), escape_xml=key != "AUTHOR")
        if not value:
            continue
        fields.setdefault(key, []).append(value)
    return fields


def _parse_pml_metadata(payload: bytes, mi) -> None:
    """
    Apply title, publisher, rights, ISBN and author fields from PML comments.

    Example:
        Exercise  parse pml metadata with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pml_metadata_source.py


    :param payload: Raw value or payload to normalize, parse or serialize.
    :param mi: Metadata object supplying or receiving the supported fields.
    :return: Parsed, normalized or serialized value described above.
    """
    authors: list[str] = []
    for comment in _COMMENT_RE.findall(payload):
        fields = _extract_comment_fields(comment)

        if "TITLE" in fields:
            mi.title = fields["TITLE"][-1]
        if "PUBLISHER" in fields:
            mi.publisher = fields["PUBLISHER"][-1]
        if "COPYRIGHT" in fields:
            mi.rights = fields["COPYRIGHT"][-1]
        if "ISBN" in fields:
            cleaned = check_isbn(fields["ISBN"][-1]) or fields["ISBN"][-1]
            mi.isbn = cleaned
        for raw_author in fields.get("AUTHOR", []):
            parsed = [x.strip() for x in string_to_authors(raw_author) if x.strip()]
            if not parsed:
                parsed = [raw_author]
            authors.extend(parsed)

    if authors:
        _set_authors(mi, authors)


def get_metadata(target_file, extract_cover: bool = True, *, fallback_on_parse_error: bool = False):
    """
    Read metadata from the supported path, bytes or stream input while applying module ownership and fallback policy.

    Example:
        Exercise get metadata with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pml_metadata_source.py


    :param target_file: Caller-supplied path, path-like object or stream described by
        this operation.
    :param extract_cover: Request cover discovery or cover payload extraction when true.
    :param fallback_on_parse_error: Return safe default metadata after parse errors when
        true; otherwise raise the format error.
    :return: Parsed, normalized or serialized value described above.
    """
    mi = _default_metadata()
    try:
        payload, source_name = _read_source_bytes(target_file)
    except Exception as err:
        default_log.log_exception(
            "Failed to read PML metadata source.",
            err,
            "ERROR",
            ("source", _source_name(target_file) or "<stream>"),
        )
        if not fallback_on_parse_error:
            raise
        return mi

    if not payload:
        if _source_name(target_file).lower().endswith(".pmlz") and not fallback_on_parse_error:
            raise PmlFormatError("Empty PMLZ payload.")
        return mi

    if _is_probable_pmlz(source_name, payload):
        pml_payload, cover_bytes = _extract_pmlz_payload(
            payload,
            source_name=source_name,
            extract_cover=extract_cover,
            fallback_on_parse_error=fallback_on_parse_error,
        )
        if extract_cover:
            mi.cover_data = ("png", cover_bytes)
    else:
        pml_payload = payload
        if extract_cover and source_name:
            try:
                parent = os.path.abspath(os.path.dirname(source_name))
                name = os.path.splitext(os.path.basename(source_name))[0]
                mi.cover_data = get_cover(name, parent)
            except Exception:
                pass

    if pml_payload:
        _parse_pml_metadata(pml_payload, mi)
    return mi


def get_metadata_inplace(target_file, extract_cover: bool = True, *, fallback_on_parse_error: bool = False):
    """
    Read metadata through the path-oriented adapter exposed to registry plugins.

    Example:
        Exercise get metadata inplace with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pml_metadata_source.py


    :param target_file: Caller-supplied path, path-like object or stream described by
        this operation.
    :param extract_cover: Request cover discovery or cover payload extraction when true.
    :param fallback_on_parse_error: Return safe default metadata after parse errors when
        true; otherwise raise the format error.
    :return: Parsed, normalized or serialized value described above.
    """
    return get_metadata(target_file, extract_cover=extract_cover, fallback_on_parse_error=fallback_on_parse_error)


def get_cover(name, tdir, top_level: bool = False):
    """
    Read cover.png from the supported PML directory layouts, returning an empty payload when absent.

    Example:
        Exercise get cover with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pml_metadata_source.py


    :param name: Name, type or encoding selector used for lookup or interpretation.
    :param tdir: Directory used for temporary or generated output files.
    :param top_level: Policy flag controlling the behavior described above.
    :return: Parsed, normalized or serialized value described above.
    """
    cover_path: Path | None = None
    root = Path(tdir)

    candidates: list[Path] = []
    if top_level:
        candidates.append(root / "cover.png")
    if name:
        candidates.append(root / f"{name}_img" / "cover.png")
    candidates.append(root / "images" / "cover.png")
    candidates.extend(sorted(root.glob("*_img/cover.png")))

    for candidate in candidates:
        try:
            if candidate.is_file():
                cover_path = candidate
                break
        except Exception:
            continue

    if cover_path is None:
        return ("png", None)

    with cover_path.open("rb") as cstream:
        return ("png", cstream.read())


__all__ = [
    "VALID_FOR",
    "PRIORITY_FOR",
    "RUN_COST",
    "PmlFormatError",
    "get_metadata",
    "get_metadata_inplace",
    "get_cover",
]
