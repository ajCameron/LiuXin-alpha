#!/usr/bin/env python

"""
PDF metadata source with dependency-light fallbacks.

This module intentionally avoids hard dependencies on compiled PDF bindings when
reading metadata. It extracts common metadata from:
1) the PDF Info dictionary
2) embedded XMP packets when present

Writing metadata requires an optional backend (`pypdf`).
"""

from __future__ import annotations

import importlib.util
import io
import os
import re
import shutil
import subprocess
import zlib
from collections import defaultdict
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

from LiuXin_alpha.metadata.constants import (
    INFO_DICT_KEY_DROP_SET,
    INFO_DICT_VALUE_DROP_SET,
    PRODUCER_DROP_REGEX_SET,
)
from LiuXin_alpha.metadata.metadata import MetaData
from LiuXin_alpha.metadata.utils import check_doi, check_isbn, string_to_authors
from LiuXin_alpha.utils.libraries.cleantext import clean_xml_chars
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.python_tools import check_against_regex_set, regex_dict_str_rekey

VALID_FOR = ["PDF"]
PRIORITY_FOR = ["PDF"]
RUN_COST = ["LOW"]

_WS = b" \t\r\n\f\x00"
_OBJ_RE = re.compile(rb"(\d+)\s+(\d+)\s+obj\b(.*?)\bendobj\b", re.DOTALL)
_TRAILER_RE = re.compile(rb"trailer\s*<<(.*?)>>", re.DOTALL | re.IGNORECASE)


class PdfParseError(Exception):
    """Raised when parsing a PDF structure fails in an unexpected way."""


def _normalize_text(raw: str | None) -> str:
    if not raw:
        return ""
    return re.sub(r"\s+", " ", clean_xml_chars(str(raw))).strip()


def _safe_decode(data: bytes | str | None) -> str:
    if data is None:
        return ""
    if isinstance(data, str):
        return _normalize_text(data)
    if not data:
        return ""

    # PDF Unicode strings are often UTF-16BE with BOM.
    if data.startswith(b"\xfe\xff"):
        try:
            return _normalize_text(data[2:].decode("utf-16-be", "replace"))
        except Exception:
            pass
    if data.startswith(b"\xff\xfe"):
        try:
            return _normalize_text(data[2:].decode("utf-16-le", "replace"))
        except Exception:
            pass
    for enc in ("utf-8", "latin-1"):
        try:
            return _normalize_text(data.decode(enc, "replace"))
        except Exception:
            continue
    return _normalize_text(data.decode("utf-8", "replace"))


def _skip_ws_and_comments(data: bytes, i: int) -> int:
    n = len(data)
    while i < n:
        c = data[i : i + 1]
        if c in _WS:
            i += 1
            continue
        if c == b"%":
            while i < n and data[i : i + 1] not in (b"\n", b"\r"):
                i += 1
            continue
        break
    return i


def _read_balanced(data: bytes, i: int, start: bytes, end: bytes) -> tuple[bytes, int]:
    """
    Read a balanced delimiter block. Supports << >> and [ ].
    """
    n = len(data)
    depth = 0
    out = bytearray()
    while i < n:
        if data[i : i + len(start)] == start:
            depth += 1
            out.extend(start)
            i += len(start)
            continue
        if data[i : i + len(end)] == end:
            depth -= 1
            out.extend(end)
            i += len(end)
            if depth <= 0:
                return bytes(out), i
            continue
        out.append(data[i])
        i += 1
    return bytes(out), i


def _read_literal_string(data: bytes, i: int) -> tuple[bytes, int]:
    """
    Read a PDF literal string `( ... )` with basic escape handling.
    """
    assert data[i : i + 1] == b"("
    i += 1
    n = len(data)
    depth = 1
    out = bytearray()
    while i < n:
        ch = data[i : i + 1]
        if ch == b"\\":
            i += 1
            if i >= n:
                break
            esc = data[i : i + 1]
            if esc in b"nrtbf":
                out.extend(
                    {
                        b"n": b"\n",
                        b"r": b"\r",
                        b"t": b"\t",
                        b"b": b"\b",
                        b"f": b"\f",
                    }[esc]
                )
            elif esc in (b"(", b")", b"\\"):
                out.extend(esc)
            elif esc in b"\r\n":
                # Line continuation.
                if esc == b"\r" and i + 1 < n and data[i + 1 : i + 2] == b"\n":
                    i += 1
            elif esc[:1].isdigit():
                oct_digits = bytes(esc)
                for _ in range(2):
                    if i + 1 < n and data[i + 1 : i + 2].isdigit():
                        i += 1
                        oct_digits += data[i : i + 1]
                    else:
                        break
                try:
                    out.append(int(oct_digits, 8) & 0xFF)
                except Exception:
                    out.extend(esc)
            else:
                out.extend(esc)
            i += 1
            continue
        if ch == b"(":
            depth += 1
            out.extend(ch)
            i += 1
            continue
        if ch == b")":
            depth -= 1
            i += 1
            if depth <= 0:
                break
            out.extend(ch)
            continue
        out.extend(ch)
        i += 1
    return bytes(out), i


def _read_hex_string(data: bytes, i: int) -> tuple[bytes, int]:
    assert data[i : i + 1] == b"<"
    i += 1
    n = len(data)
    out = bytearray()
    while i < n and data[i : i + 1] != b">":
        out.extend(data[i : i + 1])
        i += 1
    if i < n and data[i : i + 1] == b">":
        i += 1
    cleaned = re.sub(rb"\s+", b"", bytes(out))
    if len(cleaned) % 2:
        cleaned += b"0"
    try:
        return bytes.fromhex(cleaned.decode("ascii")), i
    except Exception:
        return cleaned, i


def _read_name(data: bytes, i: int) -> tuple[str, int]:
    assert data[i : i + 1] == b"/"
    i += 1
    n = len(data)
    out = bytearray()
    while i < n:
        ch = data[i : i + 1]
        if ch in _WS or ch in b"()<>[]{}/%":
            break
        if ch == b"#" and i + 2 < n:
            maybe_hex = data[i + 1 : i + 3]
            try:
                out.append(int(maybe_hex.decode("ascii"), 16))
                i += 3
                continue
            except Exception:
                pass
        out.extend(ch)
        i += 1
    return _safe_decode(bytes(out)), i


def _read_token(data: bytes, i: int) -> tuple[str, Any, int]:
    i = _skip_ws_and_comments(data, i)
    if i >= len(data):
        return "eof", None, i

    ch = data[i : i + 1]
    if ch == b"/":
        name, i = _read_name(data, i)
        return "name", name, i
    if ch == b"(":
        val, i = _read_literal_string(data, i)
        return "string", val, i
    if ch == b"<":
        if data[i : i + 2] == b"<<":
            val, i = _read_balanced(data, i, b"<<", b">>")
            return "dict", val, i
        val, i = _read_hex_string(data, i)
        return "hex", val, i
    if ch == b"[":
        val, i = _read_balanced(data, i, b"[", b"]")
        return "array", val, i

    n = len(data)
    start = i
    while i < n and data[i : i + 1] not in _WS + b"()<>[]{}/%":
        i += 1
    return "bare", data[start:i], i


def _parse_array(raw: bytes) -> list[str]:
    if not raw:
        return []
    body = raw[1:-1] if raw.startswith(b"[") and raw.endswith(b"]") else raw
    i = 0
    out: list[str] = []
    while i < len(body):
        kind, val, i = _read_token(body, i)
        if kind == "eof":
            break
        if kind in {"string", "hex"}:
            text = _safe_decode(val)
            if text:
                out.append(text)
        elif kind == "name":
            if val:
                out.append(_normalize_text(str(val)))
        elif kind == "bare":
            text = _safe_decode(val)
            if text:
                out.append(text)
    return out


def _parse_pdf_dict(raw: bytes) -> dict[str, Any]:
    raw = raw or b""
    start = raw.find(b"<<")
    if start < 0:
        return {}
    i = start + 2
    out: dict[str, Any] = {}
    while i < len(raw):
        i = _skip_ws_and_comments(raw, i)
        if raw[i : i + 2] == b">>":
            break
        k_kind, key, i = _read_token(raw, i)
        if k_kind != "name" or not key:
            # Try to recover by consuming one value-like token.
            _kind, _val, i = _read_token(raw, i)
            continue
        v_kind, val, i = _read_token(raw, i)
        if v_kind in {"string", "hex"}:
            out[key] = _safe_decode(val)
        elif v_kind == "name":
            out[key] = _normalize_text(str(val))
        elif v_kind == "array":
            out[key] = _parse_array(val)
        elif v_kind == "bare":
            out[key] = _safe_decode(val)
        else:
            out[key] = val
    return out


def _extract_objects(pdf_bytes: bytes) -> dict[tuple[int, int], bytes]:
    objects: dict[tuple[int, int], bytes] = {}
    for match in _OBJ_RE.finditer(pdf_bytes):
        num = int(match.group(1))
        gen = int(match.group(2))
        body = match.group(3).strip()
        objects[(num, gen)] = body
    return objects


def _find_info_ref(pdf_bytes: bytes) -> tuple[int, int] | None:
    trailers = list(_TRAILER_RE.finditer(pdf_bytes))
    for trailer in reversed(trailers):
        body = trailer.group(1)
        match = re.search(rb"/Info\s+(\d+)\s+(\d+)\s+R", body)
        if match:
            return int(match.group(1)), int(match.group(2))
    # Fallback: global search.
    match = re.search(rb"/Info\s+(\d+)\s+(\d+)\s+R", pdf_bytes)
    if match:
        return int(match.group(1)), int(match.group(2))
    return None


def _extract_info_dict(pdf_bytes: bytes, objects: dict[tuple[int, int], bytes]) -> dict[str, Any]:
    info_ref = _find_info_ref(pdf_bytes)
    if info_ref and info_ref in objects:
        return _parse_pdf_dict(objects[info_ref])

    # Heuristic fallback if trailer /Info is absent: choose an object containing
    # at least one common info key.
    candidate_keys = {b"/Title", b"/Author", b"/Creator", b"/Producer", b"/Subject", b"/Keywords"}
    for _obj_ref, body in objects.items():
        if b"<<" not in body or b">>" not in body:
            continue
        if any(key in body for key in candidate_keys):
            parsed = _parse_pdf_dict(body)
            if parsed:
                return parsed
    return {}


def _extract_stream_data(obj_body: bytes) -> bytes | None:
    match = re.search(rb"stream\r?\n(.*?)\r?\nendstream", obj_body, flags=re.DOTALL)
    if not match:
        return None
    data = match.group(1)
    header = _parse_pdf_dict(obj_body)
    filters = header.get("Filter")
    if isinstance(filters, str):
        if filters.lower() == "flatedecode":
            try:
                return zlib.decompress(data)
            except Exception:
                return data
    elif isinstance(filters, list):
        low = [x.lower() for x in filters if isinstance(x, str)]
        if "flatedecode" in low:
            try:
                return zlib.decompress(data)
            except Exception:
                return data
    return data


def _extract_xmp_packet(pdf_bytes: bytes, objects: dict[tuple[int, int], bytes]) -> bytes | None:
    # Prefer explicit metadata streams.
    for _obj_ref, body in objects.items():
        if b"/Type" in body and b"/Metadata" in body and b"stream" in body:
            data = _extract_stream_data(body)
            if data and b"<" in data:
                return data

    # Fallback: scan raw payload for an xmp packet.
    for start_pat, end_pat in (
        (b"<x:xmpmeta", b"</x:xmpmeta>"),
        (b"<rdf:RDF", b"</rdf:RDF>"),
    ):
        start = pdf_bytes.find(start_pat)
        if start >= 0:
            end = pdf_bytes.find(end_pat, start)
            if end >= 0:
                return pdf_bytes[start : end + len(end_pat)]
    return None


def _source_name(target_file) -> str:
    if isinstance(target_file, os.PathLike):
        return os.fspath(target_file)
    if isinstance(target_file, str):
        return target_file
    return getattr(target_file, "name", "") or ""


def _read_source_bytes(target_file) -> tuple[bytes, str]:
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

    raise TypeError("PDF metadata reader expects a path, bytes, or readable binary stream.")


def _default_metadata(source_name: str = "") -> MetaData:
    title = _("Unknown")
    if source_name:
        stem = os.path.splitext(os.path.basename(source_name))[0].strip()
        if stem:
            title = stem
    md = MetaData()
    md.title = title
    return md


def _payload_looks_like_pdf(payload: bytes) -> bool:
    return b"%PDF-" in payload[:1024]


def _field_values(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, dict):
        return [str(x) for x in raw.keys()]
    if isinstance(raw, str):
        return [raw]
    try:
        return [str(x) for x in list(raw)]
    except Exception:
        return [str(raw)]


def process_key_value_pair(key, value, info_dict_keys, md):
    """
    Compatibility helper: process one Info key/value pair onto `md`.
    """
    key = _normalize_text(str(key).lower())
    value_str = value
    if isinstance(value, (list, tuple)):
        value_str = ", ".join(_normalize_text(str(x)) for x in value if _normalize_text(str(x)))
    value_str = _normalize_text(str(value_str))
    if not key or not value_str:
        return md, False

    if check_against_regex_set(INFO_DICT_VALUE_DROP_SET, value_str):
        return md, True

    if key in {"author", "authors"}:
        for author in string_to_authors(value_str):
            author = _normalize_text(author)
            if author:
                md.authors = author
        return md, True

    if key in {"creator"}:
        if not check_against_regex_set(PRODUCER_DROP_REGEX_SET, value_str):
            md.producers = value_str
        return md, True

    if key in {"producer"}:
        if not check_against_regex_set(PRODUCER_DROP_REGEX_SET, value_str):
            md.producers = value_str
        return md, True

    if key in {"publisher", "ebx_publisher"}:
        md.publisher = value_str.lstrip("/")
        return md, True

    if key in {"title"}:
        md.title = value_str
        return md, True

    if key in {"subject"}:
        md.tags = value_str
        return md, True

    if key in {"keywords"}:
        tags = [x.strip() for x in re.split(r"[;,]", value_str) if x.strip()]
        for tag in tags:
            isbn = check_isbn(tag)
            if isbn:
                md.isbn = isbn
                continue
            doi = check_doi(tag)
            if doi:
                md.set_identifier("doi", doi)
                continue
            md.tags = tag
        return md, True

    if key in {"creationdate", "timestamp"}:
        md.timestamp = value_str
        return md, True

    if key in {"moddate", "last_modified"}:
        md.last_modified = value_str
        return md, True

    if key in {"llc", "pdfversion", "universal", "universal pdf"}:
        return md, True

    return md, False


def process_metadata_info_dict(info_dict, md):
    """
    Normalize and consume PDF Info dictionary content.
    """
    regex_rekey_dict = {
        r"^author$": "author",
        r"^.*creationdate$": "creationdate",
        r"^.*creator$": "creator",
        r"^moddate$": "moddate",
        r"^.*producer$": "producer",
        r"^(ebx_)?publisher$": "publisher",
        r"^title$": "title",
        r"^subject$": "subject",
        r"^keywords$": "keywords",
    }

    normalized_keys = set()
    for raw_key, raw_value in dict(info_dict).items():
        field_key = regex_dict_str_rekey(regex_rekey_dict, _normalize_text(str(raw_key).lower()))
        if not field_key:
            continue
        if check_against_regex_set(INFO_DICT_KEY_DROP_SET, field_key):
            continue

        normalized_keys.add(field_key)
        md, status = process_key_value_pair(field_key, raw_value, normalized_keys, md)
        if not status:
            default_log.log_variables(
                "Unhandled PDF info key/value while parsing metadata.",
                "DEBUG",
                ("field_key", field_key),
                ("field_value", raw_value),
            )
    return md


def process_xmp_metadata_dict(xmp_metadata_dict, metadata_return):
    """
    Consume parsed XMP metadata and merge onto `metadata_return`.
    """
    xmp_metadata_dict = dict(xmp_metadata_dict or {})

    xapmm = xmp_metadata_dict.get("xapmm") or {}
    if isinstance(xapmm, dict):
        doc_id = xapmm.get("DocumentID")
        if isinstance(doc_id, str):
            if doc_id.lower().startswith("uuid:"):
                metadata_return.uuid = doc_id.split(":", 1)[1]
            else:
                metadata_return.uuid = doc_id

    dc = xmp_metadata_dict.get("dc") or {}
    if not isinstance(dc, dict):
        return metadata_return

    title = dc.get("title")
    if isinstance(title, dict):
        title = title.get("x-default") or next(iter(title.values()), None)
    if isinstance(title, str) and _normalize_text(title):
        metadata_return.title = _normalize_text(title)

    creators = dc.get("creator")
    if isinstance(creators, str):
        creators = [creators]
    if isinstance(creators, list):
        # XMP creators are typically richer than Info /Author; prefer them.
        try:
            raw_data = object.__getattribute__(metadata_return, "_data")
            if isinstance(raw_data, dict) and isinstance(raw_data.get("authors"), dict):
                raw_data["authors"].clear()
        except Exception:
            pass
        for creator in creators:
            if isinstance(creator, str):
                for author in string_to_authors(creator):
                    if _normalize_text(author):
                        metadata_return.authors = _normalize_text(author)

    publisher = dc.get("publisher")
    if isinstance(publisher, str):
        publisher = [publisher]
    if isinstance(publisher, list):
        for pub in publisher:
            if isinstance(pub, str) and _normalize_text(pub):
                metadata_return.publisher = _normalize_text(pub)
                break

    description = dc.get("description")
    if isinstance(description, dict):
        description = description.get("x-default") or next(iter(description.values()), None)
    if isinstance(description, str) and _normalize_text(description):
        metadata_return.comments = _normalize_text(description)

    subject = dc.get("subject")
    if isinstance(subject, str):
        subject = [subject]
    if isinstance(subject, list):
        for tag in subject:
            if isinstance(tag, str) and _normalize_text(tag):
                metadata_return.tags = _normalize_text(tag)

    return metadata_return


class XmpParser:
    """
    Lightweight parser for extracting useful XMP namespaces from PDF metadata.
    """

    RDF_NS = "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}"
    XML_NS = "{http://www.w3.org/XML/1998/namespace}"
    NS_MAP = {
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#": "rdf",
        "http://purl.org/dc/elements/1.1/": "dc",
        "http://ns.adobe.com/xap/1.0/mm/": "xapmm",
        "http://www.w3.org/XML/1998/namespace": "xml",
    }

    def __init__(self, xmp: bytes | str):
        if isinstance(xmp, bytes):
            xmp = xmp.decode("utf-8", "replace")
        self.tree = ET.XML(xmp)
        self.rdftree = self.tree.find(self.RDF_NS + "RDF")

    def _parse_tag(self, el):
        ns = None
        tag = el.tag
        if tag.startswith("{"):
            ns, tag = tag[1:].split("}", 1)
            ns = self.NS_MAP.get(ns, ns)
        return ns, tag

    def _parse_value(self, el):
        bag = el.find(self.RDF_NS + "Bag")
        if bag is not None:
            return [li.text for li in bag.findall(self.RDF_NS + "li")]
        seq = el.find(self.RDF_NS + "Seq")
        if seq is not None:
            return [li.text for li in seq.findall(self.RDF_NS + "li")]
        alt = el.find(self.RDF_NS + "Alt")
        if alt is not None:
            out = {}
            for li in alt.findall(self.RDF_NS + "li"):
                out[li.get(self.XML_NS + "lang")] = li.text
            return out
        return el.text

    @property
    def meta(self):
        meta = defaultdict(dict)
        if self.rdftree is None:
            return {}
        for desc in self.rdftree.findall(self.RDF_NS + "Description"):
            for el in list(desc):
                ns, tag = self._parse_tag(el)
                if ns:
                    meta[ns][tag] = self._parse_value(el)
        return dict(meta)


def xmp_to_dict(xmp):
    return XmpParser(xmp).meta


def get_metadata(stream, *, fallback_on_parse_error: bool = False):
    source_name = _source_name(stream)
    try:
        pdf_bytes, source_name = _read_source_bytes(stream)
        if not pdf_bytes:
            raise PdfParseError("Empty PDF payload")
        if not _payload_looks_like_pdf(pdf_bytes):
            raise PdfParseError("PDF payload does not contain a PDF header.")
        objects = _extract_objects(pdf_bytes)
        if not objects:
            raise PdfParseError("PDF payload does not contain parseable PDF objects.")
    except Exception as err:
        default_log.log_exception(
            "Failed to read PDF metadata source.",
            err,
            "ERROR",
            ("source", source_name or "<stream>"),
        )
        if not fallback_on_parse_error:
            if isinstance(err, PdfParseError):
                raise
            raise PdfParseError("Failed to read PDF metadata source.") from err
        md = _default_metadata(source_name)
        try:
            md.finalize()
        except Exception:
            pass
        if not getattr(md, "authors", None):
            md.authors = _("Unknown Author")
        return md

    md = _default_metadata(source_name)
    try:
        info_dict = _extract_info_dict(pdf_bytes, objects)
        if info_dict:
            md = process_metadata_info_dict(info_dict, md)
    except Exception as err:
        default_log.log_exception(
            "Failed while parsing PDF info dictionary.",
            err,
            "DEBUG",
            ("source", source_name or "<stream>"),
        )

    try:
        xmp_packet = _extract_xmp_packet(pdf_bytes, objects)
        if xmp_packet:
            xmp_metadata_dict = xmp_to_dict(xmp_packet)
            md = process_xmp_metadata_dict(xmp_metadata_dict, md)
    except Exception as err:
        default_log.log_exception(
            "Failed while parsing embedded PDF XMP metadata.",
            err,
            "DEBUG",
            ("source", source_name or "<stream>"),
        )

    # Scan Info fields for recognizable identifiers if still missing.
    try:
        ids = md.get_identifiers() if hasattr(md, "get_identifiers") else {}
    except Exception:
        ids = {}
    info_dict = _extract_info_dict(pdf_bytes, objects)
    for scheme, check_func in (("doi", check_doi), ("isbn", check_isbn)):
        if scheme in ids and ids.get(scheme):
            continue
        for value in info_dict.values():
            if isinstance(value, list):
                values = value
            else:
                values = [value]
            for token in values:
                tok = _normalize_text(str(token))
                if not tok:
                    continue
                found = check_func(tok)
                if found:
                    try:
                        md.set_identifier(scheme, found)
                    except Exception:
                        try:
                            md.set_identifiers({scheme: found})
                        except Exception:
                            pass
                    break

    try:
        md.finalize()
    except Exception:
        pass

    if not getattr(md, "title", None):
        md.title = _default_metadata(source_name).title
    if not getattr(md, "authors", None):
        md.authors = _("Unknown Author")
    return md


def get_metadata_inplace(target_file, *, fallback_on_parse_error: bool = False):
    with open(target_file, "rb") as target_pdf_stream:
        return get_metadata(target_pdf_stream, fallback_on_parse_error=fallback_on_parse_error)


def _first_value(raw: Any) -> str | None:
    vals = _field_values(raw)
    for val in vals:
        normed = _normalize_text(str(val))
        if normed:
            return normed
    return None


def _metadata_to_pdf_dict(mi) -> dict[str, str]:
    out: dict[str, str] = {}
    title = _first_value(getattr(mi, "title", None))
    if title:
        out["/Title"] = title

    authors = _field_values(getattr(mi, "authors", None))
    if authors:
        out["/Author"] = ", ".join(_normalize_text(x) for x in authors if _normalize_text(x))

    comments = _first_value(getattr(mi, "comments", None))
    if comments:
        out["/Subject"] = comments

    tags = _field_values(getattr(mi, "tags", None))
    if tags:
        out["/Keywords"] = ", ".join(_normalize_text(x) for x in tags if _normalize_text(x))

    producer = _first_value(getattr(mi, "producers", None))
    if producer:
        out["/Producer"] = producer

    creator = _first_value(getattr(mi, "creator_sort", None))
    if creator:
        out["/Creator"] = creator

    publisher = _first_value(getattr(mi, "publisher", None))
    if publisher:
        out["/Publisher"] = publisher

    series = _first_value(getattr(mi, "series", None))
    if series:
        out["/Series"] = series
        series_index = _first_value(getattr(mi, "series_index", None))
        if series_index:
            out["/SeriesIndex"] = series_index
    return out


def set_metadata(stream, mi):
    """
    Write metadata into a PDF stream.

    Requires `pypdf` as an optional runtime dependency. If unavailable, this
    raises a clear RuntimeError.
    """
    if isinstance(stream, os.PathLike):
        stream = os.fspath(stream)
    if isinstance(stream, str):
        with open(stream, "r+b") as f:
            return set_metadata(f, mi)
    if not hasattr(stream, "read") or not hasattr(stream, "write"):
        raise TypeError("set_metadata expects a writable binary stream or filesystem path.")

    if importlib.util.find_spec("pypdf") is None:
        raise RuntimeError(
            "PDF metadata writing backend is unavailable. Install optional dependency `pypdf`."
        )

    from pypdf import PdfReader, PdfWriter

    pos = None
    if hasattr(stream, "tell"):
        try:
            pos = stream.tell()
        except Exception:
            pos = None

    stream.seek(0)
    reader = PdfReader(stream)
    writer = PdfWriter()
    for page in reader.pages:
        writer.add_page(page)

    existing = {}
    try:
        existing = dict(reader.metadata or {})
    except Exception:
        existing = {}
    existing.update(_metadata_to_pdf_dict(mi))
    if existing:
        writer.add_metadata(existing)

    tmp = io.BytesIO()
    writer.write(tmp)
    payload = tmp.getvalue()

    stream.seek(0)
    stream.truncate()
    stream.write(payload)
    if hasattr(stream, "flush"):
        stream.flush()

    if pos is not None and hasattr(stream, "seek"):
        try:
            stream.seek(min(pos, len(payload)))
        except Exception:
            pass


get_quick_metadata = get_metadata


def get_tool(tool_name):
    """
    Resolve an external PDF utility binary path, if available.
    """
    try:
        from LiuXin_alpha.file_formats.pdf.pdftohtml import PDFTOHTML

        base = os.path.dirname(PDFTOHTML)
        tool_path = os.path.join(base, tool_name)
        if os.path.exists(tool_path):
            return tool_path
    except Exception:
        pass

    found = shutil.which(tool_name)
    return found


def read_info(outputdir, get_cover):
    """
    Compatibility shim for legacy worker entrypoint.
    """
    src = Path(outputdir) / "src.pdf"
    if not src.is_file():
        return None
    md = get_metadata_inplace(src)
    ans = {}
    title = _first_value(getattr(md, "title", None))
    authors = _field_values(getattr(md, "authors", None))
    tags = _field_values(getattr(md, "tags", None))
    producer = _first_value(getattr(md, "producers", None))
    if title:
        ans["Title"] = title
    if authors:
        ans["Author"] = ", ".join(authors)
    if tags:
        ans["Keywords"] = ", ".join(tags)
    if producer:
        ans["Producer"] = producer
    # Cover extraction is backend-dependent and intentionally omitted in this shim.
    del get_cover
    return ans


def page_images(pdfpath, outputdir, first=1, last=1):
    """
    Render PDF pages to images using `pdftoppm` when available.
    """
    pdf_to_ppm = get_tool("pdftoppm")
    if not pdf_to_ppm:
        raise RuntimeError("pdftoppm is not available on PATH.")
    outputdir = os.path.abspath(outputdir)
    subprocess.check_call(
        [
            pdf_to_ppm,
            "-cropbox",
            "-jpeg",
            "-f",
            str(first),
            "-l",
            str(last),
            pdfpath,
            os.path.join(outputdir, "page-images"),
        ]
    )


def get_calibre_metadata(stream, cover=True):
    """
    Compatibility helper returning a calibre-like metadata object.
    """
    del cover
    md = get_metadata(stream)
    if hasattr(md, "to_calibre"):
        return md.to_calibre()
    return md


__all__ = [
    "VALID_FOR",
    "PRIORITY_FOR",
    "RUN_COST",
    "PdfParseError",
    "XmpParser",
    "get_metadata",
    "get_metadata_inplace",
    "get_quick_metadata",
    "set_metadata",
    "get_tool",
    "read_info",
    "page_images",
    "get_calibre_metadata",
    "process_key_value_pair",
    "process_metadata_info_dict",
    "process_xmp_metadata_dict",
    "xmp_to_dict",
]
