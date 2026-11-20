#!/usr/bin/env python

"""
Read and update PDF Info/XMP metadata with bounded token parsing and optional external-tool fallbacks.

Reading goes through :mod:`LiuXin_alpha.file_formats.pdf.objects`, which follows the
file's cross-reference data (object streams, incremental updates and hybrid files
included), opens files encrypted with an empty password, and bounds its work; this
module maps what it finds onto LiuXin's metadata. The module keeps malformed-input,
optional dependency and resource ownership behavior explicit for registry callers.

Example:
    Exercise pdf with pytest::

        python -m pytest -q tests/metadata/file_sources/test_pdf_metadata_source.py
"""

from __future__ import annotations

import importlib.util
import io
import os
import re
import shutil
import subprocess
from collections import defaultdict
from typing import Any
from xml.etree import ElementTree as ET

from LiuXin_alpha.metadata.constants import (
    INFO_DICT_KEY_DROP_SET,
    INFO_DICT_VALUE_DROP_SET,
    PRODUCER_DROP_REGEX_SET,
)
from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import (
    CalibreLikeLiuXinBookMetaData as MetaData,
)
from LiuXin_alpha.metadata.utils import check_doi, check_isbn, string_to_authors
from LiuXin_alpha.utils.libraries.cleantext import clean_xml_chars
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.python_tools import (
    check_against_regex_set,
    regex_dict_str_rekey,
)

VALID_FOR = ["PDF"]
PRIORITY_FOR = ["PDF"]
RUN_COST = ["LOW"]

class PdfParseError(Exception):
    """
    Signal malformed, unsupported or unreadable PDF metadata input.

    Example:
        Exercise PdfParseError with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pdf_metadata_source.py
    """


def _normalize_text(raw: str | None) -> str:
    """
    Collapse whitespace and trim a possibly absent metadata text value.

    Example:
        Exercise  normalize text with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pdf_metadata_source.py


    :param raw: Raw value or payload to normalize, parse or serialize.
    :return: Parsed, normalized or serialized value described above.
    """
    if not raw:
        return ""
    return re.sub(r"\s+", " ", clean_xml_chars(str(raw))).strip()


def _source_name(target_file) -> str:
    """
    Return the best available source label for fallback titles and diagnostics.

    Example:
        Exercise  source name with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pdf_metadata_source.py


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

            python -m pytest -q tests/metadata/file_sources/test_pdf_metadata_source.py


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

    raise TypeError("PDF metadata reader expects a path, bytes, or readable binary stream.")


def _seekable_binary(stream) -> bool:
    """
    Tell whether a stream is a seekable binary file the object reader can read ranges from.

    Example:
        >>> _seekable_binary(io.BytesIO(b"%PDF")), _seekable_binary(io.StringIO("%PDF"))
        (True, False)


    :param stream: The caller's stream.
    :return: Whether it is binary and seekable.
    """
    if not isinstance(stream, (io.BufferedIOBase, io.RawIOBase)):
        return False
    try:
        return bool(stream.seekable())
    except Exception:
        return False


def _read_pdf(target_file):
    """
    Read a PDF's Info dictionary and XMP packet with the bounded object reader.

    Paths are opened and read range by range, as are seekable binary streams, whose
    position is restored; anything else is read whole first.

    Example:
        Exercise  read pdf with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pdf_metadata_source.py


    :param target_file: A path, bytes, or a readable stream.
    :return: The :class:`~LiuXin_alpha.file_formats.pdf.objects.PdfMetadata` and the source name.
    :raises PdfParseError: The payload is not a readable PDF, or is encrypted with a password.
    """
    from LiuXin_alpha.file_formats.pdf.objects import PdfReadError, read_metadata

    source_name = _source_name(target_file)
    if isinstance(target_file, os.PathLike):
        target_file = os.fspath(target_file)
    try:
        if isinstance(target_file, str):
            with open(target_file, "rb") as stream:
                return read_metadata(stream), source_name
        if _seekable_binary(target_file):
            position = target_file.tell()
            try:
                return read_metadata(target_file), source_name
            finally:
                target_file.seek(position)
        payload, source_name = _read_source_bytes(target_file)
        if not payload:
            raise PdfParseError("Empty PDF payload")
        return read_metadata(payload), source_name
    except PdfReadError as err:
        raise PdfParseError(str(err)) from err


def _default_metadata(source_name: str = "") -> MetaData:
    """
    Build minimally usable metadata for missing or explicitly tolerated malformed input.

    Example:
        Exercise  default metadata with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pdf_metadata_source.py


    :param source_name: Source or member label used for lookup, fallback titles or
        diagnostics.
    :return: Parsed, normalized or serialized value described above.
    """
    title = _("Unknown")
    if source_name:
        stem = os.path.splitext(os.path.basename(source_name))[0].strip()
        if stem:
            title = stem
    md = MetaData()
    md.title = title
    return md


def _field_values(raw: Any) -> list[str]:
    """
    Perform the format-specific field values operation used by this metadata source.

    Example:
        Exercise  field values with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pdf_metadata_source.py


    :param raw: Raw value or payload to normalize, parse or serialize.
    :return: Parsed, normalized or serialized value described above.
    """
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
    Map one recognized PDF Info key into the destination metadata object.

    Example:
        Exercise process key value pair with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pdf_metadata_source.py


    :param key: Name, type or encoding selector used for lookup or interpretation.
    :param value: Offset, bound or scalar value used by the operation.
    :param info_dict_keys: Value supplied for info dict keys.
    :param md: Metadata object supplying or receiving the supported fields.
    :return: None.
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
    Apply supported PDF Info dictionary fields to the destination metadata object.

    Example:
        Exercise process metadata info dict with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pdf_metadata_source.py


    :param info_dict: Parsed XML, PDF or metadata node used as the operation context.
    :param md: Metadata object supplying or receiving the supported fields.
    :return: None.
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
    Apply preferred XMP fields and containers to the destination metadata object.

    Example:
        Exercise process xmp metadata dict with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pdf_metadata_source.py


    :param xmp_metadata_dict: Parsed XML, PDF or metadata node used as the operation
        context.
    :param metadata_return: Metadata object supplying or receiving the supported fields.
    :return: None.
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
    Convert XMP/RDF XML properties and containers into simple Python values.

    Example:
        Exercise XmpParser with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pdf_metadata_source.py
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
        """
        Implement init for the bound metadata helper while preserving its container invariants.

        Example:
            Exercise XmpParser.  init   with pytest::

                python -m pytest -q tests/metadata/file_sources/test_pdf_metadata_source.py


        :param xmp: Raw value or payload to normalize, parse or serialize.
        :return: None.
        """
        if isinstance(xmp, bytes):
            xmp = xmp.decode("utf-8", "replace")
        self.tree = ET.XML(xmp)
        self.rdftree = self.tree.find(self.RDF_NS + "RDF")

    def _parse_tag(self, el):
        """
        Parse tag without inventing absent metadata values.

        Example:
            Exercise XmpParser. parse tag with pytest::

                python -m pytest -q tests/metadata/file_sources/test_pdf_metadata_source.py


        :param el: Parsed XML, PDF or metadata node used as the operation context.
        :return: Parsed, normalized or serialized value described above.
        """
        ns = None
        tag = el.tag
        if tag.startswith("{"):
            ns, tag = tag[1:].split("}", 1)
            ns = self.NS_MAP.get(ns, ns)
        return ns, tag

    def _parse_value(self, el):
        """
        Parse value without inventing absent metadata values.

        Example:
            Exercise XmpParser. parse value with pytest::

                python -m pytest -q tests/metadata/file_sources/test_pdf_metadata_source.py


        :param el: Parsed XML, PDF or metadata node used as the operation context.
        :return: Parsed, normalized or serialized value described above.
        """
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
        """
        Implement meta for the bound metadata helper while preserving its container invariants.

        Example:
            Exercise XmpParser.meta with pytest::

                python -m pytest -q tests/metadata/file_sources/test_pdf_metadata_source.py


        :return: Parsed, normalized or serialized value described above.
        """
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
    """
    Parse XMP XML into the simplified property mapping used by the PDF reader.

    Example:
        Exercise xmp to dict with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pdf_metadata_source.py


    :param xmp: Raw value or payload to normalize, parse or serialize.
    :return: Parsed, normalized or serialized value described above.
    """
    return XmpParser(xmp).meta


def get_metadata(stream, *, fallback_on_parse_error: bool = False):
    """
    Read metadata from the supported path, bytes or stream input while applying module ownership and fallback policy.

    Example:
        Exercise get metadata with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pdf_metadata_source.py


    :param stream: Caller-supplied path, path-like object or stream described by this
        operation.
    :param fallback_on_parse_error: Return safe default metadata after parse errors when
        true; otherwise raise the format error.
    :return: Parsed, normalized or serialized value described above.
    """
    source_name = _source_name(stream)
    try:
        pdf_metadata, source_name = _read_pdf(stream)
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
            raise PdfParseError(f"Failed to read PDF metadata source: {err}") from err
        md = _default_metadata(source_name)
        try:
            md.finalize()
        except Exception:
            pass
        if not getattr(md, "authors", None):
            md.authors = _("Unknown Author")
        return md

    for problem in pdf_metadata.problems:
        default_log.log_variables(
            "Part of a PDF's metadata could not be read.",
            "DEBUG",
            ("source", source_name or "<stream>"),
            ("problem", problem),
        )

    md = _default_metadata(source_name)
    info_dict = pdf_metadata.info
    try:
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
        if pdf_metadata.xmp:
            xmp_metadata_dict = xmp_to_dict(pdf_metadata.xmp)
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
    """
    Read metadata through the path-oriented adapter exposed to registry plugins.

    Example:
        Exercise get metadata inplace with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pdf_metadata_source.py


    :param target_file: Caller-supplied path, path-like object or stream described by
        this operation.
    :param fallback_on_parse_error: Return safe default metadata after parse errors when
        true; otherwise raise the format error.
    :return: Parsed, normalized or serialized value described above.
    """
    with open(target_file, "rb") as target_pdf_stream:
        return get_metadata(target_pdf_stream, fallback_on_parse_error=fallback_on_parse_error)


def _first_value(raw: Any) -> str | None:
    """
    Return the first usable value under fallback policy.

    Example:
        Exercise  first value with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pdf_metadata_source.py


    :param raw: Raw value or payload to normalize, parse or serialize.
    :return: Parsed, normalized or serialized value described above.
    """
    vals = _field_values(raw)
    for val in vals:
        normed = _normalize_text(str(val))
        if normed:
            return normed
    return None


def _metadata_to_pdf_dict(mi) -> dict[str, str]:
    """
    Serialize supported metadata fields into sanitized PDF Info dictionary strings.

    Example:
        Exercise  metadata to pdf dict with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pdf_metadata_source.py


    :param mi: Metadata object supplying or receiving the supported fields.
    :return: Parsed, normalized or serialized value described above.
    """
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
    Rewrite supported metadata fields without taking ownership of a caller-supplied stream.

    Example:
        Exercise set metadata with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pdf_metadata_source.py


    :param stream: Caller-supplied path, path-like object or stream described by this
        operation.
    :param mi: Metadata object supplying or receiving the supported fields.
    :return: None.
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
    Return the first available supported PDF command-line tool for the requested operation.

    Example:
        Exercise get tool with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pdf_metadata_source.py


    :param tool_name: Name, type or encoding selector used for lookup or interpretation.
    :return: Parsed, normalized or serialized value described above.
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




def page_images(pdfpath, outputdir, first=1, last=1):
    """
    Render a bounded page range to image files with the configured PDF backend.

    Example:
        Exercise page images with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pdf_metadata_source.py


    :param pdfpath: Caller-supplied path, path-like object or stream described by this
        operation.
    :param outputdir: Directory used for temporary or generated output files.
    :param first: Offset, bound or scalar value used by the operation.
    :param last: Offset, bound or scalar value used by the operation.
    :return: Parsed, normalized or serialized value described above.
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
    Read PDF metadata through the compatibility entry point and optionally request cover data.

    Example:
        Exercise get calibre metadata with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pdf_metadata_source.py


    :param stream: Caller-supplied path, path-like object or stream described by this
        operation.
    :param cover: Request cover discovery or cover payload extraction when true.
    :return: Parsed, normalized or serialized value described above.
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
    "page_images",
    "get_calibre_metadata",
    "process_key_value_pair",
    "process_metadata_info_dict",
    "process_xmp_metadata_dict",
    "xmp_to_dict",
]
