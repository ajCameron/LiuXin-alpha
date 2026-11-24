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
from LiuXin_alpha.metadata.file_sources.subject_fields import (
    reads_subjects,
    set_subjects,
    split_subjects,
    subjects_of,
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


def _read_info_subject(md, value: str) -> None:
    """
    Read the Info Subject as LiuXin's earlier ``union`` rule does: subject headings, or a tag under calibre's reading.

    The ``read_subject_fields_as`` preference chooses (see ``subject_fields``).

    Example:
        >>> md = _default_metadata()
        >>> _read_info_subject(md, "Sea")
        >>> list(md.subject) or list(md.tags)
        ['Sea']


    :param md: The record being read into.
    :param value: The Subject's text.
    :return: None.
    """
    if reads_subjects():
        set_subjects(md, split_subjects(value))
    else:
        md.tags = value


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
        # A list of names: assigned one by one as strings, "Smith && Sons" would split again.
        md.authors = [name for name in (_normalize_text(a) for a in string_to_authors(value_str)) if name]
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
        _read_info_subject(md, value_str)
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
        metadata_return.authors = [
            name
            for creator in creators
            if isinstance(creator, str)
            for name in (_normalize_text(a) for a in string_to_authors(creator))
            if name
        ]

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
        "http://ns.adobe.com/xap/1.0/": "xmp",
        "http://ns.adobe.com/pdf/1.3/": "pdf",
        "http://ns.adobe.com/pdfx/1.3/": "pdfx",
        "http://prismstandard.org/namespaces/basic/1.2/": "prism",
        "http://prismstandard.org/namespaces/basic/2.0/": "prism",
        "http://prismstandard.org/namespaces/basic/2.1/": "prism",
        "http://prismstandard.org/namespaces/basic/3.0/": "prism",
    }

    def __init__(self, xmp: bytes | str, *, attributes: bool = False):
        """
        Implement init for the bound metadata helper while preserving its container invariants.

        Example:
            Exercise XmpParser.  init   with pytest::

                python -m pytest -q tests/metadata/file_sources/test_pdf_metadata_source.py


        :param xmp: Raw value or payload to normalize, parse or serialize.
        :param attributes: Also read properties written as attributes of
            ``rdf:Description``, the short form many writers use for simple values.
        :return: None.
        """
        if isinstance(xmp, bytes):
            xmp = xmp.decode("utf-8", "replace")
        self.tree = ET.XML(xmp)
        self.rdftree = self.tree.find(self.RDF_NS + "RDF")
        self.attributes = attributes

    def _split_name(self, name):
        """
        Split a qualified XML name into its namespace's prefix and its local name.

        Example:
            >>> XmpParser("<x/>")._split_name("{http://purl.org/dc/elements/1.1/}title")
            ('dc', 'title')


        :param name: The name, as ElementTree writes it.
        :return: The prefix (or the namespace, if it has no known prefix) and the name.
        """
        ns = None
        if name.startswith("{"):
            ns, name = name[1:].split("}", 1)
            ns = self.NS_MAP.get(ns, ns)
        return ns, name

    def _parse_tag(self, el):
        """
        Parse tag without inventing absent metadata values.

        Example:
            Exercise XmpParser. parse tag with pytest::

                python -m pytest -q tests/metadata/file_sources/test_pdf_metadata_source.py


        :param el: Parsed XML, PDF or metadata node used as the operation context.
        :return: Parsed, normalized or serialized value described above.
        """
        return self._split_name(el.tag)

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
            if self.attributes:
                for name, value in desc.attrib.items():
                    ns, tag = self._split_name(name)
                    if ns and ns not in ("rdf", "xml"):
                        meta[ns].setdefault(tag, value)
        return dict(meta)


def xmp_to_dict(xmp, *, attributes: bool = False):
    """
    Parse XMP XML into the simplified property mapping used by the PDF reader.

    Example:
        >>> packet = (
        ...     '<x:xmpmeta xmlns:x="adobe:ns:meta/"><rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">'
        ...     '<rdf:Description xmlns:xmp="http://ns.adobe.com/xap/1.0/" xmp:MetadataDate="2021-01-01"/>'
        ...     "</rdf:RDF></x:xmpmeta>"
        ... )
        >>> xmp_to_dict(packet), xmp_to_dict(packet, attributes=True)
        ({}, {'xmp': {'MetadataDate': '2021-01-01'}})


    :param xmp: Raw value or payload to normalize, parse or serialize.
    :param attributes: Also read properties written as attributes of ``rdf:Description``;
        LiuXin's earlier ``union`` rule does not.
    :return: Parsed, normalized or serialized value described above.
    """
    return XmpParser(xmp, attributes=attributes).meta


def _apply_union(md, pdf_metadata, source_name: str):
    """
    Apply LiuXin's earlier ``union`` rule: the Info dictionary, then the packet over it.

    Example:
        Exercise apply union with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pdf_merge_policies.py


    :param md: The metadata to fill.
    :param pdf_metadata: What the object reader found.
    :param source_name: The file's name, for the log.
    :return: The metadata.
    """
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
    return md


def _set_identifier(md, scheme: str, value: str) -> None:
    """
    Set one identifier, through whichever setter the metadata object offers.

    Example:
        >>> md = _default_metadata()
        >>> _set_identifier(md, "doi", "10.1000/1")
        >>> sorted(md.get_identifiers()["doi"])
        ['10.1000/1']


    :param md: The metadata.
    :param scheme: The identifier's scheme.
    :param value: The identifier.
    :return: None.
    """
    try:
        md.set_identifier(scheme, value)
    except Exception:
        try:
            md.set_identifiers({scheme: value})
        except Exception:
            pass


def _apply_fields(md, fields):
    """
    Write combined fields into a metadata object.

    Example:
        >>> from LiuXin_alpha.metadata.file_sources.pdf_merge import PdfFields
        >>> md = _apply_fields(_default_metadata("book.pdf"), PdfFields(tags=("a", "b")))
        >>> md.title, list(md.tags)
        ('book', ['a', 'b'])


    :param md: The metadata to fill; fields it already has are added to.
    :param fields: The fields.
    :return: The metadata.
    """
    if fields.title:
        md.title = fields.title
    if fields.authors:
        md.authors = list(fields.authors)
    if fields.comments:
        md.comments = fields.comments
    if fields.tags:
        md.tags = list(fields.tags)
    set_subjects(md, fields.subjects)
    if fields.publisher:
        md.publisher = fields.publisher
    for producer in fields.producers:
        md.producers = producer
    if fields.created:
        md.timestamp = fields.created
    if fields.modified:
        md.last_modified = fields.modified
    if fields.uuid:
        md.uuid = fields.uuid
    if fields.languages:
        md.languages = list(fields.languages)
    for scheme, value in fields.identifiers.items():
        _set_identifier(md, scheme, value)
    return md


def _apply_combined(md, pdf_metadata, source_name: str, policy: str):
    """
    Read both sources into fields, combine them by the policy, and write the result.

    Example:
        Exercise apply combined with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pdf_merge_policies.py


    :param md: The metadata to fill.
    :param pdf_metadata: What the object reader found.
    :param source_name: The file's name, for the log.
    :param policy: ``newest`` or ``calibre``.
    :return: The metadata.
    """
    from LiuXin_alpha.metadata.file_sources import pdf_merge

    info = pdf_merge.PdfFields()
    try:
        info = pdf_merge.info_fields(
            pdf_metadata.info or {},
            subject_is_tag=policy == "calibre",
            subject_as_subjects=reads_subjects(),
        )
    except Exception as err:
        default_log.log_exception(
            "Failed while parsing PDF info dictionary.",
            err,
            "DEBUG",
            ("source", source_name or "<stream>"),
        )

    xmp = None
    try:
        if pdf_metadata.xmp:
            xmp = pdf_merge.xmp_fields(xmp_to_dict(pdf_metadata.xmp, attributes=True))
    except Exception as err:
        default_log.log_exception(
            "Failed while parsing embedded PDF XMP metadata.",
            err,
            "DEBUG",
            ("source", source_name or "<stream>"),
        )
    try:
        md = _apply_fields(md, pdf_merge.combine(info, xmp, policy))
    except Exception as err:
        default_log.log_exception(
            "Failed while applying PDF metadata.",
            err,
            "DEBUG",
            ("source", source_name or "<stream>"),
            ("policy", policy),
        )
    return md


def get_metadata(stream, *, fallback_on_parse_error: bool = False, merge: str | None = None):
    """
    Read metadata from the supported path, bytes or stream input while applying module ownership and fallback policy.

    The Info dictionary and the XMP packet are combined by ``merge``, a policy named in
    :mod:`LiuXin_alpha.metadata.file_sources.pdf_merge`; when it is ``None`` the
    ``pdf_metadata_merge`` preference chooses.

    Example:
        Exercise get metadata with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pdf_metadata_source.py


    :param stream: Caller-supplied path, path-like object or stream described by this
        operation.
    :param fallback_on_parse_error: Return safe default metadata after parse errors when
        true; otherwise raise the format error.
    :param merge: ``newest``, ``calibre`` or ``union``; ``None`` for the preference.
    :return: Parsed, normalized or serialized value described above.
    """
    from LiuXin_alpha.metadata.file_sources.pdf_merge import merge_policy

    policy = merge_policy(merge)
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
    if policy == "union":
        md = _apply_union(md, pdf_metadata, source_name)
    else:
        md = _apply_combined(md, pdf_metadata, source_name, policy)

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
                    _set_identifier(md, scheme, found)
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


def get_metadata_inplace(target_file, *, fallback_on_parse_error: bool = False, merge: str | None = None):
    """
    Read metadata through the path-oriented adapter exposed to registry plugins.

    Example:
        Exercise get metadata inplace with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pdf_metadata_source.py


    :param target_file: Caller-supplied path, path-like object or stream described by
        this operation.
    :param fallback_on_parse_error: Return safe default metadata after parse errors when
        true; otherwise raise the format error.
    :param merge: How to combine the Info dictionary and the packet, as for
        :func:`get_metadata`.
    :return: Parsed, normalized or serialized value described above.
    """
    with open(target_file, "rb") as target_pdf_stream:
        return get_metadata(target_pdf_stream, fallback_on_parse_error=fallback_on_parse_error, merge=merge)


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


def _series_index(mi, series: str) -> str | None:
    """
    Find the series index, which LiuXin's metadata keeps by series name.

    Example:
        >>> from types import SimpleNamespace
        >>> _series_index(SimpleNamespace(series_index={"Saga": 2.0}), "Saga"), _series_index(SimpleNamespace(series_index="3"), "Saga")
        ('2.0', '3')


    :param mi: The metadata.
    :param series: The series written.
    :return: The index as text, or ``None``.
    """
    raw = getattr(mi, "series_index", None)
    if isinstance(raw, dict):
        values = list(raw.values())
        raw = raw.get(series, values[0] if values else None)
    return _first_value(raw) if raw is not None else None


def _metadata_update(mi):
    """
    Collect the fields to write into a PDF, as cleaned text.

    Example:
        >>> from types import SimpleNamespace
        >>> _metadata_update(SimpleNamespace(title="T", authors=["Ann", ""], tags=["a"])).authors
        ('Ann',)


    :param mi: Metadata object supplying the supported fields.
    :return: A :class:`~LiuXin_alpha.metadata.file_sources.pdf_write.MetadataUpdate`.
    """
    from LiuXin_alpha.metadata.file_sources.pdf_write import MetadataUpdate

    def texts(raw: Any) -> tuple[str, ...]:
        """
        Clean each value, dropping the empty ones.

        Example:
            >>> texts(["a ", ""])  # doctest: +SKIP
            ('a',)


        :param raw: The values.
        :return: The cleaned values.
        """
        return tuple(text for text in (_normalize_text(str(value)) for value in _field_values(raw)) if text)

    extra: dict[str, str] = {}
    series = _first_value(getattr(mi, "series", None))
    if series:
        extra["/Series"] = series
        series_index = _series_index(mi, series)
        if series_index:
            extra["/SeriesIndex"] = series_index
    as_subjects = reads_subjects()
    return MetadataUpdate(
        title=_first_value(getattr(mi, "title", None)),
        authors=texts(getattr(mi, "authors", None)),
        comments=_first_value(getattr(mi, "comments", None)),
        tags=texts(getattr(mi, "tags", None)),
        subjects=texts(subjects_of(mi)) if as_subjects else (),
        subject_holds_subjects=as_subjects,
        producer=_first_value(getattr(mi, "producers", None)),
        publisher=_first_value(getattr(mi, "publisher", None)),
        extra=extra,
    )


def _metadata_to_pdf_dict(mi) -> dict[str, str]:
    """
    Serialize supported metadata fields into sanitized PDF Info dictionary strings.

    The authors are joined as LiuXin's reader splits them (`` & ``). The Subject holds
    the subjects, or the comments under calibre's reading of it (the
    ``read_subject_fields_as`` preference). The author sort is not written:
    ``/Creator`` names the application that made the PDF.

    Example:
        Exercise metadata to pdf dict with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py


    :param mi: Metadata object supplying or receiving the supported fields.
    :return: Parsed, normalized or serialized value described above.
    """
    return _metadata_update(mi).info()


def set_metadata(stream, mi):
    """
    Write supported metadata fields into a PDF without losing anything else it holds.

    The change is appended to the file as an incremental update, so the original bytes,
    and with them outlines, forms, attachments and signatures, are kept; an XMP packet
    is updated alongside the Info dictionary. A file whose cross-reference data is
    damaged is rewritten whole instead, keeping its document structure, and the
    report says so. Each warning is also logged. A failure part-way through leaves the
    file as it was: a path's file is only replaced once the new one is complete, and a
    stream gets its original bytes back. See
    :mod:`LiuXin_alpha.metadata.file_sources.pdf_write`.

    Example:
        Exercise set metadata with pytest::

            python -m pytest -q tests/metadata/file_sources/test_pdf_safe_writing.py


    :param stream: Caller-supplied path, path-like object or stream described by this
        operation.
    :param mi: Metadata object supplying or receiving the supported fields.
    :return: A :class:`~LiuXin_alpha.metadata.file_sources.pdf_write.PdfWriteReport`.
    :raises PdfWriteError: The file is encrypted or cannot be written so that it reads
        back; it is left unchanged.
    """
    if isinstance(stream, os.PathLike):
        stream = os.fspath(stream)
    if not isinstance(stream, str) and not (hasattr(stream, "read") and hasattr(stream, "write")):
        raise TypeError("set_metadata expects a writable binary stream or filesystem path.")

    if importlib.util.find_spec("pypdf") is None:
        raise RuntimeError(
            "PDF metadata writing backend is unavailable. Install the `pdf` extra"
            " (pip install 'liuxin-alpha[pdf]'), which provides pypdf."
        )

    from LiuXin_alpha.metadata.file_sources.pdf_write import write_file, write_stream

    if isinstance(stream, str):
        report = write_file(stream, _metadata_update(mi))
        source = stream
    else:
        pos = None
        if hasattr(stream, "tell"):
            try:
                pos = stream.tell()
            except Exception:
                pos = None
        report = write_stream(stream, _metadata_update(mi))
        source = _source_name(stream)
        if pos is not None:
            try:
                stream.seek(min(pos, stream.seek(0, os.SEEK_END)))
            except Exception:
                pass
    for warning in report.warnings:
        default_log.log_variables(
            "PDF metadata was written with a caveat.",
            "WARNING",
            ("source", source or "<stream>"),
            ("warning", warning),
        )
    return report


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
