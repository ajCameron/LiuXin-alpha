"""
Read and update RTF document-info metadata while handling code pages, Unicode escapes and malformed-input policy.

The module keeps malformed-input, optional dependency and resource ownership
behavior explicit for registry callers.

Example:
    Exercise rtf with pytest::

        python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py
"""

from __future__ import annotations

import codecs
import os
import re
from io import StringIO
from typing import Any

from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import (
    CalibreLikeLiuXinBookMetaData as MetaInformation,
)
from LiuXin_alpha.metadata.utils import string_to_authors
from LiuXin_alpha.utils.calibre import force_unicode
from LiuXin_alpha.utils.libraries.cleantext import clean_xml_chars
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal <kovid at kovidgoyal.net>"

VALID_FOR = ["RTF"]
PRIORITY_FOR = ["RTF"]
RUN_COST = ["LOW"]

title_pat = re.compile(br"\{\\info.*?\{\\title(.*?)(?<!\\)\}", re.DOTALL)
subject_pat = re.compile(br"\{\\info.*?\{\\subject(.*?)(?<!\\)\}", re.DOTALL)
author_pat = re.compile(br"\{\\info.*?\{\\author(.*?)(?<!\\)\}", re.DOTALL)
manager_pat = re.compile(br"\{\\info.*?\{\\manager(.*?)(?<!\\)\}", re.DOTALL)
company_pat = re.compile(br"\{\\info.*?\{\\company(.*?)(?<!\\)\}", re.DOTALL)
operator_pat = re.compile(br"\{\\info.*?\{\\operator(.*?)(?<!\\)\}", re.DOTALL)
tags_pat = re.compile(br"\{\\info.*?\{\\category(.*?)(?<!\\)\}", re.DOTALL)
tags_pat_2 = re.compile(br"\{\\info.*?\{\\keywords(.*?)(?<!\\)\}", re.DOTALL)
comment_pat_2 = re.compile(br"\{\\info.*?\{\\comment(.*?)(?<!\\)\}", re.DOTALL)


class RtfFormatError(Exception):
    """
    Signal malformed or unreadable RTF metadata input.

    Example:
        Exercise RtfFormatError with pytest::

            python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py
    """
    pass


def _default_metadata() -> MetaInformation:
    """
    Build minimally usable metadata for missing or explicitly tolerated malformed input.

    Example:
        Exercise  default metadata with pytest::

            python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


    :return: Parsed, normalized or serialized value described above.
    """
    return MetaInformation(_("Unknown"), [_("Unknown")])


def _source_name(target_file) -> str:
    """
    Return the best available source label for fallback titles and diagnostics.

    Example:
        Exercise  source name with pytest::

            python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


    :param target_file: Caller-supplied path, path-like object or stream described by
        this operation.
    :return: Parsed, normalized or serialized value described above.
    """
    if isinstance(target_file, os.PathLike):
        return os.fspath(target_file)
    if isinstance(target_file, str):
        return target_file
    return getattr(target_file, "name", "") or ""


def _to_bytes(raw: bytes | str) -> bytes:
    """
    Perform the format-specific to bytes operation used by this metadata source.

    Example:
        Exercise  to bytes with pytest::

            python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


    :param raw: Raw value or payload to normalize, parse or serialize.
    :return: Parsed, normalized or serialized value described above.
    """
    if isinstance(raw, bytes):
        return raw
    return str(raw).encode("latin-1", "replace")


def _normalize_text(raw: str | None) -> str:
    """
    Collapse whitespace and trim a possibly absent metadata text value.

    Example:
        Exercise  normalize text with pytest::

            python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


    :param raw: Raw value or payload to normalize, parse or serialize.
    :return: Parsed, normalized or serialized value described above.
    """
    if not raw:
        return ""
    return re.sub(r"\s+", " ", raw).strip()


def _safe_seek(stream, pos: int) -> None:
    """
    Perform seek without propagating optional or recovery failures.

    Example:
        Exercise  safe seek with pytest::

            python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


    :param stream: Caller-supplied path, path-like object or stream described by this
        operation.
    :param pos: Offset, bound or scalar value used by the operation.
    :return: None.
    """
    try:
        stream.seek(pos)
    except Exception:
        pass


def _warn(msg: str) -> None:
    """
    Report a recoverable metadata parsing problem through the project logger.

    Example:
        Exercise  warn with pytest::

            python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


    :param msg: Value supplied for msg.
    :return: None.
    """
    logger = getattr(default_log, "warning", None) or getattr(default_log, "warn", None)
    if logger is not None:
        logger(msg)


def _log_exception(msg: str, err: Exception) -> None:
    """
    Report a recoverable metadata parsing problem through the project logger.

    Example:
        Exercise  log exception with pytest::

            python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


    :param msg: Value supplied for msg.
    :param err: Value supplied for err.
    :return: None.
    """
    if hasattr(default_log, "log_exception"):
        default_log.log_exception(msg, err, "DEBUG")
    else:
        _warn(f"{msg}: {err}")


def get_document_info(stream):
    """
    Extract the raw RTF info group while respecting nested braces and escaped delimiters.

    Example:
        Exercise get document info with pytest::

            python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


    :param stream: Caller-supplied path, path-like object or stream described by this
        operation.
    :return: Parsed, normalized or serialized value described above.
    """
    block_size = 4096
    stream.seek(0)
    found, block = False, b""
    while not found:
        prefix = block[-6:]
        chunk = stream.read(block_size)
        block = prefix + _to_bytes(chunk or b"")
        actual_block_size = len(block) - len(prefix)
        if len(block) == len(prefix):
            break
        idx = block.find(br"{\info")
        if idx >= 0:
            found = True
            pos = stream.tell() - actual_block_size + idx - len(prefix)
            stream.seek(pos)
        elif block.find(br"\sect") > -1:
            break
    if not found:
        return None, 0

    data = bytearray()
    count = 0
    pos = stream.tell()
    while True:
        ch = _to_bytes(stream.read(1))
        if not ch:
            break
        if ch == b"\\":
            data.extend(ch + _to_bytes(stream.read(1)))
            continue
        if ch == b"{":
            count += 1
        elif ch == b"}":
            count -= 1
        data.extend(ch)
        if count == 0:
            break
    return bytes(data), pos


def detect_codepage(stream):
    """
    Infer the RTF ANSI code page from header controls, falling back to cp1252.

    Example:
        Exercise detect codepage with pytest::

            python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


    :param stream: Caller-supplied path, path-like object or stream described by this
        operation.
    :return: Parsed, normalized or serialized value described above.
    """
    stream.seek(0)
    sample = _to_bytes(stream.read(512))
    pat = re.compile(br"\\ansicpg(\d+)")
    match = pat.search(sample)
    if match is not None:
        num = match.group(1)
        if num == b"0":
            num = b"1252"
        codec = (b"cp" + num).decode("ascii", "replace")
        try:
            codecs.lookup(codec)
            return codec
        except Exception:
            pass
    return None


def encode(unistr):
    """
    Escape Unicode text into RTF-safe controls and literal bytes.

    Example:
        Exercise encode with pytest::

            python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


    :param unistr: Value supplied for unistr.
    :return: Parsed, normalized or serialized value described above.
    """
    if not isinstance(unistr, str):
        unistr = force_unicode(unistr)
    unistr = clean_xml_chars(unistr)
    encoded = []
    for char in unistr:
        codepoint = ord(char)
        if char in "\\{}":
            encoded.append("\\" + char)
        elif codepoint < 128:
            encoded.append(char)
        elif codepoint <= 0xFFFF:
            signed = codepoint - 0x10000 if codepoint >= 0x8000 else codepoint
            encoded.append(f"\\u{signed}?")
        else:
            utf16 = char.encode("utf-16-be")
            for idx in range(0, len(utf16), 2):
                unit = int.from_bytes(utf16[idx : idx + 2], "big")
                signed = unit - 0x10000 if unit >= 0x8000 else unit
                encoded.append(f"\\u{signed}?")
    return "".join(encoded)


def decode(raw, codec):
    """
    Decode RTF escaped bytes, Unicode controls and fallback characters using the selected codec.

    Example:
        Exercise decode with pytest::

            python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


    :param raw: Raw value or payload to normalize, parse or serialize.
    :param codec: Name, type or encoding selector used for lookup or interpretation.
    :return: Parsed, normalized or serialized value described above.
    """
    if isinstance(raw, bytes):
        text = raw.decode("ascii", "replace")
    else:
        text = str(raw)

    if codec is not None:
        def codepage(match):
            """
            Perform the format-specific codepage operation used by this metadata source.

            Example:
                Exercise decode.codepage with pytest::

                    python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


            :param match: Value supplied for match.
            :return: Parsed, normalized or serialized value described above.
            """
            try:
                return bytes([int(match.group(1), 16)]).decode(codec)
            except Exception:
                return "?"

        text = re.sub(r"\\'([a-fA-F0-9]{2})", codepage, text)

    def uni(match):
        """
        Perform the format-specific uni operation used by this metadata source.

        Example:
            Exercise decode.uni with pytest::

                python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


        :param match: Value supplied for match.
        :return: Parsed, normalized or serialized value described above.
        """
        try:
            val = int(match.group(1))
            # RTF \u escapes are signed 16-bit values.
            if val < 0:
                val += 65536
            return chr(val)
        except Exception:
            return "?"

    text = re.sub(r"\\u(-?\d{1,6}).", uni, text)
    text = re.sub(r"\\([\\{}])", r"\1", text)
    text = text.encode("utf-16", "surrogatepass").decode("utf-16", "replace")
    return _normalize_text(clean_xml_chars(text))


def _set_authors(mi, raw_author: str) -> None:
    """
    Set authors while preserving unrelated metadata state.

    Example:
        Exercise  set authors with pytest::

            python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


    :param mi: Metadata object supplying or receiving the supported fields.
    :param raw_author: Raw value or payload to normalize, parse or serialize.
    :return: None.
    """
    authors = [x.strip() for x in string_to_authors(raw_author) if x and x.strip()]
    if len(authors) <= 1 and "," in raw_author:
        authors = [x.strip() for x in raw_author.split(",") if x.strip()]
    if authors:
        try:
            # Avoid keeping the default Unknown author when real authors exist.
            raw_data = object.__getattribute__(mi, "_data")
            if isinstance(raw_data, dict) and isinstance(raw_data.get("authors"), dict):
                raw_data["authors"].clear()
        except Exception:
            pass
        mi.authors = authors


def _set_tags(mi, tags_text: str) -> None:
    """
    Set tags while preserving unrelated metadata state.

    Example:
        Exercise  set tags with pytest::

            python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


    :param mi: Metadata object supplying or receiving the supported fields.
    :param tags_text: Value supplied for tags text.
    :return: None.
    """
    tags = [x.strip() for x in tags_text.split(",") if x.strip()]
    if tags:
        mi.tags = tags


def get_metadata(target_file, *, fallback_on_parse_error: bool = False):
    """
    Read metadata from the supported path, bytes or stream input while applying module ownership and fallback policy.

    Example:
        Exercise get metadata with pytest::

            python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


    :param target_file: Caller-supplied path, path-like object or stream described by
        this operation.
    :param fallback_on_parse_error: Return safe default metadata after parse errors when
        true; otherwise raise the format error.
    :return: Parsed, normalized or serialized value described above.
    """
    stream_needs_close = False
    source_name = _source_name(target_file)

    if isinstance(target_file, os.PathLike):
        target_file = os.fspath(target_file)

    if isinstance(target_file, str):
        stream = open(target_file, "rb")
        stream_needs_close = True
    elif hasattr(target_file, "read"):
        stream = target_file
    else:
        raise TypeError("RTF metadata reader expects a filesystem path or readable stream.")

    pos = None
    if hasattr(stream, "tell"):
        try:
            pos = stream.tell()
        except Exception:
            pos = None

    try:
        return rtf_get_metadata_from_stream(stream, fallback_on_parse_error=fallback_on_parse_error)
    finally:
        if stream_needs_close:
            stream.close()
        elif pos is not None:
            _safe_seek(stream, pos)


def rtf_get_metadata_from_stream(stream, *, fallback_on_parse_error: bool = False):
    """
    Parse RTF info fields from a caller-owned stream under explicit fallback policy.

    Example:
        Exercise rtf get metadata from stream with pytest::

            python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


    :param stream: Caller-supplied path, path-like object or stream described by this
        operation.
    :param fallback_on_parse_error: Return safe default metadata after parse errors when
        true; otherwise raise the format error.
    :return: Parsed, normalized or serialized value described above.
    """
    mi = _default_metadata()
    stream.seek(0)
    if _to_bytes(stream.read(5)) != br"{\rtf":
        if not fallback_on_parse_error:
            raise RtfFormatError("RTF payload does not start with an RTF header.")
        return mi

    block, _ = get_document_info(stream)
    if not block:
        return mi

    cpg = detect_codepage(stream)
    stream.seek(0)

    title_match = title_pat.search(block)
    if title_match is not None:
        title = decode(title_match.group(1).strip(), cpg)
        if title:
            mi.title = title

    author_match = author_pat.search(block)
    if author_match is not None:
        author = decode(author_match.group(1).strip(), cpg)
        if author:
            _set_authors(mi, author)

    subject_match = subject_pat.search(block)
    if subject_match is not None:
        comment = decode(subject_match.group(1).strip(), cpg)
        if comment:
            mi.comments = comment

    comment_match_2 = comment_pat_2.search(block)
    if comment_match_2 is not None:
        comment_2 = decode(comment_match_2.group(1).strip(), cpg)
        if comment_2:
            # Explicit \comment is usually richer than \subject.
            try:
                raw_data = object.__getattribute__(mi, "_data")
                if isinstance(raw_data, dict) and isinstance(raw_data.get("comments"), dict):
                    raw_data["comments"].clear()
            except Exception:
                pass
            mi.comments = comment_2

    tags_match = tags_pat.search(block)
    if tags_match is not None:
        tags = decode(tags_match.group(1).strip(), cpg)
        _set_tags(mi, tags)

    tags_match_2 = tags_pat_2.search(block)
    if tags_match_2 is not None:
        tags_2 = decode(tags_match_2.group(1).strip(), cpg)
        _set_tags(mi, tags_2)

    publisher_match = manager_pat.search(block)
    if publisher_match is not None:
        publisher = decode(publisher_match.group(1).strip(), cpg)
        if publisher:
            mi.publisher = publisher

    company_match = company_pat.search(block)
    if company_match is not None:
        company = decode(company_match.group(1).strip(), cpg)
        if company:
            try:
                mi.tags = company
            except Exception:
                pass

    operator_match = operator_pat.search(block)
    if operator_match is not None:
        operator = decode(operator_match.group(1).strip(), cpg)
        if operator:
            try:
                mi.add_creators({"operator": operator})
            except Exception:
                # Older metadata objects may not have this hook.
                pass

    return mi


def create_metadata(stream, options):
    """
    Create a complete RTF info group from supported metadata options.

    Example:
        Exercise create metadata with pytest::

            python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


    :param stream: Caller-supplied path, path-like object or stream described by this
        operation.
    :param options: Worker, metadata or control value described by the operation.
    :return: Parsed, normalized or serialized value described above.
    """
    md: list[str] = [r"{\info"]
    if getattr(options, "title", None):
        md.append(r"{\title %s}" % encode(options.title))

    authors = getattr(options, "authors", None)
    if authors:
        au = authors if isinstance(authors, str) else ", ".join(str(x) for x in authors)
        md.append(r"{\author %s}" % encode(au))

    comment = getattr(options, "comment", None)
    if comment is None:
        comment = getattr(options, "comments", None)
    if comment:
        md.append(r"{\subject %s}" % encode(comment))

    if getattr(options, "publisher", None):
        md.append(r"{\manager %s}" % encode(options.publisher))

    tags = getattr(options, "tags", None)
    if tags:
        if isinstance(tags, str):
            tag_text = tags
        else:
            tag_text = ", ".join(str(x) for x in tags)
        md.append(r"{\category %s}" % encode(tag_text))

    if len(md) > 1:
        md.append("}")
        stream.seek(0)
        src = _to_bytes(stream.read())
        ans = src[:6] + "".join(md).encode("ascii", "replace") + src[6:]
        stream.seek(0)
        stream.truncate()
        stream.write(ans)


def set_metadata(stream, options):
    """
    Rewrite supported metadata fields without taking ownership of a caller-supplied stream.

    Example:
        Exercise set metadata with pytest::

            python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


    :param stream: Caller-supplied path, path-like object or stream described by this
        operation.
    :param options: Worker, metadata or control value described by the operation.
    :return: None.
    """

    def add_metadata_item(src: str, name: str, val: str) -> str:
        """
        Perform the format-specific add metadata item operation used by this metadata source.

        Example:
            Exercise set metadata.add metadata item with pytest::

                python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


        :param src: Value supplied for src.
        :param name: Name, type or encoding selector used for lookup or interpretation.
        :param val: Value supplied for val.
        :return: Parsed, normalized or serialized value described above.
        """
        index = src.rindex("}")
        return src[:index] + r"{\ "[:-1] + name + " " + val + "}}"

    def replace_or_create(src: str, name: str, val: str) -> str:
        """
        Perform the format-specific replace or create operation used by this metadata source.

        Example:
            Exercise set metadata.replace or create with pytest::

                python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


        :param src: Value supplied for src.
        :param name: Name, type or encoding selector used for lookup or interpretation.
        :param val: Value supplied for val.
        :return: Parsed, normalized or serialized value described above.
        """
        val = encode(val)
        pat = re.compile(base_pat.replace("name", name), re.DOTALL)
        replacement = "{\\" + name + " " + val + "}"
        src, num = pat.subn(lambda _match: replacement, src)
        if num == 0:
            src = add_metadata_item(src, name, val)
        return src

    src, pos = get_document_info(stream)
    if src is None:
        create_metadata(stream, options)
        return

    try:
        src_text = src.decode("ascii", "replace")
    except Exception as err:
        _log_exception("Unable to decode existing RTF info block as ASCII.", err)
        create_metadata(stream, options)
        return

    olen = len(src)
    base_pat = r"\{\\name(.*?)(?<!\\)\}"

    if getattr(options, "title", None) is not None:
        src_text = replace_or_create(src_text, "title", options.title)

    comment = getattr(options, "comment", None)
    if comment is None:
        comment = getattr(options, "comments", None)
    if comment is not None:
        src_text = replace_or_create(src_text, "subject", comment)

    authors = getattr(options, "authors", None)
    if authors is not None:
        author_text = authors if isinstance(authors, str) else "& ".join(str(x) for x in authors)
        src_text = replace_or_create(src_text, "author", author_text)

    tags = getattr(options, "tags", None)
    if tags is not None:
        tag_text = tags if isinstance(tags, str) else ", ".join(str(x) for x in tags)
        src_text = replace_or_create(src_text, "category", tag_text)

    publisher = getattr(options, "publisher", None)
    if publisher is not None:
        src_text = replace_or_create(src_text, "manager", publisher)

    stream.seek(pos + olen)
    after = _to_bytes(stream.read())
    stream.seek(pos)
    stream.truncate()
    stream.write(src_text.encode("ascii", "replace"))
    stream.write(after)


__all__ = [
    "VALID_FOR",
    "PRIORITY_FOR",
    "RUN_COST",
    "RtfFormatError",
    "get_document_info",
    "detect_codepage",
    "encode",
    "decode",
    "get_metadata",
    "rtf_get_metadata_from_stream",
    "create_metadata",
    "set_metadata",
]
