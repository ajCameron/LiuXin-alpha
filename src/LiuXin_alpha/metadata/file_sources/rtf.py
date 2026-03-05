"""
Read and write metadata in RTF files.
"""

from __future__ import annotations

import codecs
import os
import re
from io import StringIO
from typing import Any

from LiuXin_alpha.metadata.metadata import MetaData as MetaInformation
from LiuXin_alpha.metadata.utils import string_to_authors
from LiuXin_alpha.utils.calibre import force_unicode
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


def _default_metadata() -> MetaInformation:
    return MetaInformation(_("Unknown"), [_("Unknown")])


def _source_name(target_file) -> str:
    if isinstance(target_file, os.PathLike):
        return os.fspath(target_file)
    if isinstance(target_file, str):
        return target_file
    return getattr(target_file, "name", "") or ""


def _to_bytes(raw: bytes | str) -> bytes:
    if isinstance(raw, bytes):
        return raw
    return str(raw).encode("latin-1", "replace")


def _normalize_text(raw: str | None) -> str:
    if not raw:
        return ""
    return re.sub(r"\s+", " ", raw).strip()


def _safe_seek(stream, pos: int) -> None:
    try:
        stream.seek(pos)
    except Exception:
        pass


def _warn(msg: str) -> None:
    logger = getattr(default_log, "warning", None) or getattr(default_log, "warn", None)
    if logger is not None:
        logger(msg)


def _log_exception(msg: str, err: Exception) -> None:
    if hasattr(default_log, "log_exception"):
        default_log.log_exception(msg, err, "DEBUG")
    else:
        _warn(f"{msg}: {err}")


def get_document_info(stream):
    r"""
    Extract the \info block from an RTF stream.
    Returns: (info_block_bytes | None, start_position)
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
    Detect RTF \ansicpgNNNN codepage.
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
    Encode unicode text for RTF metadata fields using \\uXXXX? escapes.
    """
    if not isinstance(unistr, str):
        unistr = force_unicode(unistr)
    return "".join(c if ord(c) < 128 else f"\\u{ord(c)}?" for c in unistr)


def decode(raw, codec):
    """
    Decode RTF field content containing \\'HH and \\uNNNN? escapes.
    """
    if isinstance(raw, bytes):
        text = raw.decode("ascii", "replace")
    else:
        text = str(raw)

    if codec is not None:
        def codepage(match):
            try:
                return bytes([int(match.group(1), 16)]).decode(codec)
            except Exception:
                return "?"

        text = re.sub(r"\\'([a-fA-F0-9]{2})", codepage, text)

    def uni(match):
        try:
            val = int(match.group(1))
            # RTF \u escapes are signed 16-bit values.
            if val < 0:
                val += 65536
            return chr(val)
        except Exception:
            return "?"

    text = re.sub(r"\\u(-?\d{1,5}).", uni, text)
    return _normalize_text(text)


def _set_authors(mi, raw_author: str) -> None:
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
    tags = [x.strip() for x in tags_text.split(",") if x.strip()]
    if tags:
        mi.tags = tags


def get_metadata(target_file):
    """
    Read metadata from an RTF path or stream.
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
        return rtf_get_metadata_from_stream(stream)
    finally:
        if stream_needs_close:
            stream.close()
        elif pos is not None:
            _safe_seek(stream, pos)


def rtf_get_metadata_from_stream(stream):
    """
    Read metadata from an RTF stream.
    """
    mi = _default_metadata()
    stream.seek(0)
    if _to_bytes(stream.read(5)) != br"{\rtf":
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
    Create a metadata packet and inject it near the top of an RTF stream.
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
    Modify or add RTF metadata in a read/write binary stream.
    """

    def add_metadata_item(src: str, name: str, val: str) -> str:
        index = src.rindex("}")
        return src[:index] + r"{\ "[:-1] + name + " " + val + "}}"

    def replace_or_create(src: str, name: str, val: str) -> str:
        val = encode(val)
        pat = re.compile(base_pat.replace("name", name), re.DOTALL)
        src, num = pat.subn(r"{\\" + name.replace("\\", r"\\") + " " + val.replace("\\", r"\\") + "}", src)
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
    "get_document_info",
    "detect_codepage",
    "encode",
    "decode",
    "get_metadata",
    "rtf_get_metadata_from_stream",
    "create_metadata",
    "set_metadata",
]
