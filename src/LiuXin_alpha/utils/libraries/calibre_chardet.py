#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""Calibre-derived encoding helpers.

This module exists mainly to:

* sniff declared XML/HTML encodings from the first chunk of a document
* fall back to statistical detection (chardet/charset-normalizer)
* (optionally) normalize non-XML entities to Unicode while keeping the core
  XML entities (``&amp;``, ``&lt;``...) escaped so the document remains parseable

It is intentionally conservative and aims to behave sensibly for both ``str``
and bytes-like inputs.
"""

from __future__ import annotations

import codecs
import re
import warnings
from typing import Optional, Tuple, Union


__license__ = "GPL v3"
__copyright__ = "2009, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


ENCODING_PATS = [
    # XML declaration
    re.compile(r"<\?[^<>]+encoding\s*=\s*['\"](.*?)['\"][^<>]*>", re.IGNORECASE),
    # HTML 5 charset
    re.compile(
        r"""<meta\s+charset=['\"]([-_a-z0-9]+)['\"][^<>]*>(?:\s*</meta>){0,1}""",
        re.IGNORECASE,
    ),
    # HTML 4 Pragma directive
    re.compile(
        r"""<meta\s+?[^<>]*?content\s*=\s*['\"][^'\"]*?charset=([-_a-z0-9]+)[^'\"]*?['\"][^<>]*>(?:\s*</meta>){0,1}""",
        re.IGNORECASE,
    ),
]
ENTITY_PATTERN = re.compile(r"&(\S+?);")


# Calibre code historically uses a ``unicode`` type name. Keep a runtime alias
# so older call-sites (and tests) don't explode on Python 3.
unicode = str


BytesLike = Union[bytes, bytearray, memoryview]


def _to_bytes(raw: BytesLike) -> bytes:
    if isinstance(raw, bytes):
        return raw
    if isinstance(raw, bytearray):
        return bytes(raw)
    return raw.tobytes()


def _latin1_decode(raw: BytesLike) -> str:
    # latin-1 is a 1:1 byte->codepoint mapping, perfect for safe header sniffing
    # and regex work where we want stable indices.
    return _to_bytes(raw).decode("latin-1")


def _maybe_warn(msg: str, verbose: bool) -> None:
    if verbose:
        warnings.warn(msg, RuntimeWarning, stacklevel=2)


def strip_encoding_declarations(raw: Union[str, BytesLike], limit: int = 50 * 1024):
    """Remove encoding declarations from the start of a document.

    Works on both text and bytes-like inputs. For bytes, the transformation is
    performed using latin-1 to preserve byte positions.
    """

    if isinstance(raw, str):
        prefix = raw[:limit]
        suffix = raw[limit:]
        for pat in ENCODING_PATS:
            prefix = pat.sub("", prefix)
        return prefix + suffix

    b = _to_bytes(raw)
    prefix_t = b[:limit].decode("latin-1")
    for pat in ENCODING_PATS:
        prefix_t = pat.sub("", prefix_t)
    return prefix_t.encode("latin-1") + b[limit:]


def replace_encoding_declarations(
    raw: Union[str, BytesLike],
    enc: str = "utf-8",
    limit: int = 50 * 1024,
) -> Tuple[Union[str, bytes], bool]:
    """Replace declared encodings in the header with *enc*.

    Returns ``(new_raw, changed)``.
    """

    changed = [False]

    if isinstance(raw, str):
        prefix = raw[:limit]
        suffix = raw[limit:]
    else:
        b = _to_bytes(raw)
        prefix = b[:limit].decode("latin-1")
        suffix = b[limit:]

    def sub(m):
        ans = m.group()
        if m.group(1).lower() != enc.lower():
            changed[0] = True
            start, end = m.start(1) - m.start(0), m.end(1) - m.end(0)
            ans = ans[:start] + enc + ans[end:]
        return ans

    for pat in ENCODING_PATS:
        prefix = pat.sub(sub, prefix)

    if isinstance(raw, str):
        return prefix + suffix, changed[0]
    return prefix.encode("latin-1") + suffix, changed[0]


def find_declared_encoding(raw: Union[str, BytesLike], limit: int = 50 * 1024) -> Optional[str]:
    """Return the declared encoding if present in the first *limit* characters."""

    prefix: str
    if isinstance(raw, str):
        prefix = raw[:limit]
    else:
        prefix = _latin1_decode(raw)[:limit]
    for pat in ENCODING_PATS:
        m = pat.search(prefix)
        if m is not None:
            return m.group(1)


def substitute_entites(raw: Union[str, BytesLike]):
    """Substitute entities using LiuXin's entity resolver.

    This resolver intentionally keeps core XML entities escaped (``&amp;``,
    ``&lt;`` ...) so that the resulting document remains parseable XML/HTML.
    """

    if not isinstance(raw, str):
        raw = _latin1_decode(raw)

    try:
        from LiuXin_alpha.utils.text.xml_utils import xml_entity_to_unicode
    except Exception:
        # As a last resort, only resolve numeric entities and a tiny named set.
        def xml_entity_to_unicode(m):
            ent = m.group(1)
            if ent.startswith("#"):
                try:
                    if len(ent) > 2 and ent[1] in ("x", "X"):
                        return chr(int(ent[2:], 16))
                    return chr(int(ent[1:], 10))
                except Exception:
                    return m.group(0)
            return {"nbsp": "\u00a0"}.get(ent, m.group(0))

    return ENTITY_PATTERN.sub(xml_entity_to_unicode, raw)


# Fix the historical typo while keeping backwards compatibility.
substitute_entities = substitute_entites


_CHARSET_ALIASES = {"macintosh": "mac-roman", "x-sjis": "shift-jis"}


def detect(*args, **kwargs):
    """Detect encoding.

    Prefer ``charset_normalizer`` (better maintained) when available, but fall
    back to ``chardet``.
    """

    if args:
        sample = args[0]
        if isinstance(sample, (bytes, bytearray, memoryview)):
            try:
                from charset_normalizer import from_bytes

                best = from_bytes(_to_bytes(sample)).best()
                if best is not None:
                    # charset-normalizer doesn't expose a chardet-style
                    # confidence score; percent_chaos is a reasonable proxy.
                    chaos = getattr(best, "percent_chaos", None)
                    conf = 1.0
                    if isinstance(chaos, (int, float)):
                        conf = max(0.0, min(1.0, 1.0 - (float(chaos) / 100.0)))
                    return {"encoding": best.encoding, "confidence": conf}
            except Exception:
                pass

    from chardet import detect as _detect

    return _detect(*args, **kwargs)


def force_encoding(raw: BytesLike, verbose: bool, assume_utf8: bool = False) -> str:
    from LiuXin_alpha.constants import preferred_encoding

    try:
        chardet = detect(raw[: 1024 * 50])
    except Exception:
        chardet = {"encoding": preferred_encoding, "confidence": 0}
    encoding = chardet["encoding"]
    if chardet["confidence"] < 1 and assume_utf8:
        encoding = "utf-8"
    if chardet["confidence"] < 1 and verbose:
        _maybe_warn(
            f"Encoding detection confidence {int(float(chardet['confidence']) * 100)}%",
            verbose=True,
        )
    if not encoding:
        encoding = preferred_encoding
    encoding = encoding.lower()
    encoding = _CHARSET_ALIASES.get(encoding, encoding)
    if encoding == "ascii":
        encoding = "utf-8"
    return encoding


def detect_xml_encoding(
    raw: Union[str, BytesLike],
    verbose: bool = False,
    assume_utf8: bool = False,
) -> Tuple[Union[str, bytes], Optional[str]]:
    """Return ``(raw_without_bom, encoding)``.

    If *raw* is text, returns ``(raw, None)``.
    """

    if not raw or isinstance(raw, str):
        return raw, None

    b = _to_bytes(raw)

    for x in ("utf8", "utf-16-le", "utf-16-be"):
        bom = getattr(codecs, "BOM_" + x.upper().replace("-16", "16").replace("-", "_"))
        if b.startswith(bom):
            return b[len(bom) :], x

    # Look for an encoding declaration in the header. We decode using latin-1
    # to preserve byte offsets and avoid decode errors.
    encoding = None
    header = b[: 50 * 1024].decode("latin-1")
    for pat in ENCODING_PATS:
        match = pat.search(header)
        if match is not None:
            encoding = match.group(1)
            break
    if encoding is None:
        encoding = force_encoding(b, verbose, assume_utf8=assume_utf8)
    if encoding.lower().strip() == "macintosh":
        encoding = "mac-roman"
    if encoding.lower().replace("_", "-").strip() in (
        "gb2312",
        "chinese",
        "csiso58gb231280",
        "euc-cn",
        "euccn",
        "eucgb2312-cn",
        "gb2312-1980",
        "gb2312-80",
        "iso-ir-58",
    ):
        # Microsoft Word exports to HTML with encoding incorrectly set to
        # gb2312 instead of gbk. gbk is a superset of gb2312, anyway.
        encoding = "gbk"
    try:
        codecs.lookup(encoding)
    except LookupError:
        encoding = "utf-8"

    return b, encoding


def xml_to_unicode(
    raw,
    verbose=False,
    strip_encoding_pats=False,
    resolve_entities=False,
    assume_utf8=False,
):
    """Convert bytes/XML-ish text to unicode.

    Returns ``(text, encoding_used)``.
    """

    if not raw:
        return "", None
    raw, encoding = detect_xml_encoding(raw, verbose=verbose, assume_utf8=assume_utf8)
    if not isinstance(raw, str):
        enc = encoding or "utf-8"
        raw = raw.decode(enc, "replace")

    if strip_encoding_pats:
        raw = strip_encoding_declarations(raw)
    if resolve_entities:
        raw = substitute_entites(raw)

    return raw, encoding


def recode_to_utf8(raw: BytesLike, decode_errors: str = "ignore", encode_errors: str = "ignore") -> bytes:
    """Recode a byte string to UTF-8.

    This is a small legacy convenience used by older call-sites.
    """

    if raw is None:
        return b""
    if isinstance(raw, str):
        return raw.encode("utf-8", errors=encode_errors)

    b = _to_bytes(raw)
    enc = force_encoding(b, verbose=False)
    text = b.decode(enc, errors=decode_errors)
    return text.encode("utf-8", errors=encode_errors)
