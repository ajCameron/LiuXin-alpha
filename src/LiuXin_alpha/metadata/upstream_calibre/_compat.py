# Derived from calibre (https://calibre-ebook.com) 9.18:
# src/calibre/ebooks/metadata/__init__.py, src/calibre/utils/xml_parse.py,
# src/calibre/__init__.py, src/calibre/constants.py, src/calibre/utils/__init__.py
# and src/calibre/ebooks/html_entities.c. License: GPL v3. Copyright: Kovid Goyal.
# SPDX-License-Identifier: GPL-3.0-only
"""
The calibre helpers the ported metadata readers need, ported or mapped.

``string_to_authors``, ``safe_xml_fromstring``, ``MetaInformation``,
``prints``, ``force_unicode``, ``int_to_byte``, ``prepare_string_for_xml``
and ``StreamOrPath`` are ported from calibre 9.18 so the readers behave as
upstream does. ``replace_entities`` ports the C function calibre uses for it,
``fast_html_entities.replace_all_entities``, to Python with the same rules.
``string_to_authors`` uses calibre's default ``authors_split_regex`` tweak,
because LiuXin has no tweaks file. ``_`` returns its text unchanged, as
calibre's does when no translation is installed. ``Metadata`` is calibre's
metadata class from the project's own copy, ``metadata.book.base``.

Some names map to the standard library: ``TemporaryDirectory`` to
:class:`tempfile.TemporaryDirectory`, ``ZipFile`` to :class:`zipfile.ZipFile`
(calibre uses its own copy of the module), and ``guess_type`` to
:func:`mimetypes.guess_type` (calibre adds its own types first; the ported
readers only look up common image extensions). ``load_defaults`` returns no
saved conversion settings, so calibre's defaults apply.

``InfoFieldPattern`` and ``FieldPattern`` stand in for the RTF reader's and
writer's field patterns, which backtrack for minutes on damaged files; they
find the same values in linear time.

Example:
    >>> string_to_authors("Ann Smith and Bob Jones & Cy")
    ['Ann Smith', 'Bob Jones', 'Cy']
"""

from __future__ import annotations

import codecs
import html.entities
import locale
import mimetypes
import os
import re
import sys
import tempfile
import zipfile
from collections.abc import Collection
from typing import IO, Any

from lxml import etree

from LiuXin_alpha.metadata.book.base import calibreMetadata as Metadata

__all__ = [
    "FieldMatch",
    "FieldPattern",
    "InfoFieldPattern",
    "MetaInformation",
    "Metadata",
    "StreamOrPath",
    "TemporaryDirectory",
    "ZipFile",
    "_",
    "filesystem_encoding",
    "force_unicode",
    "guess_type",
    "int_to_byte",
    "load_defaults",
    "preferred_encoding",
    "prepare_string_for_xml",
    "prints",
    "replace_entities",
    "safe_xml_fromstring",
    "string_to_authors",
]

TemporaryDirectory = tempfile.TemporaryDirectory
ZipFile = zipfile.ZipFile
guess_type = mimetypes.guess_type


def _filesystem_encoding() -> str:
    """
    Work out calibre's ``filesystem_encoding``: never ASCII, UTF-8 if unsure.

    Example:
        >>> _filesystem_encoding() != "ascii"
        True


    :return: The encoding's name.
    """
    encoding = sys.getfilesystemencoding()
    try:
        if codecs.lookup(encoding).name == "ascii":
            encoding = "utf-8"
    except Exception:
        encoding = "utf-8"
    return encoding


def _preferred_encoding() -> str:
    """
    Work out calibre's ``preferred_encoding``: the locale's, or UTF-8.

    Example:
        >>> bool(_preferred_encoding())
        True


    :return: The encoding's name.
    """
    try:
        encoding = locale.getpreferredencoding()
        codecs.lookup(encoding)
    except Exception:
        encoding = "utf-8"
    return encoding


filesystem_encoding = _filesystem_encoding()
preferred_encoding = _preferred_encoding()

_author_pat = re.compile(r"(?i),?\s+(and|with)\s+")


def _(text: str) -> str:
    """
    Return text unchanged, as calibre's ``_`` does without a translation.

    Example:
        >>> _("Unknown")
        'Unknown'


    :param text: The text to translate.
    :return: The same text.
    """
    return text


def string_to_authors(raw: str | None) -> list[str]:
    """
    Split an author string into names, as calibre 9.18 does.

    Example:
        >>> string_to_authors("Tom && Jerry with Spike")
        ['Tom & Jerry', 'Spike']


    :param raw: Names separated by ``&``, ``and`` or ``with``; ``&&`` is a
        literal ampersand.
    :return: The names, without empty ones.
    """
    if not raw:
        return []
    raw = raw.replace("&&", "￿")
    raw = _author_pat.sub("&", raw)
    authors = [a.strip().replace("￿", "&") for a in raw.split("&")]
    return [a for a in authors if a]


def MetaInformation(  # noqa: N802 - calibre's name
    title: Any, authors: Collection[str] | None = (_("Unknown"),)
) -> Any:
    """
    Make calibre ``Metadata``, as calibre 9.18's compatibility function does.

    Example:
        >>> mi = MetaInformation("A Book", ["Ann"])
        >>> mi.title, mi.authors
        ('A Book', ['Ann'])


    :param title: A title, or an object with ``title`` and ``authors``.
    :param authors: The authors' names.
    :return: calibre ``Metadata``.
    """
    mi = None
    if hasattr(title, "title") and hasattr(title, "authors"):
        mi = title
        title = mi.title
        authors = mi.authors
    return Metadata(title, authors, other=mi)


def prints(*args: Any, **kwargs: Any) -> None:
    """
    Print to a stream, as calibre's ``prints`` does for text.

    Example:
        >>> prints("a", "b", sep="-")
        a-b


    :param args: Things to print.
    :param kwargs: ``file``, ``sep`` and ``end`` as for ``print``.
    :return: None.
    """
    stream = kwargs.get("file", sys.stdout)
    print(
        *args,
        sep=kwargs.get("sep", " "),
        end=kwargs.get("end", "\n"),
        file=stream,
    )


class _Resolver(etree.Resolver):
    """
    Resolve every external entity to nothing, as calibre's parser does.

    Example:
        >>> isinstance(_Resolver(), etree.Resolver)
        True
    """

    def resolve(self, url: Any, id: Any, context: Any) -> Any:  # noqa: A002
        """
        Answer an entity reference with an empty document.

        Example:
            Used by lxml while parsing; not called directly.


        :param url: The entity's system identifier.
        :param id: The entity's public identifier.
        :param context: lxml's resolver context.
        :return: An empty document.
        """
        return self.resolve_string("", context)


def safe_xml_fromstring(string_or_bytes: str | bytes, recover: bool = True) -> Any:
    """
    Parse XML without network access or external entities, as calibre does.

    Example:
        >>> safe_xml_fromstring("<a><b>x</b></a>").find("b").text
        'x'


    :param string_or_bytes: The document.
    :param recover: Whether to recover from errors.
    :return: The root element.
    """
    encoding = None
    raw = string_or_bytes
    if isinstance(raw, str):
        raw = raw.encode("utf-8")
        encoding = "utf-8"
    parser = etree.XMLParser(recover=recover, no_network=True, encoding=encoding)
    parser.resolvers.add(_Resolver())
    return etree.fromstring(raw, parser=parser)


def force_unicode(obj: Any, enc: str = preferred_encoding) -> Any:
    """
    Decode bytes, trying further encodings in turn, as calibre does.

    Example:
        >>> force_unicode(b"caf\\xc3\\xa9", "utf-8"), force_unicode(5)
        ('café', 5)


    :param obj: Bytes to decode; anything else is returned unchanged.
    :param enc: The encoding to try first.
    :return: The text, or ``obj`` itself if it was not bytes.
    """
    if isinstance(obj, bytes):
        try:
            obj = obj.decode(enc)
        except Exception:
            try:
                obj = obj.decode(
                    filesystem_encoding
                    if enc == preferred_encoding
                    else preferred_encoding
                )
            except Exception:
                try:
                    obj = obj.decode("utf-8")
                except Exception:
                    obj = repr(obj)
                    if isinstance(obj, bytes):
                        obj = obj.decode("utf-8")
    return obj


def int_to_byte(x: int) -> bytes:
    """
    Make the one byte with value ``x``, as calibre's ``polyglot`` does.

    Example:
        >>> int_to_byte(65)
        b'A'


    :param x: A value from 0 to 255.
    :return: That byte.
    """
    return bytes((x,))


# Entity names and their text, as in calibre's table: HTML5's, plus two
# common misspellings calibre accepts.
_ENTITIES: dict[str, str] = {
    name.rstrip(";"): text for name, text in html.entities.html5.items()
}
_ENTITIES.setdefault("squot", "'")
_ENTITIES.setdefault("hellips", _ENTITIES["hellip"])

# Numeric references to these code points become windows-1252's characters,
# as HTML5 decodes them; 0x81, 0x8D, 0x8F, 0x90 and 0x9D stay as they are.
_WINDOWS_1252 = (
    "\u20ac\x81\u201a\u0192\u201e\u2026\u2020\u2021"
    "\u02c6\u2030\u0160\u2039\u0152\x8d\u017d\x8f"
    "\x90\u2018\u2019\u201c\u201d\u2022\u2013\u2014"
    "\u02dc\u2122\u0161\u203a\u0153\x9d\u017e\u0178"
)
_CODEPOINT_LIMIT = 0x110000
_HEX_DIGITS = frozenset("0123456789abcdefABCDEF")


def _entity_text(entity: str) -> str | None:
    """
    Decode one entity's name or number, as calibre's ``convert_entity`` does.

    Example:
        >>> _entity_text("amp"), _entity_text("#x41"), _entity_text("#128")
        ('&', 'A', '€')


    :param entity: What lies between ``&`` and ``;``.
    :return: The text it stands for, or ``None`` to keep it as written.
    """
    if not entity.startswith("#"):
        return _ENTITIES.get(entity)
    if len(entity) < 2:
        return None
    if entity[1] in "xX":
        digits, base = entity[2:], 16
        if not digits or not set(digits) <= _HEX_DIGITS:
            return None
    else:
        digits, base = entity[1:], 10
        if not digits.isascii() or not digits.isdigit():
            return None
    codepoint = 0
    for digit in digits:
        # Clamp while reading, so a long number cannot wrap to a valid one.
        codepoint = min(codepoint * base + int(digit, 16), _CODEPOINT_LIMIT)
    if not codepoint or codepoint >= _CODEPOINT_LIMIT or 0xD800 <= codepoint <= 0xDFFF:
        return "\ufffd"
    if 0x80 <= codepoint <= 0x9F:
        return _WINDOWS_1252[codepoint - 0x80]
    return chr(codepoint)


def _is_entity_character(character: str) -> bool:
    """
    Say whether a character can be part of an entity's name or number.

    Example:
        >>> _is_entity_character("a"), _is_entity_character("é")
        (True, False)


    :param character: One character.
    :return: Whether it is an ASCII letter or digit.
    """
    return character.isascii() and character.isalnum()


def replace_entities(raw: str) -> str:
    """
    Replace HTML5 entities and character references, as calibre's C code does.

    Only references ended by ``;`` are replaced, and only those of 3 to 64
    characters from ``&`` to ``;``. Scanning matches calibre's: a character
    that cannot be part of a reference ends the attempt and is written out
    with it, so ``&&amp;`` stays as it is.

    Example:
        >>> replace_entities("Tom &amp; Jerry &copy &#169; &#x2122; &&amp;")
        'Tom & Jerry &copy © ™ &&amp;'


    :param raw: The text.
    :return: The text with its references replaced.
    """
    out: list[str] = []
    size = len(raw)
    position = 0
    while position < size:
        ampersand = raw.find("&", position)
        if ampersand < 0:
            out.append(raw[position:])
            break
        out.append(raw[position:ampersand])
        end = ampersand + 1
        replacement: str | None = None
        while end < size:
            character = raw[end]
            end += 1
            if _is_entity_character(character) or (
                character == "#" and end == ampersand + 2
            ):
                continue
            if character == ";" and 3 <= end - ampersand <= 64:
                replacement = _entity_text(raw[ampersand + 1 : end - 1])
            break
        out.append(raw[ampersand:end] if replacement is None else replacement)
        position = end
    return "".join(out)


def prepare_string_for_xml(raw: str, attribute: bool = False) -> str:
    """
    Replace entities, then escape the text for XML, as calibre does.

    Example:
        >>> prepare_string_for_xml("A &amp; B <c>")
        'A &amp; B &lt;c&gt;'


    :param raw: The text.
    :param attribute: Whether to escape quotes too, for an attribute value.
    :return: The escaped text.
    """
    raw = replace_entities(raw)
    raw = raw.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    if attribute:
        raw = raw.replace('"', "&quot;").replace("'", "&apos;")
    return raw


class StreamOrPath:
    """
    Open a path, or use a stream as it is, as calibre's helper does.

    A stream given in is left open; a file opened from a path is closed on
    leaving the ``with`` block.

    Example:
        >>> import io
        >>> with StreamOrPath(io.BytesIO(b"x"), "rb") as stream:
        ...     stream.read()
        b'x'
    """

    def __init__(
        self,
        stream_or_path: str | bytes | IO[Any],
        mode: str,
        makedirs: bool = False,
        **kwargs: Any,
    ) -> None:
        """
        Remember the stream, or open the path.

        Example:
            >>> StreamOrPath(__file__, "rb").needs_close
            True


        :param stream_or_path: An open stream, or a path to open.
        :param mode: The mode to open a path in.
        :param makedirs: Whether to create a path's folder when writing.
        :param kwargs: Passed to :func:`open`.
        :return: None.
        """
        if hasattr(stream_or_path, "read") or hasattr(stream_or_path, "write"):
            self.stream: IO[Any] = stream_or_path  # type: ignore[assignment]
            self.needs_close = False
        else:
            path: Any = stream_or_path
            if makedirs and any(m in mode for m in "wxa"):
                dirname = os.path.dirname(path)
                if dirname and not os.path.exists(dirname):
                    os.makedirs(dirname)
            self.stream = open(path, mode, **kwargs)  # noqa: SIM115
            self.needs_close = True

    def __enter__(self) -> IO[Any]:
        """
        Give the stream.

        Example:
            >>> import io
            >>> with StreamOrPath(io.BytesIO(), "wb") as stream:
            ...     stream.write(b"x")
            1


        :return: The stream.
        """
        return self.stream

    def __exit__(self, *args: object) -> None:
        """
        Close the file if it was opened here.

        Example:
            Called on leaving the ``with`` block.


        :param args: The exception details, if any.
        :return: None.
        """
        if self.needs_close:
            self.stream.close()


def load_defaults(name: str) -> dict[str, Any]:
    """
    Return no saved conversion settings, so calibre's defaults apply.

    calibre reads the settings saved for a conversion plugin; LiuXin's ported
    readers have none.

    Example:
        >>> load_defaults("mobi_output")
        {}


    :param name: The settings group, such as ``"mobi_output"``.
    :return: An empty mapping.
    """
    return {}


def _closing_brace(text: Any, start: int) -> int:
    r"""
    Find the first ``}`` at or after ``start`` that no backslash precedes.

    This is where an RTF field's value ends, as the pattern ``(?<!\\)\}`` found it.

    Example:
        >>> _closing_brace(rb"a\}b}", 0), _closing_brace("x", 0)
        (4, -1)


    :param text: The RTF, as bytes or text.
    :param start: Where the field's value starts.
    :return: The brace's index, or ``-1`` when there is none.
    """
    brace, backslash = ("}", "\\") if isinstance(text, str) else (b"}", b"\\")
    end: int = text.find(brace, start)
    while end > 0 and text[end - 1 : end] == backslash:
        end = text.find(brace, end + 1)
    return end


class FieldMatch:
    r"""
    The one group a field pattern's match has: the field's value.

    Example:
        >>> FieldMatch(b" A").group(1)
        b' A'
    """

    def __init__(self, value: Any) -> None:
        """
        Hold the value.

        Example:
            >>> FieldMatch("x").group(1)
            'x'


        :param value: The field's raw value.
        :return: None.
        """
        self._value = value

    def group(self, index: int) -> Any:
        """
        Return the value, as ``match.group(1)`` did.

        Example:
            >>> FieldMatch("x").group(1)
            'x'


        :param index: Must be 1.
        :return: The value.
        :raises IndexError: For any other group.
        """
        if index != 1:
            raise IndexError("no such group")
        return self._value


class InfoFieldPattern:
    r"""
    A linear-time stand-in for calibre's RTF info-field patterns.

    calibre compiles ``rb'\{\\info.*?\{\\NAME(.*?)(?<!\\)\}'`` with ``re.DOTALL``
    for each field. On repeated, never-closed ``{\info{\title`` groups those
    patterns backtrack in cubic time: 15 kB took nearly a minute. ``search``
    returns what the pattern's search did, found with ``find``: the first
    ``{\NAME`` after the first ``{\info``, up to the first brace no backslash
    precedes.

    Example:
        >>> InfoFieldPattern(b"title").search(rb"{\info{\title A \} B}}").group(1)
        b' A \\} B'
        >>> InfoFieldPattern(b"title").search(rb"{\info{\title never closed") is None
        True
    """

    def __init__(self, name: bytes) -> None:
        r"""
        Remember the field's control word.

        Example:
            >>> InfoFieldPattern(b"author").opening
            b'{\\author'


        :param name: The control word, such as ``b"title"``.
        :return: None.
        """
        self.opening = b"{\\" + name

    def search(self, block: bytes) -> FieldMatch | None:
        r"""
        Find the field's value as the pattern's ``search`` did.

        Example:
            >>> InfoFieldPattern(b"title").search(b"no info") is None
            True


        :param block: The info block, as ``get_document_info`` returns it.
        :return: A match whose group 1 is the raw value, or ``None``.
        """
        info = block.find(b"{\\info")
        if info < 0:
            return None
        field = block.find(self.opening, info + len(b"{\\info"))
        if field < 0:
            return None
        start = field + len(self.opening)
        end = _closing_brace(block, start)
        return None if end < 0 else FieldMatch(block[start:end])


class FieldPattern:
    r"""
    A linear-time stand-in for ``re.compile(r'\{\\NAME(.*?)(?<!\\)\}', re.DOTALL)``.

    calibre's RTF writer only calls ``subn`` on it, which with that pattern took
    quadratic time on repeated never-closed fields: 150 kB took 15 seconds.

    Example:
        >>> FieldPattern("title").subn(r"{\\title N}", r"{\title A}{\title B")
        ('{\\title N}{\\title B', 1)
    """

    def __init__(self, name: str) -> None:
        """
        Remember the field's control word.

        Example:
            >>> FieldPattern("title").opening
            '{\\\\title'


        :param name: The control word, such as ``"title"``.
        :return: None.
        """
        self.opening = "{\\" + name

    def subn(self, template: str, string: str) -> tuple[str, int]:
        """
        Replace every closed field, left to right, as ``re.subn`` did.

        The template's escapes are expanded by ``re`` itself, as ``subn`` would;
        a field with no closing brace ends the search, since no later field can
        have one either.

        Example:
            >>> FieldPattern("title").subn("N", "no fields")
            ('no fields', 0)


        :param template: The replacement, escaped for ``re`` as calibre builds it.
        :param string: The info block as text.
        :return: The new text and how many fields were replaced.
        """
        empty = re.match("", "")
        assert empty is not None
        replacement = empty.expand(template)
        parts: list[str] = []
        count = position = 0
        while True:
            field = string.find(self.opening, position)
            if field < 0:
                break
            end = _closing_brace(string, field + len(self.opening))
            if end < 0:
                break
            parts.extend((string[position:field], replacement))
            position = end + 1
            count += 1
        parts.append(string[position:])
        return "".join(parts), count
