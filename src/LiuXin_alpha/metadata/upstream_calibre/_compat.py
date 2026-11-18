# Derived from calibre (https://calibre-ebook.com) 9.18: small parts of
# src/calibre/__init__.py, constants.py, ptempfile.py, spell/__init__.py,
# ebooks/__init__.py, ebooks/oeb/base.py, ebooks/oeb/polish/utils.py,
# ebooks/metadata/meta.py, ebooks/html_entities.c and utils/ (__init__.py,
# config_base.py, filenames.py, icu.py, localization.py, speedups.py and
# xml_parse.py), and polyglot/. License: GPL v3. Copyright: Kovid Goyal.
# SPDX-License-Identifier: GPL-3.0-only
"""
The small calibre helpers the ported modules import, ported or mapped.

Larger calibre modules are ported whole as their own modules in this package
(``metadata_base`` for ``calibre.ebooks.metadata``, ``book_base`` for its
``Metadata`` class, ``date``, ``opf2`` and so on). This module holds the
pieces taken from modules that are not ported whole: each function says which
calibre one it follows.

What differs from calibre, and why:

- Text: ``_`` and ``ngettext`` return their text untranslated, as calibre's do
  with no translation installed. ``icu_lower``, ``icu_upper`` and ``sort_key``
  use Python's case rules, as LiuXin has no ICU.
- Languages and countries: the ISO 639 and 3166 maps are built from iso-codes
  with calibre's own rules (``scripts/build_upstream_calibre_iso_codes.py``)
  and shipped as ``iso_codes.json.gz``. ``get_lang`` returns English: LiuXin
  has no interface-language setting.
- Media types: calibre's ``mime.types`` is loaded into a private table on top
  of Python's built-in types, rather than into Python's global table with the
  system's files, so answers are the same on every machine.
- Tweaks: calibre's ``default_tweaks.py`` is read as calibre reads it; there
  is no user tweaks file.
- C functions: ``replace_all_entities`` ports calibre's
  ``fast_html_entities``; ``_speedups`` ports the ``speedup`` functions the
  modules call. Encoding guesses use the ``chardet`` package where calibre uses
  uchardet. ``tzlocal`` and ``ZoneInfo`` fall back to UTC where no zone can be
  found.
- Standard library: ``TemporaryDirectory``, ``SpooledTemporaryFile`` and
  ``ZipFile`` are Python's; calibre's ``zipfile`` is its own copy.
- Not ported: the template formatter (``SafeFormat`` can be made, not used),
  Qt, and the helpers only writers use (``save_cover_data_to``,
  ``safe_replace``), which raise ``NotImplementedError``.

``InfoFieldPattern`` and ``FieldPattern`` stand in for the RTF reader's and
writer's field patterns, which backtrack for minutes on damaged files; they
find the same values in linear time.

Example:
    >>> canonicalize_lang("en-GB"), guess_type("content.opf")[0]
    ('eng', 'application/oebps-package+xml')
"""

from __future__ import annotations

import codecs
import functools
import html.entities
import locale
import mimetypes
import os
import re
import sys
import tempfile
import zipfile
from collections.abc import Collection
from pathlib import Path
from typing import IO, Any

from lxml import etree


__all__ = [
    "FieldMatch",
    "FieldPattern",
    "InfoFieldPattern",
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
]

TemporaryDirectory = tempfile.TemporaryDirectory
ZipFile = zipfile.ZipFile

_mime_types: mimetypes.MimeTypes | None = None


def _mime_db() -> mimetypes.MimeTypes:
    """
    Load calibre's media types into a private table, once.

    calibre adds its ``mime.types`` file to Python's global table, which also
    reads the system's files (and on Windows the registry). LiuXin keeps a
    private table of Python's built-in types plus calibre's file, so the answers
    do not change from machine to machine or affect other code.

    Example:
        >>> _mime_db().guess_type("toc.ncx")[0]
        'application/x-dtbncx+xml'


    :return: The table.
    """
    global _mime_types
    if _mime_types is None:
        table = mimetypes.MimeTypes()
        table.read(str(Path(__file__).with_name("mime.types")))
        _mime_types = table
    return _mime_types


@functools.lru_cache(4096)
def guess_type(url: str, strict: bool = True) -> tuple[str | None, str | None]:
    """
    Guess a file's media type from calibre's table, as calibre's ``guess_type`` does.

    Example:
        >>> guess_type("content.opf")
        ('application/oebps-package+xml', None)


    :param url: A file name or URL.
    :param strict: Use only the official types.
    :return: The media type and encoding, either of which may be ``None``.
    """
    return _mime_db().guess_type(url, strict)


def guess_all_extensions(type: str, strict: bool = True) -> list[str]:
    """
    List the extensions for a media type, from calibre's table.

    Example:
        >>> ".epub" in guess_all_extensions("application/epub+zip")
        True


    :param type: The media type.
    :param strict: Use only the official types.
    :return: The extensions, with their dots.
    """
    return _mime_db().guess_all_extensions(type, strict)


def guess_extension(type: str, strict: bool = True) -> str | None:
    """
    Choose an extension for a media type, as calibre does.

    Example:
        >>> guess_extension("application/epub+zip")
        '.epub'


    :param type: The media type.
    :param strict: Use only the official types.
    :return: The extension with its dot, or ``None``.
    """
    ext = _mime_db().guess_extension(type, strict)
    if not ext and type == "application/x-palmreader":
        ext = ".pdb"
    return ext


def get_types_map() -> dict[str, str]:
    """
    Return calibre's extension-to-type map.

    Example:
        >>> get_types_map()[".xhtml"]
        'application/xhtml+xml'


    :return: The strict map.
    """
    return _mime_db().types_map[True]


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


def _is_xml_unsafe(text: str) -> bool:
    """
    Say whether one decoded character is markup XML must keep escaped.

    Example:
        >>> _is_xml_unsafe("<"), _is_xml_unsafe("a")
        (True, False)


    :param text: A decoded entity.
    :return: Whether it is one of ``< > & " '``.
    """
    return len(text) == 1 and text in "<>&\"'"


def _entity_text(entity: str, keep_xml_entities: bool = False) -> str | None:
    """
    Decode one entity's name or number, as calibre's ``convert_entity`` does.

    Example:
        >>> _entity_text("amp"), _entity_text("#x41"), _entity_text("#128")
        ('&', 'A', '€')
        >>> _entity_text("lt", keep_xml_entities=True) is None
        True


    :param entity: What lies between ``&`` and ``;``.
    :param keep_xml_entities: Keep references to ``< > & " '`` as written.
    :return: The text it stands for, or ``None`` to keep it as written.
    """
    if not entity.startswith("#"):
        text = _ENTITIES.get(entity)
        if text is not None and keep_xml_entities and _is_xml_unsafe(text):
            return None
        return text
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
        text = "\ufffd"
    elif 0x80 <= codepoint <= 0x9F:
        text = _WINDOWS_1252[codepoint - 0x80]
    else:
        text = chr(codepoint)
    if keep_xml_entities and _is_xml_unsafe(text):
        return None
    return text


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


def _replace(raw: str, keep_xml_entities: bool, encode: Any) -> str:
    """
    Scan text for references and replace them, as calibre's C ``replace`` does.

    Example:
        >>> _replace("a &amp; b", False, str)
        'a & b'


    :param raw: The text; for bytes, their latin-1 decoding.
    :param keep_xml_entities: Keep references to ``< > & " '`` as written.
    :param encode: Turns decoded text into the form ``raw`` is in.
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
                replacement = _entity_text(
                    raw[ampersand + 1 : end - 1], keep_xml_entities
                )
            break
        out.append(raw[ampersand:end] if replacement is None else encode(replacement))
        position = end
    return "".join(out)


def replace_all_entities(raw: Any, keep_xml_entities: bool = False) -> Any:
    """
    Replace HTML5 entities and character references, as calibre's C code does.

    This ports ``fast_html_entities.replace_all_entities``. Only references
    ended by ``;`` are replaced, and only those of 3 to 64 characters from
    ``&`` to ``;``. Scanning matches calibre's: a character that cannot be part
    of a reference ends the attempt and is written out with it, so ``&&amp;``
    stays as it is. Bytes are taken as UTF-8, and references in them become
    UTF-8.

    Example:
        >>> replace_all_entities("&lt;b&gt; &amp; &copy;", keep_xml_entities=True)
        '&lt;b&gt; &amp; ©'
        >>> replace_all_entities(b"caf&eacute;")
        b'caf\\xc3\\xa9'


    :param raw: Text, or UTF-8 bytes.
    :param keep_xml_entities: Keep references to ``< > & " '`` as written.
    :return: The same type, with its references replaced.
    :raises TypeError: For anything but text or bytes.
    """
    if isinstance(raw, str):
        raw.encode("utf-8")  # the C reads UTF-8, so fails on lone surrogates
        return _replace(raw, keep_xml_entities, str)
    if isinstance(raw, bytes):
        replaced = _replace(
            raw.decode("latin-1"),
            keep_xml_entities,
            lambda text: text.encode("utf-8").decode("latin-1"),
        )
        return replaced.encode("latin-1")
    raise TypeError("string must be unicode object or UTF-8 encoded bytes")


def replace_entities(raw: Any, encoding: str | None = None) -> Any:
    """
    Replace every entity, as calibre's ``replace_entities`` does.

    Example:
        >>> replace_entities("Tom &amp; Jerry &copy &#169; &#x2122; &&amp;")
        'Tom & Jerry &copy © ™ &&amp;'


    :param raw: Text, or UTF-8 bytes.
    :param encoding: Unused, as in calibre.
    :return: The same type, with its references replaced.
    """
    return replace_all_entities(raw)


def xml_replace_entities(raw: Any, encoding: str | None = None) -> Any:
    """
    Replace every entity except XML's own, as calibre's ``xml_replace_entities`` does.

    Example:
        >>> xml_replace_entities("&lt;&nbsp;&#38;")
        '&lt;\\xa0&#38;'


    :param raw: Text, or UTF-8 bytes.
    :param encoding: Unused, as in calibre.
    :return: The same type, with its references replaced.
    """
    return replace_all_entities(raw, True)


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


# --- calibre.constants -------------------------------------------------------

__appname__ = "calibre"
# The release the ported modules come from; calibre writes it into the OPF it
# generates.
__version__ = "9.18.0"
DEBUG = False
iswindows = sys.platform.startswith("win")
ismacos = sys.platform == "darwin"


# --- calibre (package) -------------------------------------------------------


class CurrentDir:
    """
    Change into a folder for a ``with`` block, as calibre's helper does.

    Example:
        >>> import tempfile
        >>> before = os.getcwd()
        >>> with tempfile.TemporaryDirectory() as folder, CurrentDir(folder):
        ...     os.getcwd() != before
        True
    """

    def __init__(self, path: str) -> None:
        """
        Remember the folder.

        Example:
            >>> CurrentDir(".").path
            '.'


        :param path: The folder to change into.
        :return: None.
        """
        self.path = path
        self.cwd: str | None = None

    def __enter__(self, *args: object) -> str:
        """
        Change into the folder.

        Example:
            Called on entering the ``with`` block.


        :param args: Unused.
        :return: The folder that was current before.
        """
        self.cwd = os.getcwd()
        os.chdir(self.path)
        return self.cwd

    def __exit__(self, *args: object) -> None:
        """
        Change back, if the old folder still exists.

        Example:
            Called on leaving the ``with`` block.


        :param args: The exception details, if any.
        :return: None.
        """
        try:
            if self.cwd is not None:
                os.chdir(self.cwd)
        except OSError:
            pass


def strftime(fmt: str, t: Any = None) -> str:
    """
    Format a time as calibre's ``strftime`` does, including years before 1900.

    Example:
        >>> import datetime
        >>> strftime("%Y-%m", datetime.date(1850, 3, 1))
        '1850-03'


    :param fmt: The format.
    :param t: A ``struct_time`` or date; now if ``None``.
    :return: The formatted text.
    """
    import time

    if not fmt:
        return ""
    if t is None:
        t = time.localtime()
    if hasattr(t, "timetuple"):
        t = t.timetuple()
    early_year = t[0] < 1900
    orig_year = t[0]
    if early_year:
        replacement = 1900 if t[0] % 4 == 0 else 1901
        fmt = fmt.replace("%Y", "_early year hack##")
        parts = list(t)
        parts[0] = replacement
        t = time.struct_time(parts)
    if isinstance(fmt, bytes):
        fmt = fmt.decode("mbcs" if iswindows else "utf-8", "replace")
    ans = time.strftime(fmt, t)
    if early_year:
        ans = ans.replace("_early year hack##", str(orig_year))
    return ans


def escape_xpath_attr(value: str) -> str:
    """
    Quote a value for an XPath attribute test, as ``calibre.ebooks`` does.

    Example:
        >>> escape_xpath_attr('a"b'), escape_xpath_attr("plain")
        ('\\'a"b\\'', '"plain"')


    :param value: The attribute value.
    :return: An XPath string literal, or a ``concat()`` of them.
    """
    if '"' in value:
        if "'" in value:
            parts = re.split(r'("+)', value)
            ans = []
            for x in parts:
                if x:
                    q = "'" if '"' in x else '"'
                    ans.append(q + x + q)
            return "concat({})".format(", ".join(ans))
        return f"'{value}'"
    return f'"{value}"'


# --- calibre.ebooks.oeb.base: namespaces ------------------------------------

XML_NS = "http://www.w3.org/XML/1998/namespace"
OPF1_NS = "http://openebook.org/namespaces/oeb-package/1.0/"
OPF2_NS = "http://www.idpf.org/2007/opf"
DC09_NS = "http://purl.org/metadata/dublin_core"
DC10_NS = "http://purl.org/dc/elements/1.0/"
DC11_NS = "http://purl.org/dc/elements/1.1/"
XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"
DCTERMS_NS = "http://purl.org/dc/terms/"
CALIBRE_NS = "http://calibre.kovidgoyal.net/2009/metadata"
OPF2_NSMAP = {
    "opf": OPF2_NS,
    "dc": DC11_NS,
    "dcterms": DCTERMS_NS,
    "xsi": XSI_NS,
    "calibre": CALIBRE_NS,
}


OPF_NAMESPACES = {"opf": OPF2_NS, "dc": DC11_NS}
OEB_DOC_MIME = "text/x-oeb1-document"
OEB_CSS_MIME = "text/x-oeb1-css"


def barename(name: str) -> str:
    """
    Drop the namespace from a Clark-notation name, as calibre's C ``barename`` does.

    Example:
        >>> barename("{http://www.idpf.org/2007/opf}item"), barename("item")
        ('item', 'item')


    :param name: The name.
    :return: The local part.
    :raises TypeError: For anything but text.
    """
    if not isinstance(name, str):
        raise TypeError(f"{name!r} is not a unicode string")
    return name.rpartition("}")[-1]


def oeb_docs() -> set[str]:
    """
    Return calibre's ``OEB_DOCS``: the media types of book documents.

    Example:
        >>> "application/xhtml+xml" in oeb_docs()
        True


    :return: The media types.
    """
    return {get_types_map()[".xhtml"], "text/html", OEB_DOC_MIME, "text/x-oeb-document"}


def oeb_styles() -> set[str]:
    """
    Return calibre's ``OEB_STYLES``: the media types of style sheets.

    Example:
        >>> "text/css" in oeb_styles()
        True


    :return: The media types.
    """
    return {get_types_map()[".css"], OEB_CSS_MIME, "text/x-oeb-css", "xhtml/css"}


def OPF(name: str) -> str:  # noqa: N802 - calibre's name
    """
    Qualify a name with the OPF 2 namespace.

    Example:
        >>> OPF("package")
        '{http://www.idpf.org/2007/opf}package'


    :param name: The local name.
    :return: The Clark-notation name.
    """
    return f"{{{OPF2_NS}}}{name}"


def DC(name: str) -> str:  # noqa: N802 - calibre's name
    """
    Qualify a name with the Dublin Core 1.1 namespace.

    Example:
        >>> DC("title")
        '{http://purl.org/dc/elements/1.1/}title'


    :param name: The local name.
    :return: The Clark-notation name.
    """
    return f"{{{DC11_NS}}}{name}"


def polish_guess_type(x: str) -> str:
    """
    Guess a file's media type, as ``calibre.ebooks.oeb.polish.utils`` does.

    Example:
        >>> polish_guess_type("cover.jpg"), polish_guess_type("x.unknown-ext")
        ('image/jpeg', 'application/octet-stream')


    :param x: A file name.
    :return: The media type, or ``application/octet-stream``.
    """
    return guess_type(x)[0] or "application/octet-stream"


# --- calibre.utils.localization ----------------------------------------------


def ngettext(singular: str, plural: str, n: int) -> str:
    """
    Choose the singular or plural text, untranslated.

    Example:
        >>> ngettext("book", "books", 2)
        'books'


    :param singular: The text for one.
    :param plural: The text for several.
    :param n: How many.
    :return: The matching text.
    """
    return singular if n == 1 else plural


lcdata: dict[str, Any] = {
    "abday": ("Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"),
    "abmon": (
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec",
    ),
    "d_fmt": "%m/%d/%Y",
    "d_t_fmt": "%a %d %b %Y %r %Z",
    "day": (
        "Sunday",
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
    ),
    "mon": (
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ),
    "noexpr": "^[nN].*",
    "radixchar": ".",
    "t_fmt": "%r",
    "t_fmt_ampm": "%I:%M:%S %p",
    "thousep": ",",
    "yesexpr": "^[yY].*",
}

_iso_codes: dict[str, Any] | None = None


def _load_iso_codes() -> dict[str, Any]:
    """
    Load the shipped ISO 639 and 3166 maps, once.

    ``scripts/build_upstream_calibre_iso_codes.py`` builds them from the
    iso-codes project's data with calibre's own rules; lists stand in for
    calibre's sets.

    Example:
        >>> _load_iso_codes()["iso639"]["2to3"]["en"]
        'eng'


    :return: The maps, keyed ``iso639`` and ``iso3166``.
    """
    global _iso_codes
    if _iso_codes is None:
        import gzip
        import json

        raw = (Path(__file__).with_name("iso_codes.json.gz")).read_bytes()
        _iso_codes = json.loads(gzip.decompress(raw))
        for key in ("codes2", "codes3"):
            _iso_codes["iso639"][key] = frozenset(_iso_codes["iso639"][key])
        _iso_codes["iso3166"]["codes"] = frozenset(_iso_codes["iso3166"]["codes"])
    return _iso_codes


def _load_iso639() -> dict[str, Any]:
    """
    Return calibre's ISO 639 maps.

    Example:
        >>> _load_iso639()["3to2"]["fra"]
        'fr'


    :return: ``by_2``, ``by_3``, ``codes2``, ``codes3``, ``2to3``, ``3to2`` and
        ``name_map``.
    """
    return _load_iso_codes()["iso639"]


def load_iso3166() -> dict[str, Any]:
    """
    Return calibre's ISO 3166 maps.

    Example:
        >>> load_iso3166()["three_map"]["DEU"]
        'DE'


    :return: ``names``, ``codes`` and ``three_map``.
    """
    return _load_iso_codes()["iso3166"]


def canonicalize_lang(raw: str | bytes | None) -> str | None:
    """
    Turn a language code or English name into an ISO 639-3 code, as calibre does.

    Example:
        >>> canonicalize_lang("en_GB"), canonicalize_lang("FRA"), canonicalize_lang("German")
        ('eng', 'fra', 'deu')
        >>> canonicalize_lang("xx") is None
        True


    :param raw: A code such as ``en-GB``, or a name such as ``English``.
    :return: The three-letter code, or ``None`` when it is not a language.
    """
    if not raw:
        return None
    if not isinstance(raw, str):
        raw = raw.decode("utf-8", "ignore")
    raw = raw.lower().strip()
    if not raw:
        return None
    raw = raw.replace("_", "-").partition("-")[0].partition("@")[0].strip()
    if not raw:
        return None
    iso639 = _load_iso639()
    m2to3 = iso639["2to3"]

    if len(raw) == 2:
        ans = m2to3.get(raw, None)
        if ans is not None:
            return ans
    elif len(raw) == 3:
        if raw in iso639["by_3"]:
            return raw

    return iso639["name_map"].get(raw, None)


def lang_as_iso639_1(name_or_code: str | bytes | None) -> str | None:
    """
    Return a language's two-letter code, as calibre does.

    Example:
        >>> lang_as_iso639_1("deu"), lang_as_iso639_1("Klingon")
        ('de', None)


    :param name_or_code: A code or English name.
    :return: The ISO 639-1 code, or ``None`` when there is none.
    """
    code = canonicalize_lang(name_or_code)
    if code is not None:
        iso639 = _load_iso639()
        return iso639["3to2"].get(code, None)
    return None


def calibre_langcode_to_name(lc: str | None, localize: bool = True) -> str | None:
    """
    Return a language's English name, as calibre does without a translation.

    Example:
        >>> calibre_langcode_to_name("fra")
        'French'


    :param lc: An ISO 639-3 code.
    :param localize: Translate the name; LiuXin has no translations.
    :return: The name, or ``lc`` itself when it is unknown.
    """
    iso639 = _load_iso639()
    try:
        if lc is not None:
            return iso639["by_3"][lc]
    except Exception:
        pass
    return lc


def get_lang() -> str:
    """
    Return the interface language: English, as LiuXin has no such setting.

    calibre reads its interface-language preference and then the system locale.
    The ported code only uses this as a default language for OPF files it
    writes, so LiuXin uses English rather than make output depend on the
    machine.

    Example:
        >>> get_lang()
        'en'


    :return: ``"en"``.
    """
    return "en"


# --- calibre.spell ----------------------------------------------------------


class DictionaryLocale(tuple):
    """
    A language and country, as calibre's ``DictionaryLocale`` named tuple.

    Example:
        >>> DictionaryLocale("eng", "GB").countrycode
        'GB'
    """

    __slots__ = ()

    def __new__(cls, langcode: str, countrycode: str | None) -> DictionaryLocale:
        """
        Build the pair.

        Example:
            >>> tuple(DictionaryLocale("fra", None))
            ('fra', None)


        :param langcode: An ISO 639-3 code.
        :param countrycode: An ISO 3166 alpha-2 code, or ``None``.
        :return: The pair.
        """
        return tuple.__new__(cls, (langcode, countrycode))

    @property
    def langcode(self) -> str:
        """
        The language.

        Example:
            >>> DictionaryLocale("eng", None).langcode
            'eng'


        :return: An ISO 639-3 code.
        """
        return self[0]

    @property
    def countrycode(self) -> str | None:
        """
        The country, if any.

        Example:
            >>> DictionaryLocale("eng", None).countrycode is None
            True


        :return: An ISO 3166 alpha-2 code, or ``None``.
        """
        return self[1]


def parse_lang_code(raw: str | None) -> DictionaryLocale:
    """
    Split a BCP 47 tag into calibre's language and country, as calibre does.

    Example:
        >>> tuple(parse_lang_code("en-GB")), tuple(parse_lang_code("sr-Latn-RS"))
        (('eng', 'GB'), ('srp', 'RS'))


    :param raw: A tag such as ``en_US``.
    :return: The language and country.
    :raises ValueError: When the language is not one.
    """
    raw = raw or ""
    parts = raw.replace("_", "-").split("-")
    lc = canonicalize_lang(parts[0])
    if lc is None:
        raise ValueError(f"Invalid language code: {raw!r}")
    cc = None
    for sc in ["Cyrl", "Latn"]:
        if sc in parts:
            parts.remove(sc)
    if len(parts) > 1:
        data = load_iso3166()
        ccodes, ccodemap = data["codes"], data["three_map"]
        q = parts[1].upper()
        if q in ccodes:
            cc = q
        else:
            cc = ccodemap.get(q, None)
    return DictionaryLocale(lc, cc)


# --- calibre.utils.icu --------------------------------------------------------


def icu_lower(text: str) -> str:
    """
    Lower-case text; calibre uses ICU, LiuXin Python's own rules.

    Example:
        >>> icu_lower("ÉCOLE")
        'école'


    :param text: The text.
    :return: The text in lower case.
    """
    return text.lower()


def icu_upper(text: str) -> str:
    """
    Upper-case text; calibre uses ICU, LiuXin Python's own rules.

    Example:
        >>> icu_upper("straße")
        'STRASSE'


    :param text: The text.
    :return: The text in upper case.
    """
    return text.upper()


def sort_key(text: Any) -> Any:
    """
    Make a sort key; calibre uses ICU collation, LiuXin case folding.

    Example:
        >>> sorted(["b", "A", "a"], key=sort_key)
        ['A', 'a', 'b']


    :param text: The text, or bytes, to sort by.
    :return: A key that orders text case-insensitively.
    """
    if isinstance(text, bytes):
        text = text.decode("utf-8", "replace")
    return (text or "").casefold()


# --- calibre.ptempfile, calibre.utils.logging ------------------------------------------

SpooledTemporaryFile = tempfile.SpooledTemporaryFile


class _Log:
    """
    A stand-in for calibre's default log, writing to Python logging.

    Example:
        >>> default_log.debug("quiet")
    """

    def __init__(self) -> None:
        """
        Use the ``LiuXin_alpha.upstream_calibre`` logger.

        Example:
            >>> _Log().logger.name
            'LiuXin_alpha.upstream_calibre'


        :return: None.
        """
        import logging

        self.logger = logging.getLogger("LiuXin_alpha.upstream_calibre")

    def __call__(self, *args: object) -> None:
        """
        Log at info level, as calling calibre's log does.

        Example:
            >>> default_log("hello")


        :param args: The parts of the message.
        :return: None.
        """
        self.logger.info(" ".join(map(str, args)))

    def debug(self, *args: object) -> None:
        """
        Log at debug level.

        Example:
            >>> default_log.debug("x")


        :param args: The parts of the message.
        :return: None.
        """
        self.logger.debug(" ".join(map(str, args)))

    def warn(self, *args: object) -> None:
        """
        Log at warning level.

        Example:
            >>> default_log.warn("x")


        :param args: The parts of the message.
        :return: None.
        """
        self.logger.warning(" ".join(map(str, args)))

    warning = warn
    info = __call__

    def error(self, *args: object) -> None:
        """
        Log at error level.

        Example:
            >>> default_log.error("x")


        :param args: The parts of the message.
        :return: None.
        """
        self.logger.error(" ".join(map(str, args)))

    def exception(self, *args: object) -> None:
        """
        Log at error level with the current traceback.

        Example:
            Called inside an ``except`` block.


        :param args: The parts of the message.
        :return: None.
        """
        self.logger.exception(" ".join(map(str, args)))


default_log = _Log()


# --- tzlocal and zoneinfo ------------------------------------------------------


def get_localzone_name() -> str | None:
    """
    Name the local time zone, standing in for the ``tzlocal`` package.

    calibre depends on ``tzlocal``; LiuXin does not. This reads ``TZ``, then
    the ``/etc/localtime`` link; on Windows, or when neither names a zone, it
    returns ``None``, and calibre's code then uses UTC. The zone only matters
    for dates written without one.

    Example:
        >>> name = get_localzone_name()
        >>> name is None or isinstance(name, str)
        True


    :return: An IANA zone name, or ``None``.
    """
    name = os.environ.get("TZ", "").lstrip(":")
    if name and "/" in name and not name.startswith("/"):
        return name
    try:
        target = os.path.realpath("/etc/localtime")
    except OSError:
        return None
    marker = "zoneinfo" + os.sep
    if marker in target:
        return target.split(marker, 1)[1]
    return None


class tzlocal:  # noqa: N801 - the module name calibre imports
    """
    The part of the ``tzlocal`` package calibre's code calls.

    Example:
        >>> tzlocal.get_localzone_name is get_localzone_name
        True
    """

    get_localzone_name = staticmethod(get_localzone_name)


def ZoneInfo(name: str) -> Any:  # noqa: N802 - the class name calibre imports
    """
    Load a time zone, falling back to UTC where there is no zone database.

    Windows has no system zone database, and LiuXin does not depend on the
    ``tzdata`` package, so a zone that cannot be found falls back to UTC
    rather than failing at import.

    Example:
        >>> str(ZoneInfo("UTC"))
        'UTC'


    :param name: An IANA zone name.
    :return: The zone, or UTC.
    """
    import datetime
    import zoneinfo

    try:
        return zoneinfo.ZoneInfo(name)
    except (zoneinfo.ZoneInfoNotFoundError, ValueError):
        return datetime.UTC


# --- not ported yet ------------------------------------------------------------


def _not_ported(name: str) -> Any:
    """
    Make a stand-in for a calibre helper the readers do not need yet.

    Example:
        >>> _not_ported("pretty_opf")()
        Traceback (most recent call last):
        ...
        NotImplementedError: calibre's pretty_opf is not ported to LiuXin yet


    :param name: The helper's name.
    :return: A function that raises ``NotImplementedError``.
    """

    def stand_in(*args: object, **kwargs: object) -> Any:
        """
        Refuse, naming the helper.

        Example:
            See ``_not_ported``.


        :param args: Ignored.
        :param kwargs: Ignored.
        :return: Never.
        :raises NotImplementedError: Always.
        """
        raise NotImplementedError(f"calibre's {name} is not ported to LiuXin yet")

    stand_in.__name__ = name
    return stand_in


# Writing EPUB files needs these; the readers do not.
save_cover_data_to = _not_ported("save_cover_data_to")
safe_replace = _not_ported("safe_replace")


# --- more of calibre (package), polyglot and small utilities --------------------

relpath = os.path.relpath
TEMPLATE_ERROR = "TEMPLATE ERROR"


class SafeFormat:
    """
    A stand-in for calibre's template formatter, which LiuXin does not port.

    calibre's ``Metadata`` makes one for every object, but only uses it for
    composite custom columns, whose values come from templates. Making one
    works; formatting with it raises ``NotImplementedError``.

    Example:
        >>> formatter = SafeFormat()
        >>> formatter.allow_python_templates = False
        >>> formatter.safe_format("{title}", None, "", None)
        Traceback (most recent call last):
        ...
        NotImplementedError: calibre's template formatter is not ported to LiuXin yet
    """

    allow_python_templates = False

    def safe_format(self, *args: object, **kwargs: object) -> str:
        """
        Refuse: templates need calibre's formatter.

        Example:
            See the class docstring.


        :param args: Ignored.
        :param kwargs: Ignored.
        :return: Never.
        :raises NotImplementedError: Always.
        """
        raise NotImplementedError(
            "calibre's template formatter is not ported to LiuXin yet"
        )


def isbytestring(obj: object) -> bool:
    """
    Say whether something is bytes, as calibre does.

    Example:
        >>> isbytestring(b"x"), isbytestring("x")
        (True, False)


    :param obj: Anything.
    :return: Whether it is ``bytes``.
    """
    return isinstance(obj, bytes)


def as_bytes(x: Any, encoding: str = "utf-8") -> bytes:
    """
    Turn text or a buffer into bytes, as calibre's ``polyglot`` does.

    Example:
        >>> as_bytes("é"), as_bytes(5)
        (b'\\xc3\\xa9', b'5')


    :param x: Text, bytes, a buffer or anything ``str`` accepts.
    :param encoding: The encoding for text.
    :return: The bytes.
    """
    if isinstance(x, str):
        return x.encode(encoding)
    if isinstance(x, bytes):
        return x
    if isinstance(x, bytearray):
        return bytes(x)
    if isinstance(x, memoryview):
        return x.tobytes()
    return str(x).encode(encoding)


def as_unicode(x: Any, encoding: str = "utf-8", errors: str = "strict") -> str:
    """
    Turn bytes or anything else into text, as calibre's ``polyglot`` does.

    Example:
        >>> as_unicode(b"\\xc3\\xa9"), as_unicode(5)
        ('é', '5')


    :param x: Bytes, or anything ``str`` accepts.
    :param encoding: The encoding for bytes.
    :param errors: How to handle bytes that do not decode.
    :return: The text.
    """
    if isinstance(x, bytes):
        return x.decode(encoding, errors)
    return str(x)


def as_base64_unicode(x: Any, enc: str = "utf-8") -> str:
    """
    Encode text or bytes as base64 text, as calibre's ``polyglot`` does.

    Example:
        >>> as_base64_unicode("hi")
        'aGk='


    :param x: Text or bytes.
    :param enc: The encoding for text.
    :return: The base64 text.
    """
    from base64 import standard_b64encode

    if isinstance(x, str):
        x = x.encode(enc)
    return standard_b64encode(x).decode("ascii")


def from_base64_bytes(x: Any) -> bytes:
    """
    Decode base64 text or bytes, as calibre's ``polyglot`` does.

    Example:
        >>> from_base64_bytes("aGk=")
        b'hi'


    :param x: Base64 text or bytes.
    :return: The decoded bytes.
    """
    from base64 import standard_b64decode

    if isinstance(x, str):
        x = x.encode("ascii")
    return standard_b64decode(x)


class ReadOnlyFileBuffer:
    """
    A file-like view of bytes, as calibre's ``speedups.ReadOnlyFileBuffer``.

    Example:
        >>> buffer = ReadOnlyFileBuffer(b"abc")
        >>> bytes(buffer.read(2)), buffer.tell()
        (b'ab', 2)
    """

    is_close_frame: bool = False

    def __init__(self, raw: bytes | memoryview, name: str = "") -> None:
        """
        Wrap the bytes without copying.

        Example:
            >>> ReadOnlyFileBuffer(b"x").sz
            1


        :param raw: The bytes.
        :param name: A name for the buffer.
        :return: None.
        """
        self.sz, self.mv = (
            len(raw),
            (raw if isinstance(raw, memoryview) else memoryview(raw)),
        )
        self.pos = 0
        self.name: str = name

    def tell(self) -> int:
        """
        Return the position.

        Example:
            >>> ReadOnlyFileBuffer(b"x").tell()
            0


        :return: The position.
        """
        return self.pos

    def read(self, n: int | None = None) -> memoryview:
        """
        Read up to ``n`` bytes, or the rest.

        Example:
            >>> bytes(ReadOnlyFileBuffer(b"xy").read())
            b'xy'


        :param n: How many bytes, or ``None`` for all that remain.
        :return: A view of the bytes read.
        """
        if n is None:
            ans = self.mv[self.pos :]
            self.pos = self.sz
            return ans
        ans = self.mv[self.pos : self.pos + n]
        self.pos = min(self.pos + n, self.sz)
        return ans

    def seek(self, pos: int, whence: int = os.SEEK_SET) -> int:
        """
        Move, clamped to the buffer, as calibre's does.

        Example:
            >>> ReadOnlyFileBuffer(b"xyz").seek(-1, os.SEEK_END)
            2


        :param pos: The offset.
        :param whence: From the start, the current position or the end.
        :return: The new position.
        """
        if whence == os.SEEK_SET:
            self.pos = pos
        elif whence == os.SEEK_END:
            self.pos = self.sz + pos
        else:
            self.pos += pos
        self.pos = max(0, min(self.pos, self.sz))
        return self.pos

    def seekable(self) -> bool:
        """
        Say the buffer can seek.

        Example:
            >>> ReadOnlyFileBuffer(b"").seekable()
            True


        :return: True.
        """
        return True

    def getvalue(self) -> memoryview:
        """
        Return all the bytes.

        Example:
            >>> bytes(ReadOnlyFileBuffer(b"xy").getvalue())
            b'xy'


        :return: A view of the bytes.
        """
        return self.mv

    def close(self) -> None:
        """
        Do nothing; there is nothing to release.

        Example:
            >>> ReadOnlyFileBuffer(b"").close()


        :return: None.
        """


def uchardet_detect(raw: bytes) -> str:
    """
    Guess bytes' encoding; calibre uses uchardet, LiuXin the ``chardet`` package.

    The two libraries can guess differently on short or ambiguous text. calibre
    only guesses when the bytes declare no encoding and are not UTF-8.

    Example:
        >>> uchardet_detect("Grüße".encode("utf-8"))
        'utf-8'


    :param raw: The bytes.
    :return: The encoding's name, or ``""`` when there is no guess.
    """
    import chardet

    return chardet.detect(raw).get("encoding") or ""


windows_reserved_filenames = frozenset(
    ("CON", "PRN", "AUX", "CLOCK$", "NUL")
    + tuple(f"COM{i}" for i in range(10))
    + tuple(f"LPT{i}" for i in range(10))
)


def is_windows_reserved_filename(name: str) -> bool:
    """
    Say whether a name is a Windows device, as calibre does on every platform.

    Example:
        >>> is_windows_reserved_filename("con.txt"), is_windows_reserved_filename("cover.jpg")
        (True, False)


    :param name: A file name.
    :return: Whether its stem is a reserved device name.
    """
    return name.partition(".")[0].upper() in windows_reserved_filenames


BadZipfile = zipfile.BadZipFile
ZipInfo = zipfile.ZipInfo
ZIP_DEFLATED = zipfile.ZIP_DEFLATED
ZIP_STORED = zipfile.ZIP_STORED


def is_gui_thread() -> bool:
    """
    Say this is not calibre's GUI thread: LiuXin runs no calibre GUI.

    Example:
        >>> is_gui_thread()
        False


    :return: False.
    """
    return False


get_gui = _not_ported("get_gui")

# Only calibre's Qt date helpers use this; LiuXin has no Qt.
UNDEFINED_QDATETIME = None

# calibre reads the macOS short-date format through a C extension; without it
# calibre's date module falls back to month-first, as it does on any error.
macos_date_format = _not_ported("date_format")


# --- calibre.utils.filenames and calibre.ebooks.metadata.meta --------------------


def _normalize_path_for_containment(path: str, case_sensitive: bool = True) -> str:
    """
    Make a path absolute, and case-folded when asked, as calibre does.

    Example:
        >>> os.path.isabs(_normalize_path_for_containment("x"))
        True


    :param path: The path.
    :param case_sensitive: Whether case matters.
    :return: The normalised path.
    """
    ans = os.path.abspath(path)
    return ans if case_sensitive else os.path.normcase(ans).lower()


def is_path_inside(
    parent: str, child: str, allow_parent: bool = False, case_sensitive: bool = True
) -> bool:
    """
    Say whether a path is under another, by whole components, as calibre does.

    Example:
        >>> is_path_inside("/a/b", "/a/b/c"), is_path_inside("/a/b", "/a/bc")
        (True, False)


    :param parent: The containing folder.
    :param child: The path to test.
    :param allow_parent: Whether the folder itself counts.
    :param case_sensitive: Whether case matters.
    :return: Whether ``child`` is inside ``parent``.
    """
    parent = _normalize_path_for_containment(parent, case_sensitive=case_sensitive)
    child = _normalize_path_for_containment(child, case_sensitive=case_sensitive)
    try:
        if os.path.commonpath((parent, child)) != parent:
            return False
    except ValueError:
        return False
    return allow_parent or child != parent


def is_existing_subpath(child: str, parent: str) -> bool:
    """
    Say whether an existing path is inside an existing folder, links resolved.

    Example:
        >>> is_existing_subpath(__file__, os.path.dirname(__file__))
        True


    :param child: The path to test.
    :param parent: The folder.
    :return: Whether both exist and ``child`` is inside ``parent``.
    """
    try:
        parent = os.path.realpath(parent, strict=True)
        child = os.path.realpath(child, strict=True)
    except OSError:
        return False
    return is_path_inside(parent, child)
