#!/usr/bin/env python2
# vim:fileencoding=utf-8
"""
Resolve embedded and declared DOCX fonts into conversion-facing font metadata.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise fonts through a consuming regression::

        python -m pytest -q tests/file_formats/docx/test_docx_modernized.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

import os
import re
from collections import namedtuple

from LiuXin_alpha.file_formats.docx.block_styles import binary_property, inherit

from LiuXin_alpha.utils.storage.local.filenames import ascii_filename
try:
    from LiuXin_alpha.utils.fonts.scanner import font_scanner, NoFonts
except Exception:
    class NoFonts(Exception):
        """
        Font scanner backend unavailable.

        Example:
            Exercise NoFonts through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py
        """

    class _MissingFontScanner:
        """
        Provide the missingfontscanner contract for validated ebook processing.

        Example:
            Exercise  MissingFontScanner through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py
        """
        def fonts_for_family(self: _typing.Self, name: _typing.Any) -> None:
            """
            Perform the fonts for family operation under explicit file-format and conversion rules.

            Example:
                Exercise  MissingFontScanner.fonts for family through a consuming regression::

                    python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


            :param name: Field, file, function or resource name addressed by the operation.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            raise NoFonts("font scanner backend unavailable")

    font_scanner = _MissingFontScanner()

try:
    from LiuXin_alpha.utils.fonts.utils import panose_to_css_generic_family, is_truetype_font
except Exception:
    def panose_to_css_generic_family(_panose: _typing.Any) -> None:
        """
        Perform the panose to css generic family operation under explicit file-format and conversion rules.

        Example:
            Exercise panose to css generic family through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param _panose: Value supplied for panose under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return None

    def is_truetype_font(raw: _typing.Any) -> _typing.Any:
        """
        Return whether is truetype font holds for the supplied ebook data.

        Example:
            Exercise is truetype font through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param raw: Value supplied for raw under the utility contract.
        :return: True when the documented condition holds; otherwise False.
        """
        if not raw:
            return False
        return raw.startswith((b"\x00\x01\x00\x00", b"OTTO", b"true", b"ttcf"))

# Py2/Py3
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems
from LiuXin_alpha.utils.libraries.liuxin_six import memory_range

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"

Embed = namedtuple("Embed", "name key subsetted")


def has_system_fonts(name: _typing.Any) -> _typing.Any:
    """
    Return whether has system fonts holds for the supplied ebook data.

    Example:
        Exercise has system fonts through a consuming regression::

            python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


    :param name: Field, file, function or resource name addressed by the operation.
    :return: True when the documented condition holds; otherwise False.
    """
    try:
        return bool(font_scanner.fonts_for_family(name))
    except NoFonts:
        return False


def get_variant(bold: bool = False, italic: bool = False) -> _typing.Any:
    """
    Return variant under the format's safety and compatibility rules.

    Example:
        Exercise get variant through a consuming regression::

            python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


    :param bold: Value supplied for bold under the utility contract.
    :param italic: Value supplied for italic under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return {
        (False, False): "Regular",
        (False, True): "Italic",
        (True, False): "Bold",
        (True, True): "BoldItalic",
    }[(bold, italic)]


class Family(object):
    """
    Provide the family contract for validated ebook processing.

    Example:
        Exercise Family through a consuming regression::

            python -m pytest -q tests/file_formats/docx/test_docx_modernized.py
    """
    def __init__(self: _typing.Self, elem: _typing.Any, embed_relationships: _typing.Any, XPath: _typing.Any, get: _typing.Any) -> None:
        """
        Initialize and validate the family state.

        Example:
            Exercise Family.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param elem: Value supplied for elem under the utility contract.
        :param embed_relationships: Value supplied for embed relationships under the utility
            contract.
        :param XPath: Value supplied for XPath under the utility contract.
        :param get: Value supplied for get under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.name = self.family_name = get(elem, "w:name")
        self.alt_names = tuple(get(x, "w:val") for x in XPath("./w:altName")(elem))
        if self.alt_names and not has_system_fonts(self.name):
            for x in self.alt_names:
                if has_system_fonts(x):
                    self.family_name = x
                    break

        self.embedded = {}
        for x in ("Regular", "Bold", "Italic", "BoldItalic"):
            for y in XPath("./w:embed%s[@r:id]" % x)(elem):
                rid = get(y, "r:id")
                key = get(y, "w:fontKey")
                subsetted = get(y, "w:subsetted") in {"1", "true", "on"}
                if rid in embed_relationships:
                    self.embedded[x] = Embed(embed_relationships[rid], key, subsetted)

        self.generic_family = "auto"
        for x in XPath("./w:family[@w:val]")(elem):
            self.generic_family = get(x, "w:val", "auto")

        ntt = binary_property(elem, "notTrueType", XPath, get)
        self.is_ttf = ntt is inherit or not ntt

        self.panose1 = None
        self.panose_name = None
        for x in XPath("./w:panose1[@w:val]")(elem):
            try:
                v = get(x, "w:val")
                v = tuple(int(v[i : i + 2], 16) for i in memory_range(0, len(v), 2))
            except (TypeError, ValueError, IndexError):
                pass
            else:
                self.panose1 = v
                self.panose_name = panose_to_css_generic_family(v)

        self.css_generic_family = {
            "roman": "serif",
            "swiss": "sans-serif",
            "modern": "monospace",
            "decorative": "fantasy",
            "script": "cursive",
        }.get(self.generic_family, None)
        self.css_generic_family = self.css_generic_family or self.panose_name or "serif"


class Fonts(object):
    """
    Provide the fonts contract for validated ebook processing.

    Example:
        Exercise Fonts through a consuming regression::

            python -m pytest -q tests/file_formats/docx/test_docx_modernized.py
    """
    def __init__(self: _typing.Self, namespace: _typing.Any) -> None:
        """
        Initialize and validate the fonts state.

        Example:
            Exercise Fonts.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param namespace: Value supplied for namespace under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.namespace = namespace
        self.fonts = {}
        self.used = set()

    def __call__(self: _typing.Self, root: _typing.Any, embed_relationships: _typing.Any, docx: _typing.Any, dest_dir: _typing.Any) -> None:
        """
        Perform the call operation under explicit file-format and conversion rules.

        Example:
            Exercise Fonts.  call   through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param root: Root directory that bounds path resolution or traversal.
        :param embed_relationships: Value supplied for embed relationships under the utility
            contract.
        :param docx: Value supplied for docx under the utility contract.
        :param dest_dir: Value supplied for dest dir under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for elem in self.namespace.XPath("//w:font[@w:name]")(root):
            self.fonts[self.namespace.get(elem, "w:name")] = Family(
                elem, embed_relationships, self.namespace.XPath, self.namespace.get
            )

    def family_for(self: _typing.Self, name: _typing.Any, bold: bool = False, italic: bool = False) -> _typing.Any:
        """
        Perform the family for operation under explicit file-format and conversion rules.

        Example:
            Exercise Fonts.family for through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param bold: Value supplied for bold under the utility contract.
        :param italic: Value supplied for italic under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        f = self.fonts.get(name, None)
        if f is None:
            return "serif"
        variant = get_variant(bold, italic)
        self.used.add((name, variant))
        name = f.name if variant in f.embedded else f.family_name
        return '"%s", %s' % (name.replace('"', ""), f.css_generic_family)

    def embed_fonts(self: _typing.Self, dest_dir: _typing.Any, docx: _typing.Any) -> _typing.Any:
        """
        Perform the embed fonts operation under explicit file-format and conversion rules.

        Example:
            Exercise Fonts.embed fonts through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param dest_dir: Value supplied for dest dir under the utility contract.
        :param docx: Value supplied for docx under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        defs = []
        dest_dir = os.path.join(dest_dir, "fonts")
        for name, variant in self.used:
            f = self.fonts[name]
            if variant in f.embedded:
                if not os.path.exists(dest_dir):
                    os.mkdir(dest_dir)
                fname = self.write(name, dest_dir, docx, variant)
                if fname is not None:
                    d = {
                        "font-family": '"%s"' % name.replace('"', ""),
                        "src": 'url("fonts/%s")' % fname,
                    }
                    if "Bold" in variant:
                        d["font-weight"] = "bold"
                    if "Italic" in variant:
                        d["font-style"] = "italic"
                    d = ["%s: %s" % (k, v) for k, v in iteritems(d)]
                    d = ";\n\t".join(d)
                    defs.append("@font-face {\n\t%s\n}\n" % d)
        return "\n".join(defs)

    def write(self: _typing.Self, name: _typing.Any, dest_dir: _typing.Any, docx: _typing.Any, variant: _typing.Any) -> _typing.Any:
        """
        Perform the write operation under explicit file-format and conversion rules.

        Example:
            Exercise Fonts.write through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param dest_dir: Value supplied for dest dir under the utility contract.
        :param docx: Value supplied for docx under the utility contract.
        :param variant: Value supplied for variant under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        f = self.fonts[name]
        ef = f.embedded[variant]
        raw = docx.read(ef.name)
        prefix = raw[:32]
        if ef.key:
            key = re.sub(r"[^A-Fa-f0-9]", "", ef.key)
            key = bytearray(reversed(tuple(int(key[i : i + 2], 16) for i in memory_range(0, len(key), 2))))
            prefix = bytearray(prefix)
            prefix = bytes(bytearray(prefix[i] ^ key[i % len(key)] for i in memory_range(len(prefix))))
        if not is_truetype_font(prefix):
            return None
        ext = "otf" if prefix.startswith(b"OTTO") else "ttf"
        fname = ascii_filename("%s - %s.%s" % (name, variant, ext))
        with open(os.path.join(dest_dir, fname), "wb") as dest:
            dest.write(prefix)
            dest.write(raw[32:])

        return fname
