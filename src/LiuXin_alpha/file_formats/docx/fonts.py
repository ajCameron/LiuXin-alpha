#!/usr/bin/env python2
# vim:fileencoding=utf-8
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
        """Font scanner backend unavailable."""

    class _MissingFontScanner:
        def fonts_for_family(self: _typing.Self, name: _typing.Any) -> None:
            raise NoFonts("font scanner backend unavailable")

    font_scanner = _MissingFontScanner()

try:
    from LiuXin_alpha.utils.fonts.utils import panose_to_css_generic_family, is_truetype_font
except Exception:
    def panose_to_css_generic_family(_panose: _typing.Any) -> None:
        return None

    def is_truetype_font(raw: _typing.Any) -> _typing.Any:
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
    try:
        return bool(font_scanner.fonts_for_family(name))
    except NoFonts:
        return False


def get_variant(bold: bool = False, italic: bool = False) -> _typing.Any:
    return {
        (False, False): "Regular",
        (False, True): "Italic",
        (True, False): "Bold",
        (True, True): "BoldItalic",
    }[(bold, italic)]


class Family(object):
    def __init__(self: _typing.Self, elem: _typing.Any, embed_relationships: _typing.Any, XPath: _typing.Any, get: _typing.Any) -> None:
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
    def __init__(self: _typing.Self, namespace: _typing.Any) -> None:
        self.namespace = namespace
        self.fonts = {}
        self.used = set()

    def __call__(self: _typing.Self, root: _typing.Any, embed_relationships: _typing.Any, docx: _typing.Any, dest_dir: _typing.Any) -> None:
        for elem in self.namespace.XPath("//w:font[@w:name]")(root):
            self.fonts[self.namespace.get(elem, "w:name")] = Family(
                elem, embed_relationships, self.namespace.XPath, self.namespace.get
            )

    def family_for(self: _typing.Self, name: _typing.Any, bold: bool = False, italic: bool = False) -> _typing.Any:
        f = self.fonts.get(name, None)
        if f is None:
            return "serif"
        variant = get_variant(bold, italic)
        self.used.add((name, variant))
        name = f.name if variant in f.embedded else f.family_name
        return '"%s", %s' % (name.replace('"', ""), f.css_generic_family)

    def embed_fonts(self: _typing.Self, dest_dir: _typing.Any, docx: _typing.Any) -> _typing.Any:
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
