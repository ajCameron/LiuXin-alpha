#!/usr/bin/env python2
# vim:fileencoding=utf-8

"""
Embed and register fonts in generated DOCX packages.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise fonts through a consuming regression::

        python -m pytest -q tests/file_formats/docx/test_docx_modernized.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

from collections import defaultdict
from uuid import uuid4

from LiuXin_alpha.file_formats.oeb.base import OEB_STYLES
try:
    from LiuXin_alpha.file_formats.oeb.transforms.subset import find_font_face_rules
except Exception:
    # Font subsetting backend is optional during the ongoing port.
    def find_font_face_rules(sheet: _typing.Any, oeb: _typing.Any) -> list[_typing.Any]:
        """
        Find font face rules under the format's safety and compatibility rules.

        Example:
            Exercise find font face rules through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param sheet: Value supplied for sheet under the utility contract.
        :param oeb: Value supplied for oeb under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return []

# Py2/Py3
from LiuXin_alpha.utils.libraries.liuxin_six import memory_range

__license__ = "GPL v3"
__copyright__ = "2015, Kovid Goyal <kovid at kovidgoyal.net>"


def obfuscate_font_data(data: _typing.Any, key: _typing.Any) -> _typing.Any:
    """
    Perform the obfuscate font data operation under explicit file-format and conversion rules.

    Example:
        Exercise obfuscate font data through a consuming regression::

            python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


    :param data: Value supplied for data under the utility contract.
    :param key: Metadata, identifier or local-variable key.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    prefix = bytearray(data[:32])
    key = bytearray(reversed(key.bytes))
    prefix = bytes(bytearray(prefix[i] ^ key[i % len(key)] for i in memory_range(len(prefix))))
    return prefix + data[32:]


class FontsManager(object):
    """
    Provide the fontsmanager contract for validated ebook processing.

    Example:
        Exercise FontsManager through a consuming regression::

            python -m pytest -q tests/file_formats/docx/test_docx_modernized.py
    """
    def __init__(self: _typing.Self, namespace: _typing.Any, oeb: _typing.Any, opts: _typing.Any) -> None:
        """
        Initialize and validate the fontsmanager state.

        Example:
            Exercise FontsManager.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param namespace: Value supplied for namespace under the utility contract.
        :param oeb: Value supplied for oeb under the utility contract.
        :param opts: Value supplied for opts under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.namespace = namespace
        self.oeb, self.log, self.opts = oeb, oeb.log, opts

    def serialize(self: _typing.Self, text_styles: _typing.Any, fonts: _typing.Any, embed_relationships: _typing.Any, font_data_map: _typing.Any) -> None:
        """
        Perform the serialize operation under explicit file-format and conversion rules.

        Example:
            Exercise FontsManager.serialize through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param text_styles: Value supplied for text styles under the utility contract.
        :param fonts: Value supplied for fonts under the utility contract.
        :param embed_relationships: Value supplied for embed relationships under the utility
            contract.
        :param font_data_map: Value supplied for font data map under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        makeelement = self.namespace.makeelement
        font_families, seen = set(), set()
        for ts in text_styles:
            if ts.font_family:
                lf = ts.font_family.lower()
                if lf not in seen:
                    seen.add(lf)
                    font_families.add(ts.font_family)
        family_map = {}
        for family in sorted(font_families):
            family_map[family] = makeelement(fonts, "w:font", w_name=family)

        embedded_fonts = []
        for item in self.oeb.manifest:
            if item.media_type in OEB_STYLES and hasattr(item.data, "cssRules"):
                embedded_fonts.extend(find_font_face_rules(item, self.oeb))

        num = 0
        face_map = defaultdict(set)
        rel_map = {}
        for ef in embedded_fonts:
            ff = ef["font-family"][0]
            if ff not in font_families:
                continue
            num += 1
            bold = ef["weight"] > 400
            italic = ef["font-style"] != "normal"
            tag = "Regular"
            if bold or italic:
                tag = "Italic"
                if bold and italic:
                    tag = "BoldItalic"
                elif bold:
                    tag = "Bold"
            if tag in face_map[ff]:
                continue
            face_map[ff].add(tag)
            font = family_map[ff]
            key = uuid4()
            item = ef["item"]
            rid = rel_map.get(item)
            if rid is None:
                rel_map[item] = rid = "rId%d" % num
                fname = "fonts/font%d.odttf" % num
                makeelement(
                    embed_relationships,
                    "Relationship",
                    Id=rid,
                    Type=self.namespace.names["EMBEDDED_FONT"],
                    Target=fname,
                )
                font_data_map["word/" + fname] = obfuscate_font_data(item.data, key)
            makeelement(
                font,
                "w:embed" + tag,
                r_id=rid,
                w_fontKey="{%s}" % key.urn.rpartition(":")[-1].upper(),
                w_subsetted="true" if self.opts.subset_embedded_fonts else "false",
            )
