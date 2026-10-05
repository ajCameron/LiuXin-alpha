#!/usr/bin/env python
# vim:fileencoding=utf-8

"""
Inspect font families, declarations and embedded font resources.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise fonts through a consuming regression::

        python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

import re

from LiuXin_alpha.file_formats.oeb.polish.container import OEB_STYLES, OEB_DOCS
try:
    from LiuXin_alpha.file_formats.oeb.normalize_css import normalize_font
except Exception:
    def normalize_font(*args: _typing.Any, **kwargs: _typing.Any) -> dict[_typing.Any, _typing.Any]:
        """
        Normalize font under the format's safety and compatibility rules.

        Example:
            Exercise normalize font through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return {}

# Py2/Py3 compatiblity layer
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems

__license__ = "GPL v3"
__copyright__ = "2014, Kovid Goyal <kovid at kovidgoyal.net>"


def unquote(x: _typing.Any) -> _typing.Any:
    """
    Perform the unquote operation under explicit file-format and conversion rules.

    Example:
        Exercise unquote through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param x: Value supplied for x under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if x and len(x) > 1 and x[0] == x[-1] and x[0] in ('"', "'"):
        x = x[1:-1]
    return x


def font_family_data_from_declaration(style: _typing.Any, families: _typing.Any) -> None:
    """
    Perform the font family data from declaration operation under explicit file-format and conversion rules.

    Example:
        Exercise font family data from declaration through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param style: Value supplied for style under the utility contract.
    :param families: Value supplied for families under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    font_families = []
    f = style.getProperty("font")
    if f is not None:
        f = normalize_font(f.propertyValue, font_family_as_list=True).get("font-family", None)
        if f is not None:
            font_families = [unquote(x) for x in f]
    f = style.getProperty("font-family")
    if f is not None:
        font_families = [x.value for x in f.propertyValue]

    for f in font_families:
        families[f] = families.get(f, False)


def font_family_data_from_sheet(sheet: _typing.Any, families: _typing.Any) -> None:
    """
    Perform the font family data from sheet operation under explicit file-format and conversion rules.

    Example:
        Exercise font family data from sheet through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param sheet: Value supplied for sheet under the utility contract.
    :param families: Value supplied for families under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    for rule in sheet.cssRules:
        if rule.type == rule.STYLE_RULE:
            font_family_data_from_declaration(rule.style, families)
        elif rule.type == rule.FONT_FACE_RULE:
            ff = rule.style.getProperty("font-family")
            if ff is not None:
                for f in ff.propertyValue:
                    families[f.value] = True


def font_family_data(container: _typing.Any) -> _typing.Any:
    """
    Perform the font family data operation under explicit file-format and conversion rules.

    Example:
        Exercise font family data through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param container: Value supplied for container under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    families = {}
    for name, mt in iteritems(container.mime_map):
        if mt in OEB_STYLES:
            sheet = container.parsed(name)
            font_family_data_from_sheet(sheet, families)
        elif mt in OEB_DOCS:
            root = container.parsed(name)
            for style in root.xpath('//*[local-name() = "style"]'):
                if style.text and style.get("type", "text/css").lower() == "text/css":
                    sheet = container.parse_css(style.text)
                    font_family_data_from_sheet(sheet, families)
            for style in root.xpath("//*/@style"):
                if style:
                    style = container.parse_css(style, is_declaration=True)
                    font_family_data_from_declaration(style, families)
    return families


def change_font_family_value(cssvalue: _typing.Any, new_name: _typing.Any) -> None:
    # If cssvalue.type == 'IDENT' cssutils will not serialize the font
    # name properly (it will not enclose it in quotes). So we
    # use the following hack (setting an internal property of the
    # Value class)
    """
    Perform the change font family value operation under explicit file-format and conversion rules.

    Example:
        Exercise change font family value through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param cssvalue: Value supplied for cssvalue under the utility contract.
    :param new_name: Value supplied for new name under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    cssvalue.value = new_name
    cssvalue._type = "STRING"


def change_font_family_in_property(style: _typing.Any, prop: _typing.Any, old_name: _typing.Any, new_name: _typing.Any = None) -> _typing.Any:
    """
    Perform the change font family in property operation under explicit file-format and conversion rules.

    Example:
        Exercise change font family in property through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param style: Value supplied for style under the utility contract.
    :param prop: Value supplied for prop under the utility contract.
    :param old_name: Value supplied for old name under the utility contract.
    :param new_name: Value supplied for new name under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    changed = False
    families = {x.value for x in prop.propertyValue}
    _dummy_family = "d7d81cf1-1c8c-4993-b788-e1ab596c0f1f"
    if new_name and new_name in families:
        new_name = None  # new name already exists in this property, so simply remove old_name
    for val in prop.propertyValue:
        if val.value == old_name:
            change_font_family_value(val, new_name or _dummy_family)
            changed = True
    if changed and not new_name:
        # Remove dummy family, cssutils provides no clean way to do this, so we
        # roundtrip via cssText
        pat = re.compile(r"""['"]{0,1}%s['"]{0,1}\s*,{0,1}""" % _dummy_family)
        repl = pat.sub("", prop.propertyValue.cssText).strip().rstrip(",").strip()
        if repl:
            prop.propertyValue.cssText = repl
            if prop.name == "font" and not prop.validate():
                style.removeProperty(prop.name)  # no families left in font:
        else:
            style.removeProperty(prop.name)
    return changed


def change_font_in_declaration(style: _typing.Any, old_name: _typing.Any, new_name: _typing.Any = None) -> _typing.Any:
    """
    Perform the change font in declaration operation under explicit file-format and conversion rules.

    Example:
        Exercise change font in declaration through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param style: Value supplied for style under the utility contract.
    :param old_name: Value supplied for old name under the utility contract.
    :param new_name: Value supplied for new name under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    changed = False
    for x in ("font", "font-family"):
        prop = style.getProperty(x)
        if prop is not None:
            changed |= change_font_family_in_property(style, prop, old_name, new_name)
    return changed


def remove_embedded_font(container: _typing.Any, sheet: _typing.Any, rule: _typing.Any, sheet_name: _typing.Any) -> None:
    """
    Perform the remove embedded font operation under explicit file-format and conversion rules.

    Example:
        Exercise remove embedded font through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param container: Value supplied for container under the utility contract.
    :param sheet: Value supplied for sheet under the utility contract.
    :param rule: Value supplied for rule under the utility contract.
    :param sheet_name: Value supplied for sheet name under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    src = getattr(rule.style.getProperty("src"), "value")
    if src is not None:
        if src.startswith("url("):
            src = src[4:-1]
    sheet.cssRules.remove(rule)
    if src:
        src = unquote(src)
        name = container.href_to_name(src, sheet_name)
        if container.has_name(name):
            container.remove_item(name)


def change_font_in_sheet(container: _typing.Any, sheet: _typing.Any, old_name: _typing.Any, new_name: _typing.Any, sheet_name: _typing.Any) -> _typing.Any:
    """
    Perform the change font in sheet operation under explicit file-format and conversion rules.

    Example:
        Exercise change font in sheet through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param container: Value supplied for container under the utility contract.
    :param sheet: Value supplied for sheet under the utility contract.
    :param old_name: Value supplied for old name under the utility contract.
    :param new_name: Value supplied for new name under the utility contract.
    :param sheet_name: Value supplied for sheet name under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    changed = False
    removals = []
    for rule in sheet.cssRules:
        if rule.type == rule.STYLE_RULE:
            changed |= change_font_in_declaration(rule.style, old_name, new_name)
        elif rule.type == rule.FONT_FACE_RULE:
            ff = rule.style.getProperty("font-family")
            if ff is not None:
                families = {x.value for x in ff.propertyValue}
                if old_name in families:
                    changed = True
                    removals.append(rule)
    for rule in reversed(removals):
        remove_embedded_font(container, sheet, rule, sheet_name)
    return changed


def change_font(container: _typing.Any, old_name: _typing.Any, new_name: _typing.Any = None) -> _typing.Any:
    """
    Change a font family from old_name to new_name. Changes all occurrences of the font family in stylesheets, style tags and style attributes. If the old_name refers to an embedded font, it is removed. You can set new_name to None to remove the font family instead of changing it.

    Example:
        Exercise change font through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param container: Value supplied for container under the utility contract.
    :param old_name: Value supplied for old name under the utility contract.
    :param new_name: Value supplied for new name under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    changed = False
    for name, mt in tuple(iteritems(container.mime_map)):
        if mt in OEB_STYLES:
            sheet = container.parsed(name)
            if change_font_in_sheet(container, sheet, old_name, new_name, name):
                container.dirty(name)
                changed = True
        elif mt in OEB_DOCS:
            root = container.parsed(name)
            for style in root.xpath('//*[local-name() = "style"]'):
                if style.text and style.get("type", "text/css").lower() == "text/css":
                    sheet = container.parse_css(style.text)
                    if change_font_in_sheet(container, sheet, old_name, new_name, name):
                        container.dirty(name)
                        changed = True
            for elem in root.xpath("//*[@style]"):
                style = elem.get("style", "")
                if style:
                    style = container.parse_css(style, is_declaration=True)
                    if change_font_in_declaration(style, old_name, new_name):
                        style = style.cssText.strip().rstrip(";").strip()
                        if style:
                            elem.set("style", style)
                        else:
                            del elem.attrib["style"]
                        container.dirty(name)
                        changed = True
    return changed
