#!/usr/bin/env python
# vim:fileencoding=utf-8

"""
Inspect and update OPF metadata, manifests, spines and guide entries.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise opf through a consuming regression::

        python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

from lxml import etree

from LiuXin_alpha.file_formats.oeb.polish.container import OPF_NAMESPACES

from LiuXin_alpha.utils.localization import canonicalize_lang

__license__ = "GPL v3"
__copyright__ = "2014, Kovid Goyal <kovid at kovidgoyal.net>"


def get_book_language(container: _typing.Any) -> _typing.Any:
    """
    Return book language under the format's safety and compatibility rules.

    Example:
        Exercise get book language through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param container: Value supplied for container under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    for lang in container.opf_xpath("//dc:language"):
        raw = getattr(lang, "text", None)
        if not raw:
            continue
        try:
            primary = str(raw).split(",")[0].strip()
        except Exception:
            continue
        if not primary:
            continue
        try:
            code = canonicalize_lang(primary)
        except Exception:
            continue
        if code:
            return code


def set_guide_item(container: _typing.Any, item_type: _typing.Any, title: _typing.Any, name: _typing.Any, frag: _typing.Any = None) -> None:
    """
    Set guide item under the format's safety and compatibility rules.

    Example:
        Exercise set guide item through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param container: Value supplied for container under the utility contract.
    :param item_type: Value supplied for item type under the utility contract.
    :param title: Value supplied for title under the utility contract.
    :param name: Field, file, function or resource name addressed by the operation.
    :param frag: Value supplied for frag under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    ref_tag = "{%s}reference" % OPF_NAMESPACES["opf"]
    item_type = "" if item_type is None else str(item_type)
    href = None
    if name:
        try:
            href = container.name_to_href(name, container.opf_name)
        except ValueError:
            href = None
        if href and frag:
            href += "#" + frag

    guides = container.opf_xpath("//opf:guide")
    if not guides and href:
        g = container.opf.makeelement("{%s}guide" % OPF_NAMESPACES["opf"], nsmap={"opf": OPF_NAMESPACES["opf"]})
        container.insert_into_xml(container.opf, g)
        guides = [g]

    for guide in guides:
        matches = []
        for child in guide.iterchildren():
            if not isinstance(child.tag, str):
                continue
            if child.tag == ref_tag and child.get("type", "").lower() == item_type.lower():
                matches.append(child)
        if not matches and href:
            r = guide.makeelement(ref_tag, type=item_type, nsmap={"opf": OPF_NAMESPACES["opf"]})
            container.insert_into_xml(guide, r)
            matches.append(r)
        for m in matches:
            if href:
                if title is not None:
                    m.set("title", str(title))
                elif "title" in m.attrib:
                    del m.attrib["title"]
                m.set("href", href)
                m.set("type", item_type)
            else:
                container.remove_from_xml(m)
    container.dirty(container.opf_name)
