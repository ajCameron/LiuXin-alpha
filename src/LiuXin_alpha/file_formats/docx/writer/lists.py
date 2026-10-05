#!/usr/bin/env python2
# vim:fileencoding=utf-8

"""
Translate normalized HTML lists into DOCX numbering definitions and paragraphs.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise lists through a consuming regression::

        python -m pytest -q tests/file_formats/docx/test_docx_modernized.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

from collections import defaultdict
from operator import attrgetter

# Py2/Py3
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems
from LiuXin_alpha.utils.libraries.liuxin_six import dict_itervalues as itervalues

__license__ = "GPL v3"
__copyright__ = "2015, Kovid Goyal <kovid at kovidgoyal.net>"


LIST_STYLES = frozenset(
    "disc circle square decimal decimal-leading-zero lower-roman upper-roman"
    " lower-greek lower-alpha lower-latin upper-alpha upper-latin hiragana hebrew"
    " katakana-iroha cjk-ideographic".split()
)

STYLE_MAP = {
    "disc": "bullet",
    "circle": "o",
    "square": "\uf0a7",
    "decimal": "decimal",
    "decimal-leading-zero": "decimalZero",
    "lower-roman": "lowerRoman",
    "upper-roman": "upperRoman",
    "lower-alpha": "lowerLetter",
    "lower-latin": "lowerLetter",
    "upper-alpha": "upperLetter",
    "upper-latin": "upperLetter",
    "hiragana": "aiueo",
    "hebrew": "hebrew1",
    "katakana-iroha": "iroha",
    "cjk-ideographic": "chineseCounting",
}


def find_list_containers(list_tag: _typing.Any, tag_style: _typing.Any) -> _typing.Any:
    """
    Find list containers under the format's safety and compatibility rules.

    Example:
        Exercise find list containers through a consuming regression::

            python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


    :param list_tag: Value supplied for list tag under the utility contract.
    :param tag_style: Value supplied for tag style under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    node = list_tag
    stylizer = tag_style._stylizer
    ans = []
    while True:
        parent = node.getparent()
        if parent is None or parent is node:
            break
        node = parent
        style = stylizer.style(node)
        lst = (style._style.get("list-style-type", None) or "").lower()
        if lst in LIST_STYLES:
            ans.append(node)
    return ans


class NumberingDefinition(object):
    """
    Provide the numberingdefinition contract for validated ebook processing.

    Example:
        Exercise NumberingDefinition through a consuming regression::

            python -m pytest -q tests/file_formats/docx/test_docx_modernized.py
    """
    def __init__(self: _typing.Self, top_most: _typing.Any, stylizer: _typing.Any, namespace: _typing.Any) -> None:
        """
        Initialize and validate the numberingdefinition state.

        Example:
            Exercise NumberingDefinition.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param top_most: Value supplied for top most under the utility contract.
        :param stylizer: Value supplied for stylizer under the utility contract.
        :param namespace: Value supplied for namespace under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.namespace = namespace
        self.top_most = top_most
        self.stylizer = stylizer
        self.level_map = defaultdict(list)
        self.num_id = None

    def finalize(self: _typing.Self) -> None:
        """
        Perform the finalize operation under explicit file-format and conversion rules.

        Example:
            Exercise NumberingDefinition.finalize through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        items_for_level = defaultdict(list)
        container_for_level = {}
        type_for_level = {}
        for ilvl, items in iteritems(self.level_map):
            for container, list_tag, block, list_type, tag_style in items:
                items_for_level[ilvl].append(list_tag)
                container_for_level[ilvl] = container
                type_for_level[ilvl] = list_type
        self.levels = tuple(
            Level(
                type_for_level[ilvl],
                container_for_level[ilvl],
                items_for_level[ilvl],
                ilvl=ilvl,
            )
            for ilvl in sorted(self.level_map)
        )

    def __hash__(self: _typing.Self) -> _typing.Any:
        """
        Perform the hash operation under explicit file-format and conversion rules.

        Example:
            Exercise NumberingDefinition.  hash   through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return hash(self.levels)

    def link_blocks(self: _typing.Self) -> None:
        """
        Perform the link blocks operation under explicit file-format and conversion rules.

        Example:
            Exercise NumberingDefinition.link blocks through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for ilvl, items in iteritems(self.level_map):
            for container, list_tag, block, list_type, tag_style in items:
                block.numbering_id = (self.num_id + 1, ilvl)

    def serialize(self: _typing.Self, parent: _typing.Any) -> None:
        """
        Perform the serialize operation under explicit file-format and conversion rules.

        Example:
            Exercise NumberingDefinition.serialize through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        makeelement = self.namespace.makeelement
        an = makeelement(parent, "w:abstractNum", w_abstractNumId=str(self.num_id))
        makeelement(an, "w:multiLevelType", w_val="hybridMultilevel")
        makeelement(an, "w:name", w_val="List %d" % (self.num_id + 1))
        for level in self.levels:
            level.serialize(an, makeelement)


class Level(object):
    """
    Provide the level contract for validated ebook processing.

    Example:
        Exercise Level through a consuming regression::

            python -m pytest -q tests/file_formats/docx/test_docx_modernized.py
    """
    def __init__(self: _typing.Self, list_type: _typing.Any, container: _typing.Any, items: _typing.Any, ilvl: int = 0) -> None:
        """
        Initialize and validate the level state.

        Example:
            Exercise Level.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param list_type: Value supplied for list type under the utility contract.
        :param container: Value supplied for container under the utility contract.
        :param items: Value supplied for items under the utility contract.
        :param ilvl: Value supplied for ilvl under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.ilvl = ilvl
        try:
            self.start = int(container.get("start"))
        except Exception:
            self.start = 1
        if items:
            try:
                self.start = int(items[0].get("value"))
            except Exception:
                pass
        if list_type in {"disc", "circle", "square"}:
            self.num_fmt = "bullet"
            self.lvl_text = "\uf0b7" if list_type == "disc" else STYLE_MAP[list_type]
        else:
            self.lvl_text = "%{}.".format(self.ilvl + 1)
            self.num_fmt = STYLE_MAP.get(list_type, "decimal")

    def __hash__(self: _typing.Self) -> _typing.Any:
        """
        Perform the hash operation under explicit file-format and conversion rules.

        Example:
            Exercise Level.  hash   through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return hash((self.start, self.num_fmt, self.lvl_text))

    def serialize(self: _typing.Self, parent: _typing.Any, makeelement: _typing.Any) -> None:
        """
        Perform the serialize operation under explicit file-format and conversion rules.

        Example:
            Exercise Level.serialize through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param parent: Value supplied for parent under the utility contract.
        :param makeelement: Value supplied for makeelement under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        lvl = makeelement(parent, "w:lvl", w_ilvl=str(self.ilvl))
        makeelement(lvl, "w:start", w_val=str(self.start))
        makeelement(lvl, "w:numFmt", w_val=self.num_fmt)
        makeelement(lvl, "w:lvlText", w_val=self.lvl_text)
        makeelement(lvl, "w:lvlJc", w_val="left")
        makeelement(
            makeelement(lvl, "w:pPr"),
            "w:ind",
            w_hanging="360",
            w_left=str(1152 + self.ilvl * 360),
        )
        if self.num_fmt == "bullet":
            ff = {"\uf0b7": "Symbol", "\uf0a7": "Wingdings"}.get(self.lvl_text, "Courier New")
            makeelement(
                makeelement(lvl, "w:rPr"),
                "w:rFonts",
                w_ascii=ff,
                w_hAnsi=ff,
                w_hint="default",
            )


class ListsManager(object):
    """
    Provide the listsmanager contract for validated ebook processing.

    Example:
        Exercise ListsManager through a consuming regression::

            python -m pytest -q tests/file_formats/docx/test_docx_modernized.py
    """
    def __init__(self: _typing.Self, docx: _typing.Any) -> None:
        """
        Initialize and validate the listsmanager state.

        Example:
            Exercise ListsManager.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param docx: Value supplied for docx under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.namespace = docx.namespace
        self.lists = {}

    def finalize(self: _typing.Self, all_blocks: _typing.Any) -> None:
        """
        Perform the finalize operation under explicit file-format and conversion rules.

        Example:
            Exercise ListsManager.finalize through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param all_blocks: Value supplied for all blocks under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        lists = {}
        for block in all_blocks:
            if block.list_tag is not None:
                list_tag, tag_style = block.list_tag
                list_type = (tag_style["list-style-type"] or "").lower()
                if list_type not in LIST_STYLES:
                    continue
                container_tags = find_list_containers(list_tag, tag_style)
                if not container_tags:
                    continue
                top_most = container_tags[-1]
                if top_most not in lists:
                    lists[top_most] = NumberingDefinition(top_most, tag_style._stylizer, self.namespace)
                l = lists[top_most]
                ilvl = len(container_tags) - 1
                l.level_map[ilvl].append((container_tags[0], list_tag, block, list_type, tag_style))

        [nd.finalize() for nd in itervalues(lists)]
        definitions = {}
        for defn in itervalues(lists):
            try:
                defn = definitions[defn]
            except KeyError:
                definitions[defn] = defn
                defn.num_id = len(definitions) - 1
            defn.link_blocks()
        self.definitions = sorted(itervalues(definitions), key=attrgetter("num_id"))

    def serialize(self: _typing.Self, parent: _typing.Any) -> None:
        """
        Perform the serialize operation under explicit file-format and conversion rules.

        Example:
            Exercise ListsManager.serialize through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for defn in self.definitions:
            defn.serialize(parent)
        makeelement = self.namespace.makeelement
        for defn in self.definitions:
            n = makeelement(parent, "w:num", w_numId=str(defn.num_id + 1))
            makeelement(n, "w:abstractNumId", w_val=str(defn.num_id))
