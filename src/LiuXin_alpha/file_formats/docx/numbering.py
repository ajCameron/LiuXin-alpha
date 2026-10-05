#!/usr/bin/env python2
# vim:fileencoding=utf-8
"""
Resolve DOCX numbering definitions into nested list structure and styles.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise numbering through a consuming regression::

        python -m pytest -q tests/file_formats/docx/test_docx_modernized.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

import re
import string
from collections import Counter, defaultdict
from functools import partial

from lxml.html.builder import OL, UL, SPAN

from LiuXin_alpha.file_formats.docx.block_styles import ParagraphStyle
from LiuXin_alpha.file_formats.docx.char_styles import RunStyle, inherit

from LiuXin_alpha.metadata.utils import roman

# Py2/Py3 compatability layer
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"


STYLE_MAP = {
    "aiueo": "hiragana",
    "aiueoFullWidth": "hiragana",
    "hebrew1": "hebrew",
    "iroha": "katakana-iroha",
    "irohaFullWidth": "katakana-iroha",
    "lowerLetter": "lower-alpha",
    "lowerRoman": "lower-roman",
    "none": "none",
    "upperLetter": "upper-alpha",
    "upperRoman": "upper-roman",
    "chineseCounting": "cjk-ideographic",
    "decimalZero": "decimal-leading-zero",
}


def alphabet(val: _typing.Any, lower: bool = True) -> _typing.Any:
    """
    Perform the alphabet operation under explicit file-format and conversion rules.

    Example:
        Exercise alphabet through a consuming regression::

            python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


    :param val: Template or metadata value evaluated by the operation.
    :param lower: Value supplied for lower under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    x = string.ascii_lowercase if lower else string.ascii_uppercase
    return x[(abs(val - 1)) % len(x)]


alphabet_map = {
    "lower-alpha": alphabet,
    "upper-alpha": partial(alphabet, lower=False),
    "lower-roman": lambda x: roman(x).lower(),
    "upper-roman": roman,
    "decimal-leading-zero": lambda x: "0%d" % x,
}


class Level(object):
    """
    Provide the level contract for validated ebook processing.

    Example:
        Exercise Level through a consuming regression::

            python -m pytest -q tests/file_formats/docx/test_docx_modernized.py
    """
    def __init__(self: _typing.Self, namespace: _typing.Any, lvl: _typing.Any = None) -> None:
        """
        Initialize and validate the level state.

        Example:
            Exercise Level.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param namespace: Value supplied for namespace under the utility contract.
        :param lvl: Value supplied for lvl under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.namespace = namespace
        self.restart = None
        self.start = 0
        self.fmt = "decimal"
        self.para_link = None
        self.paragraph_style = self.character_style = None
        self.is_numbered = False
        self.num_template = None
        self.bullet_template = None
        self.pic_id = None

        if lvl is not None:
            self.read_from_xml(lvl)

    def copy(self: _typing.Self) -> _typing.Any:
        """
        Perform the copy operation under explicit file-format and conversion rules.

        Example:
            Exercise Level.copy through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ans = Level(self.namespace)
        for x in (
            "restart",
            "pic_id",
            "start",
            "fmt",
            "para_link",
            "paragraph_style",
            "character_style",
            "is_numbered",
            "num_template",
            "bullet_template",
        ):
            setattr(ans, x, getattr(self, x))
        return ans

    def format_template(self: _typing.Self, counter: _typing.Any, ilvl: _typing.Any, template: _typing.Any) -> _typing.Any:
        """
        Perform the format template operation under explicit file-format and conversion rules.

        Example:
            Exercise Level.format template through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param counter: Value supplied for counter under the utility contract.
        :param ilvl: Value supplied for ilvl under the utility contract.
        :param template: Template expression parsed or evaluated.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        def sub(m: _typing.Any) -> _typing.Any:
            """
            Perform the sub operation under explicit file-format and conversion rules.

            Example:
                Exercise Level.format template.sub through a consuming regression::

                    python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


            :param m: Value supplied for m under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            x = int(m.group(1)) - 1
            if x > ilvl or x not in counter:
                return ""
            val = counter[x] - (0 if x == ilvl else 1)
            formatter = alphabet_map.get(self.fmt, lambda local_x: "%d" % local_x)
            return formatter(val)

        return re.sub(r"%(\d+)", sub, template).rstrip() + "\xa0"

    def read_from_xml(self: _typing.Self, lvl: _typing.Any, override: bool = False) -> None:
        """
        Read from xml under the format's safety and compatibility rules.

        Example:
            Exercise Level.read from xml through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param lvl: Value supplied for lvl under the utility contract.
        :param override: Value supplied for override under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        xpath, get = self.namespace.XPath, self.namespace.get
        for lr in xpath("./w:lvlRestart[@w:val]")(lvl):
            try:
                self.restart = int(get(lr, "w:val"))
            except (TypeError, ValueError):
                pass

        for lr in xpath("./w:start[@w:val]")(lvl):
            try:
                self.start = int(get(lr, "w:val"))
            except (TypeError, ValueError):
                pass

        for rPr in xpath("./w:rPr")(lvl):
            ps = RunStyle(self.namespace, rPr)
            if self.character_style is None:
                self.character_style = ps
            else:
                self.character_style.update(ps)

        lt = None
        for lr in xpath("./w:lvlText[@w:val]")(lvl):
            lt = get(lr, "w:val")

        for lr in xpath("./w:numFmt[@w:val]")(lvl):
            val = get(lr, "w:val")
            if val == "bullet":
                self.is_numbered = False
                cs = self.character_style
                if lt in {"\uf0a7", "o"} or (
                    cs is not None
                    and cs.font_family is not inherit
                    and cs.font_family.lower() in {"wingdings", "symbol"}
                ):
                    self.fmt = {"\uf0a7": "square", "o": "circle"}.get(lt, "disc")
                else:
                    self.bullet_template = lt
                for lpid in xpath("./w:lvlPicBulletId[@w:val]")(lvl):
                    self.pic_id = get(lpid, "w:val")
            else:
                self.is_numbered = True
                self.fmt = STYLE_MAP.get(val, "decimal")
                if lt and re.match(r"%\d+\.$", lt) is None:
                    self.num_template = lt

        for lr in xpath("./w:pStyle[@w:val]")(lvl):
            self.para_link = get(lr, "w:val")

        for pPr in xpath("./w:pPr")(lvl):
            ps = ParagraphStyle(self.namespace, pPr)
            if self.paragraph_style is None:
                self.paragraph_style = ps
            else:
                self.paragraph_style.update(ps)

    def css(self: _typing.Self, images: _typing.Any, pic_map: _typing.Any, rid_map: _typing.Any) -> _typing.Any:
        """
        Perform the css operation under explicit file-format and conversion rules.

        Example:
            Exercise Level.css through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param images: Value supplied for images under the utility contract.
        :param pic_map: Value supplied for pic map under the utility contract.
        :param rid_map: Value supplied for rid map under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ans = {"list-style-type": self.fmt}
        if self.pic_id:
            rid = pic_map.get(self.pic_id, None)
            if rid:
                try:
                    fname = images.generate_filename(rid, rid_map=rid_map, max_width=20, max_height=20)
                except Exception:
                    fname = None
                else:
                    ans["list-style-image"] = 'url("images/%s")' % fname
        return ans

    def char_css(self: _typing.Self) -> _typing.Any:
        """
        Perform the char css operation under explicit file-format and conversion rules.

        Example:
            Exercise Level.char css through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            css = self.character_style.css
        except AttributeError:
            css = {}
        css.pop("font-family", None)
        return css


class NumberingDefinition(object):
    """
    Provide the numberingdefinition contract for validated ebook processing.

    Example:
        Exercise NumberingDefinition through a consuming regression::

            python -m pytest -q tests/file_formats/docx/test_docx_modernized.py
    """
    def __init__(self: _typing.Self, namespace: _typing.Any, parent: _typing.Any = None, an_id: _typing.Any = None) -> None:
        """
        Initialize and validate the numberingdefinition state.

        Example:
            Exercise NumberingDefinition.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param namespace: Value supplied for namespace under the utility contract.
        :param parent: Value supplied for parent under the utility contract.
        :param an_id: Value supplied for an id under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.namespace = namespace
        xpath, get = self.namespace.XPath, self.namespace.get
        self.levels = {}
        self.abstract_numbering_definition_id = an_id
        if parent is not None:
            for lvl in xpath("./w:lvl")(parent):
                try:
                    ilvl = int(get(lvl, "w:ilvl", 0))
                except (TypeError, ValueError):
                    ilvl = 0
                self.levels[ilvl] = Level(namespace, lvl)

    def copy(self: _typing.Self) -> _typing.Any:
        """
        Perform the copy operation under explicit file-format and conversion rules.

        Example:
            Exercise NumberingDefinition.copy through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ans = NumberingDefinition(self.namespace, an_id=self.abstract_numbering_definition_id)
        for l, lvl in iteritems(self.levels):
            ans.levels[l] = lvl.copy()
        return ans


class Numbering(object):
    """
    Provide the numbering contract for validated ebook processing.

    Example:
        Exercise Numbering through a consuming regression::

            python -m pytest -q tests/file_formats/docx/test_docx_modernized.py
    """
    def __init__(self: _typing.Self, namespace: _typing.Any) -> None:
        """
        Initialize and validate the numbering state.

        Example:
            Exercise Numbering.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param namespace: Value supplied for namespace under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.namespace = namespace
        self.definitions = {}
        self.instances = {}
        self.counters = defaultdict(Counter)
        self.starts = {}
        self.pic_map = {}

    def __call__(self: _typing.Self, root: _typing.Any, styles: _typing.Any, rid_map: _typing.Any) -> None:
        """
        Read all numbering style definitions

        Example:
            Exercise Numbering.  call   through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param root: Root directory that bounds path resolution or traversal.
        :param styles: Value supplied for styles under the utility contract.
        :param rid_map: Value supplied for rid map under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        xpath, get = self.namespace.XPath, self.namespace.get
        self.rid_map = rid_map
        for npb in xpath("./w:numPicBullet[@w:numPicBulletId]")(root):
            npbid = get(npb, "w:numPicBulletId")
            for idata in xpath("descendant::v:imagedata[@r:id]")(npb):
                rid = get(idata, "r:id")
                self.pic_map[npbid] = rid
        lazy_load = {}
        for an in xpath("./w:abstractNum[@w:abstractNumId]")(root):
            an_id = get(an, "w:abstractNumId")
            nsl = xpath("./w:numStyleLink[@w:val]")(an)
            if nsl:
                lazy_load[an_id] = get(nsl[0], "w:val")
            else:
                nd = NumberingDefinition(self.namespace, an, an_id=an_id)
                self.definitions[an_id] = nd

        def create_instance(local_n: _typing.Any, definition: _typing.Any) -> _typing.Any:
            """
            Create instance under the format's safety and compatibility rules.

            Example:
                Exercise Numbering.  call  .create instance through a consuming regression::

                    python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


            :param local_n: Value supplied for local n under the utility contract.
            :param definition: Value supplied for definition under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            nd = definition.copy()
            start_overrides = {}
            for lo in xpath("./w:lvlOverride")(local_n):
                try:
                    ilvl = int(get(lo, "w:ilvl"))
                except (ValueError, TypeError):
                    ilvl = None
                for so in xpath("./w:startOverride[@w:val]")(lo):
                    try:
                        start_override = int(get(so, "w:val"))
                    except (TypeError, ValueError):
                        pass
                    else:
                        start_overrides[ilvl] = start_override
                for lvl in xpath("./w:lvl")(lo)[:1]:
                    nilvl = get(lvl, "w:ilvl")
                    ilvl = nilvl if ilvl is None else ilvl
                    alvl = nd.levels.get(ilvl, None)
                    if alvl is None:
                        alvl = Level(self.namespace)
                    alvl.read_from_xml(lvl, override=True)
            for ilvl, so in iteritems(start_overrides):
                try:
                    nd.levels[ilvl].start = start_override
                except KeyError:
                    pass
            return nd

        next_pass = {}
        for n in xpath("./w:num[@w:numId]")(root):
            an_id = None
            num_id = get(n, "w:numId")
            for an in xpath("./w:abstractNumId[@w:val]")(n):
                an_id = get(an, "w:val")
            d = self.definitions.get(an_id, None)
            if d is None:
                next_pass[num_id] = (an_id, n)
                continue
            self.instances[num_id] = create_instance(n, d)

        numbering_links = styles.numbering_style_links
        for an_id, style_link in iteritems(lazy_load):
            num_id = numbering_links[style_link]
            self.definitions[an_id] = self.instances[num_id].copy()

        for num_id, (an_id, n) in iteritems(next_pass):
            d = self.definitions.get(an_id, None)
            if d is not None:
                self.instances[num_id] = create_instance(n, d)

        for num_id, d in iteritems(self.instances):
            self.starts[num_id] = {lvl: d.levels[lvl].start for lvl in d.levels}

    def get_pstyle(self: _typing.Self, num_id: _typing.Any, style_id: _typing.Any) -> _typing.Any:
        """
        Return pstyle under the format's safety and compatibility rules.

        Example:
            Exercise Numbering.get pstyle through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param num_id: Value supplied for num id under the utility contract.
        :param style_id: Value supplied for style id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        d = self.instances.get(num_id, None)
        if d is not None:
            for ilvl, lvl in iteritems(d.levels):
                if lvl.para_link == style_id:
                    return ilvl

    def get_para_style(self: _typing.Self, num_id: _typing.Any, lvl: _typing.Any) -> _typing.Any:
        """
        Return para style under the format's safety and compatibility rules.

        Example:
            Exercise Numbering.get para style through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param num_id: Value supplied for num id under the utility contract.
        :param lvl: Value supplied for lvl under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        d = self.instances.get(num_id, None)
        if d is not None:
            lvl = d.levels.get(lvl, None)
            return getattr(lvl, "paragraph_style", None)

    def update_counter(self: _typing.Self, counter: _typing.Any, levelnum: _typing.Any, levels: _typing.Any) -> None:
        """
        Perform the update counter operation under explicit file-format and conversion rules.

        Example:
            Exercise Numbering.update counter through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param counter: Value supplied for counter under the utility contract.
        :param levelnum: Value supplied for levelnum under the utility contract.
        :param levels: Value supplied for levels under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        counter[levelnum] += 1
        for ilvl, lvl in iteritems(levels):
            restart = lvl.restart
            if (restart is None and ilvl == levelnum + 1) or restart == levelnum + 1:
                counter[ilvl] = lvl.start

    def apply_markup(self: _typing.Self, items: _typing.Any, body: _typing.Any, styles: _typing.Any, object_map: _typing.Any, images: _typing.Any) -> None:
        """
        Perform the apply markup operation under explicit file-format and conversion rules.

        Example:
            Exercise Numbering.apply markup through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param items: Value supplied for items under the utility contract.
        :param body: Value supplied for body under the utility contract.
        :param styles: Value supplied for styles under the utility contract.
        :param object_map: Value supplied for object map under the utility contract.
        :param images: Value supplied for images under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        seen_instances = set()
        for p, num_id, ilvl in items:
            d = self.instances.get(num_id, None)
            if d is not None:
                lvl = d.levels.get(ilvl, None)
                if lvl is not None:
                    an_id = d.abstract_numbering_definition_id
                    counter = self.counters[an_id]
                    if ilvl not in counter or num_id not in seen_instances:
                        counter[ilvl] = self.starts[num_id][ilvl]
                    seen_instances.add(num_id)
                    p.tag = "li"
                    p.set("value", "%s" % counter[ilvl])
                    p.set("list-lvl", str(ilvl))
                    p.set("list-id", num_id)
                    if lvl.num_template is not None:
                        val = lvl.format_template(counter, ilvl, lvl.num_template)
                        p.set("list-template", val)
                    elif lvl.bullet_template is not None:
                        val = lvl.format_template(counter, ilvl, lvl.bullet_template)
                        p.set("list-template", val)
                    self.update_counter(counter, ilvl, d.levels)

        templates = {}

        def commit(current_run: _typing.Any) -> None:
            """
            Perform the commit operation under explicit file-format and conversion rules.

            Example:
                Exercise Numbering.apply markup.commit through a consuming regression::

                    python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


            :param current_run: Value supplied for current run under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if not current_run:
                return
            start = current_run[0]
            local_parent = start.getparent()
            idx = local_parent.index(start)

            local_d = self.instances[start.get("list-id")]
            local_ilvl = int(start.get("list-lvl"))
            local_lvl = local_d.levels[local_ilvl]
            local_lvlid = start.get("list-id") + start.get("list-lvl")
            has_template = "list-template" in start.attrib
            local_wrap = (OL if local_lvl.is_numbered or has_template else UL)("\n\t")
            if has_template:
                local_wrap.set("lvlid", local_lvlid)
            else:
                local_wrap.set(
                    "class",
                    styles.register(local_lvl.css(images, self.pic_map, self.rid_map), "list"),
                )
            ccss = local_lvl.char_css()
            if ccss:
                ccss = styles.register(ccss, "bullet")
            local_parent.insert(idx, local_wrap)
            last_val = None
            for local_child in current_run:
                local_wrap.append(local_child)
                local_child.tail = "\n\t"
                if has_template:
                    span = SPAN()
                    span.text = local_child.text
                    local_child.text = None
                    for gc in local_child:
                        span.append(gc)
                    local_child.append(span)
                    span = SPAN(local_child.get("list-template"))
                    if ccss:
                        span.set("class", ccss)
                    local_last = templates.get(local_lvlid, "")
                    if span.text and len(span.text) > len(local_last):
                        templates[local_lvlid] = span.text
                    local_child.insert(0, span)
                for attr in ("list-lvl", "list-id", "list-template"):
                    local_child.attrib.pop(attr, None)
                local_val = int(local_child.get("value"))
                if last_val == local_val - 1 or local_wrap.tag == "ul" or (last_val is None and local_val == 1):
                    local_child.attrib.pop("value")
                last_val = local_val
            current_run[-1].tail = "\n"
            del current_run[:]

        parents = set()
        for child in body.iterdescendants("li"):
            parents.add(child.getparent())

        for parent in parents:
            current_run = []
            for child in parent:
                if child.tag == "li":
                    if current_run:
                        last = current_run[-1]
                        if (last.get("list-id"), last.get("list-lvl")) != (
                            child.get("list-id"),
                            child.get("list-lvl"),
                        ):
                            commit(current_run)
                    current_run.append(child)
                else:
                    commit(current_run)
            commit(current_run)

        # Convert the list items that use custom text for bullets into tables
        # so that they display correctly
        for wrap in body.xpath("//ol[@lvlid]"):
            wrap.attrib.pop("lvlid")
            wrap.tag = "div"
            wrap.set("style", "display:table")
            for i, li in enumerate(wrap.iterchildren("li")):
                li.tag = "div"
                li.attrib.pop("value", None)
                li.set("style", "display:table-row")
                obj = object_map[li]
                bs = styles.para_cache[obj]
                if i == 0:
                    wrap.set(
                        "style",
                        "display:table; padding-left:%s" % bs.css.get("margin-left", "0"),
                    )
                bs.css.pop("margin-left", None)
                for child in li:
                    child.set("style", "display:table-cell")
