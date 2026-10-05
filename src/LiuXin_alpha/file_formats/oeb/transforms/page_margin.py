#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Normalize page-margin rules across OEB stylesheets.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise page margin through a consuming regression::

        python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

from collections import Counter

from LiuXin_alpha.file_formats.oeb.base import barename, XPath

from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems

__license__ = "GPL v3"
__copyright__ = "2011, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


class RemoveAdobeMargins(object):
    """
    Remove margins specified in Adobe's page templates.

    Example:
        Exercise RemoveAdobeMargins through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
    """

    def __call__(self: _typing.Self, oeb: _typing.Any, log: _typing.Any, opts: _typing.Any) -> None:
        """
        Perform the call operation under explicit file-format and conversion rules.

        Example:
            Exercise RemoveAdobeMargins.  call   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param oeb: Value supplied for oeb under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :param opts: Value supplied for opts under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.oeb, self.opts, self.log = oeb, opts, log

        for item in self.oeb.manifest:
            if item.media_type in {
                "application/vnd.adobe-page-template+xml",
                "application/vnd.adobe.page-template+xml",
                "application/adobe-page-template+xml",
                "application/adobe.page-template+xml",
            } and hasattr(item.data, "xpath"):
                self.log("Removing page margins specified in the Adobe page template")
                for elem in item.data.xpath("//*[@margin-bottom or @margin-top " "or @margin-left or @margin-right]"):
                    for margin in ("left", "right", "top", "bottom"):
                        attr = "margin-" + margin
                        elem.attrib.pop(attr, None)


class NegativeTextIndent(Exception):
    """
    Provide the negativetextindent contract for validated ebook processing.

    Example:
        Exercise NegativeTextIndent through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
    """
    pass


class RemoveFakeMargins(object):

    """
    Remove left and right margins from paragraph/divs if the same margin is specified on almost all the elements at that level.

    Example:
        Exercise RemoveFakeMargins through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
    """

    def __call__(self: _typing.Self, oeb: _typing.Any, log: _typing.Any, opts: _typing.Any) -> None:
        """
        Perform the call operation under explicit file-format and conversion rules.

        Example:
            Exercise RemoveFakeMargins.  call   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param oeb: Value supplied for oeb under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :param opts: Value supplied for opts under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not opts.remove_fake_margins:
            return
        self.oeb, self.log, self.opts = oeb, log, opts

        self.levels = {}
        self.stats = {}
        self.selector_map = {}

        stylesheet = self.oeb.manifest.main_stylesheet
        if stylesheet is None:
            return

        self.log("Removing fake margins...")

        stylesheet = stylesheet.data

        from cssutils.css import CSSRule

        for rule in stylesheet.cssRules.rulesOfType(CSSRule.STYLE_RULE):
            self.selector_map[rule.selectorList.selectorText] = rule.style

        self.find_levels()

        for level in self.levels:
            try:
                self.process_level(level)
            except NegativeTextIndent:
                self.log.debug("Negative text indent detected at level  %s, ignoring this level" % level)

    def get_margins(self: _typing.Self, elem: _typing.Any) -> tuple[_typing.Any, ...]:
        """
        Return margins under the format's safety and compatibility rules.

        Example:
            Exercise RemoveFakeMargins.get margins through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param elem: Value supplied for elem under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        cls = elem.get("class", None)
        if cls:
            style = self.selector_map.get("." + cls, None)
            if style:
                try:
                    ti = style["text-indent"]
                except:
                    pass
                else:
                    if (hasattr(ti, "startswith") and ti.startswith("-")) or isinstance(ti, (int, float)) and ti < 0:
                        raise NegativeTextIndent()
                return style.marginLeft, style.marginRight, style
        return "", "", None

    def process_level(self: _typing.Self, level: _typing.Any) -> None:
        """
        Perform the process level operation under explicit file-format and conversion rules.

        Example:
            Exercise RemoveFakeMargins.process level through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param level: Value supplied for level under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        elems = self.levels[level]
        self.stats[level + "_left"] = Counter()
        self.stats[level + "_right"] = Counter()

        for elem in elems:
            lm, rm = self.get_margins(elem)[:2]
            self.stats[level + "_left"][lm] += 1
            self.stats[level + "_right"][rm] += 1

        self.log.debug(level, " left margin stats:", self.stats[level + "_left"])
        self.log.debug(level, " right margin stats:", self.stats[level + "_right"])

        remove_left = self.analyze_stats(self.stats[level + "_left"])
        remove_right = self.analyze_stats(self.stats[level + "_right"])

        mcl = None
        mcr = None
        if remove_left:
            mcl = self.stats[level + "_left"].most_common(1)[0][0]
            self.log("Removing level %s left margin of:" % level, mcl)

        if remove_right:
            mcr = self.stats[level + "_right"].most_common(1)[0][0]
            self.log("Removing level %s right margin of:" % level, mcr)

        if remove_left or remove_right:
            for elem in elems:
                lm, rm, style = self.get_margins(elem)
                if remove_left and lm == mcl:
                    style.removeProperty("margin-left")
                if remove_right and rm == mcr:
                    style.removeProperty("margin-right")

    def find_levels(self: _typing.Self) -> None:
        """
        Find levels under the format's safety and compatibility rules.

        Example:
            Exercise RemoveFakeMargins.find levels through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        def level_of(local_elem: _typing.Any, local_body: _typing.Any) -> _typing.Any:
            """
            Perform the level of operation under explicit file-format and conversion rules.

            Example:
                Exercise RemoveFakeMargins.find levels.level of through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :param local_elem: Value supplied for local elem under the utility contract.
            :param local_body: Value supplied for local body under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            ans = 1
            while local_elem.getparent() is not local_body:
                ans += 1
                local_elem = local_elem.getparent()
            return ans

        paras = XPath("descendant::h:p|descendant::h:div")

        for item in self.oeb.spine:
            body = XPath("//h:body")(item.data)
            if not body:
                continue
            body = body[0]

            for p in paras(body):
                level = level_of(p, body)
                level = "%s_%d" % (barename(p.tag), level)
                if level not in self.levels:
                    self.levels[level] = []
                self.levels[level].append(p)

        remove = set()
        for k, v in iteritems(self.levels):
            num = len(v)
            self.log.debug("Found %d items of level:" % num, k)
            level = int(k.split("_")[-1])
            tag = k.split("_")[0]
            if tag == "p" and num < 25:
                remove.add(k)
            if tag == "div":
                if level > 2 and num < 25:
                    remove.add(k)
                elif level < 3:
                    # Check each level < 3 element and only keep those that have many child paras
                    for elem in list(v):
                        children = len(paras(elem))
                        if children < 5:
                            v.remove(elem)

        for k in remove:
            self.levels.pop(k)
            self.log.debug("Ignoring level", k)

    def analyze_stats(self: _typing.Self, stats: _typing.Any) -> bool:
        """
        Perform the analyze stats operation under explicit file-format and conversion rules.

        Example:
            Exercise RemoveFakeMargins.analyze stats through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param stats: Value supplied for stats under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not stats:
            return False
        mc = stats.most_common(1)
        if len(mc) > 1:
            return False
        mc = mc[0]
        most_common, most_common_count = mc
        if not most_common or most_common == "0":
            return False
        total = sum(stats.values())
        # True if greater than 95% of elements have the same margin
        return most_common_count / total > 0.95
