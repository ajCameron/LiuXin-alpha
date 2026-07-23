#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:fdm=marker:ai

from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

from collections import defaultdict

from LiuXin_alpha.file_formats.oeb.base import urlnormalize

# Font subsetting is optional in this port; keep module importable when absent.
try:
    from LiuXin_alpha.utils.fonts.sfnt.subset import subset, NoGlyphs, UnsupportedFont

    _HAS_FONT_SUBSETTER = True
except ModuleNotFoundError:
    subset = None
    _HAS_FONT_SUBSETTER = False

    class NoGlyphs(Exception):
        pass

    class UnsupportedFont(Exception):
        pass
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems
from LiuXin_alpha.utils.libraries.liuxin_six import dict_itervalues as itervalues
from LiuXin_alpha.utils.libraries.liuxin_six import memory_range

__license__ = "GPL v3"
__copyright__ = "2012, Kovid Goyal <kovid at kovidgoyal.net>"
__docformat__ = "restructuredtext en"


def get_font_properties(rule: _typing.Any, default: _typing.Any = None) -> _typing.Any:
    """
    Given a CSS rule, extract normalized font properties from
    it. Note that shorthand font property should already have been expanded
    by the CSS flattening code.
    :param rule:
    :param default:
    :return:
    """
    props = {}
    s = rule.style
    for q in ("font-family", "src", "font-weight", "font-stretch", "font-style"):
        g = "uri" if q == "src" else "value"

        try:
            val = s.getProperty(q).propertyValue[0]
            val = getattr(val, g)
            if q == "font-family":
                val = [x.value for x in s.getProperty(q).propertyValue if hasattr(x.value, "lower")]
                if val and val[0] == "inherit":
                    val = None
        except (IndexError, KeyError, AttributeError, TypeError, ValueError):
            val = None if q in {"src", "font-family"} else default

        if q in {"font-weight", "font-stretch", "font-style"}:
            val = six_unicode(val).lower() if (val or val == 0) else val
            if val == "inherit":
                val = default

        if q == "font-weight":
            val = {"normal": "400", "bold": "700"}.get(val, val)
            if val not in {
                "100",
                "200",
                "300",
                "400",
                "500",
                "600",
                "700",
                "800",
                "900",
                "bolder",
                "lighter",
            }:
                val = default
            if val == "normal":
                val = "400"
        elif q == "font-style":
            if val not in {"normal", "italic", "oblique"}:
                val = default
        elif q == "font-stretch":
            if val not in {
                "normal",
                "ultra-condensed",
                "extra-condensed",
                "condensed",
                "semi-condensed",
                "semi-expanded",
                "expanded",
                "extra-expanded",
                "ultra-expanded",
            }:
                val = default

        props[q] = val

    return props


def find_font_face_rules(sheet: _typing.Any, oeb: _typing.Any) -> _typing.Any:
    """
    Find all @font-face rules in the given sheet and extract the relevant info from them.
    sheet can be either a ManifestItem or a CSSStyleSheet.
    :param sheet:
    :param oeb:
    :return:
    """
    ans = []
    try:
        rules = sheet.data.cssRules
    except AttributeError:
        rules = sheet.cssRules

    for i, rule in enumerate(rules):
        if rule.type != rule.FONT_FACE_RULE:
            continue
        props = get_font_properties(rule, default="normal")
        if not props["font-family"] or not props["src"]:
            continue

        try:
            path = sheet.abshref(props["src"])
        except AttributeError:
            path = props["src"]
        ff = oeb.manifest.hrefs.get(urlnormalize(path), None)
        if not ff:
            continue
        props["item"] = ff
        if props["font-weight"] in {"bolder", "lighter"}:
            props["font-weight"] = "400"
        props["weight"] = int(props["font-weight"])
        props["rule"] = rule
        props["chars"] = set()
        ans.append(props)

    return ans


def elem_style(style_rules: _typing.Any, cls: _typing.Any, inherited_style: _typing.Any) -> _typing.Any:
    """
    Find the effective style for the given element.
    :param style_rules:
    :param cls:
    :param inherited_style:
    :return:
    """
    classes = cls.split()
    style = inherited_style.copy()
    for cls in classes:
        style.update(style_rules.get(cls, {}))
    wt = style.get("font-weight", None)
    pwt = inherited_style.get("font-weight", "400")
    if wt == "bolder":
        style["font-weight"] = {
            "100": "400",
            "200": "400",
            "300": "400",
            "400": "700",
            "500": "700",
        }.get(pwt, "900")
    elif wt == "lighter":
        style["font-weight"] = {
            "600": "400",
            "700": "400",
            "800": "700",
            "900": "700",
        }.get(pwt, "100")

    return style


class SubsetFonts(object):

    """
    Subset all embedded fonts. Must be run after CSS flattening, as it requires
    CSS normalization and flattening to work.
    """

    def __call__(self: _typing.Self, oeb: _typing.Any, log: _typing.Any, opts: _typing.Any) -> None:
        self.oeb, self.log, self.opts = oeb, log, opts
        if not _HAS_FONT_SUBSETTER or subset is None:
            self.log.warn("Font subsetter is unavailable; skipping embedded font subsetting.")
            return

        self.find_embedded_fonts()
        if not self.embedded_fonts:
            self.log.debug("No embedded fonts found")
            return
        self.find_style_rules()
        self.find_font_usage()

        totals = [0, 0]

        def remove(local_font: _typing.Any) -> None:
            totals[1] += len(local_font["item"].data)
            self.oeb.manifest.remove(local_font["item"])
            local_font["rule"].parentStyleSheet.deleteRule(local_font["rule"])

        fonts = {}
        for font in self.embedded_fonts:
            item, chars = font["item"], font["chars"]
            if item.href in fonts:
                fonts[item.href]["chars"] |= chars
            else:
                fonts[item.href] = font

        for font in itervalues(fonts):
            if not font["chars"]:
                self.log("The font %s is unused. Removing it." % font["src"])
                remove(font)
                continue
            try:
                raw, old_stats, new_stats = subset(font["item"].data, font["chars"])
            except NoGlyphs:
                self.log("The font %s has no used glyphs. Removing it." % font["src"])
                remove(font)
                continue
            except UnsupportedFont as e:
                self.log.warn("The font %s is unsupported for subsetting. %s" % (font["src"], e))
                sz = len(font["item"].data)
                totals[0] += sz
                totals[1] += sz
            else:
                font["item"].data = raw
                nlen = sum(itervalues(new_stats))
                olen = sum(itervalues(old_stats))
                self.log("Decreased the font %s to %.1f%% of its original size" % (font["src"], nlen / olen * 100))
                totals[0] += nlen
                totals[1] += olen

            font["item"].unload_data_from_memory()

        if totals[0]:
            self.log("Reduced total font size to %.1f%% of original" % (totals[0] / totals[1] * 100))

    def find_embedded_fonts(self: _typing.Self) -> None:
        """
        Find all @font-face rules and extract the relevant info from them.
        :return None:
        """
        self.embedded_fonts = []
        for item in self.oeb.manifest:
            if not hasattr(item.data, "cssRules"):
                continue
            self.embedded_fonts.extend(find_font_face_rules(item, self.oeb))

    def find_style_rules(self: _typing.Self) -> None:
        """
        Extract all font related style information from all stylesheets into a
        dict mapping classes to font properties specified by that class. All
        the heavy lifting has already been done by the CSS flattening code.
        :return:
        """
        rules = defaultdict(dict)
        for item in self.oeb.manifest:
            if not hasattr(item.data, "cssRules"):
                continue
            for i, rule in enumerate(item.data.cssRules):
                if rule.type != rule.STYLE_RULE:
                    continue
                props = {k: v for k, v in iteritems(get_font_properties(rule)) if v}
                if not props:
                    continue
                for sel in rule.selectorList:
                    sel = sel.selectorText
                    if sel and sel.startswith("."):
                        # We dont care about pseudo-selectors as the worst that
                        # can happen is some extra characters will remain in
                        # the font
                        sel = sel.partition(":")[0]
                        rules[sel[1:]].update(props)

        self.style_rules = dict(rules)

    def find_font_usage(self: _typing.Self) -> None:
        for item in self.oeb.manifest:
            if not hasattr(item.data, "xpath"):
                continue
            for body in item.data.xpath('//*[local-name()="body"]'):
                base = {
                    "font-family": ["serif"],
                    "font-weight": "400",
                    "font-style": "normal",
                    "font-stretch": "normal",
                }
                self.find_usage_in(body, base)

    def used_font(self: _typing.Self, style: _typing.Any) -> _typing.Any:
        """
        Given a style find the embedded font that matches it. Returns None if
        no match is found (can happen if no family matches).
        :param style:
        :return:
        """
        ff = style.get("font-family", [])
        lnames = {six_unicode(x).lower() for x in ff}
        matching_set = []

        # Filter on font-family
        for ef in self.embedded_fonts:
            flnames = {x.lower() for x in ef.get("font-family", [])}
            if not lnames.intersection(flnames):
                continue
            matching_set.append(ef)
        if not matching_set:
            return None

        # Filter on font-stretch
        widths = {
            x: i
            for i, x in enumerate(
                (
                    "ultra-condensed",
                    "extra-condensed",
                    "condensed",
                    "semi-condensed",
                    "normal",
                    "semi-expanded",
                    "expanded",
                    "extra-expanded",
                    "ultra-expanded",
                )
            )
        }

        width = widths[style.get("font-stretch", "normal")]
        for f in matching_set:
            f["width"] = widths[style.get("font-stretch", "normal")]

        min_dist = min(abs(width - f["width"]) for f in matching_set)
        nearest = [f for f in matching_set if abs(width - f["width"]) == min_dist]
        if width <= 4:
            lmatches = [f for f in nearest if f["width"] <= width]
        else:
            lmatches = [f for f in nearest if f["width"] >= width]
        matching_set = lmatches or nearest

        # Filter on font-style
        fs = style.get("font-style", "normal")
        order = {
            "oblique": ["oblique", "italic", "normal"],
            "normal": ["normal", "oblique", "italic"],
        }.get(fs, ["italic", "oblique", "normal"])
        for q in order:
            matches = [f for f in matching_set if f.get("font-style", "normal") == q]
            if matches:
                matching_set = matches
                break

        # Filter on font weight
        fw = int(style.get("font-weight", "400"))
        if fw == 400:
            q = [400, 500, 300, 200, 100, 600, 700, 800, 900]
        elif fw == 500:
            q = [500, 400, 300, 200, 100, 600, 700, 800, 900]
        elif fw < 400:
            q = [fw] + list(memory_range(fw - 100, -100, -100)) + list(memory_range(fw + 100, 100, 1000))
        else:
            q = [fw] + list(memory_range(fw + 100, 100, 1000)) + list(memory_range(fw - 100, -100, -100))
        for wt in q:
            matches = [f for f in matching_set if f["weight"] == wt]
            if matches:
                return matches[0]

    def find_chars(self: _typing.Self, elem: _typing.Any) -> _typing.Any:
        ans = set()
        if elem.text:
            ans |= set(elem.text)
        for child in elem:
            if child.tail:
                ans |= set(child.tail)
        return ans

    def find_usage_in(self: _typing.Self, elem: _typing.Any, inherited_style: _typing.Any) -> None:
        style = elem_style(self.style_rules, elem.get("class", "") or "", inherited_style)
        for child in elem:
            self.find_usage_in(child, style)
        font = self.used_font(style)
        if font:
            chars = self.find_chars(elem)
            if chars:
                font["chars"] |= chars
