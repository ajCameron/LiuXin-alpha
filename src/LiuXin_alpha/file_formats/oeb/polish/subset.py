#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:fdm=marker:ai

from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

import os
import sys

from LiuXin_alpha.file_formats.oeb.base import OEB_STYLES, OEB_DOCS, XPath
from LiuXin_alpha.file_formats.oeb.polish.container import OEB_FONTS
from LiuXin_alpha.file_formats.oeb.polish.utils import guess_type

from LiuXin_alpha.utils.text import as_unicode
from LiuXin_alpha.utils.logging import prints
try:
    from LiuXin_alpha.utils.fonts.sfnt.errors import UnsupportedFont
    from LiuXin_alpha.utils.fonts.sfnt.subset import subset
    from LiuXin_alpha.utils.fonts.utils import get_font_names
    _HAS_FONT_UTILS = True
except ModuleNotFoundError:
    _HAS_FONT_UTILS = False

    class UnsupportedFont(Exception):
        pass

# Py2/Py3 compatability layer
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems
from LiuXin_alpha.utils.libraries.liuxin_six import dict_itervalues as itervalues

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"
__docformat__ = "restructuredtext en"


def remove_font_face_rules(container: _typing.Any, sheet: _typing.Any, remove_names: _typing.Any, base: _typing.Any) -> _typing.Any:
    changed = False
    for rule in tuple(sheet.cssRules):
        if rule.type != rule.FONT_FACE_RULE:
            continue
        try:
            uri = rule.style.getProperty("src").propertyValue[0].uri
        except (IndexError, KeyError, AttributeError, TypeError, ValueError):
            continue
        name = container.href_to_name(uri, base)
        if name in remove_names:
            sheet.deleteRule(rule)
            changed = True
    return changed


def subset_all_fonts(container: _typing.Any, font_stats: _typing.Any, report: _typing.Any) -> _typing.Any:
    if not _HAS_FONT_UTILS:
        report("Font subsetting support is unavailable (LiuXin_alpha.utils.fonts not ported).")
        return False

    remove = set()
    total_old = total_new = 0
    changed = False
    for name, mt in iteritems(container.mime_map):
        if (mt in OEB_FONTS or name.rpartition(".")[-1].lower() in {"otf", "ttf"}) and mt != guess_type("a.woff"):
            chars = font_stats.get(name, set())
            with container.open(name, "rb") as f:
                f.seek(0, os.SEEK_END)
                total_old += f.tell()
            if not chars:
                remove.add(name)
                report("Removed unused font: %s" % name)
                continue
            with container.open(name, "r+b") as f:
                raw = f.read()
                try:
                    font_name = get_font_names(raw)[-1]
                except Exception as e:
                    container.log.warning("Corrupted font: %s, ignoring.  Error: %s" % (name, as_unicode(e)))
                    continue
                warnings = []
                container.log("Subsetting font: %s" % (font_name or name))
                try:
                    nraw, old_sizes, new_sizes = subset(raw, chars, warnings=warnings)
                except UnsupportedFont as e:
                    container.log.warning("Unsupported font: %s, ignoring.  Error: %s" % (name, as_unicode(e)))
                    continue

                for w in warnings:
                    container.log.warn(w)
                olen = sum(itervalues(old_sizes))
                nlen = sum(itervalues(new_sizes))
                total_new += len(nraw)
                if nlen == olen:
                    report("The font %s was already subset" % font_name)
                else:
                    report("Decreased the font %s to %.1f%% of its original size" % (font_name, nlen / olen * 100))
                    changed = True
                f.seek(0), f.truncate(), f.write(nraw)

    for name in remove:
        container.remove_item(name)
        changed = True

    if remove:
        for name, mt in iteritems(container.mime_map):
            if mt in OEB_STYLES:
                sheet = container.parsed(name)
                if remove_font_face_rules(container, sheet, remove, name):
                    container.dirty(name)
            elif mt in OEB_DOCS:
                for style in XPath("//h:style")(container.parsed(name)):
                    if style.get("type", "text/css") == "text/css" and style.text:
                        sheet = container.parse_css(style.text, name)
                        if remove_font_face_rules(container, sheet, remove, name):
                            style.text = sheet.cssText
                            container.dirty(name)
    if total_old > 0:
        report("Reduced total font size to %.1f%% of original" % (total_new / total_old * 100))
    else:
        report("No embedded fonts found")
    return changed


if __name__ == "__main__":

    from LiuXin_alpha.file_formats.oeb.polish.container import get_container
    from LiuXin_alpha.file_formats.oeb.polish.stats import StatsCollector

    from LiuXin_alpha.utils.logging import default_log

    default_log.filter_level = default_log.DEBUG
    inbook = sys.argv[-1]
    ebook = get_container(inbook, default_log)
    local_report = []
    stats = StatsCollector(ebook).font_stats
    subset_all_fonts(ebook, stats, local_report.append)
    outbook, ext = inbook.rpartition(".")[0::2]
    outbook += "_subset." + ext
    ebook.commit(outbook)
    prints("\nReport:")
    for msg in local_report:
        prints(msg)
    print()
    prints("Output written to:", outbook)
