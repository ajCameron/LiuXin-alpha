#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:fdm=marker:ai

from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

import sys

from lxml import etree

from LiuXin_alpha.file_formats.oeb.base import XHTML
from LiuXin_alpha.file_formats.oeb.polish.stats import normalize_font_properties

from LiuXin_alpha.utils.logging import prints
from LiuXin_alpha.utils.storage.local.filenames import ascii_filename
from LiuXin_alpha.utils.language_tools.icu import lower as icu_lower
from LiuXin_alpha.utils.localization import trans as _

# Py2/Py3 compatibility layer
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems
from LiuXin_alpha.utils.libraries.liuxin_six import dict_itervalues as itervalues
from LiuXin_alpha.utils.libraries.liuxin_six import six_string_types

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"
__docformat__ = "restructuredtext en"

props = {
    "font-family": None,
    "font-weight": "normal",
    "font-style": "normal",
    "font-stretch": "normal",
}


def matching_rule(font: _typing.Any, rules: _typing.Any) -> _typing.Any:
    ff = font["font-family"]
    if not isinstance(ff, six_string_types):
        ff = tuple(ff)[0]
    family = icu_lower(ff)
    wt = font["font-weight"]
    style = font["font-style"]
    stretch = font["font-stretch"]

    for rule in rules:
        if rule["font-style"] == style and rule["font-stretch"] == stretch and rule["font-weight"] == wt:
            ff = rule["font-family"]
            if not isinstance(ff, six_string_types):
                ff = tuple(ff)[0]
            if icu_lower(ff) == family:
                return rule


def embed_font(container: _typing.Any, font: _typing.Any, all_font_rules: _typing.Any, report: _typing.Any, warned: _typing.Any) -> _typing.Any:
    rule = matching_rule(font, all_font_rules)
    ff = font["font-family"]
    if not isinstance(ff, six_string_types):
        ff = ff[0]
    if rule is None:
        try:
            from LiuXin_alpha.utils.fonts.scanner import font_scanner, NoFonts
        except ModuleNotFoundError:
            msg = _("Font scanner support is unavailable, cannot embed font family: %s") % ff
            if msg not in warned:
                warned.add(msg)
                report(msg)
            return

        if ff in warned:
            return
        try:
            fonts = font_scanner.fonts_for_family(ff)
        except NoFonts:
            report(_("Failed to find fonts for family: %s, not embedding") % ff)
            warned.add(ff)
            return
        wt = int(font.get("font-weight", "400"))
        for f in fonts:
            if (
                f["weight"] == wt
                and f["font-style"] == font.get("font-style", "normal")
                and f["font-stretch"] == font.get("font-stretch", "normal")
            ):
                report("Embedding font %s from %s" % (f["full_name"], f["path"]))
                data = font_scanner.get_font_data(f)
                fname = f["full_name"]
                ext = "otf" if f["is_otf"] else "ttf"
                fname = ascii_filename(fname).replace(" ", "-").replace("(", "").replace(")", "")
                item = container.generate_item("fonts/%s.%s" % (fname, ext), id_prefix="font")
                name = container.href_to_name(item.get("href"), container.opf_name)
                with container.open(name, "wb") as out:
                    out.write(data)
                href = container.name_to_href(name)
                rule = {k: f.get(k, v) for k, v in iteritems(props)}
                rule["src"] = "url(%s)" % href
                rule["name"] = name
                return rule
        msg = _(
            "Failed to find font matching: family: %(family)s; weight: %(weight)s; style: %(style)s; "
            "stretch: %(stretch)s"
        ) % dict(
            family=ff,
            weight=font["font-weight"],
            style=font["font-style"],
            stretch=font["font-stretch"],
        )
        if msg not in warned:
            warned.add(msg)
            report(msg)
    else:
        name = rule["src"]
        href = container.name_to_href(name)
        rule = {k: ff if k == "font-family" else rule.get(k, v) for k, v in iteritems(props)}
        rule["src"] = "url(%s)" % href
        rule["name"] = name
        return rule


def embed_all_fonts(container: _typing.Any, stats: _typing.Any, report: _typing.Any) -> bool:
    all_font_rules = tuple(itervalues(stats.all_font_rules))
    warned = set()
    rules, nrules = [], []
    modified = set()

    for path in container.spine_items:
        name = container.abspath_to_name(path)
        fu = stats.font_usage_map.get(name, None)
        fs = stats.font_spec_map.get(name, None)
        fr = stats.font_rule_map.get(name, None)
        if None in (fs, fu, fr):
            continue
        fs = {icu_lower(x) for x in fs}
        for font in itervalues(fu):
            if icu_lower(font["font-family"]) not in fs:
                continue
            rule = matching_rule(font, fr)
            if rule is None:
                # This font was not already embedded in this HTML file, before
                # processing started
                rule = matching_rule(font, nrules)
                if rule is None:
                    rule = embed_font(container, font, all_font_rules, report, warned)
                    if rule is not None:
                        rules.append(rule)
                        nrules.append(normalize_font_properties(rule.copy()))
                        modified.add(name)
                        stats.font_stats[rule["name"]] = font["text"]
                else:
                    # This font was previously embedded by this code, update its stats
                    stats.font_stats[rule["name"]] |= font["text"]
                    modified.add(name)

    if not rules:
        report(_("No embeddable fonts found"))
        return False

    # Write out CSS
    rules = [
        ";\n\t".join(
            "%s: %s" % (k, '"%s"' % v if k == "font-family" else v)
            for k, v in iteritems(rulel)
            if (k in props and props[k] != v and v != "400") or k == "src"
        )
        for rulel in rules
    ]
    css = "\n\n".join(["@font-face {\n\t%s\n}" % r for r in rules])
    item = container.generate_item("fonts.css", id_prefix="font_embed")
    name = container.href_to_name(item.get("href"), container.opf_name)
    with container.open(name, "wb") as out:
        out.write(css.encode("utf-8"))

    # Add link to CSS in all files that need it
    for spine_name in modified:
        root = container.parsed(spine_name)
        head = root.xpath('//*[local-name()="head"][1]')[0]
        href = container.name_to_href(name, spine_name)
        etree.SubElement(head, XHTML("link"), rel="stylesheet", type="text/css", href=href).tail = "\n"
        container.dirty(spine_name)
    return True


if __name__ == "__main__":

    from LiuXin_alpha.file_formats.oeb.polish.container import get_container
    from LiuXin_alpha.file_formats.oeb.polish.stats import StatsCollector

    from LiuXin_alpha.utils.logging import default_log

    default_log.filter_level = default_log.DEBUG
    inbook = sys.argv[-1]
    ebook = get_container(inbook, default_log)
    main_report = []
    main_stats = StatsCollector(ebook, do_embed=True)
    embed_all_fonts(ebook, main_stats, main_report.append)
    outbook, book_ext = inbook.rpartition(".")[0::2]
    outbook += "_subset." + book_ext
    ebook.commit(outbook)
    prints("\nReport:")
    for main_msg in main_report:
        prints(main_msg)
    print()
    prints("Output written to:", outbook)
