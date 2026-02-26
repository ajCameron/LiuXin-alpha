#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:fdm=marker:ai

from __future__ import unicode_literals, division, absolute_import, print_function

import re
import sys
import os
import time
import logging
from collections import namedtuple
from functools import partial

from LiuXin_alpha.file_formats.oeb.polish.container import get_container
from LiuXin_alpha.file_formats.oeb.polish.cover import set_cover
from LiuXin_alpha.file_formats.oeb.polish.embed import embed_all_fonts
from LiuXin_alpha.file_formats.oeb.polish.jacket import (
    replace_jacket,
    add_or_replace_jacket,
    find_existing_jacket,
    remove_jacket,
)
from LiuXin_alpha.file_formats.oeb.polish.replace import smarten_punctuation
from LiuXin_alpha.file_formats.oeb.polish.stats import StatsCollector
from LiuXin_alpha.file_formats.oeb.polish.subset import subset_all_fonts

# Todo: Investigate what's going on here
from LiuXin_alpha.file_formats.oeb.polish.css import remove_unused_css

from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iterkeys as iterkeys
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import get_compat_logger

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"
__docformat__ = "restructuredtext en"


class Log(object):
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARN = logging.WARNING
    WARNING = logging.WARNING
    ERROR = logging.ERROR

    def __init__(self, level=logging.INFO):
        self._logger = get_compat_logger("LiuXin_alpha.oeb.polish")
        self.filter_level = level
        self._logger.setLevel(level)

    def _emit(self, level, *args):
        if not args:
            msg = ""
        else:
            msg = " ".join(str(x) for x in args)
        self._logger.log(level, msg)

    def __call__(self, *args):
        self._emit(self.INFO, *args)

    def debug(self, *args):
        self._emit(self.DEBUG, *args)

    def info(self, *args):
        self._emit(self.INFO, *args)

    def warn(self, *args):
        self._emit(self.WARN, *args)

    warning = warn

    def error(self, *args):
        self._emit(self.ERROR, *args)

    def exception(self, *args):
        self._logger.exception(" ".join(str(x) for x in args))


ALL_OPTS = {
    "embed": False,
    "subset": False,
    "opf": None,
    "cover": None,
    "jacket": False,
    "remove_jacket": False,
    "smarten_punctuation": False,
    "remove_unused_css": False,
}

CUSTOMIZATION = {
    "remove_unused_classes": False,
}

SUPPORTED = {"EPUB", "AZW3"}

# Help {{{
HELP = {
    "about": _(
        """\
<p><i>Polishing books</i> is all about putting the shine of perfection onto
your carefully crafted ebooks.</p>

<p>Polishing tries to minimize the changes to the internal code of your ebook.
Unlike conversion, it <i>does not</i> flatten CSS, rename files, change font
sizes, adjust margins, etc. Every action performs only the minimum set of
changes needed for the desired effect.</p>

<p>You should use this tool as the last step in your ebook creation process.</p>
{0}
<p>Note that polishing only works on files in the %s formats.</p>\
"""
    )
    % _(" or ").join(sorted("<b>%s</b>" % x for x in SUPPORTED)),
    "embed": _(
        """\
<p>Embed all fonts that are referenced in the document and are not already embedded.
This will scan your computer for the fonts, and if they are found, they will be
embedded into the document.</p>
<p>Please ensure that you have the proper license for embedding the fonts used in this document.</p>
"""
    ),
    "subset": _(
        """\
<p>Subsetting fonts means reducing an embedded font to contain
only the characters used from that font in the book. This
greatly reduces the size of the font files (halving the font
file sizes is common).</p>

<p>For example, if the book uses a specific font for headers,
then subsetting will reduce that font to contain only the
characters present in the actual headers in the book. Or if the
book embeds the bold and italic versions of a font, but bold
and italic text is relatively rare, or absent altogether, then
the bold and italic fonts can either be reduced to only a few
characters or completely removed.</p>

<p>The only downside to subsetting fonts is that if, at a later
date you decide to add more text to your books, the newly added
text might not be covered by the subset font.</p>
"""
    ),
    "jacket": _(
        """\
<p>Insert a "book jacket" page at the start of the book that contains
all the book metadata such as title, tags, authors, series, comments,
etc. Any previous book jacket will be replaced.</p>"""
    ),
    "remove_jacket": _(
        """\
<p>Remove a previous inserted book jacket page.</p>
"""
    ),
    "smarten_punctuation": _(
        """\
<p>Convert plain text dashes, ellipsis, quotes, multiple hyphens, etc. into their
typographically correct equivalents.</p>
<p>Note that the algorithm can sometimes generate incorrect results, especially
when single quotes at the start of contractions are involved.</p>
"""
    ),
    "remove_unused_css": _(
        """\
<p>Remove all unused CSS rules from stylesheets and &lt;style&gt; tags. Some books
created from production templates can have a large number of extra CSS rules
that dont match any actual content. These extra rules can slow down readers
that need to parse them all.</p>
"""
    ),
}


def hfix(name, raw):
    if name == "about":
        return raw.format("")
    raw = raw.replace("\n\n", "__XX__")
    raw = raw.replace("\n", " ")
    raw = raw.replace("__XX__", "\n")
    raw = raw.replace("&lt;", "<").replace("&gt;", ">")
    return raw


CLI_HELP = {x: hfix(x, re.sub("<.*?>", "", y)) for x, y in iteritems(HELP)}
# }}}


def update_metadata(ebook, new_opf):
    from LiuXin_alpha.file_formats.opf.opf2 import OPF
    from LiuXin_alpha.metadata.file_sources.epub import update_metadata

    opfpath = ebook.name_to_abspath(ebook.opf_name)
    with ebook.open(ebook.opf_name, "r+b") as stream, open(new_opf, "rb") as ns:
        opf = OPF(
            stream,
            basedir=os.path.dirname(opfpath),
            populate_spine=False,
            unquote_urls=False,
        )
        mi = OPF(ns, unquote_urls=False, populate_spine=False).to_book_metadata()
        mi.cover, mi.cover_data = None, (None, None)

        update_metadata(opf, mi, apply_null=True, update_timestamp=True)
        stream.seek(0)
        stream.truncate()
        stream.write(opf.render())


def polish_one(ebook, opts, report, customization=None):
    def rt(x):
        return report("\n### " + x)

    jacket = None
    changed = False
    customization = customization or CUSTOMIZATION.copy()

    if opts.subset or opts.embed:
        stats = StatsCollector(ebook, do_embed=opts.embed)

    if opts.opf:
        changed = True
        rt(_("Updating metadata"))
        update_metadata(ebook, opts.opf)
        jacket = find_existing_jacket(ebook)
        if jacket is not None:
            replace_jacket(ebook, jacket)
            report(_("Updated metadata jacket"))
        report(_("Metadata updated\n"))

    if opts.cover:
        changed = True
        rt(_("Setting cover"))
        set_cover(ebook, opts.cover, report)
        report("")

    if opts.jacket:
        changed = True
        rt(_("Inserting metadata jacket"))
        if jacket is None:
            if add_or_replace_jacket(ebook):
                report(_("Existing metadata jacket replaced"))
            else:
                report(_("Metadata jacket inserted"))
        else:
            report(_("Existing metadata jacket replaced"))
        report("")

    if opts.remove_jacket:
        rt(_("Removing metadata jacket"))
        if remove_jacket(ebook):
            report(_("Metadata jacket removed"))
            changed = True
        else:
            report(_("No metadata jacket found"))
        report("")

    if opts.smarten_punctuation:
        rt(_("Smartening punctuation"))
        if smarten_punctuation(ebook, report):
            changed = True
        report("")

    if opts.embed:
        rt(_("Embedding referenced fonts"))
        if embed_all_fonts(ebook, stats, report):
            changed = True
        report("")

    if opts.subset:
        rt(_("Subsetting embedded fonts"))
        if subset_all_fonts(ebook, stats.font_stats, report):
            changed = True
        report("")

    if opts.remove_unused_css:
        rt(_("Removing unused CSS rules"))
        if remove_unused_css(ebook, report, remove_unused_classes=customization["remove_unused_classes"]):
            changed = True
        report("")

    return changed


def polish(file_map, opts, log, report):
    st = time.time()
    for inbook, outbook in iteritems(file_map):
        report(_("## Polishing: %s") % (inbook.rpartition(".")[-1].upper()))
        ebook = get_container(inbook, log)
        polish_one(ebook, opts, report)
        ebook.commit(outbook)
        report("-" * 70)
    report(_("Polishing took: %.1f seconds") % (time.time() - st))


REPORT = "{0} REPORT {0}".format("-" * 30)


def gui_polish(data):
    files = data.pop("files")

    if not data.pop("metadata"):
        data.pop("opf")

    if not data.pop("do_cover"):
        data.pop("cover", None)

    file_map = {x: x for x in files}
    opts = ALL_OPTS.copy()
    opts.update(data)
    o = namedtuple("Options", " ".join(iterkeys(ALL_OPTS)))
    opts = o(**opts)
    log = Log(level=Log.DEBUG)
    report = []
    polish(file_map, opts, log, report.append)
    log("")
    log(REPORT)
    for msg in report:
        log(msg)
    return "\n\n".join(report)


def tweak_polish(container, actions, customization=None):
    opts = ALL_OPTS.copy()
    opts.update(actions)
    o = namedtuple("Options", " ".join(iterkeys(ALL_OPTS)))
    opts = o(**opts)
    report = []
    changed = polish_one(container, opts, report.append, customization=customization)
    return report, changed


def option_parser():
    from LiuXin_alpha.utils.config.config_tools import OptionParser

    usage = "%prog [options] input_file [output_file]\n\n" + re.sub(r"<.*?>", "", CLI_HELP["about"])
    parser = OptionParser(usage=usage)
    a = parser.add_option
    o = partial(a, default=False, action="store_true")
    o("--embed-fonts", "-e", dest="embed", help=CLI_HELP["embed"])
    o("--subset-fonts", "-f", dest="subset", help=CLI_HELP["subset"])
    a(
        "--cover",
        "-c",
        help=_(
            "Path to a cover image. Changes the cover specified in the ebook. "
            "If no cover is present, or the cover is not properly identified, inserts a new cover."
        ),
    )
    a(
        "--opf",
        "-o",
        help=_("Path to an OPF file. The metadata in the book is updated from the OPF file."),
    )
    o("--jacket", "-j", help=CLI_HELP["jacket"])
    o("--remove-jacket", help=CLI_HELP["remove_jacket"])
    o("--smarten-punctuation", "-p", help=CLI_HELP["smarten_punctuation"])
    o("--remove-unused-css", "-u", help=CLI_HELP["remove_unused_css"])

    o("--verbose", help=_("Produce more verbose output, useful for debugging."))

    return parser


def main(args=None):
    parser = option_parser()
    opts, args = parser.parse_args(args or sys.argv[1:])
    log = Log(level=Log.DEBUG if opts.verbose else Log.INFO)

    if not args:
        parser.print_help()
        log.error(_("You must provide the input file to polish"))
        raise SystemExit(1)

    if len(args) > 2:
        parser.print_help()
        log.error(_("Unknown extra arguments"))
        raise SystemExit(1)

    if len(args) == 1:
        inbook = args[0]
        base, ext = inbook.rpartition(".")[0::2]
        outbook = base + "_polished." + ext
    else:
        inbook, outbook = args

    popts = ALL_OPTS.copy()
    for k, v in iteritems(popts):
        popts[k] = getattr(opts, k, None)

    o = namedtuple("Options", " ".join(iterkeys(popts)))
    popts = o(**popts)
    report = []
    if not tuple(filter(None, (getattr(popts, name) for name in ALL_OPTS))):
        parser.print_help()
        log.error(_("You must specify at least one action to perform"))
        raise SystemExit(1)

    polish({inbook: outbook}, popts, log, report.append)
    log("")
    log(REPORT)
    for msg in report:
        log(msg)

    log("Output written to:", outbook)


if __name__ == "__main__":
    main()
