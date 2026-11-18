"""
Port calibre modules into ``LiuXin_alpha.metadata.upstream_calibre``.

Each module is copied from a calibre source tree with its imports rewritten by
the tables below: calibre modules that are ported whole map to their module in
the package, and smaller helpers map to names in ``_compat``. Nothing else in
the code changes, apart from mechanical edits so the copies also run on Python
3.12 and 3.13: ``from __future__ import annotations`` is added, ``except``
clauses naming several exceptions are parenthesised, and defaults are dropped
from type parameters, which only type checkers read. A header comment records
the source path and those changes.

Run it again on a newer calibre to bring its changes across. An import the
tables do not cover stops the run, so each new dependency is a deliberate
choice. The only other edits to modules listed in ``PORTS`` are the fixes in
``PATCHES``, whose lines are marked ``# LiuXin:``; a run stops if the calibre
code a fix replaces has changed. The earlier readers (``txt``, ``rtf``,
``topaz`` and the rest) were ported by hand and some carry fixes marked the
same way, which a re-port would need to keep.

Example:
    Re-port the modules from a calibre checkout::

        python scripts/port_upstream_calibre.py ~/src/calibre
"""

from __future__ import annotations

import argparse
import ast
import re
from pathlib import Path

DESTINATION = (
    Path(__file__).resolve().parent.parent / "src" / "LiuXin_alpha" / "metadata" / "upstream_calibre"
)
PKG = "LiuXin_alpha.metadata.upstream_calibre"
COMPAT = f"{PKG}._compat"

# Upstream path under src/calibre, module name here, and extra header lines.
PORTS: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("ebooks/metadata/__init__.py", "metadata_base.py", ()),
    ("ebooks/metadata/book/__init__.py", "book_fields.py", ()),
    ("library/field_metadata.py", "field_metadata.py", ()),
    ("ebooks/metadata/book/base.py", "book_base.py", ()),
    ("ebooks/metadata/book/json_codec.py", "json_codec.py", ()),
    (
        "utils/iso8601.py",
        "iso8601.py",
        (
            "The tzlocal package and zoneinfo.ZoneInfo come from _compat, which fall back to UTC",
            "where no zone can be found rather than failing at import.",
        ),
    ),
    ("utils/date.py", "date.py", ()),
    ("utils/cleantext.py", "cleantext.py", ()),
    ("ebooks/chardet.py", "chardet.py", ()),
    ("utils/imghdr.py", "imghdr.py", ()),
    ("ebooks/metadata/toc.py", "toc.py", ()),
    ("ebooks/metadata/utils.py", "metadata_utils.py", ()),
    ("ebooks/metadata/opf2.py", "opf2.py", ()),
    ("ebooks/metadata/opf3.py", "opf3.py", ()),
    ("ebooks/metadata/opf.py", "opf.py", ()),
    ("ebooks/metadata/opf3_test.py", "opf3_test.py", ()),
    ("ebooks/metadata/test_author_sort.py", "author_sort_test.py", ()),
    (
        "ebooks/__init__.py",
        "ebooks.py",
        (
            "render_html_data needs calibre's Qt worker, which LiuXin does not port: _compat's",
            "fork_job stands in and raises WorkerError, so HTML cover pages are not rendered.",
        ),
    ),
    (
        "ebooks/BeautifulSoup.py",
        "beautifulsoup.py",
        (
            "html5-parser's soup builder is replaced by Beautiful Soup's lxml builder",
            "(_compat.html5_soup_parse); Beautiful Soup itself stays optional.",
        ),
    ),
    (
        "utils/localunzip.py",
        "localunzip.py",
        ("zlib is imported directly rather than through Python 3.14's compression package.",),
    ),
    ("ebooks/metadata/epub.py", "epub.py", ()),
    ("ebooks/metadata/extz.py", "extz.py", ()),
    (
        "ebooks/metadata/html.py",
        "html.py",
        ("html5-parser is replaced by lxml's HTML parser (_compat.html5_parse).",),
    ),
    ("ebooks/fb2/__init__.py", "fb2_package.py", ()),
    ("utils/html2text.py", "html_to_text.py", ("The html2text package it wraps is optional.",)),
    ("ebooks/metadata/fb2.py", "fb2.py", ()),
    (
        "ebooks/metadata/odt.py",
        "odt.py",
        ("calibre's copy of odfpy is replaced by the copy LiuXin ships in file_formats.odf.",),
    ),
    ("ebooks/mobi/__init__.py", "mobi_package.py", ()),
    ("ebooks/mobi/langcodes.py", "mobi_langcodes.py", ()),
    (
        "ebooks/mobi/utils.py",
        "mobi_utils.py",
        ("Its Qt image helpers and tinycss colour parsing, which only MOBI writing uses, are not ported.",),
    ),
    ("ebooks/mobi/huffcdic.py", "huffcdic.py", ()),
    (
        "ebooks/compression/palmdoc.py",
        "palmdoc.py",
        ("calibre's cPalmdoc extension comes from LiuXin's utils plugin system (_compat.cPalmdoc).",),
    ),
    ("ebooks/mobi/reader/headers.py", "mobi_headers.py", ()),
    ("ebooks/mobi/reader/mobi6.py", "mobi6.py", ()),
    ("ebooks/metadata/mobi.py", "mobi.py", ()),
)

# LiuXin fixes, by upstream path: calibre's text and its replacement. They are
# applied before the imports are rewritten, so they are written in calibre's
# terms.
PATCHES: dict[str, tuple[tuple[str, str], ...]] = {
    "ebooks/metadata/epub.py": (
        (
            """            cpage = os.path.join(tdir, cpage)
            if not os.path.exists(cpage):
                return
""",
            """            cpage = os.path.join(tdir, cpage)
            if not os.path.exists(cpage):
                return
            # LiuXin: only a page inside the extracted book can be its cover. A
            # spine entry climbing out of the book would read a file outside it.
            from calibre.utils.filenames import is_existing_subpath

            if not is_existing_subpath(cpage, tdir):  # LiuXin:
                return
""",
        ),
    ),
    "ebooks/metadata/mobi.py": (
        (
            """            mi.cover_data = ('jpg', save_cover_data_to(data))
""",
            """            # LiuXin: keep the cover as the book stores it. calibre re-encodes it
            # as a JPEG no larger than 1650x2200, which needs Qt; LiuXin names
            # the format from the bytes instead.
            mi.cover_data = ('jpg', data)  # LiuXin:
""",
        ),
    ),
    "ebooks/mobi/reader/headers.py": (
        (
            """            idx, size = struct.unpack('>LL', raw[pos : pos + 8])
            content = raw[pos + 8 : pos + size]
""",
            """            idx, size = struct.unpack('>LL', raw[pos : pos + 8])
            # LiuXin: a record claiming size 0 would be read again for every item
            # the EXTH header claims, up to 2**32 times.
            if size == 0:  # LiuXin:
                break
            content = raw[pos + 8 : pos + size]
""",
        ),
    ),
    "ebooks/mobi/huffcdic.py": (
        (
            r"""from calibre.ebooks.mobi import MobiError
""",
            r"""from calibre.ebooks.mobi import MobiError

# LiuXin: the most text one book may unpack to; see Reader.unpack.
MAX_UNPACKED = 32 * 1024 * 1024  # LiuXin:
""",
        ),
        (
            r"""        self.q = struct.Struct(b'>Q').unpack_from
""",
            r"""        self.q = struct.Struct(b'>Q').unpack_from
        self.unpacked = 0  # LiuXin: bytes unpacked so far, dictionary entries included
""",
        ),
        (
            r"""            s.append(slice_)
        return b''.join(s)
""",
            r"""            s.append(slice_)
            # LiuXin: dictionary entries unpack to other entries, so a few bytes
            # can stand for gigabytes. The metadata reader unpacks text only for
            # books under 4 MB, whose text is a small part of this limit.
            self.unpacked += len(slice_)  # LiuXin:
            if self.unpacked > MAX_UNPACKED:  # LiuXin:
                raise MobiError('HUFF/CDIC text unpacks to more than %d bytes' % MAX_UNPACKED)
        return b''.join(s)
""",
        ),
    ),
    "ebooks/chardet.py": (
        (
            r"""from calibre import xml_replace_entities
""",
            r"""from calibre import xml_replace_entities
from LiuXin_alpha.metadata.upstream_calibre import _linear_time  # LiuXin:
""",
        ),
        (
            r"""        yield re.compile(raw, flags=re.IGNORECASE)
""",
            r"""        # LiuXin: two of these patterns retry every quote after every encoding=
        # or content= against the rest of the tag: over half an hour on 50 KB of
        # unclosed declarations. linear_pattern finds the same matches in linear time.
        yield _linear_time.linear_pattern(re.compile(raw, flags=re.IGNORECASE))  # LiuXin:
""",
        ),
    ),
    "ebooks/mobi/reader/mobi6.py": (
        (
            r"""from calibre.utils.xml_parse import safe_html_fromstring
""",
            r"""from calibre.utils.xml_parse import safe_html_fromstring
from LiuXin_alpha.metadata.upstream_calibre import _linear_time  # LiuXin:
""",
        ),
        (
            r"""    PAGE_BREAK_PAT = re.compile(
        r'<\s*/{0,1}\s*mbp:pagebreak((?:\s+[^/>]*){0,1})/{0,1}\s*>\s*(?:<\s*/{0,1}\s*mbp:pagebreak\s*/{0,1}\s*>)*',
        re.IGNORECASE,
    )
""",
            r"""    # LiuXin: calibre's pattern, with its runs of whitespace and its attributes
    # read possessively. They can end only one way, but calibre's pattern tried
    # every split of a long run of spaces: minutes for a few kilobytes.
    PAGE_BREAK_PAT = re.compile(
        r'<\s*+/{0,1}\s*+mbp:pagebreak((?>(?:\s+[^/>]*){0,1}))/{0,1}\s*>\s*(?:<\s*+/{0,1}\s*+mbp:pagebreak\s*+/{0,1}\s*>)*',  # LiuXin:
        re.IGNORECASE,
    )
""",
        ),
        (
            r"""        for i in range(self.num_sections):
            self.sections.append((section(i), self.section_headers[i]))
""",
            r"""        # LiuXin: a section runs to the next one's offset, so a table whose offsets
        # go back and forth holds the same bytes again for each entry: 65535 copies
        # of a few megabytes. Sections of a well-formed book do not overlap; they
        # may hold twice the file before it is refused.
        held = 0  # LiuXin:
        for i in range(self.num_sections):
            self.sections.append((section(i), self.section_headers[i]))
            held += len(self.sections[-1][0])  # LiuXin:
            if held > 2 * len(raw_bytes):  # LiuXin:
                raise MobiError('MOBI sections overlap')
""",
        ),
        (
            r"""        self.processed_html = re.sub(r'</{0,1}[a-zA-Z]+:\s+[^>]*>', '', self.processed_html)
""",
            r"""        # LiuXin: tries after the last '>' scan to the end and fail; they are not made.
        self.processed_html = _linear_time.sub_before_last(re.compile(r'</{0,1}[a-zA-Z]+:\s+[^>]*>'), '', self.processed_html, '>')  # LiuXin:
""",
        ),
        (
            r"""            self.processed_html = re.sub(r'<metadata>.+?</metadata>', '', self.processed_html, flags=re.I)
            self.processed_html = re.sub(r'<guide>.+?</guide>', '', self.processed_html, flags=re.I)
""",
            r"""            # LiuXin: a failed try scans to the end of its line, as would every later
            # try on that line; they are not made.
            self.processed_html = _linear_time.sub_skipping_failures(  # LiuXin:
                re.compile(r'<metadata>.+?</metadata>', re.I), '', self.processed_html, re.compile(r'<metadata>', re.I), '\n'
            )
            self.processed_html = _linear_time.sub_skipping_failures(  # LiuXin:
                re.compile(r'<guide>.+?</guide>', re.I), '', self.processed_html, re.compile(r'<guide>', re.I), '\n'
            )
""",
        ),
        (
            r"""                    anchors = root.xpath(f'//*[@id="{href}"]')
""",
            r"""                    # LiuXin: the href is passed as a value, not pasted into the XPath,
                    # where a quote in it failed the read and the rest ran as XPath.
                    anchors = root.xpath('//*[@id=$id]', id=href)  # LiuXin:
""",
        ),
        (
            r"""        self.processed_html = re.sub(r'<\?xml[^>]*>', '', self.processed_html)
        self.processed_html = re.sub(r'<\s*(/?)\s*o:p[^>]*>', '', self.processed_html)
        # Swap inline and block level elements, and order block level elements according to priority
        # - lxml and beautifulsoup expect/assume a specific order based on xhtml spec
        self.processed_html = re.sub(
            r'(?i)(?P<styletags>(<(h\d+|i|b|u|em|small|big|strong|tt)>\s*){1,})(?P<para><p[^>]*>)',
            r'\g<para>\g<styletags>',
            self.processed_html,
        )
        self.processed_html = re.sub(
            r'(?i)(?P<para></p[^>]*>)\s*(?P<styletags>(</(h\d+|i|b|u|em|small|big|strong|tt)>\s*){1,})',
            r'\g<styletags>\g<para>',
            self.processed_html,
        )
        self.processed_html = re.sub(
            r'(?i)(?P<blockquote>(</(blockquote|div)[^>]*>\s*){1,})(?P<para></p[^>]*>)',
            r'\g<para>\g<blockquote>',
            self.processed_html,
        )
        self.processed_html = re.sub(
            r'(?i)(?P<para><p[^>]*>)\s*(?P<blockquote>(<(blockquote|div)[^>]*>\s*){1,})',
            r'\g<blockquote>\g<para>',
            self.processed_html,
        )
""",
            r"""        # LiuXin: these passes give calibre's results in linear time; calibre's
        # took hours on crafted books of a few megabytes. Its tries scanned to a
        # '>' that never came, tried every tag of a long run, and split runs of
        # spaces every way. Tries that must fail are not made, and the o:p
        # pattern reads its spaces possessively.
        self.processed_html = _linear_time.sub_before_last(re.compile(r'<\?xml[^>]*>'), '', self.processed_html, '>')  # LiuXin:
        self.processed_html = _linear_time.sub_before_last(re.compile(r'<\s*+(/?)\s*+o:p[^>]*>'), '', self.processed_html, '>')  # LiuXin:
        # Swap inline and block level elements, and order block level elements according to priority
        # - lxml and beautifulsoup expect/assume a specific order based on xhtml spec
        self.processed_html = _linear_time.sub_from_run_starts(  # LiuXin:
            re.compile(r'(?i)(?P<styletags>(<(h\d+|i|b|u|em|small|big|strong|tt)>\s*){1,})(?P<para><p[^>]*>)'),
            r'\g<para>\g<styletags>',
            self.processed_html,
            re.compile(r'(?i)(?:<(?:h\d+|i|b|u|em|small|big|strong|tt)>\s*)+'),
        )
        self.processed_html = _linear_time.sub_skipping_failures(  # LiuXin:
            re.compile(r'(?i)(?P<para></p[^>]*>)\s*(?P<styletags>(</(h\d+|i|b|u|em|small|big|strong|tt)>\s*){1,})'),
            r'\g<styletags>\g<para>',
            self.processed_html,
            re.compile(r'(?i)</p'),
            '>',
        )
        self.processed_html = _linear_time.sub_from_run_starts(  # LiuXin:
            re.compile(r'(?i)(?P<blockquote>(</(blockquote|div)[^>]*>\s*){1,})(?P<para></p[^>]*>)'),
            r'\g<para>\g<blockquote>',
            self.processed_html,
            re.compile(r'(?i)(?:</(?:blockquote|div)[^>]*>\s*)+'),
            '>',
        )
        self.processed_html = _linear_time.sub_skipping_failures(  # LiuXin:
            re.compile(r'(?i)(?P<para><p[^>]*>)\s*(?P<blockquote>(<(blockquote|div)[^>]*>\s*){1,})'),
            r'\g<blockquote>\g<para>',
            self.processed_html,
            re.compile(r'(?i)<p'),
            '>',
        )
""",
        ),
        (
            r"""        BLOCK_TAGS = {'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'div', 'p'}
""",
            r"""        BLOCK_TAGS = {'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'div', 'p'}
        # LiuXin: the first class given each rule, for looking rules up below.
        classes_by_rule: dict[str, str] = {}  # LiuXin:
        for sel, srule in self.tag_css_rules.items():  # LiuXin:
            classes_by_rule.setdefault(srule, sel)
""",
        ),
        (
            r"""                ncls = None
                rule = '; '.join(styles)
                for sel, srule in self.tag_css_rules.items():
                    if srule == rule:
                        ncls = sel
                        break
                if ncls is None:
                    ncls = f'calibre_{i}'
                    self.tag_css_rules[ncls] = rule
""",
            r"""                rule = '; '.join(styles)
                # LiuXin: the rule is looked up, not compared with every rule so far,
                # which took minutes for a book of many differently styled tags.
                ncls = classes_by_rule.get(rule)  # LiuXin:
                if ncls is None:
                    ncls = f'calibre_{i}'
                    self.tag_css_rules[ncls] = rule
                    classes_by_rule.setdefault(rule, ncls)  # LiuXin:
""",
        ),
        (
            r"""            if images and hasattr(parent, 'find'):
                index = parent.index(tag)
                for img in images:
                    img.getparent().remove(img)
                    img.tail = img.text = None
                    parent.insert(index, img)
""",
            r"""            if images and hasattr(parent, 'find'):
                # LiuXin: each image goes where inserting it at the svg's index puts
                # it, before the one put there last, without counting the svg's
                # siblings again for each: books of many svgs took minutes.
                before = tag  # LiuXin:
                for img in images:
                    img.getparent().remove(img)
                    img.tail = img.text = None
                    before.addprevious(img)  # LiuXin:
                    before = img  # LiuXin:
""",
        ),
        (
            r"""            p.insert(p.index(tag) + 1, a)
""",
            r"""            tag.addnext(a)  # LiuXin: where p.insert(p.index(tag) + 1, a) puts it, without counting siblings
""",
        ),
        (
            r"""            elems = root.xpath('//*[@id="{}"]'.format(toc.partition('#')[-1]))
""",
            r"""            # LiuXin: the fragment is passed as a value, not pasted into the XPath.
            elems = root.xpath('//*[@id=$id]', id=toc.partition('#')[-1])  # LiuXin:
""",
        ),
        (
            r"""        self.processed_html = self.PAGE_BREAK_PAT.sub(r'<div \1 class="mbp_pagebreak" />', cast('str', self.processed_html))
""",
            r"""        self.processed_html = _linear_time.sub_page_breaks(  # LiuXin: linear time
            self.PAGE_BREAK_PAT, r'<div \1 class="mbp_pagebreak" />', cast('str', self.processed_html)
        )
""",
        ),
        (
            r"""        link_pattern = re.compile(rb'''<[^<>]+filepos=['"]{0,1}(\d+)[^<>]*>''', re.IGNORECASE)
""",
            r"""        # LiuXin: calibre's pattern, first checking that the tag closes. An open
        # tag was retried at every filepos= in it against the rest of the tag.
        link_pattern = re.compile(rb'''<(?=[^<>]*>)[^<>]+filepos=['"]{0,1}(\d+)[^<>]*>''', re.IGNORECASE)  # LiuXin:
""",
        ),
        (
            r"""        end_tag_re = re.compile(rb'<\s*/')
""",
            r"""        end_tag_re = re.compile(rb'<\s*/')
        # LiuXin: link targets come in order, so a search from one reuses what the
        # last found instead of crossing the same text again.
        finder = _linear_time.ForwardFinder(self.mobi_html)  # LiuXin:
""",
        ),
        (
            r"""            l = self.mobi_html.find(b'<', end)
            r = self.mobi_html.find(b'>', end)
""",
            r"""            l = finder.find(b'<', end)  # LiuXin:
            r = finder.find(b'>', end)  # LiuXin:
""",
        ),
        (
            r"""                p = self.mobi_html.rfind(b'<', 0, end + 1)
""",
            r"""                p = finder.rfind(b'<', end + 1)  # LiuXin:
""",
        ),
        (
            r"""        self.processed_html = re.sub(rb'&([^;]*?)(<a id="filepos\d+"></a>)([^;]*);', rb'&\1\3;\2', processed_html)
""",
            r"""        # LiuXin: tries after the last ';' fail, as does every try between an '&'
        # whose try failed and the next ';'; they are not made.
        self.processed_html = _linear_time.sub_skipping_failures(  # LiuXin:
            re.compile(rb'&([^;]*?)(<a id="filepos\d+"></a>)([^;]*);'), rb'&\1\3;\2', processed_html, re.compile(b'&'), b';', b';'
        )
""",
        ),
    ),
    "ebooks/metadata/html.py": (
        (
            """_handle_comment_pat: re.Pattern[str] | None = None
""",
            """_handle_comment_pat: re.Pattern[str] | None = None
_token_start_pat = re.compile(r'\\s\\S')  # LiuXin:


def _comment_matches(pat: re.Pattern[str], data: str) -> list[re.Match[str]]:  # LiuXin:
    # LiuXin: the same matches as pat.finditer(data), in linear time. finditer()
    # tries the pattern at every character, and on a long run of characters
    # without a space each try backtracks over the rest of the run, so a comment
    # of 150,000 characters took minutes. A match starting inside a run would
    # also match from the run's start, so only run starts need trying.
    ans = []
    pos = 0
    while pos < len(data):
        match = pat.match(data, pos)
        if match is not None:
            ans.append(match)
            pos = match.end()
            continue
        nxt = _token_start_pat.search(data, pos)
        if nxt is None:
            break
        pos = nxt.start() + 1
    return ans
""",
        ),
        (
            """    for match in _handle_comment_pat.finditer(data):
""",
            """    for match in _comment_matches(_handle_comment_pat, data):  # LiuXin: linear time
""",
        ),
    ),
}

# calibre modules ported whole: every name imported from them maps to the same
# name in the ported module.
MODULES = {
    "calibre.ebooks": f"{PKG}.ebooks",
    "calibre.ebooks.BeautifulSoup": f"{PKG}.beautifulsoup",
    "calibre.ebooks.metadata": f"{PKG}.metadata_base",
    "calibre.ebooks.metadata.book": f"{PKG}.book_fields",
    "calibre.ebooks.metadata.book.base": f"{PKG}.book_base",
    "calibre.ebooks.metadata.book.json_codec": f"{PKG}.json_codec",
    "calibre.library.field_metadata": f"{PKG}.field_metadata",
    "calibre.ebooks.compression.palmdoc": f"{PKG}.palmdoc",
    "calibre.ebooks.fb2": f"{PKG}.fb2_package",
    "calibre.ebooks.metadata.epub": f"{PKG}.epub",
    "calibre.ebooks.metadata.extz": f"{PKG}.extz",
    "calibre.ebooks.metadata.fb2": f"{PKG}.fb2",
    "calibre.ebooks.metadata.html": f"{PKG}.html",
    "calibre.ebooks.metadata.mobi": f"{PKG}.mobi",
    "calibre.ebooks.metadata.odt": f"{PKG}.odt",
    "calibre.ebooks.metadata.opf2": f"{PKG}.opf2",
    "calibre.ebooks.metadata.opf3": f"{PKG}.opf3",
    "calibre.ebooks.metadata.opf": f"{PKG}.opf",
    "calibre.ebooks.metadata.toc": f"{PKG}.toc",
    "calibre.ebooks.metadata.utils": f"{PKG}.metadata_utils",
    "calibre.ebooks.metadata.meta": f"{PKG}.meta",
    "calibre.ebooks.metadata.topaz": f"{PKG}.topaz",
    "calibre.ebooks.oeb.polish.pretty": f"{PKG}.pretty",
    "calibre.ebooks.chardet": f"{PKG}.chardet",
    "calibre.utils.cleantext": f"{PKG}.cleantext",
    "calibre.utils.config": f"{PKG}.config_base",
    "calibre.utils.config_base": f"{PKG}.config_base",
    "calibre.utils.date": f"{PKG}.date",
    "calibre.utils.iso8601": f"{PKG}.iso8601",
    "calibre.utils.imghdr": f"{PKG}.imghdr",
    "calibre.ebooks.mobi": f"{PKG}.mobi_package",
    "calibre.ebooks.mobi.huffcdic": f"{PKG}.huffcdic",
    "calibre.ebooks.mobi.langcodes": f"{PKG}.mobi_langcodes",
    "calibre.ebooks.mobi.reader.headers": f"{PKG}.mobi_headers",
    "calibre.ebooks.mobi.reader.mobi6": f"{PKG}.mobi6",
    "calibre.ebooks.mobi.utils": f"{PKG}.mobi_utils",
    "calibre.utils.html2text": f"{PKG}.html_to_text",
    "calibre.utils.localunzip": f"{PKG}.localunzip",
}

# Helpers from modules that are not ported whole: the names _compat provides,
# with any renaming.
NAMES: dict[str, dict[str, str]] = {
    "calibre": {
        name: name
        for name in (
            "prints",
            "guess_type",
            "guess_all_extensions",
            "CurrentDir",
            "strftime",
            "xml_replace_entities",
            "replace_entities",
            "force_unicode",
            "prepare_string_for_xml",
            "relpath",
            "isbytestring",
        )
    },
    "calibre.constants": {
        name: name
        for name in (
            "__appname__",
            "__version__",
            "filesystem_encoding",
            "preferred_encoding",
            "DEBUG",
            "iswindows",
            "ismacos",
        )
    },
    "calibre.ebooks.conversion.config": {"load_defaults": "load_defaults"},
    "calibre.ebooks.covers": {
        "calibre_cover2": "calibre_cover2",
        "generate_masthead": "covers_generate_masthead",
    },
    "calibre.ebooks.oeb.base": {
        name: name
        for name in (
            "DC",
            "OPF",
            "OPF2_NSMAP",
            "CALIBRE_NS",
            "DC11_NS",
            "OPF2_NS",
            "XML_NS",
            "SVG",
            "SVG_NS",
            "XLINK",
            "XPath",
            "OEBBook",
        )
    },
    "calibre.ebooks.oeb.polish.utils": {"guess_type": "polish_guess_type"},
    "calibre.spell": {"parse_lang_code": "parse_lang_code", "DictionaryLocale": "DictionaryLocale"},
    "calibre.utils": {"StreamOrPath": "StreamOrPath"},
    "calibre.utils.icu": {"lower": "icu_lower", "upper": "icu_upper", "sort_key": "sort_key"},
    "calibre.utils.localization": {
        name: name
        for name in (
            "_",
            "ngettext",
            "canonicalize_lang",
            "get_lang",
            "lang_as_iso639_1",
            "lcdata",
            "calibre_langcode_to_name",
            "load_iso3166",
        )
    },
    "calibre.utils.xml_parse": {
        "safe_xml_fromstring": "safe_xml_fromstring",
        "safe_html_fromstring": "safe_html_fromstring",
    },
    "calibre.utils.speedups": {"ReadOnlyFileBuffer": "ReadOnlyFileBuffer"},
    "calibre.utils.img": {
        name: name
        for name in (
            "save_cover_data_to",
            "image_to_data",
            "image_from_data",
            "png_data_to_gif_data",
            "gif_data_to_png_data",
            "AnimatedGIF",
            "resize_image",
            "scale_image",
        )
    },
    "calibre.utils.ipc.simple_worker": {"fork_job": "fork_job", "WorkerError": "WorkerError"},
    "calibre.utils.zipfile": {
        name: name
        for name in ("ZipFile", "BadZipfile", "safe_replace", "ZIP_DEFLATED", "ZIP_STORED", "ZipInfo")
    },
    "calibre.ptempfile": {
        "TemporaryDirectory": "TemporaryDirectory",
        "SpooledTemporaryFile": "SpooledTemporaryFile",
        "PersistentTemporaryFile": "PersistentTemporaryFile",
    },
    "calibre.utils.logging": {"default_log": "default_log", "Log": "Log"},
    "calibre.utils.formatter": {"TEMPLATE_ERROR": "TEMPLATE_ERROR"},
    "calibre.ebooks.metadata.book.formatter": {"SafeFormat": "SafeFormat"},
    "calibre.utils.filenames": {
        "is_windows_reserved_filename": "is_windows_reserved_filename",
        "is_existing_subpath": "is_existing_subpath",
    },
    "calibre.gui2": {"is_gui_thread": "is_gui_thread", "UNDEFINED_QDATETIME": "UNDEFINED_QDATETIME"},
    "calibre.gui2.ui": {"get_gui": "get_gui"},
    "polyglot.builtins": {"as_bytes": "as_bytes", "as_unicode": "as_unicode"},
    "polyglot.binary": {
        "as_base64_unicode": "as_base64_unicode",
        "from_base64_bytes": "from_base64_bytes",
    },
}

# Single names from elsewhere: C extensions, polyglot and standard-library
# imports that LiuXin replaces. An empty module imports the name as a module.
EXTERNAL: dict[tuple[str, str], tuple[str, str]] = {
    ("compression", "zlib"): ("", "zlib"),
    ("dateutil.parser", "parse"): (COMPAT, "dateutil_parse"),
    ("html5_parser", "parse"): (COMPAT, "html5_parse"),
    ("html5_parser.soup", "parse"): (COMPAT, "html5_soup_parse"),
    ("odf.draw", "Frame"): ("LiuXin_alpha.file_formats.odf.draw", "Frame"),
    ("odf.draw", "Image"): ("LiuXin_alpha.file_formats.odf.draw", "Image"),
    ("odf.namespaces", "DCNS"): ("LiuXin_alpha.file_formats.odf.namespaces", "DCNS"),
    ("odf.namespaces", "METANS"): ("LiuXin_alpha.file_formats.odf.namespaces", "METANS"),
    ("odf.namespaces", "OFFICENS"): ("LiuXin_alpha.file_formats.odf.namespaces", "OFFICENS"),
    ("odf.opendocument", "load"): ("LiuXin_alpha.file_formats.odf.opendocument", "load"),
    ("polyglot.urllib", "unquote"): ("urllib.parse", "unquote"),
    ("calibre_extensions", "speedup"): (PKG, "_speedups"),
    ("calibre_extensions.speedup", "clean_xml_chars"): (f"{PKG}._speedups", "clean_xml_chars"),
    ("calibre_extensions.speedup", "parse_iso8601"): (f"{PKG}._speedups", "parse_iso8601"),
    ("calibre_extensions.uchardet", "detect"): (COMPAT, "uchardet_detect"),
    ("calibre_extensions", "cPalmdoc"): (COMPAT, "cPalmdoc"),
    ("tinycss.color4", "parse_color_string"): (COMPAT, "parse_color_string"),
    ("calibre_extensions.usbobserver", "date_format"): (COMPAT, "macos_date_format"),
    ("zoneinfo", "ZoneInfo"): (COMPAT, "ZoneInfo"),
}

# Whole-module imports LiuXin replaces.
IMPORT_MODULES = {"tzlocal": f"from {COMPAT} import tzlocal"}

# Imports left as they are: only functions the readers do not call use them.
LEAVE = frozenset({"qt.core"})

REWRITTEN_ROOTS = frozenset(
    {
        "calibre",
        "polyglot",
        "calibre_extensions",
        "tzlocal",
        "zoneinfo",
        "compression",
        "html5_parser",
        "odf",
        "dateutil",
        "tinycss",
    }
)


def parenthesise_excepts(source: str) -> str:
    """
    Parenthesise ``except A, B:`` clauses (PEP 758) for Python before 3.14.

    Example:
        >>> parenthesise_excepts("try:\\n    x\\nexcept A, B as e:\\n    y\\n").splitlines()[2]
        'except (A, B) as e:'


    :param source: Python source.
    :return: The source with such clauses parenthesised.
    """
    return re.sub(
        r"^(\s*)except ([^():\n]+,[^():\n]+?)( as \w+)?:",
        r"\1except (\2)\3:",
        source,
        flags=re.M,
    )


def drop_type_parameter_defaults(source: str) -> str:
    """
    Drop the defaults of type parameters (PEP 696) for Python before 3.13.

    Only type checkers read them, so the code runs the same without them.

    Example:
        >>> drop_type_parameter_defaults("class A[T = str]:\\n    pass\\n").splitlines()[0]
        'class A[T]:'


    :param source: Python source, parsed with Python 3.13 or later.
    :return: The source without type-parameter defaults.
    :raises SystemExit: For a default not on one line with its ``=``.
    """
    lines = source.split("\n")
    cuts: list[tuple[int, int, int]] = []
    for node in ast.walk(ast.parse(source)):
        default = getattr(node, "default_value", None)
        if not isinstance(node, ast.TypeVar | ast.ParamSpec | ast.TypeVarTuple) or default is None:
            continue
        line = lines[default.lineno - 1]
        encoded = line.encode()
        start = len(encoded[: default.col_offset].decode())
        if default.end_lineno != default.lineno or "=" not in line[:start]:
            raise SystemExit(f"cannot drop the type-parameter default on line {default.lineno}")
        end = len(encoded[: default.end_col_offset].decode())
        cut = len(line[: line.rindex("=", 0, start)].rstrip())
        cuts.append((default.lineno - 1, cut, end))
    for row, cut, end in sorted(cuts, reverse=True):
        lines[row] = lines[row][:cut] + lines[row][end:]
    return "\n".join(lines)


def rewrite_import(node: ast.Import | ast.ImportFrom, line: str, typing_only: bool) -> list[str]:
    """
    Rewrite one import statement by the tables.

    Imports used only for type checking that the tables do not cover become
    ``typing.Any``.

    Example:
        >>> node = ast.parse("from calibre.utils.icu import lower").body[0]
        >>> rewrite_import(node, "from calibre.utils.icu import lower", False)
        ['from LiuXin_alpha.metadata.upstream_calibre._compat import icu_lower as lower']


    :param node: The import.
    :param line: Its first source line, for the indentation.
    :param typing_only: Whether it sits in an ``if TYPE_CHECKING:`` block.
    :return: The replacement lines.
    :raises SystemExit: For an import the tables do not cover.
    """
    indent = re.match(r"\s*", line).group(0)  # type: ignore[union-attr]
    out = []
    if isinstance(node, ast.Import):
        for alias in node.names:
            if alias.name not in IMPORT_MODULES or alias.asname is not None:
                raise SystemExit(f"no mapping for import {alias.name}")
            out.append(indent + IMPORT_MODULES[alias.name])
        return out
    module = node.module or ""
    for alias in node.names:
        local = alias.asname or alias.name
        if (module, alias.name) in EXTERNAL:
            target_module, target_name = EXTERNAL[(module, alias.name)]
        elif module in MODULES:
            target_module, target_name = MODULES[module], alias.name
        elif module in NAMES and alias.name in NAMES[module]:
            target_module, target_name = COMPAT, NAMES[module][alias.name]
        elif typing_only:
            out.append(f"{indent}from typing import Any as {local}")
            continue
        else:
            raise SystemExit(f"no mapping for {module}.{alias.name}")
        suffix = f" as {local}" if local != target_name else ""
        if not target_module:
            out.append(f"{indent}import {target_name}{suffix}")
            continue
        out.append(f"{indent}from {target_module} import {target_name}{suffix}")
    return out


def apply_patches(source: str, source_path: str) -> str:
    """
    Apply the LiuXin fixes listed for one calibre module.

    Example:
        >>> apply_patches("x = 1\\n", "utils/imghdr.py")
        'x = 1\\n'


    :param source: The calibre module's source.
    :param source_path: Its path under ``src/calibre``.
    :return: The source with the fixes applied.
    :raises SystemExit: If the calibre code a fix replaces is not there once.
    """
    for old, new in PATCHES.get(source_path, ()):
        if source.count(old) != 1:
            raise SystemExit(f"the LiuXin fix for {source_path} no longer matches calibre's code")
        source = source.replace(old, new)
    return source


def port_source(source: str, source_path: str, extra_header: tuple[str, ...] = ()) -> str:
    """
    Turn one calibre module's source into the ported module's source.

    Example:
        >>> print(port_source("from calibre import prints\\n", "x.py").splitlines()[-1])
        from LiuXin_alpha.metadata.upstream_calibre._compat import prints


    :param source: The calibre module's source.
    :param source_path: Its path under ``src/calibre``, for the header.
    :param extra_header: More lines for the header comment.
    :return: The ported source.
    """
    patched = apply_patches(source, source_path)
    excepts_fixed = parenthesise_excepts(patched)
    fixed = drop_type_parameter_defaults(excepts_fixed)
    tree = ast.parse(fixed)
    lines = fixed.split("\n")
    typing_lines: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.If) and getattr(node.test, "id", None) == "TYPE_CHECKING":
            for child in node.body:
                typing_lines.update(range(child.lineno, (child.end_lineno or child.lineno) + 1))
    replacements = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module in LEAVE or (node.module or "").split(".")[0] not in REWRITTEN_ROOTS:
                continue
        elif isinstance(node, ast.Import):
            if all(alias.name.split(".")[0] not in REWRITTEN_ROOTS for alias in node.names):
                continue
        else:
            continue
        new = rewrite_import(node, lines[node.lineno - 1], node.lineno in typing_lines)
        replacements.append((node.lineno, node.end_lineno or node.lineno, new))
    for start, end, new in sorted(replacements, reverse=True):
        lines[start - 1 : end] = new
    text = "\n".join(lines)
    if "from __future__ import annotations" not in text:
        tree = ast.parse(text)
        lines = text.split("\n")
        first = tree.body[0] if tree.body else None
        if isinstance(first, ast.Expr) and isinstance(getattr(first, "value", None), ast.Constant):
            lines[first.end_lineno : first.end_lineno] = ["", "from __future__ import annotations"]  # type: ignore[index]
        else:
            at = next((i for i, row in enumerate(lines) if row.strip() and not row.startswith("#")), len(lines))
            lines[at:at] = ["from __future__ import annotations", ""]
        text = "\n".join(lines)
    lines = text.split("\n")
    at = 0
    while at < len(lines) and lines[at].startswith("#"):
        at += 1
    header = [
        f"# Ported from calibre (https://calibre-ebook.com) 9.18: src/calibre/{source_path}",
        "# Changes from upstream: calibre imports point at upstream_calibre and its _compat, and",
        "# 'from __future__ import annotations' is added.",
    ]
    if excepts_fixed != patched:
        header.append("# Except clauses naming several exceptions are parenthesised, for Python before 3.14.")
    if fixed != excepts_fixed:
        header.append("# Type-parameter defaults are dropped, for Python before 3.13.")
    if patched != source:
        header.append("# LiuXin fixes are marked '# LiuXin:'.")
    header += [f"# {line}" for line in extra_header]
    header.append("# SPDX-License-Identifier: GPL-3.0-only")
    lines[at:at] = header
    out = "\n".join(lines)
    ast.parse(out)
    return out


def main() -> None:
    """
    Port every module in ``PORTS`` from the calibre tree given.

    Example:
        Run from the command line; see the module docstring.


    :return: None.
    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("calibre", type=Path, help="a calibre source checkout")
    args = parser.parse_args()
    root = args.calibre / "src" / "calibre"
    for source_path, name, extra in PORTS:
        source = (root / source_path).read_text(encoding="utf-8")
        (DESTINATION / name).write_text(port_source(source, source_path, extra), encoding="utf-8")
        print(f"ported {source_path} -> {name}")


if __name__ == "__main__":
    main()
