#!/usr/bin/env python
# vim:fileencoding=utf-8

from __future__ import unicode_literals, division, absolute_import, print_function

import re
import os
from bisect import bisect

from LiuXin_alpha.utils.mine_types import guess_type as _guess_type
from LiuXin_alpha.utils.text.xml_utils import prepare_string_for_xml, replace_entities
from LiuXin_alpha.utils.language_tools.icu import upper as icu_upper

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"


def guess_type(x):
    return _guess_type(x)[0] or "application/octet-stream"


def setup_cssutils_serialization(tab_width=2):
    try:
        import cssutils
    except ModuleNotFoundError:
        return

    prefs = cssutils.ser.prefs
    prefs.indent = tab_width * " "
    prefs.indentClosingBrace = False
    prefs.omitLastSemicolon = False


def actual_case_for_name(container, name):
    from LiuXin_alpha.utils.storage.local.filenames import samefile

    if not container.exists(name):
        raise ValueError("Cannot get actual case for %s as it does not exist" % name)
    parts = name.split("/")
    ans = []
    for i, x in enumerate(parts):
        base = "/".join(ans + [x])
        path = container.name_to_abspath(base)
        pdir = os.path.dirname(path)
        candidates = {os.path.join(pdir, q) for q in os.listdir(pdir)}
        if x in candidates:
            correctx = x
        else:
            for q in candidates:
                if samefile(q, path):
                    correctx = os.path.basename(q)
                    break
            else:
                raise RuntimeError("Something bad happened")
        ans.append(correctx)
    return "/".join(ans)


def corrected_case_for_name(container, name):
    parts = name.split("/")
    ans = []
    for i, x in enumerate(parts):
        base = "/".join(ans + [x])
        if container.exists(base):
            correctx = x
        else:
            try:
                candidates = {q for q in os.listdir(os.path.dirname(container.name_to_abspath(base)))}
            except EnvironmentError:
                return None  # one of the non-terminal components of name is a file instead of a directory
            for q in candidates:
                if q.lower() == x.lower():
                    correctx = q
                    break
            else:
                return None
        ans.append(correctx)
    return "/".join(ans)


class PositionFinder(object):
    def __init__(self, raw):
        pat = rb"\n" if isinstance(raw, bytes) else r"\n"
        self.new_lines = tuple(m.start() + 1 for m in re.finditer(pat, raw))

    def __call__(self, pos):
        lnum = bisect(self.new_lines, pos)
        try:
            offset = abs(pos - self.new_lines[lnum - 1])
        except IndexError:
            offset = pos
        return lnum + 1, offset


class CommentFinder(object):
    def __init__(self, raw, pat=r"(?s)/\*.*?\*/"):
        self.starts, self.ends = [], []
        for m in re.finditer(pat, raw):
            start, end = m.span()
            self.starts.append(start), self.ends.append(end)

    def __call__(self, offset):
        if not self.starts:
            return False
        q = bisect(self.starts, offset) - 1
        return q >= 0 and self.starts[q] <= offset <= self.ends[q]


def link_stylesheets(container, names, sheets, remove=False, mtype="text/css"):
    from LiuXin_alpha.file_formats.oeb.base import XPath, XHTML

    changed_names = set()
    snames = set(sheets)
    lp = XPath("//h:link[@href]")
    hp = XPath("//h:head")
    for name in names:
        root = container.parsed(name)
        if remove:
            for link in lp(root):
                if (link.get("type", mtype) or mtype) == mtype:
                    container.remove_from_xml(link)
                    changed_names.add(name)
                    container.dirty(name)
        existing = {
            container.href_to_name(l.get("href"), name) for l in lp(root) if (l.get("type", mtype) or mtype) == mtype
        }
        extra = snames - existing
        if extra:
            changed_names.add(name)
            try:
                parent = hp(root)[0]
            except (TypeError, IndexError):
                parent = root.makeelement(XHTML("head"))
                container.insert_into_xml(root, parent, index=0)
            for sheet in sheets:
                if sheet in extra:
                    container.insert_into_xml(
                        parent,
                        parent.makeelement(
                            XHTML("link"),
                            rel="stylesheet",
                            type=mtype,
                            href=container.name_to_href(sheet, name),
                        ),
                    )
            container.dirty(name)

    return changed_names


def lead_text(top_elem, num_words=10):
    """
    Return the leading text contained in top_elem (including descendants)
    upto a maximum of num_words words. More efficient than using
    etree.tostring(method='text') as it does not have to serialize the entire
    sub-tree rooted at top_elem.
    :param top_elem:
    :param num_words:
    :return:
    """
    pat = re.compile(r"\s+", flags=re.UNICODE)
    words = []

    def get_text(x, local_attr="text"):
        ans = getattr(x, local_attr)
        if ans:
            words.extend(filter(None, pat.split(ans)))

    stack = [(top_elem, "text")]
    while stack and len(words) < num_words:
        elem, attr = stack.pop()
        get_text(elem, attr)
        if attr == "text":
            if elem is not top_elem:
                stack.append((elem, "tail"))
            stack.extend(reversed(list((c, "text") for c in elem.iterchildren("*"))))
    return " ".join(words[:num_words])


class SimpleCSSRule(object):
    CHARSET_RULE = 2
    STYLE_RULE = 1

    def __init__(self, css_text):
        self.cssText = css_text
        stripped = css_text.lstrip().lower()
        self.type = self.CHARSET_RULE if stripped.startswith("@charset") else self.STYLE_RULE


class SimpleCSSStyleSheet(object):
    def __init__(self, text=""):
        self.cssRules = []
        self.cssText = ""
        self.set_css_text(text)

    def _parse_rules(self, text):
        matches = re.findall(r"@charset\s+[^;]+;|[^{}]+{[^{}]*}", text, flags=re.I | re.S)
        if not matches:
            stripped = text.strip()
            matches = [stripped] if stripped else []
        return [SimpleCSSRule(x.strip()) for x in matches if x.strip()]

    def set_css_text(self, text):
        self.cssText = text or ""
        self.cssRules = self._parse_rules(self.cssText)

    def add(self, rule):
        self.cssRules.append(SimpleCSSRule(getattr(rule, "cssText", str(rule))))
        self.cssText = "\n".join(r.cssText for r in self.cssRules)

    def deleteRule(self, index):
        del self.cssRules[index]
        self.cssText = "\n".join(r.cssText for r in self.cssRules)


def parse_css(
    data,
    fname="<string>",
    is_declaration=False,
    decode=None,
    log_level=None,
    css_preprocessor=None,
):
    if log_level is None:
        import logging

        log_level = logging.WARNING
    data = data or ""
    if isinstance(data, bytes):
        data = data.decode("utf-8") if decode is None else decode(data)
    if css_preprocessor is not None:
        data = css_preprocessor(data)
    try:
        from cssutils import CSSParser, log
        from LiuXin_alpha.file_formats.oeb.base import _css_logger
    except ModuleNotFoundError:
        # Lightweight fallback for environments without cssutils.
        return SimpleCSSStyleSheet(data)
    log.setLevel(log_level)
    log.raiseExceptions = False
    # fetcher is a set to a lmabda function - We dont care about @import rules
    parser = CSSParser(loglevel=log_level, fetcher=lambda x: (None, None), log=_css_logger)
    if is_declaration:
        data = parser.parseStyle(data, validate=False)
    else:
        data = parser.parseString(data, href=fname, validate=False)
    return data


def handle_entities(text, func):
    return prepare_string_for_xml(func(replace_entities(text)))


def apply_func_to_match_groups(match, func=icu_upper, handle_entities=handle_entities):
    """
    Apply the specified function to individual groups in the match object (the result of re.search() or
    the whole match if no groups were defined. Returns the replaced string.
    :param match:
    :param func:
    :param handle_entities:
    :return:
    """
    found_groups = False
    i = 0
    parts, pos = [], match.start()

    def f(text):
        return handle_entities(text, func)

    while True:
        i += 1
        try:
            start, end = match.span(i)
        except IndexError:
            break
        found_groups = True
        if start > -1:
            parts.append(match.string[pos:start])
            parts.append(f(match.string[start:end]))
            pos = end
    if not found_groups:
        return f(match.group())
    parts.append(match.string[pos : match.end()])
    return "".join(parts)
