#!/usr/bin/env python
# vim:fileencoding=utf-8

"""
Provide shared EPUB/OEB polishing helpers and resource operations.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise utils through a consuming regression::

        python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

import re
import os
from bisect import bisect

from LiuXin_alpha.utils.mine_types import guess_type as _guess_type
from LiuXin_alpha.utils.text.xml_utils import prepare_string_for_xml, replace_entities
from LiuXin_alpha.utils.language_tools.icu import upper as icu_upper

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"


def guess_type(x: _typing.Any) -> bool:
    """
    Perform the guess type operation under explicit file-format and conversion rules.

    Example:
        Exercise guess type through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param x: Value supplied for x under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return _guess_type(x)[0] or "application/octet-stream"


def setup_cssutils_serialization(tab_width: int = 2) -> None:
    """
    Perform the setup cssutils serialization operation under explicit file-format and conversion rules.

    Example:
        Exercise setup cssutils serialization through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param tab_width: Value supplied for tab width under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    try:
        import cssutils
    except ModuleNotFoundError:
        return

    prefs = cssutils.ser.prefs
    prefs.indent = tab_width * " "
    prefs.indentClosingBrace = False
    prefs.omitLastSemicolon = False


def actual_case_for_name(container: _typing.Any, name: _typing.Any) -> _typing.Any:
    """
    Perform the actual case for name operation under explicit file-format and conversion rules.

    Example:
        Exercise actual case for name through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param container: Value supplied for container under the utility contract.
    :param name: Field, file, function or resource name addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
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


def corrected_case_for_name(container: _typing.Any, name: _typing.Any) -> _typing.Any:
    """
    Perform the corrected case for name operation under explicit file-format and conversion rules.

    Example:
        Exercise corrected case for name through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param container: Value supplied for container under the utility contract.
    :param name: Field, file, function or resource name addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
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
    """
    Provide the positionfinder contract for validated ebook processing.

    Example:
        Exercise PositionFinder through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    def __init__(self: _typing.Self, raw: _typing.Any) -> None:
        """
        Initialize and validate the positionfinder state.

        Example:
            Exercise PositionFinder.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param raw: Value supplied for raw under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        pat = rb"\n" if isinstance(raw, bytes) else r"\n"
        self.new_lines = tuple(m.start() + 1 for m in re.finditer(pat, raw))

    def __call__(self: _typing.Self, pos: _typing.Any) -> tuple[_typing.Any, ...]:
        """
        Perform the call operation under explicit file-format and conversion rules.

        Example:
            Exercise PositionFinder.  call   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param pos: Value supplied for pos under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        lnum = bisect(self.new_lines, pos)
        try:
            offset = abs(pos - self.new_lines[lnum - 1])
        except IndexError:
            offset = pos
        return lnum + 1, offset


class CommentFinder(object):
    """
    Provide the commentfinder contract for validated ebook processing.

    Example:
        Exercise CommentFinder through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    def __init__(self: _typing.Self, raw: _typing.Any, pat: str = r"(?s)/\*.*?\*/") -> None:
        """
        Initialize and validate the commentfinder state.

        Example:
            Exercise CommentFinder.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param raw: Value supplied for raw under the utility contract.
        :param pat: Value supplied for pat under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.starts, self.ends = [], []
        for m in re.finditer(pat, raw):
            start, end = m.span()
            self.starts.append(start), self.ends.append(end)

    def __call__(self: _typing.Self, offset: _typing.Any) -> bool:
        """
        Perform the call operation under explicit file-format and conversion rules.

        Example:
            Exercise CommentFinder.  call   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param offset: Value supplied for offset under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not self.starts:
            return False
        q = bisect(self.starts, offset) - 1
        return q >= 0 and self.starts[q] <= offset <= self.ends[q]


def link_stylesheets(container: _typing.Any, names: _typing.Any, sheets: _typing.Any, remove: bool = False, mtype: str = "text/css") -> _typing.Any:
    """
    Perform the link stylesheets operation under explicit file-format and conversion rules.

    Example:
        Exercise link stylesheets through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param container: Value supplied for container under the utility contract.
    :param names: Value supplied for names under the utility contract.
    :param sheets: Value supplied for sheets under the utility contract.
    :param remove: Value supplied for remove under the utility contract.
    :param mtype: Value supplied for mtype under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
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


def lead_text(top_elem: _typing.Any, num_words: int = 10) -> _typing.Any:
    """
    Return the leading text contained in top_elem (including descendants) upto a maximum of num_words words. More efficient than using etree.tostring(method='text') as it does not have to serialize the entire sub-tree rooted at top_elem.

    Example:
        Exercise lead text through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param top_elem: Value supplied for top elem under the utility contract.
    :param num_words: Value supplied for num words under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    pat = re.compile(r"\s+", flags=re.UNICODE)
    words = []

    def get_text(x: _typing.Any, local_attr: str = "text") -> None:
        """
        Return text under the format's safety and compatibility rules.

        Example:
            Exercise lead text.get text through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param x: Value supplied for x under the utility contract.
        :param local_attr: Value supplied for local attr under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
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
    """
    Provide the simplecssrule contract for validated ebook processing.

    Example:
        Exercise SimpleCSSRule through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    CHARSET_RULE = 2
    STYLE_RULE = 1

    def __init__(self: _typing.Self, css_text: _typing.Any) -> None:
        """
        Initialize and validate the simplecssrule state.

        Example:
            Exercise SimpleCSSRule.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param css_text: Value supplied for css text under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.cssText = css_text
        stripped = css_text.lstrip().lower()
        self.type = self.CHARSET_RULE if stripped.startswith("@charset") else self.STYLE_RULE


class SimpleCSSStyleSheet(object):
    """
    Provide the simplecssstylesheet contract for validated ebook processing.

    Example:
        Exercise SimpleCSSStyleSheet through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    def __init__(self: _typing.Self, text: str = "") -> None:
        """
        Initialize and validate the simplecssstylesheet state.

        Example:
            Exercise SimpleCSSStyleSheet.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param text: Text parsed, normalized or rendered.
        :return: None; validated state is stored on the receiving object.
        """
        self.cssRules = []
        self.cssText = ""
        self.set_css_text(text)

    def _parse_rules(self: _typing.Self, text: _typing.Any) -> _typing.Any:
        """
        Parse rules under the format's safety and compatibility rules.

        Example:
            Exercise SimpleCSSStyleSheet. parse rules through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param text: Text parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        matches = re.findall(r"@charset\s+[^;]+;|[^{}]+{[^{}]*}", text, flags=re.I | re.S)
        if not matches:
            stripped = text.strip()
            matches = [stripped] if stripped else []
        return [SimpleCSSRule(x.strip()) for x in matches if x.strip()]

    def set_css_text(self: _typing.Self, text: _typing.Any) -> None:
        """
        Set css text under the format's safety and compatibility rules.

        Example:
            Exercise SimpleCSSStyleSheet.set css text through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param text: Text parsed, normalized or rendered.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.cssText = text or ""
        self.cssRules = self._parse_rules(self.cssText)

    def add(self: _typing.Self, rule: _typing.Any) -> None:
        """
        Perform the add operation under explicit file-format and conversion rules.

        Example:
            Exercise SimpleCSSStyleSheet.add through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param rule: Value supplied for rule under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.cssRules.append(SimpleCSSRule(getattr(rule, "cssText", str(rule))))
        self.cssText = "\n".join(r.cssText for r in self.cssRules)

    def deleteRule(self: _typing.Self, index: _typing.Any) -> None:
        """
        Perform the deleteRule operation under explicit file-format and conversion rules.

        Example:
            Exercise SimpleCSSStyleSheet.deleteRule through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param index: Value supplied for index under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        del self.cssRules[index]
        self.cssText = "\n".join(r.cssText for r in self.cssRules)


def parse_css(
    data: _typing.Any,
    fname: str = "<string>",
    is_declaration: bool = False,
    decode: _typing.Any = None,
    log_level: _typing.Any = None,
    css_preprocessor: _typing.Any = None,
) -> _typing.Any:
    """
    Parse css under the format's safety and compatibility rules.

    Example:
        Exercise parse css through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param data: Value supplied for data under the utility contract.
    :param fname: Value supplied for fname under the utility contract.
    :param is_declaration: Value supplied for is declaration under the utility contract.
    :param decode: Value supplied for decode under the utility contract.
    :param log_level: Value supplied for log level under the utility contract.
    :param css_preprocessor: Value supplied for css preprocessor under the utility
        contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
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


def handle_entities(text: _typing.Any, func: _typing.Any) -> _typing.Any:
    """
    Perform the handle entities operation under explicit file-format and conversion rules.

    Example:
        Exercise handle entities through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param text: Text parsed, normalized or rendered.
    :param func: Value supplied for func under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return prepare_string_for_xml(func(replace_entities(text)))


def apply_func_to_match_groups(match: _typing.Any, func: _typing.Any = icu_upper, handle_entities: _typing.Any = handle_entities) -> _typing.Any:
    """
    Apply the specified function to individual groups in the match object (the result of re.search() or the whole match if no groups were defined. Returns the replaced string.

    Example:
        Exercise apply func to match groups through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param match: Value supplied for match under the utility contract.
    :param func: Value supplied for func under the utility contract.
    :param handle_entities: Value supplied for handle entities under the utility
        contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    found_groups = False
    i = 0
    parts, pos = [], match.start()

    def f(text: _typing.Any) -> _typing.Any:
        """
        Perform the f operation under explicit file-format and conversion rules.

        Example:
            Exercise apply func to match groups.f through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param text: Text parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
