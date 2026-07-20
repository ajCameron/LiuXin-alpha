#!/usr/bin/env python
# vim:fileencoding=utf-8

from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

import sys

try:
    import regex as _regex  # type: ignore

    _HAS_REGEX = True
except ModuleNotFoundError:
    import re as _regex

    _HAS_REGEX = False

# Py2/Py3
from LiuXin_alpha.utils.libraries.liuxin_six import six_map, six_zip

__license__ = "GPL v3"
__copyright__ = "2014, Kovid Goyal <kovid at kovidgoyal.net>"

is_narrow_build = sys.maxunicode < 0x10FFFF


class Parser(object):

    """
    See epubcfi.ebnf for the specification that this parser tries to
    follow. I have implemented it manually, since I dont want to depend on
    grako, and the grammar is pretty simple. This parser is thread-safe, i.e.
    it can be used from multiple threads simulataneously.
    """

    def __init__(self: _typing.Self) -> None:
        # All allowed unicode characters + escaped special characters
        special_char = r"[\[\](),;=^]"
        if _HAS_REGEX and is_narrow_build:
            unescaped_char = "[[\t\n\r -\ud7ff\ue000-\ufffd]--%s]" % special_char
        elif _HAS_REGEX:
            unescaped_char = "[[\t\n\r -\ud7ff\ue000-\ufffd\U00010000-\U0010ffff]--%s]" % special_char
        else:
            # stdlib `re` does not support character class set subtraction.
            unescaped_char = r"[^\[\](),;=^]"
        escaped_char = r"\^" + special_char
        chars = r"(?:%s|(?:%s))+" % (unescaped_char, escaped_char)
        chars_no_space = r"(?:[^\s\[\](),;=^]|(?:\^[\[\](),;=^]))+"
        # No leading zeros allowed for integers
        integer = r"(?:[1-9][0-9]*)|0"
        # No leading zeros, except for numbers in (0, 1) and no trailing zeros for the fractional part
        frac = r"\.[0-9]*[1-9]"
        number = r"(?:[1-9][0-9]*(?:{0})?)|(?:0{0})|(?:0)".format(frac)

        def c(x: _typing.Any) -> _typing.Any:
            if _HAS_REGEX:
                return _regex.compile(x, flags=_regex.VERSION1)
            return _regex.compile(x, flags=_regex.UNICODE)

        # A step of the form /integer
        self.step_pat = c(r"/(%s)" % integer)
        # An id assertion of the form [characters]
        self.id_assertion_pat = c(r"\[(%s)\]" % chars)

        # A text offset of the form :integer
        self.text_offset_pat = c(r":(%s)" % integer)
        # A temporal offset of the form ~number
        self.temporal_offset_pat = c(r"~(%s)" % number)
        # A spatial offset of the form @number:number
        self.spatial_offset_pat = c(r"@({0}):({0})".format(number))
        # A spatio-temporal offset of the form ~number@number:number
        self.st_offset_pat = c(r"~({0})@({0}):({0})".format(number))

        # Text assertion patterns
        self.ta1_pat = c(r"({0})(?:,({0})){{0,1}}".format(chars))
        self.ta2_pat = c(r",(%s)" % chars)
        self.parameters_pat = c(r"(?:;(%s)=((?:%s,?)+))+" % (chars_no_space, chars))
        self.csv_pat = c(r"(?:(%s),?)+" % chars)

        # Unescape characters
        unescape_pat = c(r"%s(%s)" % (escaped_char[:2], escaped_char[2:]))
        self.unescape = lambda x: unescape_pat.sub(r"\1", x)

    def parse_epubcfi(self: _typing.Self, raw: _typing.Any) -> _typing.Any:
        """
        Parse a full epubcfi of the form epubcfi(path [ , path , path ])
        :param raw:
        :return:
        """
        null = {}, {}, {}, raw
        if not raw.startswith("epubcfi("):
            return null
        raw = raw[len("epubcfi(") :]
        parent_cfi, raw = self.parse_path(raw)
        if not parent_cfi:
            return null
        start_cfi, end_cfi = {}, {}
        if raw.startswith(","):
            start_cfi, raw = self.parse_path(raw[1:])
            if raw.startswith(","):
                end_cfi, raw = self.parse_path(raw[1:])
            if not start_cfi or not end_cfi:
                return null
        if raw.startswith(")"):
            raw = raw[1:]
        else:
            return null

        return parent_cfi, start_cfi, end_cfi, raw

    def parse_path(self: _typing.Self, raw: _typing.Any) -> tuple[_typing.Any, ...]:
        """
        Parse the path component of an epubcfi of the form /step...
        :param raw:
        :return:
        """
        path = {"steps": []}
        raw = self._parse_path(raw, path)
        if not path["steps"]:
            path = {}
        return path, raw

    def do_match(self: _typing.Self, pat: _typing.Any, raw: _typing.Any) -> tuple[_typing.Any, ...]:
        m = pat.match(raw)
        if m is not None:
            raw = raw[len(m.group()) :]
        return m, raw

    def _parse_path(self: _typing.Self, raw: _typing.Any, ans: _typing.Any) -> _typing.Any:
        m, raw = self.do_match(self.step_pat, raw)
        if m is None:
            return raw
        ans["steps"].append({"num": int(m.group(1))})
        m, raw = self.do_match(self.id_assertion_pat, raw)
        if m is not None:
            ans["steps"][-1]["id"] = self.unescape(m.group(1))
        if raw.startswith("!"):
            ans["redirect"] = r = {"steps": []}
            return self._parse_path(raw[1:], r)
        else:
            remaining_raw = self.parse_offset(raw, ans["steps"][-1])
            return self._parse_path(raw, ans) if remaining_raw is None else remaining_raw

    def parse_offset(self: _typing.Self, raw: _typing.Any, ans: _typing.Any) -> _typing.Any:
        m, raw = self.do_match(self.text_offset_pat, raw)
        if m is not None:
            ans["text_offset"] = int(m.group(1))
            return self.parse_text_assertion(raw, ans)
        m, raw = self.do_match(self.st_offset_pat, raw)
        if m is not None:
            t, x, y = m.groups()
            ans["temporal_offset"] = float(t)
            ans["spatial_offset"] = tuple(six_map(float, (x, y)))
            return raw
        m, raw = self.do_match(self.temporal_offset_pat, raw)
        if m is not None:
            ans["temporal_offset"] = float(m.group(1))
            return raw
        m, raw = self.do_match(self.spatial_offset_pat, raw)
        if m is not None:
            ans["spatial_offset"] = tuple(six_map(float, m.groups()))
            return raw

    def parse_text_assertion(self: _typing.Self, raw: _typing.Any, ans: _typing.Any) -> _typing.Any:
        oraw = raw
        if not raw.startswith("["):
            return oraw
        raw = raw[1:]
        ta = {}
        m, raw = self.do_match(self.ta1_pat, raw)
        if m is not None:
            before, after = m.groups()
            ta["before"] = self.unescape(before)
            if after is not None:
                ta["after"] = self.unescape(after)
        else:
            m, raw = self.do_match(self.ta2_pat, raw)
            if m is not None:
                ta["after"] = self.unescape(m.group(1))

        # parse parameters
        if _HAS_REGEX:
            m, raw = self.do_match(self.parameters_pat, raw)
            if m is not None:
                params = {}
                for name, value in six_zip(m.captures(1), m.captures(2)):
                    params[name] = tuple(six_map(self.unescape, self.csv_pat.match(value).captures(1)))
                if params:
                    ta["params"] = params
        else:
            params, raw, ok = self.parse_params_without_regex(raw)
            if not ok:
                return oraw
            if params:
                ta["params"] = params

        if not raw.startswith("]"):
            return oraw  # no closing ] or extra content in the assertion

        if ta:
            ans["text_assertion"] = ta
        return raw[1:]

    def consume_chars(self: _typing.Self, raw: _typing.Any, stop_chars: _typing.Any) -> tuple[_typing.Any, ...]:
        out = []
        idx = 0
        while idx < len(raw):
            ch = raw[idx]
            if ch in stop_chars:
                break
            if ch == "^":
                if idx + 1 >= len(raw):
                    return None, raw
                nxt = raw[idx + 1]
                if nxt not in "[](),;=^":
                    return None, raw
                out.append("^")
                out.append(nxt)
                idx += 2
                continue
            out.append(ch)
            idx += 1
        token = "".join(out)
        if not token:
            return None, raw
        return token, raw[idx:]

    def parse_params_without_regex(self: _typing.Self, raw: _typing.Any) -> tuple[_typing.Any, ...]:
        params = {}
        while raw.startswith(";"):
            raw = raw[1:]
            name, remainder = self.consume_chars(raw, stop_chars={"=", ";", ",", "]"})
            if name is None:
                return {}, raw, False
            unescaped_name = self.unescape(name)
            if any(ch.isspace() for ch in unescaped_name):
                return {}, raw, False
            if not remainder.startswith("="):
                return {}, raw, False
            raw = remainder[1:]
            values = []
            value, raw = self.consume_chars(raw, stop_chars={",", ";", "]"})
            if value is None:
                return {}, raw, False
            values.append(self.unescape(value))
            while raw.startswith(","):
                raw = raw[1:]
                value, raw = self.consume_chars(raw, stop_chars={",", ";", "]"})
                if value is None:
                    return {}, raw, False
                values.append(self.unescape(value))
            params[unescaped_name] = tuple(values)
        return params, raw, True


_parser = None


def parser() -> _typing.Any:
    global _parser
    if _parser is None:
        _parser = Parser()
    return _parser


def get_steps(pcfi: _typing.Any) -> _typing.Any:
    ans = tuple(pcfi["steps"])
    if "redirect" in pcfi:
        ans += get_steps(pcfi["redirect"])
    return ans


def cfi_sort_key(cfi: _typing.Any, only_path: bool = True) -> tuple[_typing.Any, ...]:
    p = parser()
    try:
        if only_path:
            pcfi = p.parse_path(cfi)[0]
        else:
            parent, start = p.parse_epubcfi(cfi)[:2]
            pcfi = start or parent
    except Exception as e:
        import traceback

        traceback.print_exc()
        return ()
    if not pcfi:
        import sys

        print("Failed to parse CFI: %r" % pcfi, file=sys.stderr)
        return ()
    steps = get_steps(pcfi)
    step_nums = tuple(s.get("num", 0) for s in steps)
    step = steps[-1] if steps else {}
    offsets = (
        step.get("temporal_offset", 0),
        tuple(reversed(step.get("spatial_offset", (0, 0)))),
        step.get("text_offset", 0),
    )
    return (step_nums, offsets)
