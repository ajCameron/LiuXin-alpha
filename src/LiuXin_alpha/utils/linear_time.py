"""
Find the matches of a few backtracking patterns in linear time, with the same results.

``re`` tries a pattern at every position, and some patterns scan a long way
from each before failing. On crafted text each try costs the length of the
text, so a search or substitution costs its square, or worse:

- The XML-declaration and HTML 4 charset patterns that ``calibre_chardet``
  inherited from calibre retry every quote after every ``encoding=`` against
  the rest of the tag. 8 KB of unclosed ``<?…encoding="`` took 6.7 s, and the
  time grows with the cube.
- ``&(\\S+?);``, which resolves entities, scans from every ``&`` in a run with
  no ``;`` to the end of the run: 80 KB of ``&`` took 33 s.

``LinearPattern`` stands in for such a compiled pattern. It offers ``search``,
``finditer`` and ``sub``, and finds the same matches, by hand or with an
equivalent pattern that never scans the same text twice. The tests compare
each with the original pattern on random text.

Example:
    >>> entity = LinearPattern(re.compile(r"&(\\S+?);"), entities)
    >>> entity.sub(lambda m: m.group(1).upper(), "&amp; && &lt;")
    'AMP && LT'
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterator
from typing import Any, AnyStr

__all__ = ["ForwardFinder", "LinearPattern", "entities", "html4_charsets", "xml_declarations"]

XML_DECLARATION = r"<\?[^<>]+encoding\s*=\s*['\"](.*?)['\"][^<>]*>"
HTML4_CHARSET = (
    r"""<meta\s+?[^<>]*?content\s*=\s*['\"][^'\"]*?charset=([-_a-z0-9]+)[^'\"]*?['\"][^<>]*>(?:\s*</meta>){0,1}"""
)


class _Pieces:
    """
    The fixed pieces the declaration scanners look for, in the text's type.

    Example:
        >>> _Pieces("x").gt, _Pieces(b"x").gt
        ('>', b'>')
    """

    def __init__(self, text: str | bytes) -> None:
        """
        Compile the pieces for text of this type.

        Example:
            See ``_Pieces``.


        :param text: Text of the type to scan.
        :return: None.
        """

        def typed(value: str) -> Any:
            """
            Give ``value`` the text's type.

            Example:
                See ``_Pieces``.


            :param value: An ASCII string.
            :return: It, or its bytes.
            """
            return value if isinstance(text, str) else value.encode("ascii")

        self.typed = typed
        self.gt = typed(">")
        self.newline = typed("\n")
        self.xml_open = typed("<?")
        self.angle = re.compile(typed("[<>]"))
        self.quote = re.compile(typed("['\"]"))
        self.encoding_head = re.compile(typed(r"encoding\s*=\s*['\"]"), re.IGNORECASE)
        self.meta_start = re.compile(typed(r"<meta\s"), re.IGNORECASE)
        self.content_head = re.compile(typed(r"content\s*=\s*['\"]"), re.IGNORECASE)
        self.charset_start = re.compile(typed(r"charset=[-_a-z0-9]"), re.IGNORECASE)
        self.meta_close = re.compile(typed(r"\s*</meta>"), re.IGNORECASE)


class _ClosedQuotes:
    """
    Find, for positions that only grow, the first quote on the line whose tag closes.

    ``(.*?)['"][^<>]*>`` ends its group at the first quote, before the next
    newline, after which the next ``<`` or ``>`` is a ``>``. Answers are kept
    and reused while they still hold, so each stretch of text is crossed once.

    Example:
        >>> _ClosedQuotes("a'b<c'd>e", _Pieces("")).first_after(0)
        (5, 7)
    """

    def __init__(self, text: str | bytes, pieces: _Pieces) -> None:
        """
        Keep the text to scan.

        Example:
            See ``_ClosedQuotes``.


        :param text: The text.
        :param pieces: Its pieces.
        :return: None.
        """
        self.text = text
        self.pieces = pieces
        self.newlines = ForwardFinder(text)
        self.last: tuple[int, int, int, int] | None = None

    def first_after(self, pos: int) -> tuple[int, int]:
        """
        Find the first quote at or after ``pos`` that ends the group.

        Example:
            See ``_ClosedQuotes``.


        :param pos: Where the group starts.
        :return: The quote and the ``>`` closing its tag, or ``(-1, -1)``.
        """
        if self.last is not None and self.last[0] <= pos:
            _, quote, close, holds_until = self.last
            if quote >= pos:
                return quote, close
            if quote < 0 and pos <= holds_until:
                return -1, -1
        quote, close, holds_until = self._scan(pos)
        self.last = (pos, quote, close, holds_until)
        return quote, close

    def _scan(self, pos: int) -> tuple[int, int, int]:
        """
        Scan from ``pos`` for the quote.

        Example:
            See ``_ClosedQuotes``.


        :param pos: Where the group starts.
        :return: The quote, the ``>`` after it, and, when there is no quote,
            the last position from which there is none either.
        """
        text, pieces = self.text, self.pieces
        newline = self.newlines.find(pieces.newline, pos)
        limit = len(text) if newline < 0 else newline
        while True:
            found = pieces.quote.search(text, pos, limit)
            if found is None:
                return -1, -1, limit
            quote = found.start()
            after = pieces.angle.search(text, quote + 1)
            if after is None:
                return -1, -1, len(text)
            if text[after.start() : after.start() + 1] == pieces.gt:
                return quote, after.start(), -1
            pos = after.start() + 1


def xml_declarations(text: AnyStr) -> Iterator[re.Match[AnyStr]]:
    """
    Find the matches of the pattern ``XML_DECLARATION``, as ``finditer`` would.

    A try at ``<?`` takes the last ``encoding=`` before the tag's first
    ``<`` or ``>`` whose value ends at a quote that closes; failing that, any
    earlier one whose value's first quote is on its line, if that first
    ``<`` or ``>`` is a ``>``.

    Example:
        >>> [m.group(1) for m in xml_declarations('<?xml encoding="a" encoding="b"?><?x encoding="c<d"?>')]
        ['b', 'c<d']


    :param text: The text.
    :return: The matches.
    """
    pieces = _Pieces(text)
    closed = _ClosedQuotes(text, pieces)
    pos = 0
    while True:
        start = text.find(pieces.xml_open, pos)
        if start < 0:
            return
        found = pieces.angle.search(text, start + 2)
        stop = len(text) if found is None else found.start()
        heads = list(pieces.encoding_head.finditer(text, start + 3, stop))
        span: tuple[int, int, int] | None = None
        if heads:
            quote, close = closed.first_after(heads[-1].end())
            if quote >= 0:
                span = (heads[-1].start(), quote, close)
            elif text[stop : stop + 1] == pieces.gt:
                for head in reversed(heads[:-1]):
                    first = pieces.quote.search(text, head.end())
                    assert first is not None  # the next head's own quote comes later
                    if text.find(pieces.newline, head.end(), first.start()) < 0:
                        span = (head.start(), first.start(), stop)
                        break
        if span is None:
            pos = start + 1
            continue
        head_start, quote, close = span
        value_start = pieces.encoding_head.match(text, head_start)
        assert value_start is not None
        exact = re.compile(
            pieces.typed(r"<\?[^<>]{%d}encoding\s*=\s*['\"](.{%d})['\"][^<>]*>")
            % (head_start - start - 2, quote - value_start.end()),
            re.IGNORECASE,
        )
        match = exact.match(text, start)
        assert match is not None and match.end() == close + 1
        yield match
        pos = match.end()


def html4_charsets(text: AnyStr) -> Iterator[re.Match[AnyStr]]:
    """
    Find the matches of the pattern ``HTML4_CHARSET``, as ``finditer`` would.

    A try at ``<meta`` takes the first ``content=`` before the tag's first
    ``<`` or ``>`` whose quoted value holds ``charset=`` and whose closing
    quote is followed, at the next ``<`` or ``>``, by a ``>``.

    Example:
        >>> [m.group(1) for m in html4_charsets('<meta http-equiv="x" content="text/html; charset=utf-8">')]
        ['utf-8']


    :param text: The text.
    :return: The matches.
    """
    pieces = _Pieces(text)
    pos = 0
    while True:
        found = pieces.meta_start.search(text, pos)
        if found is None:
            return
        start = found.start()
        after = pieces.angle.search(text, start + 6)
        stop = len(text) if after is None else after.start()
        span: tuple[int, int] | None = None
        for head in pieces.content_head.finditer(text, start + 6, stop):
            closing = pieces.quote.search(text, head.end())
            if closing is None:
                break
            quote = closing.start()
            charset = pieces.charset_start.search(text, head.end(), quote)
            if charset is None:
                continue
            if quote < stop:
                close = stop
            else:
                after_quote = pieces.angle.search(text, quote + 1)
                close = -1 if after_quote is None else after_quote.start()
            if close >= 0 and text[close : close + 1] == pieces.gt:
                span = (head.start(), charset.start())
                break
        if span is None:
            pos = start + 1
            continue
        head_start, charset_start = span
        content = pieces.content_head.match(text, head_start)
        assert content is not None
        exact = re.compile(
            pieces.typed(
                r"<meta\s+?[^<>]{%d}content\s*=\s*['\"][^'\"]{%d}charset=([-_a-z0-9]+)[^'\"]*?['\"][^<>]*>(?:\s*</meta>){0,1}"
            )
            % (head_start - start - 6, charset_start - content.end()),
            re.IGNORECASE,
        )
        match = exact.match(text, start)
        assert match is not None
        yield match
        pos = match.end()


_ENTITY_OR_RUN: dict[type, re.Pattern[Any]] = {
    str: re.compile(r"&(\S[^\s;]*);|&\S[^\s;]*"),
    bytes: re.compile(rb"&(\S[^\s;]*);|&\S[^\s;]*"),
}


def entities(text: AnyStr) -> Iterator[re.Match[AnyStr]]:
    """
    Find the matches of ``&(\\S+?);``, as ``finditer`` would.

    A try at ``&`` succeeds when, after at least one character, its run of
    non-space characters reaches a ``;``, and the group is what lies between.
    The equivalent ``&(\\S[^\\s;]*);`` reads that run once. When it fails, the
    second branch takes the rest of the run, where every try fails the same
    way, and that match is not given.

    Example:
        >>> [m.group(1) for m in entities("&amp; &&& &;; &x y;")]
        ['amp', ';']


    :param text: The text.
    :return: The matches, with the original pattern's groups and spans.
    """
    for match in _ENTITY_OR_RUN[type(text)].finditer(text):
        if match.group(1) is not None:
            yield match


class LinearPattern:
    """
    Stand in for a compiled pattern, finding its matches with a linear-time function.

    Example:
        >>> pattern = LinearPattern(re.compile(XML_DECLARATION, re.IGNORECASE), xml_declarations)
        >>> pattern.sub("", '<?xml version="1.0" encoding="utf-8"?><p>')
        '<p>'
    """

    def __init__(self, pattern: re.Pattern[Any], matches: Callable[[Any], Iterator[re.Match[Any]]]) -> None:
        """
        Wrap the pattern.

        Example:
            See ``LinearPattern``.


        :param pattern: The compiled pattern.
        :param matches: Finds its matches in a text, as ``finditer`` would.
        :return: None.
        """
        self.pattern = pattern.pattern
        self.flags = pattern.flags
        self.groups = pattern.groups
        self._matches = matches

    def finditer(self, string: Any) -> Iterator[re.Match[Any]]:
        """
        Iterate over the matches, as ``re.Pattern.finditer`` does.

        Example:
            See ``LinearPattern``.


        :param string: The text.
        :return: The matches.
        """
        return self._matches(string)

    def search(self, string: Any) -> re.Match[Any] | None:
        """
        Find the first match, as ``re.Pattern.search`` does.

        Example:
            See ``LinearPattern``.


        :param string: The text.
        :return: The match, or None.
        """
        return next(self._matches(string), None)

    def sub(self, repl: Any, string: Any, count: int = 0) -> Any:
        """
        Replace the matches, as ``re.Pattern.sub`` does.

        Example:
            See ``LinearPattern``.


        :param repl: A template or a function of the match.
        :param string: The text.
        :param count: The most matches to replace; 0 for all.
        :return: The text with the matches replaced.
        """
        out: list[Any] = []
        copied = 0
        for number, match in enumerate(self._matches(string), 1):
            out.append(string[copied : match.start()])
            out.append(repl(match) if callable(repl) else match.expand(repl))
            copied = match.end()
            if number == count:
                break
        out.append(string[copied:])
        return string[:0].join(out)


class ForwardFinder:
    """
    Answer ``find`` and ``rfind`` on one text for positions that only grow.

    The MOBI 6 reader's anchor pass searches from each link target, in order, for
    the next ``<`` and ``>`` and the last ``<`` before it. Done afresh, each
    search can cross the same long stretch again. Here each answer is kept
    and reused while it still holds, so the text is crossed once per byte
    searched for. Positions that go back are searched afresh.

    Example:
        >>> finder = ForwardFinder(b"ab<cd<ef")
        >>> finder.find(b"<", 1), finder.find(b"<", 3), finder.rfind(b"<", 4), finder.rfind(b"<", 8)
        (2, 5, 2, 5)
    """

    def __init__(self, text: str | bytes) -> None:
        """
        Keep the text to search.

        Example:
            See ``ForwardFinder``.


        :param text: The text.
        :return: None.
        """
        self.text: Any = text
        self._found: dict[Any, tuple[int, int]] = {}
        self._found_before: dict[Any, tuple[int, int]] = {}

    def find(self, needle: Any, pos: int) -> int:
        """
        Return ``text.find(needle, pos)``.

        Example:
            See ``ForwardFinder``.


        :param needle: What to look for.
        :param pos: Where to look from.
        :return: Where it next appears, or -1.
        """
        last = self._found.get(needle)
        if last is not None and last[0] <= pos and (last[1] < 0 or last[1] >= pos):
            return last[1]
        found: int = self.text.find(needle, pos)
        self._found[needle] = (pos, found)
        return found

    def rfind(self, needle: Any, end: int) -> int:
        """
        Return ``text.rfind(needle, 0, end)``.

        Example:
            See ``ForwardFinder``.


        :param needle: A single byte to look for.
        :param end: Where to stop looking.
        :return: Where it last appears before ``end``, or -1.
        """
        last = self._found_before.get(needle)
        found: int
        if last is not None and last[0] <= end:
            found = self.text.rfind(needle, last[0], end)
            if found < 0:
                found = last[1]
        else:
            found = self.text.rfind(needle, 0, end)
        self._found_before[needle] = (end, found)
        return found
