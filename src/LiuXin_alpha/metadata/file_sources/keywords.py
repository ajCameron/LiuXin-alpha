"""
Split a document's keywords into tags, using the tags the library already knows.

Keywords arrive as one string, and formats and tools join them differently:
"science fiction, space opera", "scifi spaceopera", "spaceopera,military scifi
history political". The ``keyword_splitting`` preference chooses how they are split:

``smart`` (the default)
    Split at commas and semicolons when there are any. Then match what is left
    against the tags the library already knows (see :func:`using_known_tags`):

    - a piece that is a known tag, ignoring case, spaces and punctuation, takes
      that tag's spelling ("spaceopera" gives "space opera", "SciFi" gives "sci-fi");
    - a run of words is cut into the known tags that cover most of it, the fewest
      of them on a tie ("military scifi history political" gives "military
      sci-fi", "history" and "political" when those are known);
    - words no known tag covers stay together where the keywords had separators,
      and are split on spaces where they had none;
    - where the keywords had separators, a piece is cut only when known tags cover
      more than half its words, so "hard boiled detective" stays one tag though
      "detective" is known.

    With no known tags, this is the ``separators`` rule.
``separators``
    LiuXin's earlier rule: split at commas and semicolons when there are any, so a
    tag may hold spaces; otherwise split on spaces.
``calibre``
    calibre's rule: split on spaces and on commas, always.

Known tags are given by whoever reads the file for a library, for the length of the
read; the metadata layer cannot look in the database itself.

Example:
    >>> with using_known_tags(["space opera", "military sci-fi", "history", "political"]):
    ...     split_keywords("spaceopera,military scifi history political")
    ['space opera', 'military sci-fi', 'history', 'political']
    >>> split_keywords("spaceopera,military scifi history political")
    ['spaceopera', 'military scifi history political']
    >>> split_keywords("science fiction, space opera", "calibre")
    ['science', 'fiction', 'space', 'opera']
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

__all__ = [
    "DEFAULT_KEYWORD_SPLITTING",
    "KEYWORD_SPLITTING_PREFERENCE",
    "KEYWORD_SPLITTINGS",
    "KnownTags",
    "keyword_splitting",
    "known_tags",
    "split_keywords",
    "tag_key",
    "using_known_tags",
]

#: The preference that chooses the splitting.
KEYWORD_SPLITTING_PREFERENCE = "keyword_splitting"
#: The splittings offered, with what each means.
KEYWORD_SPLITTINGS: Mapping[str, str] = {
    "smart": "split at separators, then match the words against the library's known tags",
    "separators": "split at commas and semicolons when there are any, else on spaces",
    "calibre": "split on spaces and commas, as calibre does",
}
DEFAULT_KEYWORD_SPLITTING = "smart"

#: The most words of a keyword run matched against one known tag.
_MAX_TAG_WORDS = 8
_SEPARATORS = re.compile(r"[,;]")


def keyword_splitting(explicit: str | None = None) -> str:
    """
    Choose a splitting: the one asked for, else the preference, else ``smart``.

    Example:
        >>> keyword_splitting("calibre"), keyword_splitting("sideways")
        ('calibre', 'smart')


    :param explicit: A splitting named by the caller.
    :return: A key of :data:`KEYWORD_SPLITTINGS`.
    """
    chosen: Any = explicit
    if chosen is None:
        try:
            from LiuXin_alpha.configuration import preferences

            chosen = preferences.get(KEYWORD_SPLITTING_PREFERENCE)
        except Exception:  # an unreadable store leaves the default
            chosen = None
    chosen = str(chosen or "").strip().lower()
    return chosen if chosen in KEYWORD_SPLITTINGS else DEFAULT_KEYWORD_SPLITTING


def tag_key(text: str) -> str:
    """
    Reduce a tag to what matching compares: its letters and digits, case folded.

    Example:
        >>> tag_key("Sci-Fi"), tag_key("space opera"), tag_key("Spaceopera")
        ('scifi', 'spaceopera', 'spaceopera')


    :param text: A tag, or words that may be one.
    :return: The key; empty when the text has no letters or digits.
    """
    return "".join(char for char in unicodedata.normalize("NFKC", text).casefold() if char.isalnum())


class KnownTags:
    """
    The tags a library knows, indexed by :func:`tag_key`.

    Where several tags share a key, the first given keeps it, so a caller can list
    the most used spelling first.

    Example:
        >>> known = KnownTags(["Sci-Fi", "scifi", "space opera"])
        >>> known.spelling("SCI FI"), known.spelling("dragons"), known.max_words
        ('Sci-Fi', None, 2)
    """

    def __init__(self, tags: Iterable[str]) -> None:
        """
        Index the tags.

        Example:
            See :class:`KnownTags`.


        :param tags: The tags' names.
        :return: None.
        """
        self._by_key: dict[str, str] = {}
        self.max_words = 1
        for tag in tags:
            name = " ".join(str(tag).split())
            key = tag_key(name)
            if key and key not in self._by_key:
                self._by_key[key] = name
                self.max_words = min(_MAX_TAG_WORDS, max(self.max_words, len(name.split())))

    def __bool__(self) -> bool:
        """
        Tell whether any tag is known.

        Example:
            >>> bool(KnownTags([])), bool(KnownTags(["history"]))
            (False, True)


        :return: True when there is at least one.
        """
        return bool(self._by_key)

    def spelling(self, words: str) -> str | None:
        """
        Give the known tag the words match, in the tag's spelling.

        Example:
            See :class:`KnownTags`.


        :param words: Words from the keywords.
        :return: The tag, or ``None`` when none matches.
        """
        return self._by_key.get(tag_key(words))


class _Source:
    """
    Where a scope's known tags come from: a list, or a callable asked once, when first needed.

    Example:
        >>> _Source(lambda: ["history"]).tags.spelling("History")
        'history'
    """

    def __init__(self, source: Iterable[str] | Callable[[], Iterable[str]]) -> None:
        """
        Keep the source.

        Example:
            See :class:`_Source`.


        :param source: The tags, or a callable that gives them.
        :return: None.
        """
        self._source = source
        self._tags: KnownTags | None = None

    @property
    def tags(self) -> KnownTags:
        """
        Index the tags the first time they are needed.

        Example:
            See :class:`_Source`.


        :return: The known tags.
        """
        if self._tags is None:
            source = self._source
            self._tags = KnownTags(source() if callable(source) else source)
        return self._tags


_current: ContextVar[_Source | None] = ContextVar("liuxin_known_tags", default=None)


@contextmanager
def using_known_tags(source: Iterable[str] | Callable[[], Iterable[str]]) -> Iterator[None]:
    """
    Let readers in this scope match keywords against these tags.

    A callable is called at most once, and only if a reader needs the tags, so a
    library can pass a query without running it for files that have no keywords.

    Example:
        >>> with using_known_tags(lambda: ["space opera"]):
        ...     known_tags().spelling("SpaceOpera")
        'space opera'
        >>> bool(known_tags())
        False


    :param source: The tags, or a callable that gives them.
    :return: A context manager.
    """
    token = _current.set(_Source(source))
    try:
        yield
    finally:
        _current.reset(token)


def known_tags() -> KnownTags:
    """
    Give the tags known in this scope; none outside :func:`using_known_tags`.

    Example:
        See :func:`using_known_tags`.


    :return: The known tags.
    """
    source = _current.get()
    return source.tags if source is not None else KnownTags(())


def split_keywords(text: str, how: str | None = None, *, known: KnownTags | None = None) -> list[str]:
    """
    Split a keywords string into tags, as the ``keyword_splitting`` preference or ``how`` says.

    Example:
        >>> split_keywords("space opera dragons", known=KnownTags(["Space Opera"]))
        ['Space Opera', 'dragons']
        >>> split_keywords("space opera dragons", "separators", known=KnownTags(["Space Opera"]))
        ['space', 'opera', 'dragons']


    :param text: The keywords.
    :param how: A key of :data:`KEYWORD_SPLITTINGS`; the preference when ``None``.
    :param known: The known tags; those of the current scope when ``None``.
    :return: The tags, in order; the smart splitting also drops repeats, ignoring case.
    """
    how = keyword_splitting(how)
    if how == "calibre":
        return [part.strip() for word in text.split() for part in word.split(",") if part.strip()]
    separated = bool(_SEPARATORS.search(text))
    pieces = _SEPARATORS.split(text) if separated else text.split()
    if how == "separators":
        return [piece.strip() for piece in pieces if piece.strip()]
    known = known_tags() if known is None else known
    if not known:
        return [piece.strip() for piece in pieces if piece.strip()]
    chunks = _SEPARATORS.split(text) if separated else [text]
    tags: list[str] = []
    for chunk in chunks:
        tags.extend(_segment(chunk.split(), known, keep_unknown_together=separated))
    return _unique(tags)


def _segment(words: list[str], known: KnownTags, *, keep_unknown_together: bool) -> list[str]:
    """
    Cut a run of words into known tags covering most of it, the fewest on a tie.

    With ``keep_unknown_together`` (the words were one piece between separators),
    the piece is kept whole unless known tags cover more than half its words.

    Example:
        >>> known = KnownTags(["science fiction", "science", "fiction", "history", "detective"])
        >>> _segment("science fiction history dragons".split(), known, keep_unknown_together=True)
        ['science fiction', 'history', 'dragons']
        >>> _segment("hard boiled detective".split(), known, keep_unknown_together=True)
        ['hard boiled detective']
        >>> _segment("hard boiled detective".split(), known, keep_unknown_together=False)
        ['hard', 'boiled', 'detective']


    :param words: The words.
    :param known: The known tags.
    :param keep_unknown_together: Keep each run of unknown words as one tag, rather than one per word.
    :return: The tags.
    """
    count = len(words)
    # best[i]: (words covered by known tags, minus the number of them, pieces) for words[i:].
    best: list[tuple[int, int, list[tuple[str, bool]]]] = [(0, 0, [])] * (count + 1)
    for start in range(count - 1, -1, -1):
        covered, fewer, rest = best[start + 1]
        choice = (covered, fewer, [(words[start], False), *rest])
        for end in range(min(count, start + known.max_words), start, -1):
            spelling = known.spelling(" ".join(words[start:end]))
            if spelling is None:
                continue
            covered, fewer, rest = best[end]
            option = (covered + end - start, fewer - 1, [(spelling, True), *rest])
            if option[:2] > choice[:2]:
                choice = option
        best[start] = choice
    covered, _fewer, pieces = best[0]
    if keep_unknown_together and 2 * covered <= count and covered < count:
        return [" ".join(words)]
    return _join_unknown(pieces, keep_unknown_together)


def _join_unknown(pieces: list[tuple[str, bool]], together: bool) -> list[str]:
    """
    Give the tags of a segmentation, joining each run of unknown words when asked.

    Example:
        >>> _join_unknown([("hard", False), ("boiled", False), ("crime", True)], True)
        ['hard boiled', 'crime']


    :param pieces: ``(text, known)`` pairs, in order.
    :param together: Join a run of unknown words into one tag.
    :return: The tags.
    """
    tags: list[str] = []
    run: list[str] = []
    for text, is_known in pieces:
        if not is_known:
            run.append(text)
            continue
        if run:
            tags.extend([" ".join(run)] if together else run)
            run = []
        tags.append(text)
    if run:
        tags.extend([" ".join(run)] if together else run)
    return tags


def _unique(tags: list[str]) -> list[str]:
    """
    Drop repeated tags, ignoring case, keeping the first.

    Example:
        >>> _unique(["Sci-Fi", "sci-fi", "history"])
        ['Sci-Fi', 'history']


    :param tags: The tags.
    :return: The tags without repeats.
    """
    seen: set[str] = set()
    out: list[str] = []
    for tag in tags:
        if tag.casefold() not in seen:
            seen.add(tag.casefold())
            out.append(tag)
    return out
