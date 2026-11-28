"""
Turn normalised file facts into planned books using Catalog's read-only matchers.

The rules are the ones agreed for ingest on 2026-11-25:

* a book is always planned as new; a plausible existing Work only flags it as a
  possible duplicate, because a wrong merge is worse than a reviewable copy;
* names (authors, publisher, tags, series) reuse a row only on a unique match,
  are new when nothing matches, and are flagged when the match is ambiguous;
* languages are seeded constants, so an unrecognised code is reported, never created.

Formats of one book are files in the same folder with the same title, authors
and languages; each becomes its own Manifestation and Item under one Work. The
same book found in different folders is planned twice and flagged both times.
Matching only reads; nothing here writes to the catalogue.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import replace
from typing import Any

from LiuXin_alpha.catalog.api.common import MetadataCandidate
from LiuXin_alpha.ingest.embedded_metadata.models import (
    DECISION_AMBIGUOUS,
    DECISION_ERROR,
    DECISION_NEW,
    DECISION_REUSE,
    DECISION_UNKNOWN,
    FLAG_AMBIGUOUS_NAME,
    FLAG_DUPLICATE_IN_CATALOGUE,
    FLAG_DUPLICATE_IN_RUN,
    FLAG_FORMATS_DISAGREE,
    FLAG_MATCH_ERROR,
    FLAG_UNKNOWN_LANGUAGE,
    FileFacts,
    PlannedBook,
    Resolution,
    WorkCheck,
)

# Preferred primary format when one book has several files: richest metadata first.
_FORMAT_PREFERENCE = (
    "epub",
    "azw3",
    "kepub",
    "mobi",
    "azw",
    "fb2",
    "docx",
    "odt",
    "htmlz",
    "pdf",
    "rtf",
    "lit",
    "pdb",
    "html",
    "txt",
)

BookIdentity = tuple[str, tuple[str, ...], tuple[str, ...]]
LanguageKey = Callable[[str], str]
Lookup = Callable[[], Any]


class NameResolver:
    """
    Resolve names against one catalogue, answering each distinct name once per run.

    Example:
        >>> resolver = NameResolver(catalog)  # doctest: +SKIP
        >>> resolver.tag("Gothic").decision  # doctest: +SKIP
        'reuse'
    """

    def __init__(self, catalog: Any) -> None:
        """
        Bind the catalogue whose matchers answer lookups.

        Example:
            >>> NameResolver(object())._memo
            {}


        :param catalog: Catalog exposing ``agents``, ``tags``, ``series`` and ``languages``.
        :return: None.
        """
        self._catalog = catalog
        self._memo: dict[tuple[str, str], Resolution] = {}

    def agent(self, name: str) -> Resolution:
        """
        Resolve a person or organisation credited on a book.

        Example:
            >>> NameResolver(catalog).agent("Mary Shelley")  # doctest: +SKIP


        :param name: Name as read.
        :return: Reuse, new, ambiguous or error resolution.
        """
        return self._cached(
            "agent",
            name,
            lambda: self._catalog.agents.match(MetadataCandidate({"name": name})),
        )

    def tag(self, name: str) -> Resolution:
        """
        Resolve a tag by exact, case-insensitive name.

        Example:
            >>> NameResolver(catalog).tag("Gothic")  # doctest: +SKIP


        :param name: Tag as read.
        :return: Reuse, new, ambiguous or error resolution.
        """
        return self._cached("tag", name, lambda: self._catalog.tags.exact(name))

    def series(self, name: str) -> Resolution:
        """
        Resolve a series by exact name.

        Example:
            >>> NameResolver(catalog).series("Barsoom")  # doctest: +SKIP


        :param name: Series name as read.
        :return: Reuse, new, ambiguous or error resolution.
        """
        return self._cached("series", name, lambda: self._catalog.series.exact(name))

    def language(self, code: str) -> Resolution:
        """
        Resolve a language code, trying its primary subtag when a region is attached.

        Example:
            >>> NameResolver(catalog).language("en-GB")  # doctest: +SKIP


        :param code: Code as read, such as ``eng``, ``en`` or ``en-GB``.
        :return: Reuse when seeded, ``unknown`` when not, or ambiguous/error.
        """
        resolution = self._cached("language", code, lambda: self._language_match(code))
        if resolution.decision == DECISION_NEW:
            return Resolution(code, DECISION_UNKNOWN, reason="not a seeded language")
        return resolution

    def language_key(self, code: str) -> str:
        """
        Return a comparison key for a language code: its catalogue row when seeded.

        Example:
            >>> NameResolver(catalog).language_key("en") == NameResolver(catalog).language_key("eng")  # doctest: +SKIP
            True


        :param code: Language code as read.
        :return: ``#<language id>`` for a seeded language, otherwise the folded code.
        """
        resolution = self.language(code)
        if resolution.entity_id is not None:
            return f"#{resolution.entity_id}"
        return code.casefold()

    def _language_match(self, code: str) -> Any:
        """
        Match a code exactly, then by its primary subtag.

        Example:
            >>> NameResolver(catalog)._language_match("pt-BR")  # doctest: +SKIP


        :param code: Language code.
        :return: The first matching result, or the last result tried.
        """
        result = self._catalog.languages.exact(code)
        primary = code.replace("_", "-").partition("-")[0]
        if result.decision == "no_match" and primary and primary != code:
            result = self._catalog.languages.exact(primary)
        return result

    def _cached(self, kind: str, value: str, lookup: Lookup) -> Resolution:
        """
        Return the remembered resolution for a name, looking it up the first time.

        Example:
            >>> NameResolver(object())._cached("tag", "x", lambda: (_ for _ in ()).throw(KeyError("k"))).decision
            'error'


        :param kind: Kind of name, keeping tags and series with one name apart.
        :param value: Name as read.
        :param lookup: Callable returning a Catalog ``MatchResult``.
        :return: The resolution, renamed to this spelling of the name.
        """
        key = (kind, value.casefold())
        remembered = self._memo.get(key)
        if remembered is None:
            remembered = _resolution(value, lookup)
            self._memo[key] = remembered
        return _respelled(remembered, value)


def plan_books(catalog: Any, facts: Sequence[FileFacts]) -> list[PlannedBook]:
    """
    Group file facts into books and plan each against the catalogue.

    Languages are compared by the catalogue row they resolve to, so readers
    reporting ``en`` and ``eng`` for one book still group its formats.

    Example:
        >>> books = plan_books(catalog, facts)  # doctest: +SKIP
        >>> books[0].work_check.decision  # doctest: +SKIP
        'no_match'


    :param catalog: Catalog exposing ``works``, ``agents``, ``tags``, ``series`` and ``languages``.
    :param facts: Facts for every file read, in Store-key order.
    :return: Planned books in the order their first file appeared.
    """
    resolver = NameResolver(catalog)
    groups = group_formats(facts, language_key=resolver.language_key)
    identities: dict[int, BookIdentity] = {}
    seen: dict[BookIdentity, list[int]] = {}
    books = []
    for index, group in enumerate(groups, start=1):
        books.append(_plan_book(index, group, resolver, catalog))
        identity = book_identity(group[0], language_key=resolver.language_key)
        identities[index] = identity
        seen.setdefault(identity, []).append(index)
    return [_with_run_duplicates(book, seen[identities[book.index]]) for book in books]


def group_formats(
    facts: Sequence[FileFacts], *, language_key: LanguageKey = str.casefold
) -> list[list[FileFacts]]:
    """
    Group files that are formats of one book: same folder, title, authors and languages.

    Within a group the richest format comes first and supplies book-level values.

    Example:
        >>> from LiuXin_alpha.ingest.embedded_metadata.models import IngestFile
        >>> a = FileFacts(IngestFile(1, "x/a.pdf", "pdf"), title="A", authors=("B",))
        >>> b = FileFacts(IngestFile(2, "x/a.epub", "epub"), title="a", authors=("b",))
        >>> [[f.file.extension for f in group] for group in group_formats([a, b])]
        [['epub', 'pdf']]


    :param facts: Facts in Store-key order.
    :param language_key: Maps a language code to the value compared; defaults to the folded code.
    :return: Groups in the order their first file appeared.
    """
    groups: dict[tuple[str, BookIdentity], list[FileFacts]] = {}
    for item in facts:
        key = (item.file.folder, book_identity(item, language_key=language_key))
        groups.setdefault(key, []).append(item)
    return [sorted(group, key=_format_rank) for group in groups.values()]


def book_identity(
    facts: FileFacts, *, language_key: LanguageKey = str.casefold
) -> BookIdentity:
    """
    Return the case-insensitive title, author set and language set of a file.

    Example:
        >>> from LiuXin_alpha.ingest.embedded_metadata.models import IngestFile
        >>> book_identity(FileFacts(IngestFile(1, "a.epub", "epub"), title="The  End", authors=("B", "A")))
        ('the end', ('a', 'b'), ())


    :param facts: Facts for one file.
    :param language_key: Maps a language code to the value compared; defaults to the folded code.
    :return: Identity used for grouping formats and spotting duplicates in a run.
    """
    title = " ".join(facts.title.casefold().split())
    authors = tuple(sorted(" ".join(name.casefold().split()) for name in facts.authors))
    return (
        title,
        authors,
        tuple(sorted({language_key(code) for code in facts.languages})),
    )


def _plan_book(
    index: int, group: list[FileFacts], resolver: NameResolver, catalog: Any
) -> PlannedBook:
    """
    Plan one book from its formats.

    Example:
        >>> book = _plan_book(1, group, NameResolver(catalog), catalog)  # doctest: +SKIP


    :param index: One-based book index.
    :param group: The book's formats, primary first.
    :param resolver: Shared name resolver.
    :param catalog: Catalog for the Work check.
    :return: The planned book, without in-run duplicate references.
    """
    primary = group[0]
    authors = tuple(resolver.agent(name) for name in primary.authors)
    publisher = None if primary.publisher is None else resolver.agent(primary.publisher)
    tags = tuple(resolver.tag(name) for name in primary.tags)
    series = None if primary.series is None else resolver.series(primary.series)
    languages = tuple(resolver.language(code) for code in primary.languages)
    work_check = check_work(catalog, primary)
    flags = set(primary.notes)
    resolutions = (*authors, *tags, *languages, *(r for r in (publisher, series) if r))
    flags.update(_resolution_flags(resolutions))
    if work_check.flags_duplicate:
        flags.add(FLAG_DUPLICATE_IN_CATALOGUE)
    if work_check.decision == DECISION_ERROR:
        flags.add(FLAG_MATCH_ERROR)
    if _formats_disagree(group):
        flags.add(FLAG_FORMATS_DISAGREE)
    return PlannedBook(
        index=index,
        files=tuple(group),
        authors=authors,
        publisher=publisher,
        tags=tags,
        series=series,
        languages=languages,
        work_check=work_check,
        flags=tuple(sorted(flags)),
    )


def check_work(catalog: Any, facts: FileFacts) -> WorkCheck:
    """
    Ask the Work matcher whether the catalogue already holds this book.

    Title, authors and identifiers are all offered as evidence; author
    disagreement lowers confidence, and an identifier owned by one Work is decisive.

    Example:
        >>> check_work(catalog, facts).decision  # doctest: +SKIP
        'no_match'


    :param catalog: Catalog exposing ``works.match``.
    :param facts: Primary file's facts.
    :return: The matcher's decision, or an ``error`` check if matching raised.
    """
    hints: dict[str, Any] = {}
    if facts.authors:
        hints["agents"] = list(facts.authors)
    if facts.identifiers:
        hints["identifiers"] = [
            {"scheme": scheme, "value": value} for scheme, value in facts.identifiers
        ]
    try:
        result = catalog.works.match(
            MetadataCandidate({"title": facts.title}, hints=hints)
        )
    except Exception as exc:  # Matching must not stop a dry run.
        return WorkCheck(DECISION_ERROR, reason=f"{type(exc).__name__}: {exc}"[:300])
    return WorkCheck(
        decision=str(result.decision or "no_match"),
        work_id=None if result.entity_id is None else int(result.entity_id),
        alternatives=tuple(int(item) for item in result.alternatives),
        reason=str(result.reason or ""),
    )


def _resolution(value: str, lookup: Lookup) -> Resolution:
    """
    Run one Catalog lookup and translate its decision.

    Example:
        >>> from types import SimpleNamespace
        >>> _resolution("x", lambda: SimpleNamespace(decision="match", entity_id=3, alternatives=(), reason="")).decision
        'reuse'


    :param value: Name being resolved.
    :param lookup: Callable returning a ``MatchResult``.
    :return: Reuse on match, new on no match, ambiguous on ambiguity or conflict, error on exception.
    """
    try:
        result = lookup()
    except Exception as exc:  # Matching must not stop a dry run.
        return Resolution(
            value, DECISION_ERROR, reason=f"{type(exc).__name__}: {exc}"[:300]
        )
    decision = str(result.decision or "no_match")
    if decision == "match" and result.entity_id is not None:
        return Resolution(value, DECISION_REUSE, entity_id=int(result.entity_id))
    if decision in {"ambiguous", "conflict"}:
        return Resolution(
            value,
            DECISION_AMBIGUOUS,
            alternatives=tuple(int(item) for item in result.alternatives),
            reason=str(result.reason or ""),
        )
    return Resolution(value, DECISION_NEW)


def _respelled(resolution: Resolution, value: str) -> Resolution:
    """
    Return a remembered resolution carrying this occurrence's spelling.

    Example:
        >>> _respelled(Resolution("Gothic", "reuse", 2), "gothic").value
        'gothic'


    :param resolution: Remembered resolution.
    :param value: Spelling seen on this book.
    :return: The same decision with the new spelling.
    """
    return resolution if resolution.value == value else replace(resolution, value=value)


def _resolution_flags(resolutions: Sequence[Resolution]) -> set[str]:
    """
    Collect the review flags implied by a book's name resolutions.

    Example:
        >>> sorted(_resolution_flags([Resolution("x", "ambiguous"), Resolution("zz", "unknown")]))
        ['ambiguous_name', 'unknown_language']


    :param resolutions: Every resolution on one book.
    :return: Flags for ambiguous names, unknown languages and lookup errors.
    """
    by_decision = {
        DECISION_AMBIGUOUS: FLAG_AMBIGUOUS_NAME,
        DECISION_UNKNOWN: FLAG_UNKNOWN_LANGUAGE,
        DECISION_ERROR: FLAG_MATCH_ERROR,
    }
    return {
        by_decision[item.decision]
        for item in resolutions
        if item.decision in by_decision
    }


def _formats_disagree(group: Sequence[FileFacts]) -> bool:
    """
    Report whether a book's formats carry different edition details.

    Example:
        >>> from LiuXin_alpha.ingest.embedded_metadata.models import IngestFile
        >>> a = FileFacts(IngestFile(1, "a.epub", "epub"), title="A", publisher="P")
        >>> b = FileFacts(IngestFile(2, "a.pdf", "pdf"), title="A", publisher="Q")
        >>> _formats_disagree([a, b])
        True


    :param group: Formats of one book.
    :return: True when date, publisher, identifiers or series details differ.
    """
    details = {
        (item.pubdate, item.publisher, item.identifiers, item.series, item.series_index)
        for item in group
    }
    return len(details) > 1


def _format_rank(facts: FileFacts) -> tuple[int, str]:
    """
    Sort key putting preferred formats first, then by storage key.

    Example:
        >>> from LiuXin_alpha.ingest.embedded_metadata.models import IngestFile
        >>> _format_rank(FileFacts(IngestFile(1, "a.epub", "epub"), title="A"))
        (0, 'a.epub')


    :param facts: Facts for one file.
    :return: Preference rank and key.
    """
    extension = facts.file.extension
    rank = (
        _FORMAT_PREFERENCE.index(extension)
        if extension in _FORMAT_PREFERENCE
        else len(_FORMAT_PREFERENCE)
    )
    return (rank, facts.file.storage_key)


def _with_run_duplicates(
    book: PlannedBook, same_identity: Sequence[int]
) -> PlannedBook:
    """
    Point a book at other planned books with the same identity, flagging it.

    Example:
        >>> from LiuXin_alpha.ingest.embedded_metadata.models import IngestFile
        >>> book = PlannedBook(1, (FileFacts(IngestFile(1, "a.epub", "epub"), title="A"),))
        >>> _with_run_duplicates(book, [1, 4]).duplicate_of
        (4,)


    :param book: Planned book.
    :param same_identity: Indexes of every planned book sharing this book's identity.
    :return: The book, with references and a flag when it has in-run duplicates.
    """
    others = tuple(index for index in same_identity if index != book.index)
    if not others:
        return book
    flags = tuple(sorted({*book.flags, FLAG_DUPLICATE_IN_RUN}))
    return replace(book, flags=flags, duplicate_of=others)


__all__ = [
    "NameResolver",
    "book_identity",
    "check_work",
    "group_formats",
    "plan_books",
]
