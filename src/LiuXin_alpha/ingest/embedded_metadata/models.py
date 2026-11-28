"""
Describe what a metadata ingest dry run read, matched and would create.

The records are plain, frozen values so a plan can cross a job boundary and be
written as JSON. Nothing here touches a catalogue, a Store or a file: reading
lives in :mod:`.reading`, matching and grouping in :mod:`.planning`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# Flags explain why a planned book needs a reviewer's attention. They never
# change what would be created; the write phase decides how to act on them.
FLAG_TITLE_FROM_FILENAME = "title_from_filename"
FLAG_NO_AUTHORS = "no_authors"
FLAG_UNDEFINED_DATE = "undefined_date_dropped"
FLAG_PARTIAL_DATE = "date_may_be_partial"
FLAG_DUPLICATE_IN_CATALOGUE = "possible_duplicate_in_catalogue"
FLAG_DUPLICATE_IN_RUN = "possible_duplicate_in_run"
FLAG_AMBIGUOUS_NAME = "ambiguous_name"
FLAG_UNKNOWN_LANGUAGE = "unknown_language"
FLAG_FORMATS_DISAGREE = "formats_disagree"
FLAG_MATCH_ERROR = "match_error"

DECISION_REUSE = "reuse"
DECISION_NEW = "new"
DECISION_AMBIGUOUS = "ambiguous"
DECISION_UNKNOWN = "unknown"
DECISION_ERROR = "error"


@dataclass(frozen=True, slots=True)
class IngestFile:
    """
    Identify one registered file a dry run considers, without its bytes.

    Example:
        >>> IngestFile(7, "novels/a.epub", "epub", 1024).folder
        'novels'


    :ivar file_id: ``files.file_id`` of the registered row.
    :ivar storage_key: Store-relative key used to open the bytes.
    :ivar extension: Lower-case extension without a dot, used to pick a reader.
    :ivar size_bytes: Recorded size, or None when the sync did not record one.
    """

    file_id: int
    storage_key: str
    extension: str
    size_bytes: int | None = None

    @property
    def folder(self) -> str:
        """
        Return the key's parent folder, which groups formats of one book.

        Example:
            >>> IngestFile(1, "a.epub", "epub").folder
            ''


        :return: Everything before the last ``/`` of the key, or an empty string.
        """
        head, _, _ = self.storage_key.rpartition("/")
        return head

    @property
    def stem(self) -> str:
        """
        Return the file name without folder or extension.

        Example:
            >>> IngestFile(1, "x/My_Book.epub", "epub").stem
            'My_Book'


        :return: The last key segment up to its final dot.
        """
        name = self.storage_key.rpartition("/")[2]
        base, dot, _ = name.rpartition(".")
        return base if dot else name

    def to_dict(self) -> dict[str, Any]:
        """
        Project the file identity for a JSON report.

        Example:
            >>> IngestFile(1, "a.epub", "epub", 3).to_dict()["storage_key"]
            'a.epub'


        :return: File id, storage key, extension and recorded size.
        """
        return {
            "file_id": self.file_id,
            "storage_key": self.storage_key,
            "extension": self.extension,
            "size_bytes": self.size_bytes,
        }


@dataclass(frozen=True, slots=True)
class FileFacts:
    """
    Hold the normalised metadata read from one file.

    Values are already cleaned for the catalogue boundary: placeholder titles
    and authors are gone, calibre's undefined date is dropped, and dates are
    calendar dates in the reading machine's time zone.

    Example:
        >>> facts = FileFacts(IngestFile(1, "a.epub", "epub"), title="A")
        >>> facts.authors, facts.notes
        ((), ())


    :ivar file: The file the facts came from.
    :ivar title: Embedded title, or a title derived from the file name.
    :ivar title_sort: Embedded sort title, if any.
    :ivar authors: Author names in file order.
    :ivar author_sort: Embedded author sort string, if any.
    :ivar languages: Language codes as the reader reported them.
    :ivar pubdate: ISO calendar date, or None.
    :ivar publisher: Publisher name, or None.
    :ivar identifiers: ``(scheme, value)`` pairs.
    :ivar tags: Tag names in file order.
    :ivar series: Series name, or None.
    :ivar series_index: Position in the series, or None.
    :ivar comments: The description, as the file holds it (often HTML), or None.
    :ivar cover: ``(image extension, byte count)`` of an embedded cover, or None.
    :ivar notes: Flags raised while normalising this file.
    """

    file: IngestFile
    title: str
    title_sort: str | None = None
    authors: tuple[str, ...] = ()
    author_sort: str | None = None
    languages: tuple[str, ...] = ()
    pubdate: str | None = None
    publisher: str | None = None
    identifiers: tuple[tuple[str, str], ...] = ()
    tags: tuple[str, ...] = ()
    series: str | None = None
    series_index: float | None = None
    comments: str | None = None
    cover: tuple[str, int] | None = None
    notes: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        """
        Project the facts for a JSON report.

        Example:
            >>> FileFacts(IngestFile(1, "a.epub", "epub"), title="A").to_dict()["title"]
            'A'


        :return: The file identity plus every normalised field.
        """
        return {
            "file": self.file.to_dict(),
            "title": self.title,
            "title_sort": self.title_sort,
            "authors": list(self.authors),
            "author_sort": self.author_sort,
            "languages": list(self.languages),
            "pubdate": self.pubdate,
            "publisher": self.publisher,
            "identifiers": {scheme: value for scheme, value in self.identifiers},
            "tags": list(self.tags),
            "series": self.series,
            "series_index": self.series_index,
            "comments": self.comments,
            "cover": (
                None
                if self.cover is None
                else {"format": self.cover[0], "bytes": self.cover[1]}
            ),
            "notes": list(self.notes),
        }


@dataclass(frozen=True, slots=True)
class FileFailure:
    """
    Record why one file produced no facts.

    Example:
        >>> FileFailure(IngestFile(1, "a.pdf", "pdf"), "read", "bad xref").to_dict()["stage"]
        'read'


    :ivar file: The file that failed.
    :ivar stage: ``too_large``, ``open`` or ``read``.
    :ivar error: Short exception type and message.
    """

    file: IngestFile
    stage: str
    error: str

    def to_dict(self) -> dict[str, Any]:
        """
        Project the failure for a JSON report.

        Example:
            >>> FileFailure(IngestFile(1, "a.pdf", "pdf"), "open", "gone").to_dict()["error"]
            'gone'


        :return: File identity, failing stage and error text.
        """
        return {"file": self.file.to_dict(), "stage": self.stage, "error": self.error}


@dataclass(frozen=True, slots=True)
class Resolution:
    """
    Say how one named value would be stored: reuse a row, create one, or neither.

    Example:
        >>> Resolution("Mary Shelley", "reuse", 4).to_dict()
        {'value': 'Mary Shelley', 'decision': 'reuse', 'entity_id': 4}


    :ivar value: The name as read.
    :ivar decision: ``reuse``, ``new``, ``ambiguous``, ``unknown`` or ``error``.
    :ivar entity_id: The row to reuse, when the decision is ``reuse``.
    :ivar alternatives: Competing rows, when the decision is ``ambiguous``.
    :ivar reason: Matcher explanation for ambiguous or failed lookups.
    """

    value: str
    decision: str
    entity_id: int | None = None
    alternatives: tuple[int, ...] = ()
    reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        """
        Project the resolution for a JSON report, omitting empty fields.

        Example:
            >>> Resolution("x", "new").to_dict()
            {'value': 'x', 'decision': 'new'}


        :return: Value and decision, plus any id, alternatives or reason.
        """
        payload: dict[str, Any] = {"value": self.value, "decision": self.decision}
        if self.entity_id is not None:
            payload["entity_id"] = self.entity_id
        if self.alternatives:
            payload["alternatives"] = list(self.alternatives)
        if self.reason:
            payload["reason"] = self.reason
        return payload


@dataclass(frozen=True, slots=True)
class WorkCheck:
    """
    Summarise what the Work matcher said about a planned book.

    A matching Work never merges the book: per the agreed policy it only flags
    a possible duplicate for review.

    Example:
        >>> WorkCheck("no_match").flags_duplicate
        False


    :ivar decision: Matcher decision, or ``error``.
    :ivar work_id: The matched Work, if any.
    :ivar alternatives: Other plausible Works.
    :ivar reason: Matcher explanation.
    """

    decision: str
    work_id: int | None = None
    alternatives: tuple[int, ...] = ()
    reason: str = ""

    @property
    def flags_duplicate(self) -> bool:
        """
        Return whether the catalogue already holds a plausible match.

        Example:
            >>> WorkCheck("ambiguous", alternatives=(2, 3)).flags_duplicate
            True


        :return: True for match, ambiguous and conflict decisions.
        """
        return self.decision in {"match", "ambiguous", "conflict"}

    def to_dict(self) -> dict[str, Any]:
        """
        Project the check for a JSON report.

        Example:
            >>> WorkCheck("match", 3).to_dict()["work_id"]
            3


        :return: Decision, matched Work, alternatives and reason.
        """
        return {
            "decision": self.decision,
            "work_id": self.work_id,
            "alternatives": list(self.alternatives),
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class PlannedBook:
    """
    Describe one book the write phase would create, with its formats.

    The first file supplies the book-level values; every file becomes its own
    Manifestation and Item under one Work and Expression. In the JSON plan each
    book carries ``"action": "create"``; a reviewer can change it to ``"skip"``.

    Example:
        >>> book = PlannedBook(1, (FileFacts(IngestFile(1, "a.epub", "epub"), title="A"),))
        >>> book.title
        'A'


    :ivar index: One-based position in the plan, used for cross references.
    :ivar files: Facts for each format, primary first.
    :ivar authors: Resolution per author.
    :ivar publisher: Resolution for the publisher, if any.
    :ivar tags: Resolution per tag.
    :ivar series: Resolution for the series, if any.
    :ivar languages: Resolution per language code.
    :ivar work_check: What the Work matcher said.
    :ivar flags: Review flags, sorted.
    :ivar duplicate_of: Indexes of other planned books that look like this one.
    """

    index: int
    files: tuple[FileFacts, ...]
    authors: tuple[Resolution, ...] = ()
    publisher: Resolution | None = None
    tags: tuple[Resolution, ...] = ()
    series: Resolution | None = None
    languages: tuple[Resolution, ...] = ()
    work_check: WorkCheck = field(default_factory=lambda: WorkCheck("not_checked"))
    flags: tuple[str, ...] = ()
    duplicate_of: tuple[int, ...] = ()

    @property
    def title(self) -> str:
        """
        Return the book-level title, taken from the primary file.

        Example:
            >>> PlannedBook(1, (FileFacts(IngestFile(1, "a.epub", "epub"), title="T"),)).title
            'T'


        :return: The primary file's normalised title.
        """
        return self.files[0].title

    def to_dict(self) -> dict[str, Any]:
        """
        Project the planned book for a JSON report.

        Example:
            >>> PlannedBook(1, (FileFacts(IngestFile(1, "a.epub", "epub"), title="T"),)).to_dict()["index"]
            1


        :return: Book-level values, per-value resolutions, flags and every format's facts.
        """
        primary = self.files[0]
        return {
            "index": self.index,
            "action": "create",
            "title": primary.title,
            "pubdate": primary.pubdate,
            "series_index": primary.series_index,
            "authors": [item.to_dict() for item in self.authors],
            "publisher": None if self.publisher is None else self.publisher.to_dict(),
            "tags": [item.to_dict() for item in self.tags],
            "series": None if self.series is None else self.series.to_dict(),
            "languages": [item.to_dict() for item in self.languages],
            "work_check": self.work_check.to_dict(),
            "flags": list(self.flags),
            "duplicate_of": list(self.duplicate_of),
            "formats": [facts.to_dict() for facts in self.files],
        }


@dataclass(slots=True)
class MetadataIngestPlan:
    """
    Collect a whole dry run: the Store, what was skipped, the books and the failures.

    Example:
        >>> plan = MetadataIngestPlan(store={"store_id": 1})
        >>> plan.summary()["books"]
        0


    :ivar store: Store id, name, kind and root URI.
    :ivar skipped: Counts of registered files not read, by reason.
    :ivar books: Planned books in folder order.
    :ivar failures: Files that could not be read.
    :ivar files_read: Number of files read successfully.
    :ivar after_folder: Folder this run started after, or None from the start.
    :ivar last_folder: Folder of the last file chosen, or None when none was.
    """

    store: dict[str, Any]
    skipped: dict[str, int] = field(default_factory=dict)
    books: list[PlannedBook] = field(default_factory=list)
    failures: list[FileFailure] = field(default_factory=list)
    files_read: int = 0
    after_folder: str | None = None
    last_folder: str | None = None

    def summary(self) -> dict[str, Any]:
        """
        Count books, files, flags and new or reused names across the plan.

        Example:
            >>> MetadataIngestPlan(store={}).summary()["files_failed"]
            0


        :return: Totals suitable for a terminal summary.
        """
        flags: dict[str, int] = {}
        names: dict[str, dict[str, int]] = {}
        seen: set[tuple[str, str, str]] = set()
        for book in self.books:
            for flag in book.flags:
                flags[flag] = flags.get(flag, 0) + 1
            for kind, resolutions in _book_resolutions(book):
                _count_unique(names.setdefault(kind, {}), seen, kind, resolutions)
        return {
            "books": len(self.books),
            "files_read": self.files_read,
            "files_failed": len(self.failures),
            "files_skipped": dict(sorted(self.skipped.items())),
            "multi_format_books": sum(1 for book in self.books if len(book.files) > 1),
            "flags": dict(sorted(flags.items())),
            "names": {
                kind: dict(sorted(counts.items()))
                for kind, counts in sorted(names.items())
            },
        }

    def to_dict(self) -> dict[str, Any]:
        """
        Project the full plan for a JSON report file.

        Example:
            >>> sorted(MetadataIngestPlan(store={}).to_dict())
            ['books', 'dry_run', 'failures', 'selection', 'store', 'summary']


        :return: Store, the folders covered, summary, every book and every failure.
        """
        return {
            "dry_run": True,
            "store": dict(self.store),
            "selection": {
                "after_folder": self.after_folder,
                "last_folder": self.last_folder,
            },
            "summary": self.summary(),
            "books": [book.to_dict() for book in self.books],
            "failures": [failure.to_dict() for failure in self.failures],
        }


def _book_resolutions(book: PlannedBook) -> list[tuple[str, tuple[Resolution, ...]]]:
    """
    List a book's resolutions grouped by the kind of name they resolve.

    Example:
        >>> _book_resolutions(PlannedBook(1, (FileFacts(IngestFile(1, "a.epub", "epub"), title="T"),)))[0]
        ('authors', ())


    :param book: Planned book to inspect.
    :return: ``(kind, resolutions)`` pairs for authors, publishers, tags, series and languages.
    """
    return [
        ("authors", book.authors),
        ("publishers", () if book.publisher is None else (book.publisher,)),
        ("tags", book.tags),
        ("series", () if book.series is None else (book.series,)),
        ("languages", book.languages),
    ]


def _count_unique(
    counts: dict[str, int],
    seen: set[tuple[str, str, str]],
    kind: str,
    resolutions: tuple[Resolution, ...],
) -> None:
    """
    Count distinct names per decision, so a new tag used by ten books counts once.

    Example:
        >>> counts, seen = {}, set()
        >>> _count_unique(counts, seen, "tags", (Resolution("x", "new"), Resolution("X", "new")))
        >>> counts
        {'new': 1}


    :param counts: Decision counts for one kind, updated in place.
    :param seen: ``(kind, decision, folded name)`` keys already counted, updated in place.
    :param kind: Kind of name being counted.
    :param resolutions: Resolutions to add.
    :return: None.
    """
    for resolution in resolutions:
        key = (kind, resolution.decision, resolution.value.casefold())
        if key in seen:
            continue
        seen.add(key)
        counts[resolution.decision] = counts.get(resolution.decision, 0) + 1
