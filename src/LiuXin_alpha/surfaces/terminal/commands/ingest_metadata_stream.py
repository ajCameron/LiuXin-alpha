"""
Run ``ingest metadata <store> --stream``: read a few folders, write their books, repeat.

Writes stay in the process that owns the cache, and job workers only read, so
a stream alternates the two halves of the step in small batches. Each batch is
a dry-run job over the next whole folders, then ``ingest.metadata.apply`` on
that batch's plan. Each book is shown as it is added, and every batch's plan
and apply report are kept in the stream's folder. Nothing waits for review:
the plan's flags are reported, and the rules that make a dry run safe (a book
is always new, names reuse only a unique match) still decide what is written.
"""

from __future__ import annotations

import dataclasses
from collections import Counter
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path, PurePath
from typing import Any

from LiuXin_alpha.surfaces.terminal.commands.sync import _wait_for_job

DEFAULT_BATCH_FILES = 50
STOP_FINISHED = "finished"
STOP_LIMIT = "limit reached"
STOP_INTERRUPTED = "interrupted"
STOP_FAILED = "failed"


@dataclasses.dataclass(frozen=True)
class StreamOptions:
    """
    Hold what one stream reads, how much it adds, and where its batch files go.

    Example:
        >>> StreamOptions(store_id=1).batch_files
        50


    :ivar store_id: Store to read.
    :ivar extensions: Optional extensions to read.
    :ivar max_file_mb: Optional largest file to read, in MiB.
    :ivar limit: Optional number of books to add before stopping.
    :ivar batch_files: Files after which a batch starts no new folder.
    :ivar report_folder: Optional folder for the batch files; None uses one beside the catalogue.
    :ivar job_backend: Optional job backend for the read jobs.
    :ivar job_timeout_s: Optional timeout for each read job.
    :ivar show_books: Whether to print each book as it is added.
    """

    store_id: int
    extensions: list[str] | None = None
    max_file_mb: int | None = None
    limit: int | None = None
    batch_files: int = DEFAULT_BATCH_FILES
    report_folder: str | None = None
    job_backend: str | None = None
    job_timeout_s: float | None = None
    show_books: bool = True


@dataclasses.dataclass
class StreamTotals:
    """
    Add up what a stream's batches read and wrote.

    Example:
        >>> totals = StreamTotals()
        >>> totals.add_applied({"summary": {"works_created": 2, "outcomes": {"created": 2}}})
        >>> totals.works_created, dict(totals.outcomes)
        (2, {'created': 2})


    :ivar batches: Batches read.
    :ivar files_read: Files whose metadata was read.
    :ivar files_unreadable: Files that could not be read.
    :ivar books_planned: Books the batches planned.
    :ivar works_created: Books added.
    :ivar manifestations_created: Formats added.
    :ivar files_attached: Files attached to new Items.
    :ivar outcomes: Books by apply outcome.
    :ivar names_created: New names by kind.
    :ivar not_stored: Values not stored, by kind.
    :ivar flags: Review flags on the books added.
    :ivar folder: Where the batch plans and reports are.
    :ivar stopped: Why the stream stopped.
    :ivar error: The failure, when it stopped on one.
    """

    batches: int = 0
    files_read: int = 0
    files_unreadable: int = 0
    books_planned: int = 0
    works_created: int = 0
    manifestations_created: int = 0
    files_attached: int = 0
    outcomes: Counter[str] = dataclasses.field(default_factory=Counter)
    names_created: Counter[str] = dataclasses.field(default_factory=Counter)
    not_stored: Counter[str] = dataclasses.field(default_factory=Counter)
    flags: Counter[str] = dataclasses.field(default_factory=Counter)
    folder: str = ""
    stopped: str = ""
    error: str = ""

    def add_planned(self, planned: Mapping[str, Any]) -> None:
        """
        Count one batch's reading.

        Example:
            >>> totals = StreamTotals()
            >>> totals.add_planned({"summary": {"files_read": 3, "books": 2}, "report_path": "/s/batch-0001.json"})
            >>> totals.files_read, totals.folder
            (3, '/s')


        :param planned: Result of the batch's read job.
        :return: None.
        """
        summary = dict(planned.get("summary") or {})
        self.batches += 1
        self.files_read += int(summary.get("files_read") or 0)
        self.files_unreadable += int(summary.get("files_failed") or 0)
        self.books_planned += int(summary.get("books") or 0)
        if not self.folder and planned.get("report_path"):
            self.folder = str(PurePath(str(planned["report_path"])).parent)

    def add_applied(self, applied: Mapping[str, Any]) -> None:
        """
        Count one batch's writing.

        Example:
            >>> totals = StreamTotals()
            >>> totals.add_applied({"summary": {"names_created": {"tag": 1}}, "books": [{"outcome": "created", "flags": ["no_authors"]}]})
            >>> dict(totals.names_created), dict(totals.flags)
            ({'tag': 1}, {'no_authors': 1})


        :param applied: Result of ``ingest.metadata.apply`` with every book.
        :return: None.
        """
        summary = dict(applied.get("summary") or {})
        self.works_created += int(summary.get("works_created") or 0)
        self.manifestations_created += int(summary.get("manifestations_created") or 0)
        self.files_attached += int(summary.get("files_attached") or 0)
        self.outcomes.update(dict(summary.get("outcomes") or {}))
        self.names_created.update(dict(summary.get("names_created") or {}))
        self.not_stored.update(dict(summary.get("not_stored") or {}))
        for book in applied.get("books") or ():
            if book.get("outcome") == "created":
                self.flags.update(str(flag) for flag in book.get("flags") or ())

    def to_dict(self) -> dict[str, Any]:
        """
        Project the totals for ``--json``.

        Example:
            >>> StreamTotals(stopped="finished").to_dict()["stopped"]
            'finished'


        :return: Every count, with counters as sorted mappings.
        """
        values = dataclasses.asdict(self)
        for name in ("outcomes", "names_created", "not_stored", "flags"):
            values[name] = dict(sorted(getattr(self, name).items()))
        return values


def stream_name(store_id: int, *, now: datetime | None = None) -> str:
    """
    Name a stream for its Store and start time; it becomes its batches' folder.

    Example:
        >>> stream_name(3, now=datetime(2026, 11, 28, 2, 0, tzinfo=UTC))
        'metadata-stream-store-3-20261128T020000Z'


    :param store_id: Store streamed.
    :param now: Start time; defaults to the current UTC time.
    :return: The name.
    """
    moment = (now or datetime.now(UTC)).astimezone(UTC)
    return f"metadata-stream-store-{store_id}-{moment:%Y%m%dT%H%M%SZ}"


def run_stream(
    browser: Any, options: StreamOptions, *, now: datetime | None = None
) -> StreamTotals:
    """
    Read and write batches until the Store is done, the limit is met, or a batch fails.

    Interrupting stops after the book being written, which rolls back; books
    already added stay, and running the stream again continues with what is
    left, since attached files are not read again.

    Example:
        >>> totals = run_stream(browser, StreamOptions(store_id=1))  # doctest: +SKIP
        >>> totals.stopped  # doctest: +SKIP
        'finished'


    :param browser: Host providing Core commands, job queries and output.
    :param options: What to read and how much to add.
    :param now: Start time for the stream's name.
    :return: What the batches read and wrote, and why the stream stopped.
    """
    name = stream_name(options.store_id, now=now)
    totals = StreamTotals()
    after_folder: str | None = None
    try:
        while not totals.stopped:
            planned = _read_batch(browser, options, name, totals, after_folder)
            totals.add_planned(planned)
            if int(dict(planned.get("summary") or {}).get("books") or 0):
                applied = _write_batch(browser, options, planned, totals)
                totals.add_applied(applied)
                _emit_books(browser, options, applied)
            _emit_unreadable(browser, options, planned)
            after_folder = planned.get("last_folder")
            totals.stopped = _stop_reason(planned, totals, options) or ""
    except KeyboardInterrupt:
        totals.stopped = STOP_INTERRUPTED
    except RuntimeError as exc:
        totals.stopped = STOP_FAILED
        totals.error = str(exc)
    return totals


def stream_sections(
    totals: StreamTotals, store: Mapping[str, Any]
) -> list[tuple[str, list[tuple[str, Any]]]]:
    """
    Arrange a stream's totals as terminal detail sections.

    Example:
        >>> [title for title, _ in stream_sections(StreamTotals(), {"store_id": 1})]
        ['Store', 'Stream', 'Outcomes', 'Review flags', 'Names created', 'Not stored']


    :param totals: The stream's totals.
    :param store: The Store row streamed.
    :return: ``(title, rows)`` pairs.
    """
    return [
        (
            "Store",
            [
                (key, store.get(key, ""))
                for key in ("store_id", "store_name", "store_kind")
            ],
        ),
        (
            "Stream",
            [
                ("batches", totals.batches),
                ("files_read", totals.files_read),
                ("files_unreadable", totals.files_unreadable),
                ("books_added", totals.works_created),
                ("manifestations_created", totals.manifestations_created),
                ("files_attached", totals.files_attached),
                ("stopped", totals.stopped),
                ("batch_files", totals.folder),
            ],
        ),
        ("Outcomes", _rows(totals.outcomes)),
        ("Review flags", _rows(totals.flags)),
        ("Names created", _rows(totals.names_created)),
        ("Not stored", _rows(totals.not_stored)),
    ]


def book_line(book: Mapping[str, Any]) -> str:
    """
    Describe one applied book in a line.

    Example:
        >>> book_line({"index": 3, "title": "T", "outcome": "created", "authors": ["A"], "formats": ["epub"], "work_id": 9, "flags": []})
        '  + #3 T — A [epub] → Work 9'
        >>> book_line({"index": 4, "title": "U", "outcome": "skipped", "reason": "file 3 has moved"})
        '  skipped #4 U: file 3 has moved'


    :param book: One entry of the apply result's ``books``.
    :return: The line.
    """
    index, title = book.get("index"), book.get("title")
    if book.get("outcome") != "created":
        return f"  {book.get('outcome')} #{index} {title}: {book.get('reason')}"
    authors = ", ".join(book.get("authors") or []) or "no authors"
    formats = ", ".join(book.get("formats") or [])
    flags = ", ".join(book.get("flags") or [])
    suffix = f" ({flags})" if flags else ""
    return f"  + #{index} {title} — {authors} [{formats}] → Work {book.get('work_id')}{suffix}"


def _read_batch(
    browser: Any,
    options: StreamOptions,
    name: str,
    totals: StreamTotals,
    after_folder: str | None,
) -> dict[str, Any]:
    """
    Plan the next batch of whole folders in a read-only job and wait for it.

    Example:
        >>> _read_batch(browser, StreamOptions(store_id=1), "s", StreamTotals(), None)  # doctest: +SKIP


    :param browser: Host providing Core commands and job queries.
    :param options: Stream options.
    :param name: Stream name.
    :param totals: Totals so far, for the batch number and first book index.
    :param after_folder: Last folder of the previous batch, or None for the first.
    :return: The read job's result.
    :raises RuntimeError: If no job id comes back or the job fails.
    """
    batch = totals.batches + 1
    report_path = None
    if options.report_folder:
        report_path = str(Path(options.report_folder) / f"batch-{batch:04d}.json")
    submitted = browser.execute_core_command(
        "ingest.metadata.plan",
        payload={
            "store_id": options.store_id,
            "extensions": options.extensions,
            "limit": options.batch_files,
            "max_file_mb": options.max_file_mb,
            "report_path": report_path,
            "progress_output": False,
            "after_folder": after_folder,
            "first_index": totals.books_planned + 1,
            "stream": name,
            "batch": batch,
            "job_backend": options.job_backend,
            "job_timeout_s": options.job_timeout_s,
            "label": f"ingest metadata stream:{options.store_id} batch {batch}",
        },
    )
    job_id = str((submitted or {}).get("job_id") or "")
    if not job_id:
        raise RuntimeError(f"Core did not return a job id for stream batch {batch}.")
    try:
        return _wait_for_job(browser, job_id)
    except RuntimeError as exc:
        where = "" if after_folder is None else f" after folder {after_folder!r}"
        raise RuntimeError(
            f"Stream batch {batch}{where} failed while reading: {exc}"
        ) from exc


def _write_batch(
    browser: Any,
    options: StreamOptions,
    planned: Mapping[str, Any],
    totals: StreamTotals,
) -> dict[str, Any]:
    """
    Apply one batch's plan in Core, asking for every book's outcome.

    Example:
        >>> _write_batch(browser, StreamOptions(store_id=1, limit=5), planned, StreamTotals())  # doctest: +SKIP


    :param browser: Host providing Core commands.
    :param options: Stream options.
    :param planned: The batch's read job result.
    :param totals: Totals so far, for what is left of the limit.
    :return: The apply result.
    """
    remaining = None if options.limit is None else options.limit - totals.works_created
    return browser.execute_core_command(
        "ingest.metadata.apply",
        payload={
            "plan_path": planned.get("report_path"),
            "store_id": options.store_id,
            "limit": remaining,
            "books_in_result": True,
        },
    )


def _stop_reason(
    planned: Mapping[str, Any], totals: StreamTotals, options: StreamOptions
) -> str | None:
    """
    Decide whether the batch just done was the last.

    Example:
        >>> _stop_reason({"last_folder": None}, StreamTotals(), StreamOptions(store_id=1))
        'finished'


    :param planned: The batch's read job result.
    :param totals: Totals including this batch.
    :param options: Stream options.
    :return: Why to stop, or None to read another batch.
    """
    skipped = dict(dict(planned.get("summary") or {}).get("files_skipped") or {})
    more = planned.get("last_folder") is not None and bool(skipped.get("over_limit"))
    left = more or bool(totals.outcomes.get("not_attempted"))
    if options.limit is not None and totals.works_created >= options.limit and left:
        return STOP_LIMIT
    return None if more else STOP_FINISHED


def _emit_books(
    browser: Any, options: StreamOptions, applied: Mapping[str, Any]
) -> None:
    """
    Print each book of a batch as it was added, skipped or failed.

    Example:
        >>> _emit_books(browser, StreamOptions(store_id=1), applied)  # doctest: +SKIP


    :param browser: Host providing output.
    :param options: Stream options.
    :param applied: The apply result with every book.
    :return: None.
    """
    if not options.show_books:
        return
    for book in applied.get("books") or ():
        browser.emit(book_line(book))


def _emit_unreadable(
    browser: Any, options: StreamOptions, planned: Mapping[str, Any]
) -> None:
    """
    Print a batch's unreadable files, up to the read job's sample.

    Example:
        >>> _emit_unreadable(browser, StreamOptions(store_id=1), planned)  # doctest: +SKIP


    :param browser: Host providing output.
    :param options: Stream options.
    :param planned: The batch's read job result.
    :return: None.
    """
    if not options.show_books:
        return
    failures = list(planned.get("failures_sample") or ())
    for failure in failures:
        key = dict(failure.get("file") or {}).get("storage_key", "")
        browser.emit(
            f"  unreadable {key}: {failure.get('stage')}: {failure.get('error')}"
        )
    count = int(dict(planned.get("summary") or {}).get("files_failed") or 0)
    if count > len(failures):
        browser.emit(
            f"  ... and {count - len(failures)} more unreadable; see {planned.get('report_path')}"
        )


def _rows(counts: Counter[str]) -> list[tuple[str, Any]]:
    """
    Turn a counter into sorted rows, or one ``none`` row when empty.

    Example:
        >>> _rows(Counter()), _rows(Counter({"b": 1}))
        ([('none', '')], [('b', 1)])


    :param counts: Counter to show.
    :return: Rows.
    """
    rows: list[tuple[str, Any]] = sorted(counts.items())
    return rows or [("none", "")]


__all__ = [
    "DEFAULT_BATCH_FILES",
    "StreamOptions",
    "StreamTotals",
    "book_line",
    "run_stream",
    "stream_name",
    "stream_sections",
]
