"""
Run a metadata ingest dry run over one Store and write its plan as JSON.

The caller supplies an open database, Library and Catalog; this module only
reads from them. Tags the catalogue already knows are offered to the readers,
so keyword splitting can recognise them. The plan file is the one thing written.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Collection, Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from LiuXin_alpha.ingest.embedded_metadata.models import FileFacts, MetadataIngestPlan
from LiuXin_alpha.ingest.embedded_metadata.planning import plan_books
from LiuXin_alpha.ingest.embedded_metadata.reading import (
    DEFAULT_MAX_BYTES,
    read_file,
    select_files,
)

_FILE_COLUMNS = (
    "file_id",
    "file_storage_key",
    "file_extension",
    "file_item_id",
    "file_size_bytes",
)
_STORE_COLUMNS = (
    "store_id",
    "store_uuid",
    "store_name",
    "store_kind",
    "store_root_uri",
)
# The file readers log each failure with a traceback; the plan already records
# every failure, so a large run would otherwise bury its progress lines.
_READER_LOGGER = "LiuXin_alpha-default-log"
SAMPLE_SIZE = 10

Progress = Callable[[int, int], None]


def plan_store_metadata(
    *,
    database: Any,
    library: Any,
    catalog: Any,
    store_id: int,
    extensions: Collection[str] | None = None,
    limit: int | None = None,
    max_bytes: int = DEFAULT_MAX_BYTES,
    progress: Progress | None = None,
    quiet_readers: bool = True,
) -> MetadataIngestPlan:
    """
    Read every unattached file in a Store and plan the books it would add.

    Example:
        >>> plan = plan_store_metadata(database=db, library=lib, catalog=cat, store_id=1)  # doctest: +SKIP
        >>> plan.summary()["books"]  # doctest: +SKIP
        12


    :param database: Open database supporting ``get_row_from_id`` and ``search``.
    :param library: Library whose ``get_store`` opens the Store by UUID.
    :param catalog: Catalog used for read-only matching.
    :param store_id: ``stores.store_id`` of the Store to read.
    :param extensions: Optional extensions to read; None reads every readable one.
    :param limit: Optional maximum number of files to read.
    :param max_bytes: Largest file to read.
    :param progress: Optional callback receiving files done and files chosen.
    :param quiet_readers: Whether to drop the readers' own error logs while reading.
    :return: The plan, including skipped counts and per-file failures.
    :raises ValueError: If the Store does not exist.
    """
    from LiuXin_alpha.metadata.file_sources import (
        get_metadata,
        known_metadata_file_types,
    )
    from LiuXin_alpha.metadata.file_sources.keywords import using_known_tags

    store_row = _store_row(database, store_id)
    store = library.get_store(UUID(str(store_row["store_uuid"])))
    rows = [
        _columns(row, _FILE_COLUMNS)
        for row in database.search("files", "file_store_id", store_id)
    ]
    files, skipped = select_files(
        rows,
        readable=set(known_metadata_file_types()),
        extensions=extensions,
        limit=limit,
    )
    plan = MetadataIngestPlan(store=store_row, skipped=skipped)
    facts: list[FileFacts] = []
    with (
        using_known_tags(lambda: _known_tags(catalog)),
        _quiet_logger(_READER_LOGGER, quiet_readers),
    ):
        for position, file in enumerate(files, start=1):
            outcome = read_file(
                store.open_file, file, reader=get_metadata, max_bytes=max_bytes
            )
            if isinstance(outcome, FileFacts):
                facts.append(outcome)
            else:
                plan.failures.append(outcome)
            if progress is not None:
                progress(position, len(files))
    plan.files_read = len(facts)
    plan.books = plan_books(catalog, facts)
    return plan


def write_plan(plan: MetadataIngestPlan, path: Path) -> Path:
    """
    Write a plan as indented JSON, creating the folder if needed.

    Example:
        >>> write_plan(MetadataIngestPlan(store={}), tmp_path / "plan.json")  # doctest: +SKIP


    :param plan: Plan to write.
    :param path: Destination file.
    :return: The resolved path written.
    """
    target = Path(path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(plan.to_dict(), ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return target.resolve()


def default_plan_path(
    database_path: str, store_id: int, *, now: datetime | None = None
) -> Path:
    """
    Choose a plan file beside the catalogue, named for the Store and time.

    Example:
        >>> from datetime import datetime, timezone
        >>> default_plan_path("/lib/books.sqlite", 3, now=datetime(2026, 11, 27, 18, 0, tzinfo=timezone.utc)).as_posix()
        '/lib/ingest-plans/metadata-plan-store-3-20261127T180000Z.json'


    :param database_path: Catalogue file path.
    :param store_id: Store the plan covers.
    :param now: Time used in the name; defaults to the current UTC time.
    :return: Path in an ``ingest-plans`` folder next to the catalogue.
    """
    moment = (now or datetime.now(UTC)).astimezone(UTC)
    name = f"metadata-plan-store-{store_id}-{moment:%Y%m%dT%H%M%SZ}.json"
    return Path(database_path).expanduser().parent / "ingest-plans" / name


def plan_result(plan: MetadataIngestPlan, report_path: Path) -> dict[str, Any]:
    """
    Build the compact job result: summary, plan path and a small sample.

    Example:
        >>> sorted(plan_result(MetadataIngestPlan(store={}), Path("p.json")))
        ['books_sample', 'dry_run', 'failures_sample', 'report_path', 'store', 'summary']


    :param plan: Completed plan.
    :param report_path: Where the full plan was written.
    :return: JSON-ready mapping small enough for a job result.
    """
    return {
        "dry_run": True,
        "store": dict(plan.store),
        "summary": plan.summary(),
        "report_path": str(report_path),
        "books_sample": [_book_line(book) for book in plan.books[:SAMPLE_SIZE]],
        "failures_sample": [
            failure.to_dict() for failure in plan.failures[:SAMPLE_SIZE]
        ],
    }


def _book_line(book: Any) -> dict[str, Any]:
    """
    Summarise a planned book for the job result.

    Example:
        >>> from LiuXin_alpha.ingest.embedded_metadata.models import FileFacts, IngestFile, PlannedBook
        >>> _book_line(PlannedBook(1, (FileFacts(IngestFile(1, "a.epub", "epub"), title="A"),)))["formats"]
        ['epub']


    :param book: Planned book.
    :return: Index, title, author names, formats, Work decision and flags.
    """
    return {
        "index": book.index,
        "title": book.title,
        "authors": [item.value for item in book.authors],
        "formats": [facts.file.extension for facts in book.files],
        "work_decision": book.work_check.decision,
        "flags": list(book.flags),
    }


@contextmanager
def _quiet_logger(name: str, enabled: bool) -> Iterator[None]:
    """
    Drop a logger's records below CRITICAL for the duration of the block.

    Example:
        >>> with _quiet_logger("example", True):
        ...     logging.getLogger("example").error("hidden")


    :param name: Logger name.
    :param enabled: Whether to filter at all.
    :return: Context manager; the filter is removed on exit.
    """
    if not enabled:
        yield
        return
    logger = logging.getLogger(name)

    def keep(record: logging.LogRecord) -> bool:
        """
        Keep only critical records.

        Example:
            >>> keep(logging.makeLogRecord({"levelno": logging.ERROR}))  # doctest: +SKIP
            False


        :param record: Log record.
        :return: True for CRITICAL and above.
        """
        return record.levelno >= logging.CRITICAL

    logger.addFilter(keep)
    try:
        yield
    finally:
        logger.removeFilter(keep)


def _store_row(database: Any, store_id: int) -> dict[str, Any]:
    """
    Load the Store's identifying columns.

    Example:
        >>> _store_row(db, 1)["store_kind"]  # doctest: +SKIP
        'on_disk_existing_unmanaged_drive'


    :param database: Open database.
    :param store_id: Store id.
    :return: Id, UUID, name, kind and root URI.
    :raises ValueError: If no such Store exists.
    """
    row = database.get_row_from_id("stores", int(store_id))
    if row is None:
        raise ValueError(f"No store found for id {store_id}.")
    return _columns(row, _STORE_COLUMNS)


def _columns(row: Any, names: tuple[str, ...]) -> dict[str, Any]:
    """
    Copy named columns out of a database row facade.

    Example:
        >>> _columns({"a": 1, "b": 2}, ("a",))
        {'a': 1}


    :param row: Row supporting item access.
    :param names: Columns to copy.
    :return: Plain mapping of those columns.
    """
    return {name: row[name] for name in names}


def _known_tags(catalog: Any) -> list[str]:
    """
    List every tag name in the catalogue, for keyword splitting.

    Example:
        >>> _known_tags(catalog)  # doctest: +SKIP
        ['Gothic', 'Science fiction']


    :param catalog: Catalog exposing ``tags.iter_rows``.
    :return: Non-empty tag names.
    """
    names = []
    for row in catalog.tags.iter_rows():
        value = row.get("tag") if isinstance(row, Mapping) else None
        if value:
            names.append(str(value))
    return names


__all__ = [
    "SAMPLE_SIZE",
    "default_plan_path",
    "plan_result",
    "plan_store_metadata",
    "write_plan",
]
