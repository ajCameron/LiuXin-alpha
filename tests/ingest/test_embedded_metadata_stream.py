"""
Check the metadata ingest stream: whole-folder batches, books added as they are read, and its wiring.

The end-to-end tests register four folders of small books and run the stream
through the terminal, as a user would. The unit tests drive the stream loop
with a scripted host, to pin down its stopping rules and what each batch asks
Core for.
"""

from __future__ import annotations

import io
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from LiuXin_alpha.core.commands import CoreCommand
from LiuXin_alpha.core.errors import CoreDispatchError
from LiuXin_alpha.core.library_services import LibraryServices
from LiuXin_alpha.core.program_services.ingest import ingest_metadata_plan_start
from LiuXin_alpha.ingest.embedded_metadata import (
    plan_store_metadata,
    select_files,
    stream_batch_path,
)
from LiuXin_alpha.surfaces.terminal.commands.ingest_metadata_stream import (
    STOP_FAILED,
    STOP_FINISHED,
    STOP_INTERRUPTED,
    STOP_LIMIT,
    StreamOptions,
    run_stream,
)
from tests.support.embedded_metadata_books import epub_bytes, fb2_bytes


def _row(file_id: int, key: str, *, item_id: int | None = None) -> dict[str, Any]:
    """
    Build a minimal ``files`` row.

    Example:
        >>> _row(1, "a/b.epub")["file_extension"]
        'epub'


    :param file_id: File id.
    :param key: Storage key.
    :param item_id: Attached Item, if any.
    :return: Row mapping.
    """
    return {
        "file_id": file_id,
        "file_storage_key": key,
        "file_extension": key.rpartition(".")[2],
        "file_item_id": item_id,
        "file_size_bytes": 10,
    }


def test_select_files_takes_whole_folders_and_starts_after_a_folder() -> None:
    """
    A limit never splits a folder, folders come in order, and a cursor skips folders already done.

    Example:
        python -m pytest -q tests/ingest/test_embedded_metadata_stream.py


    :return: None.
    """
    rows = [
        _row(1, "a/x.epub"),
        _row(2, "a/c/z.epub"),
        _row(3, "a/y.epub"),
        _row(4, "b/x.epub"),
        _row(5, "top.epub"),
    ]

    files, skipped = select_files(rows, readable={"epub"}, limit=1)
    assert [item.storage_key for item in files] == ["top.epub"]
    assert skipped == {"over_limit": 4}

    files, skipped = select_files(rows, readable={"epub"}, limit=1, after_folder="")
    assert [item.storage_key for item in files] == ["a/x.epub", "a/y.epub"]
    assert skipped == {"earlier_folder": 1, "over_limit": 2}

    files, skipped = select_files(rows, readable={"epub"}, after_folder="a")
    assert [item.storage_key for item in files] == ["a/c/z.epub", "b/x.epub"]
    assert skipped == {"earlier_folder": 3}


def _open(database: str, *, create: bool = False) -> LibraryServices:
    """
    Open a catalogue the way Core does.

    Example:
        >>> _open("library.sqlite")  # doctest: +SKIP


    :param database: Catalogue path.
    :param create: Whether to create it.
    :return: Open LibraryServices.
    """
    return LibraryServices.open(
        database_path=database,
        db_type="SQLite",
        create=create,
        backup=False,
        storage_startup_on_add=False,
        enable_maintenance=False,
    )


def _catalogue(root: Path) -> str:
    """
    Create an empty catalogue with four folders of books registered as Store 1.

    ``a`` holds Frankenstein as an EPUB and an FB2, ``b`` The Last Man, ``c``
    a broken file, and ``d`` Mathilda, also by Mary Shelley.

    Example:
        >>> _catalogue(tmp_path)  # doctest: +SKIP


    :param root: Empty working folder.
    :return: Catalogue path.
    """
    books = root / "books"
    for folder in "abcd":
        (books / folder).mkdir(parents=True)
    (books / "a/frankenstein.epub").write_bytes(
        epub_bytes("Frankenstein", ["Mary Shelley"], tags=["Gothic"])
    )
    (books / "a/frankenstein.fb2").write_bytes(
        fb2_bytes("Frankenstein", [("Mary", "Shelley")])
    )
    (books / "b/last_man.epub").write_bytes(
        epub_bytes("The Last Man", ["Mary Shelley"], tags=["Gothic"])
    )
    (books / "c/broken.epub").write_bytes(b"not a zip")
    (books / "d/mathilda.epub").write_bytes(epub_bytes("Mathilda", ["Mary Shelley"]))
    database = str(root / "library.sqlite")
    with _open(database, create=True) as lifecycle:
        lifecycle.library.register_unmanaged_disk(
            disk_path=str(books),
            store_name="shelf",
            compute_hash=False,
            follow_symlinks=False,
            attach_store_links=True,
            refresh_storage_manager=False,
        )
    return database


def _count(database: str, sql: str) -> int:
    """
    Run a counting query against the catalogue file.

    Example:
        >>> _count("library.sqlite", "SELECT COUNT(*) FROM works")  # doctest: +SKIP


    :param database: Catalogue path.
    :param sql: Query returning one integer.
    :return: The count.
    """
    with sqlite3.connect(database) as connection:
        return int(connection.execute(sql).fetchone()[0])


def _shell(database: str, line: str) -> str:
    """
    Run one terminal line against the catalogue and return what it printed.

    Example:
        >>> _shell("library.sqlite", "tables")  # doctest: +SKIP


    :param database: Catalogue path.
    :param line: Command line.
    :return: Rendered output.
    """
    from LiuXin_alpha.databases.database import Database
    from LiuXin_alpha.surfaces.terminal.browser import TextDatabaseBrowser

    output = io.StringIO()
    with Database(
        metadata={"database_path": database},
        db_type="SQLite",
        create=False,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        TextDatabaseBrowser(db, output=output).execute_line(line)
    return output.getvalue()


def test_plan_records_its_folders_and_numbers_books_from_first_index(
    tmp_path: Path,
) -> None:
    """
    One batch plans whole folders after the cursor, and says where the next batch starts.

    Example:
        python -m pytest -q tests/ingest/test_embedded_metadata_stream.py


    :param tmp_path: Pytest temporary folder.
    :return: None.
    """
    database = _catalogue(tmp_path)
    with _open(database) as lifecycle:
        plan = plan_store_metadata(
            database=lifecycle.database,
            library=lifecycle.library,
            catalog=lifecycle.core_services.catalog,
            store_id=1,
            limit=1,
            after_folder="a",
            first_index=7,
        )

    assert [book.index for book in plan.books] == [7]
    assert plan.books[0].title == "The Last Man"
    assert plan.to_dict()["selection"] == {"after_folder": "a", "last_folder": "b"}
    assert plan.skipped == {"earlier_folder": 2, "over_limit": 2}
    assert stream_batch_path(database, "s-1", 3) == (
        tmp_path / "ingest-plans" / "s-1" / "batch-0003.json"
    )
    with pytest.raises(ValueError):
        stream_batch_path(database, "../up", 1)


def test_stream_adds_books_batch_by_batch_and_a_rerun_adds_nothing(
    tmp_path: Path,
) -> None:
    """
    One folder per batch: every book is added and shown, the broken file is reported, and a rerun only retries it.

    Example:
        python -m pytest -q tests/ingest/test_embedded_metadata_stream.py


    :param tmp_path: Pytest temporary folder.
    :return: None.
    """
    database = _catalogue(tmp_path)
    folder = tmp_path / "stream"

    rendered = _shell(
        database, f'ingest metadata shelf --stream --batch 1 --report "{folder}"'
    )

    assert "Metadata ingest stream" in rendered
    assert "+ #1 Frankenstein — Mary Shelley [epub, fb2] → Work" in rendered
    assert "+ #2 The Last Man — Mary Shelley [epub] → Work" in rendered
    assert "+ #3 Mathilda — Mary Shelley [epub] → Work" in rendered
    assert "unreadable c/broken.epub" in rendered
    assert _count(database, "SELECT COUNT(*) FROM works") == 3
    assert _count(database, "SELECT COUNT(*) FROM agents WHERE agent_id > 0") == 1
    assert _count(database, "SELECT COUNT(*) FROM tags") == 1
    assert (
        _count(database, "SELECT COUNT(*) FROM files WHERE file_item_id IS NULL") == 1
    )
    plans = sorted(folder.glob("batch-*.json"))
    assert [path.name for path in plans if ".applied-" not in path.name] == [
        "batch-0001.json",
        "batch-0002.json",
        "batch-0003.json",
        "batch-0004.json",
    ]
    assert len([path for path in plans if ".applied-" in path.name]) == 3

    again = _shell(database, "ingest metadata shelf --stream --json")
    totals = json.loads(again[again.index("{") :])
    assert totals["works_created"] == 0
    assert totals["files_unreadable"] == 1
    assert totals["batches"] == 1
    assert totals["stopped"] == STOP_FINISHED
    assert "ingest-plans/metadata-stream-store-1-" in totals["folder"]


def test_stream_limit_stops_after_that_many_books(tmp_path: Path) -> None:
    """
    ``--limit`` counts books added; the rest of the batch is left for a later run, which picks it up.

    Example:
        python -m pytest -q tests/ingest/test_embedded_metadata_stream.py


    :param tmp_path: Pytest temporary folder.
    :return: None.
    """
    database = _catalogue(tmp_path)

    rendered = _shell(
        database, "ingest metadata shelf --stream --limit 1 --batch 10 --no-progress"
    )

    assert "+ #" not in rendered
    assert STOP_LIMIT in rendered
    assert "not_attempted" in rendered
    assert _count(database, "SELECT COUNT(*) FROM works") == 1

    _shell(database, "ingest metadata shelf --stream --no-progress")
    assert _count(database, "SELECT COUNT(*) FROM works") == 3


def test_stream_needs_one_mode_and_refuses_background(tmp_path: Path) -> None:
    """
    A stream is one of three modes, runs in the foreground, and ``--batch`` belongs to it alone.

    Example:
        python -m pytest -q tests/ingest/test_embedded_metadata_stream.py


    :param tmp_path: Pytest temporary folder.
    :return: None.
    """
    database = _catalogue(tmp_path)
    for line, message in (
        ("ingest metadata shelf --stream --dry-run", "`--stream`"),
        ("ingest metadata shelf --stream --background", "--background can't"),
        ("ingest metadata shelf --dry-run --batch 5", "--batch can't"),
        ("ingest metadata shelf --apply p.json --batch=5", "--batch can't"),
        ("ingest metadata shelf --stream --batch 0", "--batch must be"),
    ):
        with pytest.raises(ValueError, match=message):
            _shell(database, line)


def test_core_plan_start_passes_the_stream_options() -> None:
    """
    The Core service keeps an empty cursor, defaults the first index, and checks the stream name.

    Example:
        python -m pytest -q tests/ingest/test_embedded_metadata_stream.py


    :return: None.
    """
    submitted: list[Any] = []

    def submit(request: Any, **_kwargs: Any) -> str:
        """
        Capture a job request.

        Example:
            >>> submit(object())  # doctest: +SKIP


        :param request: Job request.
        :param _kwargs: Submission options.
        :return: Job id.
        """
        submitted.append(request)
        return "job-1"

    runtime: Any = SimpleNamespace(
        database=SimpleNamespace(
            metadata={"database_path": "/lib/books.sqlite"}, type="SQLite"
        ),
        job_manager=SimpleNamespace(submit=submit),
    )
    ingest_metadata_plan_start(
        runtime,
        CoreCommand(
            "ingest.metadata.plan",
            {"store_id": 1, "after_folder": "", "stream": "s-1", "batch": 2},
        ),
    )
    ingest_metadata_plan_start(
        runtime, CoreCommand("ingest.metadata.plan", {"store_id": 1})
    )

    first, plain = (request.kwargs for request in submitted)
    assert (first["after_folder"], first["first_index"]) == ("", 1)
    assert (first["stream"], first["batch"]) == ("s-1", 2)
    assert (plain["after_folder"], plain["stream"], plain["batch"]) == (
        None,
        None,
        None,
    )
    for bad in ({"stream": "../x"}, {"first_index": 0}, {"batch": 0}):
        with pytest.raises(CoreDispatchError):
            ingest_metadata_plan_start(
                runtime, CoreCommand("ingest.metadata.plan", {"store_id": 1, **bad})
            )


class _ScriptedHost:
    """
    Answer the stream's Core calls from a script of batch results.

    Example:
        >>> _ScriptedHost([]).lines
        []
    """

    def __init__(self, batches: list[Any], applied: list[Any] | None = None) -> None:
        """
        Hold the read results (or exceptions) and apply results to hand out.

        Example:
            >>> len(_ScriptedHost([{}]).batches)
            1


        :param batches: One read job result, or an exception to raise, per batch.
        :param applied: One apply result per batch that has books.
        :return: None.
        """
        self.batches = list(batches)
        self.applied = list(applied or [])
        self.plan_payloads: list[dict[str, Any]] = []
        self.apply_payloads: list[dict[str, Any]] = []
        self.lines: list[str] = []
        self._current: Any = None

    def execute_core_command(self, name: str, payload: dict[str, Any]) -> Any:
        """
        Record a plan or apply request and answer it.

        Example:
            >>> _ScriptedHost([{}]).execute_core_command("ingest.metadata.plan", {})
            {'job_id': 'job'}


        :param name: Core command.
        :param payload: Its payload.
        :return: A job receipt, or the next apply result.
        """
        if name == "ingest.metadata.plan":
            self.plan_payloads.append(payload)
            self._current = self.batches.pop(0)
            return {"job_id": "job"}
        self.apply_payloads.append(payload)
        return self.applied.pop(0)

    def execute_core_query(self, name: str, payload: dict[str, Any]) -> Any:
        """
        Report the job done, then hand out the current batch's result.

        Example:
            >>> _ScriptedHost([]).execute_core_query("jobs.get", {})
            {'job': {'state': 'succeeded'}}


        :param name: Core query.
        :param payload: Its payload.
        :return: Job state or execution result.
        :raises BaseException: The scripted exception, if the batch has one.
        """
        del payload
        if name == "jobs.get":
            return {"job": {"state": "succeeded"}}
        if isinstance(self._current, BaseException):
            raise self._current
        return {"execution": {"ok": True, "result": self._current}}

    def emit(self, line: str) -> None:
        """
        Collect a printed line.

        Example:
            >>> host = _ScriptedHost([]); host.emit("x"); host.lines
            ['x']


        :param line: Line printed.
        :return: None.
        """
        self.lines.append(line)


def _read(books: int, last: str | None, *, more: int = 0) -> dict[str, Any]:
    """
    Build a read job result.

    Example:
        >>> _read(1, "a")["last_folder"]
        'a'


    :param books: Books planned.
    :param last: Last folder read.
    :param more: Files left after the batch.
    :return: Result mapping.
    """
    skipped = {"over_limit": more} if more else {}
    return {
        "last_folder": last,
        "report_path": f"/s/batch-{last}.json",
        "summary": {"books": books, "files_read": books, "files_skipped": skipped},
    }


def _applied(created: int, left: int = 0) -> dict[str, Any]:
    """
    Build an apply result.

    Example:
        >>> _applied(1)["summary"]["works_created"]
        1


    :param created: Books created.
    :param left: Books not attempted.
    :return: Result mapping.
    """
    outcomes = {"created": created, **({"not_attempted": left} if left else {})}
    books = [{"outcome": "created", "index": 1, "title": "T"}] * created
    return {
        "summary": {"works_created": created, "outcomes": outcomes},
        "books": books,
    }


def test_run_stream_carries_the_cursor_and_index_and_stops_on_the_limit() -> None:
    """
    Each batch starts after the last batch's folder, numbers on, and the last one gets what is left of the limit.

    Example:
        python -m pytest -q tests/ingest/test_embedded_metadata_stream.py


    :return: None.
    """
    host = _ScriptedHost(
        [_read(2, "a", more=5), _read(0, "b", more=3), _read(2, "c", more=1)],
        [_applied(2), _applied(1, left=1)],
    )

    totals = run_stream(host, StreamOptions(store_id=4, limit=3, batch_files=2))

    assert [p["after_folder"] for p in host.plan_payloads] == [None, "a", "b"]
    assert [p["first_index"] for p in host.plan_payloads] == [1, 3, 3]
    assert [p["batch"] for p in host.plan_payloads] == [1, 2, 3]
    assert {p["limit"] for p in host.plan_payloads} == {2}
    assert [p["limit"] for p in host.apply_payloads] == [3, 1]
    assert all(p["books_in_result"] for p in host.apply_payloads)
    assert totals.stopped == STOP_LIMIT
    assert (totals.works_created, totals.batches) == (3, 3)
    assert dict(totals.outcomes) == {"created": 3, "not_attempted": 1}
    assert totals.folder == "/s"


def test_run_stream_stops_on_an_interrupt_or_a_failed_batch() -> None:
    """
    An interrupt or a failed read keeps the totals so far and says why it stopped.

    Example:
        python -m pytest -q tests/ingest/test_embedded_metadata_stream.py


    :return: None.
    """
    interrupted = run_stream(
        _ScriptedHost([_read(1, "a", more=1), KeyboardInterrupt()], [_applied(1)]),
        StreamOptions(store_id=1),
    )
    assert (interrupted.stopped, interrupted.works_created) == (STOP_INTERRUPTED, 1)

    failed = run_stream(
        _ScriptedHost([_read(0, "a", more=1), RuntimeError("reader hung")]),
        StreamOptions(store_id=1),
    )
    assert failed.stopped == STOP_FAILED
    assert "batch 2 after folder 'a'" in failed.error
    assert "reader hung" in failed.error

    empty = run_stream(_ScriptedHost([_read(0, None)]), StreamOptions(store_id=1))
    assert (empty.stopped, empty.batches) == (STOP_FINISHED, 1)
