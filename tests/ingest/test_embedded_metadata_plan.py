"""
Check the metadata ingest dry run: file choice, reading, planning, Core and terminal wiring.

Unit tests use stand-in catalogues and streams. The end-to-end tests build a real
SQLite catalogue with one existing book, register a folder of small EPUB and FB2
files, and check both the plan and that the catalogue's change log did not move.
"""

from __future__ import annotations

import io
import json
import sqlite3
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, BinaryIO, cast

import pytest

from LiuXin_alpha.core.commands import CoreCommand
from LiuXin_alpha.core.errors import CoreDispatchError
from LiuXin_alpha.core.library_services import LibraryServices
from LiuXin_alpha.core.program_services.ingest import ingest_metadata_plan_start
from LiuXin_alpha.ingest.embedded_metadata import (
    FileFacts,
    FileFailure,
    IngestFile,
    normalise,
    plan_books,
    plan_store_metadata,
    read_file,
    select_files,
)
from LiuXin_alpha.ingest.embedded_metadata.reading import calendar_date
from tests.support.embedded_metadata_books import epub_bytes, fb2_bytes

TODAY = date(2026, 11, 27)


def _row(
    file_id: int, key: str, *, item_id: int | None = None, size: int | None = 10
) -> dict[str, Any]:
    """
    Build a ``files`` row as the planner reads it.

    Example:
        >>> _row(1, "a.epub")["file_extension"]
        'epub'


    :param file_id: Row id.
    :param key: Storage key; its suffix becomes the extension.
    :param item_id: Attached Item, or None.
    :param size: Recorded size.
    :return: Mapping with the columns the planner uses.
    """
    return {
        "file_id": file_id,
        "file_storage_key": key,
        "file_extension": key.rpartition(".")[2],
        "file_item_id": item_id,
        "file_size_bytes": size,
    }


def test_select_files_counts_each_skip_reason_and_keeps_folder_order() -> None:
    """
    Attached, filtered, unreadable and over-limit files are counted; the rest come in folder order.

    Example:
        python -m pytest -q tests/ingest/test_embedded_metadata_plan.py


    :return: None.
    """
    rows = [
        _row(1, "x/c.epub"),
        _row(2, "w/a.epub"),
        _row(3, "x/b.epub", item_id=9),
        _row(4, "x/d.bin"),
        _row(5, "x/e.pdf"),
        _row(6, "y/f.epub"),
    ]
    files, skipped = select_files(
        rows, readable={"epub", "pdf"}, extensions=["EPUB"], limit=2
    )

    assert [item.storage_key for item in files] == ["w/a.epub", "x/c.epub"]
    assert skipped == {
        "already_attached": 1,
        "extension_filtered": 2,
        "over_limit": 1,
    }


def test_normalise_drops_placeholders_and_falls_back_to_the_file_name() -> None:
    """
    Placeholder titles and authors vanish, with flags; record identifiers are not Work evidence.

    Example:
        python -m pytest -q tests/ingest/test_embedded_metadata_plan.py


    :return: None.
    """
    metadata = SimpleNamespace(
        title="Unknown",
        authors=["Unknown", "  "],
        identifiers={
            "ISBN": " 978-0-306-40615-7 ",
            "uri": "urn:uuid:1",
            "calibre": "4",
        },
        tags=["Gothic", "gothic", "Horror"],
        languages=["ENG", "und"],
        publisher="Unknown",
        series=None,
        series_index=3,
        cover_data=("jpeg", b"abcd"),
        comments="  ",
    )
    facts = normalise(
        metadata, IngestFile(1, "shelf/The_Last_Man.epub", "epub"), today=TODAY
    )

    assert facts.title == "The Last Man"
    assert facts.authors == ()
    assert facts.notes == ("title_from_filename", "no_authors")
    assert facts.identifiers == (("isbn", "978-0-306-40615-7"),)
    assert facts.tags == ("Gothic", "Horror")
    assert facts.languages == ("eng",)
    assert facts.publisher is None
    assert facts.series_index is None
    assert facts.cover == ("jpeg", 4)
    assert facts.comments is None


def test_calendar_date_applies_the_boundary_rules() -> None:
    """
    Undefined dates drop, today-shaped dates flag, and old dates keep their day.

    Example:
        python -m pytest -q tests/ingest/test_embedded_metadata_plan.py


    :return: None.
    """
    assert calendar_date(datetime(101, 1, 1, tzinfo=UTC)) == (
        None,
        ("undefined_date_dropped",),
    )
    assert calendar_date(datetime(1818, 11, 27, tzinfo=UTC), today=TODAY) == (
        "1818-11-27",
        ("date_may_be_partial",),
    )
    assert calendar_date(datetime(1818, 1, 1, tzinfo=UTC), today=TODAY) == (
        "1818-01-01",
        (),
    )
    assert calendar_date("1818") == (None, ())


def test_read_file_isolates_size_open_and_read_failures() -> None:
    """
    Each failure is reported for its own file, at the stage it happened.

    Example:
        python -m pytest -q tests/ingest/test_embedded_metadata_plan.py


    :return: None.
    """

    def boom(*_args: Any, **_kwargs: Any) -> Any:
        """
        Fail like a broken reader or Store.

        Example:
            >>> boom()  # doctest: +SKIP


        :param _args: Ignored positional arguments.
        :param _kwargs: Ignored keyword arguments.
        :return: Never returns.
        :raises ValueError: Always.
        """
        raise ValueError("bad data\nsecond line")

    big = IngestFile(1, "big.pdf", "pdf", size_bytes=11)
    small = IngestFile(2, "a.pdf", "pdf", size_bytes=3)
    too_large = read_file(lambda _key: io.BytesIO(b""), big, reader=boom, max_bytes=10)
    unopenable = read_file(boom, small, reader=boom)
    unreadable = read_file(lambda _key: io.BytesIO(b"abc"), small, reader=boom)

    assert isinstance(too_large, FileFailure) and too_large.stage == "too_large"
    assert isinstance(unopenable, FileFailure) and unopenable.stage == "open"
    assert isinstance(unreadable, FileFailure)
    assert (unreadable.stage, unreadable.error) == ("read", "ValueError: bad data")


class _Unseekable(io.RawIOBase):
    """
    Serve bytes without seeking, like a network download.

    Example:
        >>> _Unseekable(b"ab").seekable()
        False
    """

    def __init__(self, payload: bytes) -> None:
        """
        Hold the bytes to serve.

        Example:
            >>> _Unseekable(b"x").readable()
            True


        :param payload: Bytes returned by reads.
        :return: None.
        """
        self._source = io.BytesIO(payload)

    def readable(self) -> bool:
        """
        Report that the stream can be read.

        Example:
            >>> _Unseekable(b"").readable()
            True


        :return: True.
        """
        return True

    def seekable(self) -> bool:
        """
        Report that the stream cannot seek.

        Example:
            >>> _Unseekable(b"").seekable()
            False


        :return: False.
        """
        return False

    def readinto(self, buffer: Any) -> int:
        """
        Fill a buffer from the payload.

        Example:
            >>> _Unseekable(b"ab").readinto(bytearray(2))
            2


        :param buffer: Writable buffer.
        :return: Bytes copied.
        """
        data = self._source.read(len(buffer))
        buffer[: len(data)] = data
        return len(data)


def test_read_file_copies_unseekable_streams_within_the_limit() -> None:
    """
    An unseekable stream is copied before reading, and a stream past the limit fails cleanly.

    Example:
        python -m pytest -q tests/ingest/test_embedded_metadata_plan.py


    :return: None.
    """
    seen: list[bytes] = []

    def reader(stream: Any, force_type: str) -> Any:
        """
        Record what the reader received.

        Example:
            >>> reader(io.BytesIO(b"x"), force_type="epub")  # doctest: +SKIP


        :param stream: Stream passed to the reader.
        :param force_type: Extension hint.
        :return: Minimal metadata.
        """
        stream.seek(0)
        seen.append(stream.read())
        return SimpleNamespace(title=force_type, authors=["A"])

    file = IngestFile(1, "a.epub", "epub", size_bytes=None)
    facts = read_file(
        lambda _key: cast(BinaryIO, _Unseekable(b"payload")), file, reader=reader
    )
    failure = read_file(
        lambda _key: cast(BinaryIO, _Unseekable(b"x" * 20)),
        file,
        reader=reader,
        max_bytes=10,
    )

    assert isinstance(facts, FileFacts) and facts.title == "epub"
    assert seen == [b"payload"]
    assert isinstance(failure, FileFailure) and failure.stage == "too_large"


@dataclass
class _Matches:
    """
    Stand in for a Catalog repository, answering from a table and counting calls.

    Example:
        >>> _Matches({}).exact("x").decision
        'no_match'
    """

    answers: dict[str, Any]
    calls: list[str] = field(default_factory=list)

    def _answer(self, value: str) -> Any:
        """
        Look up the canned answer for a value.

        Example:
            >>> _Matches({"a": ("match", 1)})._answer("A").entity_id
            1


        :param value: Name looked up.
        :return: MatchResult-shaped namespace.
        :raises RuntimeError: When the canned answer is ``"raise"``.
        """
        self.calls.append(value)
        answer = self.answers.get(value.casefold(), ("no_match", None))
        if answer == "raise":
            raise RuntimeError("matcher broke")
        decision, entity_id, *alternatives = answer
        return SimpleNamespace(
            decision=decision,
            entity_id=entity_id,
            alternatives=tuple(alternatives),
            reason=decision,
        )

    def exact(self, value: str) -> Any:
        """
        Answer an exact lookup.

        Example:
            >>> _Matches({}).exact("x").entity_id is None
            True


        :param value: Name looked up.
        :return: MatchResult-shaped namespace.
        """
        return self._answer(value)

    def match(self, candidate: Any) -> Any:
        """
        Answer a candidate lookup by its name or title.

        Example:
            >>> _Matches({}).match(SimpleNamespace(data={"name": "x"})).decision
            'no_match'


        :param candidate: MetadataCandidate-shaped object.
        :return: MatchResult-shaped namespace.
        """
        return self._answer(
            str(candidate.data.get("name") or candidate.data.get("title"))
        )


def _catalog(**answers: dict[str, Any]) -> SimpleNamespace:
    """
    Build a stand-in catalogue from canned answers per repository.

    Example:
        >>> _catalog().tags.exact("x").decision
        'no_match'


    :param answers: Canned answers keyed by repository name.
    :return: Namespace with works, agents, tags, series and languages.
    """
    return SimpleNamespace(
        **{
            name: _Matches(dict(answers.get(name, {})))
            for name in ("works", "agents", "tags", "series", "languages")
        }
    )


def _facts(
    key: str,
    title: str,
    *authors: str,
    languages: tuple[str, ...] = ("en",),
    **extra: Any,
) -> FileFacts:
    """
    Build facts for one file.

    Example:
        >>> _facts("a.epub", "A").file.extension
        'epub'


    :param key: Storage key.
    :param title: Title.
    :param authors: Author names.
    :param languages: Language codes.
    :param extra: Other FileFacts fields.
    :return: Facts.
    """
    file = IngestFile(len(key), key, key.rpartition(".")[2])
    return FileFacts(
        file, title=title, authors=tuple(authors), languages=languages, **extra
    )


def test_plan_books_applies_the_agreed_rules() -> None:
    """
    A matched Work only flags; names reuse, create, flag or fail per lookup; each name is looked up once.

    Example:
        python -m pytest -q tests/ingest/test_embedded_metadata_plan.py


    :return: None.
    """
    catalog = _catalog(
        works={"frankenstein": ("match", 7)},
        agents={"mary shelley": ("match", 3), "j. smith": ("ambiguous", None, 4, 5)},
        tags={"gothic": ("match", 2), "broken": "raise"},
        languages={"en": ("match", 124), "eng": ("match", 124)},
    )
    facts = [
        _facts("shelf/frankenstein.pdf", "Frankenstein", "Mary Shelley", publisher="Q"),
        _facts(
            "shelf/frankenstein.epub",
            "Frankenstein",
            "Mary Shelley",
            languages=("eng",),
            tags=("Gothic",),
        ),
        _facts(
            "other/frankenstein.epub",
            "frankenstein",
            "mary shelley",
            tags=("gothic", "Broken"),
        ),
        _facts("shelf/notes.epub", "Notes", "J. Smith", languages=("xx",)),
    ]
    books = plan_books(catalog, facts)

    assert [[f.file.extension for f in book.files] for book in books] == [
        ["epub", "pdf"],
        ["epub"],
        ["epub"],
    ]
    first, second, notes = books
    assert first.work_check.work_id == 7
    assert first.flags == (
        "formats_disagree",
        "possible_duplicate_in_catalogue",
        "possible_duplicate_in_run",
    )
    assert first.duplicate_of == (2,) and second.duplicate_of == (1,)
    assert [(item.decision, item.entity_id) for item in first.authors] == [("reuse", 3)]
    assert [(item.value, item.decision) for item in second.tags] == [
        ("gothic", "reuse"),
        ("Broken", "error"),
    ]
    assert "match_error" in second.flags
    assert notes.authors[0].decision == "ambiguous" and notes.authors[
        0
    ].alternatives == (4, 5)
    assert notes.languages[0].decision == "unknown"
    assert {"ambiguous_name", "unknown_language"} <= set(notes.flags)
    assert catalog.agents.calls.count("Mary Shelley") == 1
    assert catalog.tags.calls.count("Gothic") == 1


def test_plan_summary_counts_distinct_names() -> None:
    """
    A name shared by several books counts once in the summary.

    Example:
        python -m pytest -q tests/ingest/test_embedded_metadata_plan.py


    :return: None.
    """
    from LiuXin_alpha.ingest.embedded_metadata import MetadataIngestPlan

    catalog = _catalog()
    plan = MetadataIngestPlan(store={"store_id": 1})
    plan.books = plan_books(
        catalog,
        [_facts("a.epub", "A", tags=("New",)), _facts("b.epub", "B", tags=("new",))],
    )
    summary = plan.summary()

    assert summary["names"]["tags"] == {"new": 1}
    assert summary["flags"] == {"unknown_language": 2}


def _build_catalogue(root: Path) -> tuple[str, Path]:
    """
    Create a catalogue holding Frankenstein by Mary Shelley, and a folder of books to plan.

    Example:
        >>> _build_catalogue(tmp_path)  # doctest: +SKIP


    :param root: Empty working folder.
    :return: Catalogue path and the books folder.
    """
    books = root / "books"
    (books / "shelf").mkdir(parents=True)
    (books / "other").mkdir()
    details = {
        "date": "1818-01-01",
        "publisher": "Lackington",
        "identifiers": [("ISBN", "978-0-306-40615-7")],
        "tags": ["Gothic", "Horror"],
    }
    (books / "shelf/frankenstein.epub").write_bytes(
        epub_bytes("Frankenstein", ["Mary Shelley"], **details)
    )
    (books / "shelf/frankenstein.fb2").write_bytes(
        fb2_bytes("Frankenstein", [("Mary", "Shelley")])
    )
    (books / "other/frankenstein.epub").write_bytes(
        epub_bytes("Frankenstein", ["Mary Shelley"], **details)
    )
    (books / "shelf/last_man.epub").write_bytes(
        epub_bytes("The Last Man", ["Mary Shelley"], series="Shelley", series_index=2)
    )
    (books / "shelf/no_meta.epub").write_bytes(epub_bytes("Unknown", [], language=None))
    (books / "shelf/broken.epub").write_bytes(b"not a zip")
    database = str(root / "library.sqlite")
    with _open(database, create=True) as lifecycle:
        catalog = lifecycle.core_services.catalog
        created = catalog.mutations.writer.create_wemi_stack(
            work={"title": "Frankenstein"},
            expression={"label": "Text"},
            manifestation={"format_detail": "EPUB"},
            items=(),
        )
        person = catalog.agents.create_person({"name": "Mary Shelley"})
        catalog.agents.link_to_wemi(
            agent_id=person, level="work", entity_id=created.work_id, role="aut"
        )
        catalog.tags.create({"tag": "Gothic"})
        report = lifecycle.library.register_unmanaged_disk(
            disk_path=str(books),
            store_name="shelf",
            compute_hash=False,
            follow_symlinks=False,
            attach_store_links=True,
            refresh_storage_manager=False,
        )
        assert report.inserted_files == 6
    return database, books


def _open(database: str, *, create: bool = False) -> LibraryServices:
    """
    Open a catalogue the way Core's job workers do.

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


def _last_change(database: str) -> int:
    """
    Return the newest change-log entry id.

    Example:
        >>> _last_change("library.sqlite")  # doctest: +SKIP


    :param database: Catalogue path.
    :return: Highest ``catalog_change_id``.
    """
    with sqlite3.connect(database) as connection:
        return int(
            connection.execute(
                "SELECT max(catalog_change_id) FROM catalog_changes"
            ).fetchone()[0]
        )


def test_plan_store_metadata_end_to_end_writes_nothing(tmp_path: Path) -> None:
    """
    A real dry run plans formats, duplicates, reuse and failures, and the change log stays put.

    Example:
        python -m pytest -q tests/ingest/test_embedded_metadata_plan.py


    :param tmp_path: Pytest temporary folder.
    :return: None.
    """
    database, _books = _build_catalogue(tmp_path)
    before = _last_change(database)
    with _open(database) as lifecycle:
        plan = plan_store_metadata(
            database=lifecycle.database,
            library=lifecycle.library,
            catalog=lifecycle.core_services.catalog,
            store_id=1,
        )
    assert _last_change(database) == before

    by_key = {book.files[0].file.storage_key: book for book in plan.books}
    assert sorted(by_key) == [
        "other/frankenstein.epub",
        "shelf/frankenstein.epub",
        "shelf/last_man.epub",
        "shelf/no_meta.epub",
    ]
    shelf = by_key["shelf/frankenstein.epub"]
    assert [facts.file.extension for facts in shelf.files] == ["epub", "fb2"]
    assert shelf.work_check.decision == "match"
    assert set(shelf.flags) == {
        "formats_disagree",
        "possible_duplicate_in_catalogue",
        "possible_duplicate_in_run",
    }
    assert shelf.files[0].pubdate == "1818-01-01"
    assert [(item.value, item.decision) for item in shelf.tags] == [
        ("Gothic", "reuse"),
        ("Horror", "new"),
    ]
    assert shelf.authors[0].decision == "reuse"
    assert by_key["shelf/last_man.epub"].work_check.decision == "no_match"
    assert by_key["shelf/last_man.epub"].series is not None
    assert by_key["shelf/no_meta.epub"].title == "no meta"
    assert [failure.file.storage_key for failure in plan.failures] == [
        "shelf/broken.epub"
    ]
    assert plan.summary()["multi_format_books"] == 1


def test_opening_a_catalogue_records_no_change(tmp_path: Path) -> None:
    """
    Opening a catalogue leaves its sentinel rows, and so its change log, untouched.

    Example:
        python -m pytest -q tests/ingest/test_embedded_metadata_plan.py


    :param tmp_path: Pytest temporary folder.
    :return: None.
    """
    database = str(tmp_path / "library.sqlite")
    with _open(database, create=True):
        pass
    before = _last_change(database)
    with _open(database):
        pass
    assert _last_change(database) == before


def test_core_service_submits_the_plan_job_with_normalised_options() -> None:
    """
    The Core service validates its options and submits the worker with byte limits.

    Example:
        python -m pytest -q tests/ingest/test_embedded_metadata_plan.py


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
    receipt = ingest_metadata_plan_start(
        runtime,
        CoreCommand(
            "ingest.metadata.plan",
            {"store_id": "3", "extensions": ["epub"], "max_file_mb": 2},
        ),
    )

    assert receipt["job_id"] == "job-1"
    request = submitted[0]
    assert request.function_name == "run_ingest_metadata_plan_job"
    assert request.kwargs["store_id"] == 3
    assert request.kwargs["max_bytes"] == 2 * 1024 * 1024
    assert request.kwargs["limit"] is None
    with pytest.raises(CoreDispatchError):
        ingest_metadata_plan_start(
            runtime, CoreCommand("ingest.metadata.plan", {"store_id": 0})
        )
    with pytest.raises(CoreDispatchError):
        ingest_metadata_plan_start(
            runtime, CoreCommand("ingest.metadata.plan", {"store_id": 1, "limit": 0})
        )


def test_worker_writes_the_plan_and_returns_a_summary(tmp_path: Path) -> None:
    """
    The Core worker writes the full plan as JSON and returns a compact summary.

    Example:
        python -m pytest -q tests/ingest/test_embedded_metadata_plan.py


    :param tmp_path: Pytest temporary folder.
    :return: None.
    """
    from LiuXin_alpha.core.workflow_jobs import run_ingest_metadata_plan_job

    database, _books = _build_catalogue(tmp_path)
    report = tmp_path / "plans" / "plan.json"
    result = run_ingest_metadata_plan_job(
        database_path=database,
        db_type="SQLite",
        store_id=1,
        report_path=str(report),
        progress_output=False,
    )

    assert result["dry_run"] is True
    assert result["summary"]["books"] == 4
    assert result["report_path"] == str(report.resolve())
    written = json.loads(report.read_text(encoding="utf-8"))
    assert len(written["books"]) == 4
    assert written["failures"][0]["file"]["storage_key"] == "shelf/broken.epub"


def test_terminal_command_requires_a_mode_and_shows_the_plan(tmp_path: Path) -> None:
    """
    Without ``--dry-run`` or ``--apply`` the command refuses; a dry run prints the summary and sample.

    Example:
        python -m pytest -q tests/ingest/test_embedded_metadata_plan.py


    :param tmp_path: Pytest temporary folder.
    :return: None.
    """
    from LiuXin_alpha.databases.database import Database
    from LiuXin_alpha.surfaces.terminal.browser import TextDatabaseBrowser

    database, _books = _build_catalogue(tmp_path)
    output = io.StringIO()
    report = tmp_path / "plan.json"
    with Database(
        metadata={"database_path": database},
        db_type="SQLite",
        create=False,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, output=output)
        with pytest.raises(
            ValueError, match="Pass `--dry-run` to plan, `--apply <plan>` to write"
        ):
            shell.execute_line("ingest metadata 1")
        shell.execute_line(
            f'ingest metadata shelf --dry-run --no-progress --report "{report}"'
        )

    rendered = output.getvalue()
    assert "Metadata plan (dry run, nothing written)" in rendered
    assert "possible_duplicate_in_catalogue" in rendered
    assert "Sample books:" in rendered
    assert report.exists()
