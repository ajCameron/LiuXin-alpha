"""
Check the metadata ingest apply step: writing a reviewed plan, re-checking it, and its wiring.

The end-to-end tests build a real SQLite catalogue holding Frankenstein by Mary
Shelley and a Gothic tag, register a folder of small books, run the dry run,
optionally edit the plan, and apply it through the cache-aware metadata API, as
Core does.
"""

from __future__ import annotations

import io
import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from LiuXin_alpha.core.commands import CoreCommand
from LiuXin_alpha.core.errors import CoreDispatchError
from LiuXin_alpha.core.library_services import LibraryServices
from LiuXin_alpha.core.program_services.ingest import ingest_metadata_apply
from LiuXin_alpha.ingest.embedded_metadata import (
    PlanError,
    apply_plan,
    applying,
    load_plan,
    plan_store_metadata,
    write_plan,
)
from tests.support.embedded_metadata_books import epub_bytes, fb2_bytes

PROMETHEUS = "<p>A modern Prometheus.</p>"


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
    Create a catalogue holding Frankenstein and a Gothic tag, with a folder of books registered.

    The folder holds Frankenstein (two authors) as an EPUB and an FB2, The Last
    Man in a series, and a broken file.

    Example:
        >>> _catalogue(tmp_path)  # doctest: +SKIP


    :param root: Empty working folder.
    :return: Catalogue path.
    """
    shelf = root / "books" / "shelf"
    shelf.mkdir(parents=True)
    (shelf / "frankenstein.epub").write_bytes(
        epub_bytes(
            "Frankenstein",
            ["Mary Shelley", "Percy Shelley"],
            date="1818-01-01",
            publisher="Lackington",
            identifiers=[("ISBN", "978-0-306-40615-7")],
            tags=["Gothic", "Horror"],
            description=PROMETHEUS,
        )
    )
    (shelf / "frankenstein.fb2").write_bytes(
        fb2_bytes("Frankenstein", [("Mary", "Shelley"), ("Percy", "Shelley")])
    )
    (shelf / "last_man.epub").write_bytes(
        epub_bytes(
            "The Last Man",
            ["Mary Shelley"],
            tags=["Horror"],
            series="Shelley Novels",
            series_index=2,
        )
    )
    (shelf / "broken.epub").write_bytes(b"not a zip")
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
        lifecycle.library.register_unmanaged_disk(
            disk_path=str(root / "books"),
            store_name="shelf",
            compute_hash=False,
            follow_symlinks=False,
            attach_store_links=True,
            refresh_storage_manager=False,
        )
    return database


def _plan(database: str, path: Path) -> dict[str, Any]:
    """
    Run the dry run over Store 1 and return the written plan.

    Example:
        >>> _plan("library.sqlite", tmp_path / "plan.json")  # doctest: +SKIP


    :param database: Catalogue path.
    :param path: Where to write the plan.
    :return: The plan as loaded back from disk.
    """
    with _open(database) as lifecycle:
        plan = plan_store_metadata(
            database=lifecycle.database,
            library=lifecycle.library,
            catalog=lifecycle.core_services.catalog,
            store_id=1,
        )
    write_plan(plan, path)
    return load_plan(path)


def _apply(database: str, plan: dict[str, Any], **options: Any) -> applying.ApplyReport:
    """
    Apply a plan through the cache-aware metadata API, as Core does.

    Example:
        >>> _apply("library.sqlite", plan)  # doctest: +SKIP


    :param database: Catalogue path.
    :param plan: Plan to apply.
    :param options: Extra apply_plan options.
    :return: The apply report.
    """
    with _open(database) as lifecycle:
        services = lifecycle.core_services
        return apply_plan(
            plan,
            metadata=services.metadata,
            catalog=services.catalog,
            database=lifecycle.database,
            **options,
        )


def _book(plan: dict[str, Any], title: str) -> dict[str, Any]:
    """
    Find a planned book by title.

    Example:
        >>> _book({"books": [{"title": "T"}]}, "T")
        {'title': 'T'}


    :param plan: Plan.
    :param title: Book title.
    :return: The planned book.
    """
    return next(book for book in plan["books"] if book["title"] == title)


def _count(database: str, sql: str) -> int:
    """
    Run a counting query.

    Example:
        >>> _count("library.sqlite", "SELECT COUNT(*) FROM works")  # doctest: +SKIP


    :param database: Catalogue path.
    :param sql: Query returning one number.
    :return: The number.
    """
    with sqlite3.connect(database) as connection:
        return int(connection.execute(sql).fetchone()[0])


def test_apply_writes_the_plan_and_keeps_the_cache_current(tmp_path: Path) -> None:
    """
    Applying creates Works, formats, credits, tags, identifiers and descriptions, and attaches the files.

    Example:
        python -m pytest -q tests/ingest/test_embedded_metadata_apply.py


    :param tmp_path: Pytest temporary folder.
    :return: None.
    """
    database = _catalogue(tmp_path)
    plan = _plan(database, tmp_path / "plan.json")
    _book(plan, "Frankenstein")["formats"][0]["notes"].append("date_may_be_partial")
    report = _apply(database, plan)

    summary = report.summary()
    assert summary["outcomes"] == {"created": 2}
    assert summary["manifestations_created"] == 3
    assert summary["files_attached"] == 3
    assert summary["names_created"] == {"agents": 2, "series": 1, "tags": 1}
    by_title = {book.title: book for book in report.books}
    frankenstein, last_man = by_title["Frankenstein"], by_title["The Last Man"]
    assert last_man.not_stored == ("series_position: 2",)
    assert (
        _count(database, "SELECT COUNT(*) FROM files WHERE file_item_id IS NULL") == 1
    )
    with sqlite3.connect(database) as connection:
        authors = [
            row[0]
            for row in connection.execute(
                "SELECT agent_canonical_name FROM agent_work_links JOIN agents"
                " ON agent_id = agent_work_link_agent_id WHERE agent_work_link_work_id = ?"
                " ORDER BY agent_work_link_priority DESC",
                (frankenstein.work_id,),
            )
        ]
        year, day = connection.execute(
            "SELECT manifestation_pub_year, manifestation_pub_date FROM manifestations WHERE manifestation_id = ?",
            (frankenstein.manifestation_ids[0],),
        ).fetchone()
    assert authors == ["Mary Shelley", "Percy Shelley"]
    assert (year, day) == (1818, None)
    assert (
        _count(
            database,
            "SELECT COUNT(*) FROM agents WHERE agent_canonical_name = 'Mary Shelley'",
        )
        == 1
    )
    assert _count(database, "SELECT COUNT(*) FROM tags WHERE tag = 'Horror'") == 1
    with _open(database) as lifecycle:
        services = lifecycle.core_services
        expression = services.catalog.repositories.manifestations.list_for_expression
        work = services.metadata.works.get(frankenstein.work_id)
        identifiers = services.metadata.identifiers.list_for_wemi(
            level="manifestation", entity_id=frankenstein.manifestation_ids[0]
        )
        comments = services.metadata.comments.list_for_wemi(
            level="work", entity_id=frankenstein.work_id
        )
        stack = services.catalog.repositories.expressions.list_for_work(
            frankenstein.work_id
        )
        formats = expression(stack[0]["expression_id"])
    assert work is not None
    assert "9780306406157" in json.dumps(
        [dict(item.fields) for item in identifiers], default=str
    ).replace("-", "")
    assert PROMETHEUS in json.dumps(
        [dict(item.fields) for item in comments], default=str
    )
    assert sorted(row["manifestation_format_detail"] for row in formats) == [
        "EPUB",
        "FB2",
    ]


def test_apply_skips_books_marked_skip_or_whose_files_changed(tmp_path: Path) -> None:
    """
    A book marked skip, or with a moved file, is left alone; a second run skips attached files.

    Example:
        python -m pytest -q tests/ingest/test_embedded_metadata_apply.py


    :param tmp_path: Pytest temporary folder.
    :return: None.
    """
    database = _catalogue(tmp_path)
    plan = _plan(database, tmp_path / "plan.json")
    _book(plan, "Frankenstein")["action"] = "skip"
    moved = _book(plan, "The Last Man")["formats"][0]["file"]["file_id"]
    with sqlite3.connect(database) as connection:
        connection.execute(
            "UPDATE files SET file_storage_key = 'elsewhere.epub' WHERE file_id = ?",
            (moved,),
        )
    works = _count(database, "SELECT COUNT(*) FROM works")

    report = _apply(database, plan)

    reasons = {book.title: book.reason for book in report.books}
    assert report.summary()["outcomes"] == {"skipped": 2}
    assert reasons == {
        "Frankenstein": "marked skip in the plan",
        "The Last Man": f"file {moved} has moved",
    }
    assert _count(database, "SELECT COUNT(*) FROM works") == works

    _book(plan, "Frankenstein")["action"] = "create"
    assert _apply(database, plan).summary()["outcomes"] == {"created": 1, "skipped": 1}
    again = _apply(database, plan)
    assert again.summary()["outcomes"] == {"skipped": 2}
    assert (
        "already attached"
        in _book({"books": [b.to_dict() for b in again.books]}, "Frankenstein")[
            "reason"
        ]
    )


def test_apply_skips_a_book_whose_reused_name_is_gone(tmp_path: Path) -> None:
    """
    A planned reuse of a row deleted since the dry run skips that book only.

    Example:
        python -m pytest -q tests/ingest/test_embedded_metadata_apply.py


    :param tmp_path: Pytest temporary folder.
    :return: None.
    """
    database = _catalogue(tmp_path)
    plan = _plan(database, tmp_path / "plan.json")
    gothic = next(
        tag for tag in _book(plan, "Frankenstein")["tags"] if tag["value"] == "Gothic"
    )
    assert gothic["decision"] == "reuse"
    with _open(database) as lifecycle:
        lifecycle.core_services.metadata.tags.delete(gothic["entity_id"])

    report = _apply(database, plan)

    outcomes = {book.title: (book.outcome, book.reason) for book in report.books}
    assert outcomes["Frankenstein"] == (
        "skipped",
        f"tags {gothic['entity_id']} ('Gothic') no longer exists",
    )
    assert outcomes["The Last Man"][0] == "created"


def test_apply_rolls_back_a_book_that_fails_part_way(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    A failure after some writes leaves none of that book behind, and the run carries on.

    Example:
        python -m pytest -q tests/ingest/test_embedded_metadata_apply.py


    :param tmp_path: Pytest temporary folder.
    :param monkeypatch: Pytest fixture used to break one book's manifestation write.
    :return: None.
    """
    database = _catalogue(tmp_path)
    plan = _plan(database, tmp_path / "plan.json")
    works = _count(database, "SELECT COUNT(*) FROM works")
    original = applying._BookWriter._attach_manifestation_values

    def broken(
        self: Any, api: Any, manifestation_id: int, publisher: Any, facts: Any
    ) -> None:
        """
        Fail for the FB2 format, after the book's Work and credits exist.

        Example:
            >>> broken(writer, api, 1, None, facts)  # doctest: +SKIP


        :param self: Writer.
        :param api: Batch metadata API.
        :param manifestation_id: Manifestation being written.
        :param publisher: Publisher id or None.
        :param facts: Format facts.
        :return: None.
        :raises RuntimeError: For the FB2 format.
        """
        if facts["file"]["extension"] == "fb2":
            raise RuntimeError("disk on fire")
        original(self, api, manifestation_id, publisher, facts)

    monkeypatch.setattr(applying._BookWriter, "_attach_manifestation_values", broken)
    report = _apply(database, plan)

    outcomes = {book.title: (book.outcome, book.reason) for book in report.books}
    assert outcomes["Frankenstein"] == ("failed", "RuntimeError: disk on fire")
    assert outcomes["The Last Man"][0] == "created"
    assert _count(database, "SELECT COUNT(*) FROM works") == works + 1
    assert (
        _count(
            database,
            "SELECT COUNT(*) FROM agents WHERE agent_canonical_name = 'Percy Shelley'",
        )
        == 0
    )
    assert (
        _count(database, "SELECT COUNT(*) FROM files WHERE file_item_id IS NOT NULL")
        == 1
    )


def test_apply_limit_leaves_the_rest_for_a_later_run(tmp_path: Path) -> None:
    """
    A limit stops after that many books are created; the rest are reported as not attempted.

    Example:
        python -m pytest -q tests/ingest/test_embedded_metadata_apply.py


    :param tmp_path: Pytest temporary folder.
    :return: None.
    """
    database = _catalogue(tmp_path)
    plan = _plan(database, tmp_path / "plan.json")

    first = _apply(database, plan, limit=1)
    second = _apply(database, plan, limit=1)

    assert first.summary()["outcomes"] == {"created": 1, "not_attempted": 1}
    assert second.summary()["outcomes"] == {"created": 1, "skipped": 1}


def test_load_plan_rejects_files_that_are_not_plans(tmp_path: Path) -> None:
    """
    Unreadable files, other JSON and plans with fileless books are refused before any write.

    Example:
        python -m pytest -q tests/ingest/test_embedded_metadata_apply.py


    :param tmp_path: Pytest temporary folder.
    :return: None.
    """
    cases = {
        "missing.json": None,
        "text.json": "not json",
        "other.json": json.dumps({"books": []}),
        "nostore.json": json.dumps({"dry_run": True, "books": []}),
        "nofiles.json": json.dumps(
            {
                "dry_run": True,
                "store": {"store_id": 1},
                "books": [{"index": 1, "formats": []}],
            }
        ),
    }
    for name, text in cases.items():
        path = tmp_path / name
        if text is not None:
            path.write_text(text, encoding="utf-8")
        with pytest.raises(PlanError):
            load_plan(path)


def test_core_service_refuses_a_plan_for_another_store(tmp_path: Path) -> None:
    """
    Core checks the plan's Store against the one named before applying anything.

    Example:
        python -m pytest -q tests/ingest/test_embedded_metadata_apply.py


    :param tmp_path: Pytest temporary folder.
    :return: None.
    """
    path = tmp_path / "plan.json"
    plan = {
        "dry_run": True,
        "store": {"store_id": 2},
        "books": [{"index": 1, "formats": [{"file": {"file_id": 4}}]}],
    }
    path.write_text(json.dumps(plan), encoding="utf-8")
    runtime: Any = object()

    with pytest.raises(CoreDispatchError, match="for Store 2, not Store 1"):
        ingest_metadata_apply(
            runtime,
            CoreCommand(
                "ingest.metadata.apply", {"plan_path": str(path), "store_id": 1}
            ),
        )
    with pytest.raises(CoreDispatchError, match="Cannot read plan"):
        ingest_metadata_apply(
            runtime,
            CoreCommand(
                "ingest.metadata.apply", {"plan_path": str(tmp_path / "none.json")}
            ),
        )


def test_terminal_applies_a_plan_and_checks_its_options(tmp_path: Path) -> None:
    """
    The terminal needs exactly one mode, refuses dry-run options with --apply, and shows the outcome.

    Example:
        python -m pytest -q tests/ingest/test_embedded_metadata_apply.py


    :param tmp_path: Pytest temporary folder.
    :return: None.
    """
    from LiuXin_alpha.databases.database import Database
    from LiuXin_alpha.surfaces.terminal.browser import TextDatabaseBrowser

    database = _catalogue(tmp_path)
    plan = tmp_path / "plan.json"
    output = io.StringIO()
    with Database(
        metadata={"database_path": database},
        db_type="SQLite",
        create=False,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, output=output)
        for line in (
            "ingest metadata shelf",
            f'ingest metadata shelf --dry-run --apply "{plan}"',
            f'ingest metadata shelf --apply "{plan}" --background',
        ):
            with pytest.raises(ValueError):
                shell.execute_line(line)
        shell.execute_line(
            f'ingest metadata shelf --dry-run --no-progress --report "{plan}"'
        )
        shell.execute_line(f'ingest metadata shelf --apply "{plan}"')

    rendered = output.getvalue()
    assert "Metadata ingest applied" in rendered
    assert "works_created" in rendered
    assert list(tmp_path.glob("plan.applied-*.json"))
