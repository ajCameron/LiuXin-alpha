"""
Record every changed catalogue row in the same transaction as the change.

The change log is what lets a cache refresh only changed rows. These tests pin
what it records on SQLite: every catalogue table, cascades and trigger side
effects, other connections and plain SQL writers, never a rolled-back change,
and both ends of a removed link.
"""

from __future__ import annotations

import sqlite3
from collections import Counter
from contextlib import closing
from shutil import copyfile

import pytest

from LiuXin_alpha.caches.metadata import MetadataAPI
from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.databases import change_log
from LiuXin_alpha.databases.change_log import ChangeLogUnavailable
from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.database.constants import HELPER_TABLES
from LiuXin_alpha.databases.search_functions import register_search_functions

OPTIONS = dict(
    backup=False,
    repair_bootstrap_rows=False,
    enable_storage_manager=False,
    enable_maintenance=False,
)


@pytest.fixture(scope="module")
def template(tmp_path_factory):
    """
    Create one empty catalogue for the module.

    Example:
        >>> def test_example(template): ...  # doctest: +SKIP


    :param tmp_path_factory: Pytest's session temporary-directory factory.
    :return: Path of the empty catalogue file.
    """
    path = tmp_path_factory.mktemp("change-log") / "empty.sqlite"
    with Database(metadata={"database_path": str(path)}, create=True, **OPTIONS):
        pass
    return path


@pytest.fixture
def catalogue(template, tmp_path):
    """
    Copy the empty catalogue for one test.

    Example:
        >>> def test_example(catalogue): ...  # doctest: +SKIP


    :param template: Module-scoped empty catalogue.
    :param tmp_path: Per-test directory.
    :return: Path of this test's catalogue.
    """
    path = tmp_path / "catalogue.sqlite"
    copyfile(template, path)
    return path


def _raw(path):
    """
    Open a plain SQLite connection outside LiuXin's database layer.

    Only the catalogue's own index functions are registered, which any writer
    needs; the change log's triggers need nothing further.

    Example:
        >>> connection = _raw(path)  # doctest: +SKIP


    :param path: Catalogue file.
    :return: Connection enforcing foreign keys, as LiuXin's own connections do.
    """
    connection = sqlite3.connect(path)
    register_search_functions(connection)
    connection.execute("PRAGMA foreign_keys=ON")
    return connection


def _stack(api, title):
    """
    Create a minimal WEMI stack.

    Example:
        >>> _stack(api, "Title")  # doctest: +SKIP


    :param api: Metadata service.
    :param title: Work title.
    :return: The creation result.
    """
    return api.mutations.create_wemi_stack(
        work={"title": title},
        expression={"label": "text"},
        manifestation={"edition_statement": "first"},
        items=({"inventory_code": title},),
    ).result


def test_new_catalogue_logs_every_catalogue_table(catalogue):
    """
    A new catalogue starts with an empty log covering every non-helper table.

    Example:
        >>> test_new_catalogue_logs_every_catalogue_table(path)  # doctest: +SKIP


    :param catalogue: Fresh catalogue file.
    :return: ``None`` after checking coverage.
    """
    with closing(_raw(catalogue)) as connection:
        assert change_log.present(connection)
        eligible = set(change_log.eligible_tables(connection))
        assert {"works", "tags", "tag_work_links", "work_work_intralinks"} <= eligible
        assert not eligible & set(HELPER_TABLES)
        assert change_log.covered_tables(connection) == eligible
        assert connection.execute("SELECT COUNT(*) FROM catalog_changes").fetchone() == (0,)
        assert len(change_log.log_uuid(connection)) == 36


def test_metadata_writes_log_rows_and_both_ends_of_removed_links(catalogue):
    """
    Each written row is logged once per change, and a removed link keeps its ends.

    Example:
        >>> test_metadata_writes_log_rows_and_both_ends_of_removed_links(path)  # doctest: +SKIP


    :param catalogue: Fresh catalogue file.
    :return: ``None`` after checking the entries.
    """
    with Database(metadata={"database_path": str(catalogue)}, **OPTIONS) as db:
        with closing(_raw(catalogue)) as reader:
            start = change_log.current_cursor(reader)
            api = MetadataAPI(Catalog(db))
            created = _stack(api, "Logged")
            work = created.work_id
            entries, after = change_log.read_changes(reader, start)
            assert Counter((e.table, e.operation) for e in entries) == {
                ("works", "insert"): 1,
                ("expressions", "insert"): 1,
                ("expression_work_links", "insert"): 1,
                ("manifestations", "insert"): 1,
                ("expression_manifestation_links", "insert"): 1,
                ("items", "insert"): 1,
            }
            assert {e.row_id for e in entries if e.table == "works"} == {work}

            tag = api.works.add_tag(work, text="Fantasy").result
            api.works.remove_tag(work, tag)
            entries, after = change_log.read_changes(reader, after)
            removed = [
                e for e in entries if e.table == "tag_work_links" and e.operation == "delete"
            ]
            assert len(removed) == 1
            assert removed[0].old_ids is not None
            assert removed[0].old_ids["tag_work_link_tag_id"] == tag
            assert removed[0].old_ids["tag_work_link_work_id"] == work
            assert ("tags", "insert") in {(e.table, e.operation) for e in entries}

            api.works.update(work, {"title": "Renamed"})
            entries, _ = change_log.read_changes(reader, after)
            assert {(e.table, e.row_id, e.operation) for e in entries} == {
                ("works", work, "update")
            }


def test_cascades_other_connections_and_plain_sql_are_logged(catalogue):
    """
    Deleting a Work logs the links its foreign keys cascade, from a plain connection.

    The triggers use only standard SQL, so a writer outside LiuXin's database
    layer is logged too.

    Example:
        >>> test_cascades_other_connections_and_plain_sql_are_logged(path)  # doctest: +SKIP


    :param catalogue: Fresh catalogue file.
    :return: ``None`` after checking the cascade entries.
    """
    with Database(metadata={"database_path": str(catalogue)}, **OPTIONS) as db:
        api = MetadataAPI(Catalog(db))
        work = _stack(api, "Cascaded").work_id
        tag = api.works.add_tag(work, text="Doomed link").result
    with closing(_raw(catalogue)) as writer, closing(_raw(catalogue)) as reader:
        start = change_log.current_cursor(reader)
        with writer:
            writer.execute("DELETE FROM works WHERE work_id = ?", (work,))
        entries, _ = change_log.read_changes(reader, start)
    operations = {(e.table, e.operation) for e in entries}
    assert ("works", "delete") in operations
    assert ("tag_work_links", "delete") in operations
    assert ("expression_work_links", "delete") in operations
    link = next(e for e in entries if e.table == "tag_work_links")
    assert link.old_ids["tag_work_link_tag_id"] == tag
    assert link.old_ids["tag_work_link_work_id"] == work


def test_rolled_back_changes_leave_no_entries(catalogue):
    """
    Entries roll back with the change that made them.

    Example:
        >>> test_rolled_back_changes_leave_no_entries(path)  # doctest: +SKIP


    :param catalogue: Fresh catalogue file.
    :return: ``None`` after checking the log is unchanged.
    """
    with closing(_raw(catalogue)) as connection:
        start = change_log.current_cursor(connection)
        connection.execute("INSERT INTO tags (tag) VALUES ('kept')")
        connection.commit()
        connection.execute("INSERT INTO tags (tag) VALUES ('discarded')")
        connection.rollback()
        entries, after = change_log.read_changes(connection, start)
        assert [(e.table, e.operation) for e in entries] == [("tags", "insert")]
        connection.execute("INSERT INTO tags (tag) VALUES ('next')")
        connection.commit()
        entries, _ = change_log.read_changes(connection, after)
        assert len(entries) == 1 and entries[0].change_id > after.position


def test_tables_without_an_integer_id_log_the_table(catalogue):
    """
    A table keyed by text logs entries with no row ID, meaning "this table changed".

    Example:
        >>> test_tables_without_an_integer_id_log_the_table(path)  # doctest: +SKIP


    :param catalogue: Fresh catalogue file.
    :return: ``None`` after checking the entry.
    """
    with closing(_raw(catalogue)) as connection:
        keyed_by_text = next(
            table
            for table in change_log.eligible_tables(connection)
            if change_log.inspect_table(connection, table).id_column is None
        )
        start = change_log.current_cursor(connection)
        with connection:
            connection.execute(
                f'INSERT INTO "{keyed_by_text}" (type) VALUES (?)', ("logged_type",)
            )
        entries, _ = change_log.read_changes(connection, start)
    assert [(e.table, e.row_id, e.operation) for e in entries] == [
        (keyed_by_text, None, "insert")
    ]


def test_existing_catalogue_installs_explicitly_and_keeps_history(catalogue):
    """
    A catalogue without the log opens normally; install adds it, and reinstalling keeps entries.

    Example:
        >>> test_existing_catalogue_installs_explicitly_and_keeps_history(path)  # doctest: +SKIP


    :param catalogue: Fresh catalogue file, stripped of its log for this test.
    :return: ``None`` after checking install and reinstall.
    """
    with closing(_raw(catalogue)) as connection, connection:
        for (name,) in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'liuxin_c_%'"
        ).fetchall():
            connection.execute(f'DROP TRIGGER "{name}"')
        connection.execute("DROP TABLE catalog_changes")
        connection.execute("DROP TABLE catalog_changes_metadata")

    with Database(metadata={"database_path": str(catalogue)}, **OPTIONS) as db:
        assert "works" in db.main_tables
    with closing(_raw(catalogue)) as connection:
        assert not change_log.present(connection)
        assert change_log.covered_tables(connection) == frozenset()
        with pytest.raises(ChangeLogUnavailable):
            change_log.current_cursor(connection)
        plan = change_log.plan_install(connection)
        assert plan[0].startswith('CREATE TABLE "catalog_changes"')
        assert not change_log.present(connection), "planning must not write"

        with connection:
            installed = change_log.install(connection)
        assert set(installed["tables"]) == set(change_log.eligible_tables(connection))
        assert change_log.covered_tables(connection) == set(installed["tables"])
        start = change_log.current_cursor(connection)
        with connection:
            connection.execute("INSERT INTO tags (tag) VALUES ('after install')")
        entries, _ = change_log.read_changes(connection, start)
        assert [(e.table, e.operation) for e in entries] == [("tags", "insert")]

        with connection:
            again = change_log.install(connection)
        assert again["log_uuid"] == installed["log_uuid"]
        assert connection.execute("SELECT COUNT(*) FROM catalog_changes").fetchone() == (1,)
        with pytest.raises(ChangeLogUnavailable, match="triggers were reinstalled"):
            change_log.read_changes(connection, start)


def test_reinstalled_log_or_large_backlog_needs_a_full_reload(catalogue):
    """
    A cursor from a replaced log, or more pending entries than the limit, is refused.

    Example:
        >>> test_reinstalled_log_or_large_backlog_needs_a_full_reload(path)  # doctest: +SKIP


    :param catalogue: Fresh catalogue file.
    :return: ``None`` after checking both refusals.
    """
    with closing(_raw(catalogue)) as connection:
        start = change_log.current_cursor(connection)
        with connection:
            connection.executemany(
                "INSERT INTO tags (tag) VALUES (?)", [(f"t{n}",) for n in range(5)]
            )
        with pytest.raises(ChangeLogUnavailable):
            change_log.read_changes(connection, start, limit=4)
        entries, _ = change_log.read_changes(connection, start, limit=5)
        assert len(entries) == 5

        with connection:
            connection.execute("DROP TABLE catalog_changes")
            connection.execute("DROP TABLE catalog_changes_metadata")
            change_log.install(connection)
        with pytest.raises(ChangeLogUnavailable):
            change_log.read_changes(connection, start)


def test_a_table_whose_triggers_are_missing_is_not_covered(catalogue):
    """
    Coverage is read from the triggers present, so a dropped trigger is noticed.
    Reinstalling it invalidates older cursors, since writes in between were not logged.

    Example:
        >>> test_a_table_whose_triggers_are_missing_is_not_covered(path)  # doctest: +SKIP


    :param catalogue: Fresh catalogue file.
    :return: ``None`` after checking coverage.
    """
    with closing(_raw(catalogue)) as connection:
        name = change_log._sqlite_trigger_names("tags")[1]
        with connection:
            connection.execute(f'DROP TRIGGER "{name}"')
        covered = change_log.covered_tables(connection)
        assert "tags" not in covered and "works" in covered
        before = change_log.current_cursor(connection)
        with connection:
            connection.execute("UPDATE tags SET tag = tag")
            assert change_log.install_table(connection, "tags")
        assert "tags" in change_log.covered_tables(connection)
        with pytest.raises(ChangeLogUnavailable):
            change_log.read_changes(connection, before)


def test_native_custom_field_tables_are_logged_from_creation(catalogue):
    """
    Creating a custom field installs triggers on its new tables in the same transaction.

    Example:
        >>> test_native_custom_field_tables_are_logged_from_creation(path)  # doctest: +SKIP


    :param catalogue: Fresh catalogue file.
    :return: ``None`` after checking coverage and a logged value.
    """
    from LiuXin_alpha.databases.custom_columns.native_types import OwnerRef

    with Database(metadata={"database_path": str(catalogue)}, **OPTIONS) as db:
        api = MetadataAPI(Catalog(db))
        work = api.works.create({"title": "Shelved"}).result
        with closing(_raw(catalogue)) as connection:
            before = set(change_log.eligible_tables(connection))
        definition = api.custom_fields.create_field("works", "shelf", mode="set").result
        with closing(_raw(catalogue)) as connection:
            created = set(change_log.eligible_tables(connection)) - before
            assert created
            assert created <= change_log.covered_tables(connection)
            start = change_log.current_cursor(connection)
        api.custom_fields.set_value(OwnerRef("works", work), definition.id, ["Study"])
        assert api.custom_fields.audit() == ()
        with closing(_raw(catalogue)) as connection:
            entries, _ = change_log.read_changes(connection, start)
        assert {e.table for e in entries} & created
