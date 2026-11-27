"""
Record every changed catalogue row on PostgreSQL, and read entries in commit order.

PostgreSQL assigns ``change_id`` before commit, so a later entry can commit
first. A reader must neither skip the earlier entry when it finally commits nor
return the later one twice.
"""

from __future__ import annotations

import os
from contextlib import closing
from uuid import uuid4

import pytest

from LiuXin_alpha.caches.metadata import MetadataAPI
from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.databases import change_log
from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import (
    PostgresConnectionAdapter,
    connect_postgres,
)


@pytest.fixture
def pg_catalogue():
    """
    Create a catalogue in a private schema and drop the schema afterwards.

    Example:
        >>> def test_example(pg_catalogue): ...  # doctest: +SKIP


    :return: Generator yielding the open Database and its connection metadata.
    """
    url = os.environ.get("LIUXIN_TEST_POSTGRES_URL", "").strip()
    if not url:
        pytest.skip("LIUXIN_TEST_POSTGRES_URL is not configured.")
    metadata = {"postgres_url": url, "schema": f"liuxin_change_log_{uuid4().hex}"}
    with closing(connect_postgres(metadata, prompt_for_password=False)) as cleanup:
        db = None
        try:
            db = Database(
                metadata=metadata,
                db_type="PostgreSQL",
                create=True,
                enable_storage_manager=False,
                enable_maintenance=False,
            )
            yield db, metadata
        finally:
            if db is not None:
                db.close()
            cleanup.rollback()
            with cleanup:
                with cleanup.cursor() as cursor:
                    cursor.execute(
                        f'DROP SCHEMA IF EXISTS "{metadata["schema"]}" CASCADE'
                    )


def _connect(metadata):
    """
    Open an independent connection, as another process would.

    Example:
        >>> raw, adapter = _connect(metadata)  # doctest: +SKIP


    :param metadata: Connection metadata from the fixture.
    :return: The raw psycopg2 connection and an adapter accepting ``?`` placeholders.
    """
    raw = connect_postgres(metadata, prompt_for_password=False)
    return raw, PostgresConnectionAdapter(raw)


def _insert_tag(raw, schema, name):
    """
    Insert a Tag through raw SQL in the connection's open transaction.

    Example:
        >>> _insert_tag(raw, "library", "Fantasy")  # doctest: +SKIP


    :param raw: psycopg2 connection.
    :param schema: Catalogue schema.
    :param name: Tag text.
    :return: ``None``; the caller commits or rolls back.
    """
    with raw.cursor() as cursor:
        cursor.execute(f'INSERT INTO "{schema}".tags (tag) VALUES (%s)', (name,))


def test_new_postgres_catalogue_logs_every_catalogue_table(pg_catalogue):
    """
    A new PostgreSQL catalogue has the log, covering every eligible table.

    Opening it may already have logged bootstrap rows; a write after a fresh
    cursor is the only entry read from it.

    Example:
        >>> test_new_postgres_catalogue_logs_every_catalogue_table(target)  # doctest: +SKIP


    :param pg_catalogue: Fresh catalogue and its metadata.
    :return: ``None`` after checking coverage and one logged write.
    """
    _, metadata = pg_catalogue
    schema = metadata["schema"]
    raw, connection = _connect(metadata)
    with closing(raw):
        assert change_log.present(connection, schema=schema)
        eligible = set(change_log.eligible_tables(connection, schema=schema))
        assert {"works", "tags", "tag_work_links"} <= eligible
        assert change_log.covered_tables(connection, schema=schema) == eligible
        start = change_log.current_cursor(connection, schema=schema)
        raw.rollback()
        _insert_tag(raw, schema, "Logged")
        raw.commit()
        entries, _ = change_log.read_changes(connection, start, schema=schema)
        raw.rollback()
    assert [(e.table, e.operation) for e in entries] == [("tags", "insert")]


def test_postgres_cascades_keep_both_ends_of_removed_links(pg_catalogue):
    """
    Deleting a Work through raw SQL logs its cascaded links with their ends.

    Example:
        >>> test_postgres_cascades_keep_both_ends_of_removed_links(target)  # doctest: +SKIP


    :param pg_catalogue: Fresh catalogue and its metadata.
    :return: ``None`` after checking the entries.
    """
    db, metadata = pg_catalogue
    schema = metadata["schema"]
    api = MetadataAPI(Catalog(db))
    work = api.mutations.create_wemi_stack(
        work={"title": "Cascaded"},
        expression={"label": "text"},
        manifestation={"edition_statement": "first"},
        items=({"inventory_code": "C-1"},),
    ).result.work_id
    tag = api.works.add_tag(work, text="Doomed link").result
    raw, connection = _connect(metadata)
    with closing(raw):
        start = change_log.current_cursor(connection, schema=schema)
        raw.rollback()
        with raw:
            with raw.cursor() as cursor:
                cursor.execute(f'DELETE FROM "{schema}".works WHERE work_id = %s', (work,))
        entries, _ = change_log.read_changes(connection, start, schema=schema)
        raw.rollback()
    operations = {(e.table, e.operation) for e in entries}
    assert {("works", "delete"), ("tag_work_links", "delete")} <= operations
    link = next(e for e in entries if e.table == "tag_work_links")
    assert link.old_ids["tag_work_link_tag_id"] == tag
    assert link.old_ids["tag_work_link_work_id"] == work


def test_out_of_order_commits_are_neither_skipped_nor_repeated(pg_catalogue):
    """
    An entry that commits after a later one is still returned, exactly once each.

    Writer A inserts first but commits last. The reader sees B's entry first;
    its cursor stays at A's still-running transaction, so A's entry arrives on
    the next read and B's is not returned again.

    Example:
        >>> test_out_of_order_commits_are_neither_skipped_nor_repeated(target)  # doctest: +SKIP


    :param pg_catalogue: Fresh catalogue and its metadata.
    :return: ``None`` after checking both reads.
    """
    _, metadata = pg_catalogue
    schema = metadata["schema"]
    reader_raw, reader = _connect(metadata)
    first, _ = _connect(metadata)
    second, _ = _connect(metadata)
    with closing(reader_raw), closing(first), closing(second):
        start = change_log.current_cursor(reader, schema=schema)
        reader_raw.rollback()

        _insert_tag(first, schema, "inserted first, committed last")
        _insert_tag(second, schema, "inserted second, committed first")
        second.commit()

        entries, cursor = change_log.read_changes(reader, start, schema=schema)
        reader_raw.rollback()
        assert [e.operation for e in entries] == ["insert"]
        later = entries[0].change_id

        first.commit()
        entries, cursor = change_log.read_changes(reader, cursor, schema=schema)
        reader_raw.rollback()
        assert [e.operation for e in entries] == ["insert"]
        assert entries[0].change_id < later

        entries, cursor = change_log.read_changes(reader, cursor, schema=schema)
        reader_raw.rollback()
        assert entries == ()
        assert cursor.seen == frozenset()
