"""Check metadata values and borrowed resources on a partial facade composition."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from types import SimpleNamespace
from uuid import UUID
from unittest.mock import Mock

import pytest

from LiuXin_alpha.databases.database.metadata_mixin import DatabaseMetadataMixin
from LiuXin_alpha.databases.database.search_mixin import DatabaseSearchMixin
from LiuXin_alpha.databases.api.macros_api import MacrosAPI


class MetadataFacade(DatabaseMetadataMixin):
    """Supply metadata collaborators backed by caller-owned SQLite state."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.conn = connection
        self._uuid = None
        self._library_id_ = None
        self._database_version_ = None
        self.driver_wrapper = SimpleNamespace(
            fetch_scalar=self.read_scalar,
            get_uuid=lambda: None,
        )
        self.macros = Mock(spec=MacrosAPI, set_library_id=self.store_library_id)

    def read_scalar(self, sql: str) -> str | None:
        assert self.conn is not None
        cursor = self.conn.execute(sql)
        try:
            row = cursor.fetchone()
            return None if row is None else row[0]
        finally:
            cursor.close()

    def store_library_id(self, value: str) -> None:
        assert self.conn is not None
        if self.conn.execute("SELECT COUNT(*) FROM library_id").fetchone()[0]:
            self.conn.execute("UPDATE library_id SET library_id_uuid=?", (value,))
        else:
            self.conn.execute("INSERT INTO library_id VALUES (?)", (value,))


@pytest.fixture
def metadata() -> Iterator[MetadataFacade]:
    connection = sqlite3.connect(":memory:")
    connection.executescript(
        "CREATE TABLE library_id(library_id_uuid TEXT);"
        "CREATE TABLE database_version(database_version_version TEXT);"
    )
    try:
        yield MetadataFacade(connection)
    finally:
        connection.close()


@pytest.mark.parametrize("mapping_rows", [False, True])
def test_library_id_returns_the_stored_scalar_and_keeps_the_transaction(
    metadata, mapping_rows
):
    connection = metadata.conn
    connection.execute("INSERT INTO library_id VALUES ('stored-library')")
    if mapping_rows:
        connection.row_factory = sqlite3.Row
    assert metadata.library_id == "stored-library"
    assert connection.in_transaction
    connection.rollback()
    assert metadata.library_id == "stored-library"  # Existing cache semantics.
    assert connection.execute("SELECT COUNT(*) FROM library_id").fetchone()[0] == 0


@pytest.mark.parametrize("null_row", [False, True])
def test_missing_library_id_generates_scalar_without_committing(metadata, null_row):
    connection = metadata.conn
    if null_row:
        connection.execute("INSERT INTO library_id VALUES (NULL)")
    identity = metadata.library_id
    assert str(UUID(identity)) == identity
    assert connection.execute("SELECT library_id_uuid FROM library_id").fetchone() == (
        identity,
    )
    assert connection.in_transaction


def test_empty_metadata_values_and_last_version_preserve_the_borrowed_transaction(
    metadata,
):
    connection = metadata.conn
    assert metadata.uuid is None
    assert metadata.database_version is None
    connection.executemany(
        "INSERT INTO database_version VALUES (?)", [("old",), ("latest",)]
    )
    assert metadata.database_version == "latest"
    assert connection.in_transaction
    connection.rollback()
    assert (
        connection.execute("SELECT COUNT(*) FROM database_version").fetchone()[0] == 0
    )


def test_empty_legacy_search_result_is_an_empty_facade_list():
    facade = DatabaseSearchMixin()
    facade.driver = SimpleNamespace(direct_multi_column_search=lambda **kwargs: None)
    assert facade.multi_column_search([]) == []
