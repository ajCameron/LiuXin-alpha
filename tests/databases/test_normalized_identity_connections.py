"""
Read normalized-identity declarations on the transaction already in progress.

Every catalogue write reads these declarations, often many times per book. Each
read used to open a new connection, and each new connection re-validates the
schema revision; on an ingest run that was most of the write time. Inside a
managed transaction the declarations are now read on that transaction's own
connection, which also means they are seen as the transaction sees them.
Outside one, a read still opens a connection and closes it.
"""

from __future__ import annotations

import os
import shutil
from contextlib import closing
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from LiuXin_alpha.caches.metadata import MetadataAPI
from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.databases.database import Database


@pytest.fixture(scope="module")
def sqlite_template(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """
    Create one empty SQLite catalogue for the module to copy.

    Example:
        >>> def test_example(sqlite_template): ...  # doctest: +SKIP


    :param tmp_path_factory: Pytest temporary folder factory.
    :return: Path of the template catalogue.
    """
    path = tmp_path_factory.mktemp("identity-connections") / "template.sqlite"
    with Database(
        metadata={"database_path": str(path)},
        db_type="SQLite",
        create=True,
        enable_maintenance=False,
    ):
        pass
    return path


@pytest.fixture(params=["SQLite", "SQLite_apsw", "PostgreSQL"])
def db(request: pytest.FixtureRequest, sqlite_template: Path, tmp_path: Path) -> Any:
    """
    Open a fresh catalogue on each backend; PostgreSQL needs ``LIUXIN_TEST_POSTGRES_URL``.

    Example:
        >>> def test_example(db): ...  # doctest: +SKIP


    :param request: Pytest request carrying the backend name.
    :param sqlite_template: Empty SQLite catalogue to copy.
    :param tmp_path: Pytest temporary folder.
    :return: Generator yielding the open Database.
    """
    backend = request.param
    if backend == "PostgreSQL":
        yield from _postgres_catalogue()
        return
    if backend == "SQLite_apsw":
        pytest.importorskip("apsw")
    path = tmp_path / "library.sqlite"
    shutil.copyfile(sqlite_template, path)
    with Database(
        metadata={"database_path": str(path)},
        db_type=backend,
        enable_maintenance=False,
    ) as database:
        yield database


def _postgres_catalogue() -> Any:
    """
    Create a catalogue in a private PostgreSQL schema and drop it afterwards.

    Example:
        >>> next(_postgres_catalogue())  # doctest: +SKIP


    :return: Generator yielding the open Database.
    """
    url = os.environ.get("LIUXIN_TEST_POSTGRES_URL", "").strip()
    if not url:
        pytest.skip("LIUXIN_TEST_POSTGRES_URL is not configured.")
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import (
        connect_postgres,
    )

    metadata = {"postgres_url": url, "schema": f"liuxin_identity_{uuid4().hex}"}
    with closing(connect_postgres(metadata, prompt_for_password=False)) as cleanup:
        database = None
        try:
            database = Database(
                metadata=metadata,
                db_type="PostgreSQL",
                create=True,
                enable_storage_manager=False,
                enable_maintenance=False,
            )
            yield database
        finally:
            if database is not None:
                database.close()
            cleanup.rollback()
            with cleanup:
                with cleanup.cursor() as cursor:
                    cursor.execute(
                        f'DROP SCHEMA IF EXISTS "{metadata["schema"]}" CASCADE'
                    )


class _Tracked:
    """
    Wrap a connection to record whether it was closed.

    Example:
        >>> _Tracked(object()).closed
        False
    """

    def __init__(self, connection: Any) -> None:
        """
        Hold the real connection.

        Example:
            >>> _Tracked(object()).connection is not None
            True


        :param connection: Connection to wrap.
        :return: None.
        """
        self.connection = connection
        self.closed = False

    def __getattr__(self, name: str) -> Any:
        """
        Delegate everything else to the real connection.

        Example:
            >>> _Tracked("abc").upper()
            'ABC'


        :param name: Attribute name.
        :return: The real connection's attribute.
        """
        return getattr(self.connection, name)

    def close(self) -> None:
        """
        Record the close, then close the real connection.

        Example:
            >>> _Tracked(open(__file__)).close()


        :return: None.
        """
        self.closed = True
        self.connection.close()


def _count_connections(db: Any, monkeypatch: pytest.MonkeyPatch) -> list[_Tracked]:
    """
    Record every connection the driver opens from now on.

    Example:
        >>> opened = _count_connections(db, monkeypatch)  # doctest: +SKIP


    :param db: Open Database.
    :param monkeypatch: Pytest monkeypatch.
    :return: The list that collects each opened connection, in order.
    """
    opened: list[_Tracked] = []
    real = db.driver.get_connection

    def counting() -> _Tracked:
        """
        Open a real connection and record it.

        Example:
            >>> counting()  # doctest: +SKIP


        :return: The wrapped connection.
        """
        connection = _Tracked(real())
        opened.append(connection)
        return connection

    monkeypatch.setattr(db.driver, "get_connection", counting)
    return opened


def test_declaration_reads_borrow_the_active_transaction(
    db: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Inside a managed transaction both readers open nothing and agree with a read outside it.

    Example:
        python -m pytest -q tests/databases/test_normalized_identity_connections.py


    :param db: Open Database.
    :param monkeypatch: Pytest monkeypatch.
    :return: None.
    """
    outside = tuple(db.driver.direct_iter_normalized_identity_specs())
    with db.macros.transaction():
        opened = _count_connections(db, monkeypatch)
        inside = tuple(db.driver.direct_iter_normalized_identity_specs())
        tags = db.driver.direct_get_normalized_identity_spec("tags", "tag")
        assert opened == []
    assert inside == outside
    assert tags is not None and tags in inside


def test_declaration_reads_outside_a_transaction_close_their_connection(
    db: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Without a transaction each read opens one connection and closes it.

    Example:
        python -m pytest -q tests/databases/test_normalized_identity_connections.py


    :param db: Open Database.
    :param monkeypatch: Pytest monkeypatch.
    :return: None.
    """
    opened = _count_connections(db, monkeypatch)
    specs = list(db.driver.direct_iter_normalized_identity_specs(table="tags"))
    spec = db.driver.direct_get_normalized_identity_spec("tags", "tag")

    assert [item.table for item in specs] == ["tags"]
    assert spec == specs[0]
    assert len(opened) == 2
    assert all(connection.closed for connection in opened)


def test_declarations_are_read_as_the_transaction_sees_them(db: Any) -> None:
    """
    A declaration removed inside a transaction is gone for readers in it, and back after rollback.

    Example:
        python -m pytest -q tests/databases/test_normalized_identity_connections.py


    :param db: Open Database.
    :return: None.
    """
    with pytest.raises(RuntimeError, match="undo"):
        with db.macros.transaction() as connection:
            connection.execute(
                "DELETE FROM normalized_identities"
                " WHERE normalized_identity_table_name = ?",
                ("tags",),
            )
            assert db.driver.direct_get_normalized_identity_spec("tags", "tag") is None
            assert "tags" not in {
                spec.table for spec in db.driver.direct_iter_normalized_identity_specs()
            }
            raise RuntimeError("undo")

    assert db.driver.direct_get_normalized_identity_spec("tags", "tag") is not None


def test_a_catalogue_write_opens_only_its_own_connection(
    db: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Writing a WEMI stack with a tag through the metadata API opens one connection, its transaction's.

    Example:
        python -m pytest -q tests/databases/test_normalized_identity_connections.py


    :param db: Open Database.
    :param monkeypatch: Pytest monkeypatch.
    :return: None.
    """
    api = MetadataAPI(Catalog(db))

    def write(edit: Any) -> int:
        """
        Create a book and tag it.

        Example:
            >>> write(api)  # doctest: +SKIP


        :param edit: Metadata API inside the batch.
        :return: The new Work's id.
        """
        work = edit.mutations.create_wemi_stack(
            work={"title": "Counted"},
            expression={"label": "text"},
            manifestation={"edition_statement": "first"},
            items=({"inventory_code": "C-1"},),
        ).result.work_id
        edit.works.add_tag(work, text="Counted tag")
        return int(work)

    write(api)  # Warm up any lazily built state before counting.
    opened = _count_connections(db, monkeypatch)
    work = api.batch(write).result

    assert work > 0
    assert len(opened) == 1
