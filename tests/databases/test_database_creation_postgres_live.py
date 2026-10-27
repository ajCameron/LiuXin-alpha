"""Check real PostgreSQL create-only semantics in disposable, uniquely named schemas."""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import pytest

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL import (
    schema as schema_module,
)
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import (
    PostgresConnectionAdapter,
    connect_postgres,
)
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.databasedriver import (
    DatabaseDriver,
)
from LiuXin_alpha.errors import DatabaseDriverError


@pytest.fixture
def target():
    url = os.environ.get("LIUXIN_TEST_POSTGRES_URL", "").strip()
    if not url:
        pytest.skip("LIUXIN_TEST_POSTGRES_URL is not configured.")
    metadata = {"postgres_url": url, "schema": f"liuxin_creation_{uuid4().hex}"}
    raw = connect_postgres(metadata, prompt_for_password=False)
    try:
        yield metadata, raw
    finally:
        raw.rollback()
        with raw:
            with raw.cursor() as cursor:
                cursor.execute(f'DROP SCHEMA IF EXISTS "{metadata["schema"]}" CASCADE')
        raw.close()


@pytest.mark.parametrize("route", ["builder", "driver", "facade"])
def test_populated_schema_survives_create_request(target, route):
    metadata, raw = target
    schema = metadata["schema"]
    with raw:
        with raw.cursor() as cursor:
            cursor.execute(f'CREATE SCHEMA "{schema}"')
            cursor.execute(f'CREATE TABLE "{schema}".sentinel (value TEXT)')
            cursor.execute(f"INSERT INTO \"{schema}\".sentinel VALUES ('keep me')")
    with pytest.raises(DatabaseDriverError, match="empty schema"):
        if route == "builder":
            schema_module.create_new_database(metadata)
        elif route == "driver":
            driver = DatabaseDriver(metadata, set_conn=False)
            try:
                driver.direct_create_new_database()
            finally:
                driver.close()
        else:
            Database(metadata=metadata, db_type="PostgreSQL", create=True)
    with raw.cursor() as cursor:
        cursor.execute(f'SELECT value FROM "{schema}".sentinel')
        assert cursor.fetchall() == [("keep me",)]
        cursor.execute("SELECT tablename FROM pg_tables WHERE schemaname=%s", (schema,))
        assert cursor.fetchall() == [("sentinel",)]


@pytest.mark.parametrize("precreated", [False, True])
def test_new_or_empty_schema_can_be_created_once(target, precreated):
    metadata, raw = target
    if precreated:
        with raw:
            with raw.cursor() as cursor:
                cursor.execute(f'CREATE SCHEMA "{metadata["schema"]}"')
    schema_module.create_new_database(metadata)
    with raw.cursor() as cursor:
        cursor.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname=%s", (metadata["schema"],)
        )
        assert {"works", "stores", "digital_assets"} <= {row[0] for row in cursor}
    raw.rollback()
    with pytest.raises(DatabaseDriverError, match="empty schema"):
        schema_module.create_new_database(metadata)


def test_failed_creation_rolls_back_namespace_and_partial_tables(target, monkeypatch):
    metadata, raw = target
    monkeypatch.setattr(
        schema_module,
        "build_schema_statements",
        lambda **kwargs: ("CREATE TABLE partial (value TEXT)", "invalid SQL"),
    )
    with pytest.raises(DatabaseDriverError):
        schema_module.create_postgres_schema(
            PostgresConnectionAdapter(raw), schema=metadata["schema"]
        )
    with raw.cursor() as cursor:
        cursor.execute(
            "SELECT 1 FROM pg_namespace WHERE nspname=%s", (metadata["schema"],)
        )
        assert cursor.fetchone() is None


def test_concurrent_creators_have_one_winner(target):
    metadata, _ = target
    ready = Barrier(2)

    def create(_):
        ready.wait(timeout=10)
        try:
            schema_module.create_new_database(metadata)
            return "created"
        except DatabaseDriverError as error:
            assert "empty schema" in str(error)
            return "refused"

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(create, range(2))) == ["created", "refused"]
