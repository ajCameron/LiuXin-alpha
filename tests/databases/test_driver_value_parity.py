"""Keep portable value and transaction behavior aligned across all driver names."""

import os
from contextlib import closing
from uuid import uuid4

import pytest

from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import (
    connect_postgres,
)
from LiuXin_alpha.databases.database_driver_plugins.registry import load_database_driver


@pytest.fixture(params=["SQLite", "SQLite_apsw", "PostgreSQL"])
def driver(request, tmp_path):
    backend = request.param
    if backend == "SQLite_apsw":
        pytest.importorskip("apsw")
    if backend == "PostgreSQL":
        url = os.environ.get("LIUXIN_TEST_POSTGRES_URL", "")
        if not url:
            pytest.skip("LIUXIN_TEST_POSTGRES_URL is not configured")
        metadata = {"postgres_url": url, "schema": "value_parity_" + uuid4().hex}
        with closing(connect_postgres(metadata, prompt_for_password=False)) as cleanup:
            with cleanup:
                with cleanup.cursor() as cursor:
                    cursor.execute(f'CREATE SCHEMA "{metadata["schema"]}"')
            try:
                with closing(load_database_driver(backend)(metadata)) as instance:
                    yield instance
            finally:
                with cleanup:
                    with cleanup.cursor() as cursor:
                        cursor.execute(f'DROP SCHEMA "{metadata["schema"]}" CASCADE')
    else:
        with closing(
            load_database_driver(backend)(
                {"database_path": str(tmp_path / "parity.sqlite")}
            )
        ) as instance:
            yield instance


@pytest.mark.parametrize("batch", [False, True], ids=["execute", "executemany"])
@pytest.mark.parametrize("fail", [False, True], ids=["commit", "rollback"])
@pytest.mark.parametrize(
    "text",
    ["prefix'`literal`'", "名📚e\u0301\n\\'tail", "?; COMMIT; %s %(name)s 100% %%"],
    ids=["quoted", "unicode_multiline", "sql_looking"],
)
def test_bound_values_and_rollback_match_across_drivers(driver, batch, fail, text):
    binary_type = "BYTEA" if getattr(driver, "schema", None) else "BLOB"
    driver.conn.execute(
        f"CREATE TABLE payloads(id INTEGER PRIMARY KEY,body TEXT,payload {binary_type})"
    ).close()
    driver.conn.commit()
    payload = b"\x00\xff?%`'\x80"
    rows = [(1, text, payload), (2, text + " tail", payload)]

    def write():
        with driver.conn:
            if batch:
                driver.conn.executemany(
                    "INSERT INTO payloads VALUES(?,?,?)", rows
                ).close()
            else:
                for row in rows:
                    driver.conn.execute(
                        "INSERT INTO payloads VALUES(?,?,?)", row
                    ).close()
            if fail:
                raise RuntimeError("abort the value batch")

    if fail:
        with pytest.raises(RuntimeError, match="abort the value batch"):
            write()
    else:
        write()
    with closing(driver.get_connection()) as reader:
        native = getattr(reader, "raw_connection", reader)
        with closing(native.cursor()) as cursor:
            cursor.execute("SELECT id,body,payload FROM payloads ORDER BY id")
            actual = [(row[0], row[1], bytes(row[2])) for row in cursor.fetchall()]
    assert actual == ([] if fail else rows)
