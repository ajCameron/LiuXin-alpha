"""Check native-adapter row shapes and ownership without needing a server."""

from types import SimpleNamespace
from contextlib import closing
import os

import pytest

from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import (
    PostgresConnectionAdapter,
    connect_postgres,
)


@pytest.mark.parametrize(
    "operation", ["get_all", "get_scalar", "get_rows", "get_row", "script"]
)
@pytest.mark.parametrize("fail", [False, True])
def test_materialized_operations_close_their_cursor_without_ending_the_connection(
    operation, fail
):
    class Cursor:
        closed = False

        def execute(self, sql, parameters=()):
            if operation == "script" and fail:
                raise RuntimeError("injected execution failure")

        def fetchall(self):
            if fail:
                raise RuntimeError("injected fetch failure")
            return [(7,)]

        def fetchone(self):
            if fail:
                raise RuntimeError("injected fetch failure")
            return (7,)

        def close(self):
            self.closed = True

    cursor = Cursor()
    forbidden = []
    raw = SimpleNamespace(
        cursor=lambda: cursor,
        commit=lambda: forbidden.append("commit"),
        rollback=lambda: forbidden.append("rollback"),
        close=lambda: forbidden.append("close"),
    )
    adapter = PostgresConnectionAdapter(raw)
    actions = {
        "get_all": lambda: adapter.get("SELECT 7"),
        "get_scalar": lambda: adapter.get("SELECT 7", all=False),
        "get_rows": lambda: adapter.get_row("SELECT 7"),
        "get_row": lambda: adapter.get_row("SELECT 7", all=False),
        "script": lambda: adapter.executescript("SELECT 7; SELECT 8;"),
    }
    if fail:
        with pytest.raises(RuntimeError, match="injected"):
            actions[operation]()
    else:
        expected = {
            "get_all": [(7,)],
            "get_scalar": 7,
            "get_rows": [(7,)],
            "get_row": (7,),
            "script": None,
        }
        assert actions[operation]() == expected[operation]
    assert cursor.closed
    assert forbidden == []


@pytest.mark.parametrize("mapping", [False, True])
def test_native_cursor_factories_preserve_row_shapes(mapping):
    url = os.environ.get("LIUXIN_TEST_POSTGRES_URL", "")
    if not url:
        pytest.skip("LIUXIN_TEST_POSTGRES_URL is not configured.")
    from psycopg2.extras import RealDictCursor

    with closing(
        connect_postgres({"postgres_url": url}, prompt_for_password=False)
    ) as raw:
        adapter = PostgresConnectionAdapter(raw)
        options = {"cursor_factory": RealDictCursor} if mapping else {}
        with closing(adapter.cursor(**options)) as cursor:
            cursor.execute("SELECT ? AS number", (7,))
            row = cursor.fetchone()
            assert row == {"number": 7} if mapping else row == (7,)
            cursor.execute("SELECT n AS number FROM generate_series(1,3) n")
            first = cursor.fetchmany(1)
            remaining = cursor.fetchall()
            expected = (
                [{"number": n} for n in (1, 2, 3)] if mapping else [(1,), (2,), (3,)]
            )
            assert first + remaining == expected
            cursor.execute("SELECT ? AS number", (8,))
            assert list(cursor) == ([{"number": 8}] if mapping else [(8,)])
        assert not raw.closed
        raw.rollback()
