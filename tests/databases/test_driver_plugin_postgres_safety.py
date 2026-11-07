"""Exercise driver recovery and SQL adaptation on a real PostgreSQL server."""

import os
from contextlib import closing
from types import SimpleNamespace
from uuid import uuid4

import pytest

from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import (
    connect_postgres,
)
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.databasedriver import (
    DatabaseDriver,
)
from LiuXin_alpha.databases.driver_wrapper import DriverWrapper
from LiuXin_alpha.errors import DatabaseDriverError


@pytest.mark.parametrize("binding", ["?", "%s"], ids=["qmark", "native"])
@pytest.mark.parametrize("operator", ["?", "?|", "?&", "@?"])
@pytest.mark.parametrize(
    "qualification",
    [
        "OPERATOR(pg_catalog.{})",
        'OPERATOR("pg_catalog".{})',
        "OPERATOR(/* outer /* inner */ tail */ pg_catalog. /* gap */ {})",
        "OPERATOR({})",
        "OPERATOR(`pg_catalog`.{})",
    ],
    ids=["schema", "quoted_schema", "comments", "unqualified", "backticks"],
)
def test_explicit_json_operator_names_remain_operators(
    driver, binding, operator, qualification
):
    operand = f"ARRAY[{binding}]" if operator in {"?|", "?&"} else binding
    sql = f"SELECT {binding}::jsonb {qualification.format(operator)} {operand}"
    values = ('{"key":1}', "$.key" if operator == "@?" else "key")
    native_operand = "ARRAY[%s]" if operator in {"?|", "?&"} else "%s"
    with driver.conn.raw_connection.cursor() as raw:
        native_operator = qualification.format(operator).replace("`", '"')
        raw.execute(f"SELECT %s::jsonb {native_operator} {native_operand}", values)
        expected = raw.fetchone()
    with closing(driver.conn.execute(sql, values)) as cursor:
        assert cursor.fetchone() == expected == (True,)


@pytest.mark.parametrize(
    "sql,values",
    [
        ("SELECT INTERVAL ? DAY", ("2",)),
        ("SELECT INTERVAL ? DAY TO SECOND", ("2 days 03:00:00",)),
        ("SELECT INTERVAL ? YEAR TO MONTH", ("1 year 2 months",)),
        ("SELECT TIME ? WHERE TRUE", ("12:34:00",)),
        ("SELECT INTERVAL ? OFFSET 0", ("2 days",)),
        ("SELECT INTERVAL ? FETCH FIRST 1 ROW ONLY", ("2 days",)),
        ("SELECT TIME ? UNION ALL SELECT TIME ?", ("12:00:00", "13:00:00")),
        ('SELECT INTERVAL ? "label"', ("2 days",)),
        ("SELECT TIME(3) ?", ("12:34:00.12345",)),
        ("SELECT INTERVAL(2) ?", ("2.12345 seconds",)),
        ("SELECT TIME(3) WITH TIME ZONE ?", ("12:34:00.12345+02",)),
        ("SELECT TIMESTAMP WITHOUT TIME ZONE ?", ("2026-11-06 12:34:00",)),
        ("SELECT DATE ?, TIMESTAMPTZ ?", ("2026-11-06", "2026-11-06 12:34:00+00")),
        ("SELECT TIME /* gap */ ? AT TIME ZONE ?", ("12:00:00+00", "UTC")),
    ],
    ids=[
        "day",
        "day_second",
        "year_month",
        "where",
        "offset",
        "fetch",
        "union",
        "quoted_alias",
        "time_precision",
        "interval_precision",
        "time_zone",
        "timestamp",
        "date_timestamptz",
        "at_zone",
    ],
)
def test_temporal_qmarks_match_native_parameters(driver, sql, values):
    """Compare engine results for temporal grammar, without assuming a suffix list."""
    with driver.conn.raw_connection.cursor() as native:
        native.execute(sql.replace("?", "%s"), values)
        expected = native.fetchall()
    with closing(driver.conn.execute(sql, values)) as cursor:
        assert cursor.fetchall() == expected


@pytest.mark.parametrize("column", ["time", "interval", "date", "timestamp"])
@pytest.mark.parametrize("operand", ["day", '"key"', "?", "(day)"])
def test_temporal_names_remain_json_operands_with_other_bindings(
    driver, column, operand
):
    sql = (
        f"SELECT {column} ? {operand}, TIME ? FROM "
        f"(SELECT '{{\"key\":1}}'::jsonb AS {column}, 'key'::text AS day, 'key'::text AS key) t"
    )
    values = ("key", "12:00:00") if operand == "?" else ("12:00:00",)
    with closing(driver.conn.execute(sql, values)) as cursor:
        result = cursor.fetchone()
        assert result[0] is True
        assert str(result[1]) == "12:00:00"


def test_ambiguous_temporal_assignments_require_native_or_explicit_cast(driver):
    from datetime import timedelta

    from LiuXin_alpha.databases.transactions import transaction_active

    suffix = " FROM (SELECT '{\"key\":1}'::jsonb AS time, 'key'::text AS day) t"
    ambiguous = "SELECT INTERVAL ? DAY, time ? day" + suffix
    with pytest.raises(ValueError, match="Ambiguous temporal qmark"):
        driver.conn.execute(ambiguous, ("2",))
    assert not transaction_active(driver.conn)
    for sql in (
        "SELECT INTERVAL %s DAY, time ? day" + suffix,
        "SELECT CAST(? AS INTERVAL), time ? day" + suffix,
    ):
        with closing(driver.conn.execute(sql, ("2 days",))) as cursor:
            assert cursor.fetchone() == (timedelta(days=2), True)


@pytest.mark.parametrize("failure", [False, True], ids=["success", "late_failure"])
def test_temporal_batch_uses_first_arity_without_replaying_or_losing_rows(
    driver, failure
):
    from datetime import timedelta

    driver.conn.execute("CREATE TABLE intervals(value INTERVAL)").close()
    driver.conn.commit()
    visits = []

    def values():
        for value in ("1", "2", "3"):
            visits.append(value)
            yield (value,)
        if failure:
            raise RuntimeError("late input failure")

    sql = "INSERT INTO intervals VALUES (INTERVAL ? DAY)"
    if failure:
        with pytest.raises(DatabaseDriverError, match="late input failure"):
            driver.direct_executemany(sql, values())
    else:
        driver.direct_executemany(sql, values())
    assert visits == ["1", "2", "3"]
    with closing(driver.get_connection()) as reader:
        assert reader.execute(
            "SELECT value FROM intervals ORDER BY value"
        ).fetchall() == (
            [] if failure else [(timedelta(days=value),) for value in (1, 2, 3)]
        )


@pytest.mark.parametrize(
    "column", ["first", "next", "time", "zone", "by", "at", "interval"]
)
@pytest.mark.parametrize("binding", ["?", "%s"], ids=["qmark", "native"])
@pytest.mark.parametrize("operator", ["?", "?|", "?&", "@?"])
def test_unqualified_keywords_remain_json_operands(driver, column, binding, operator):
    operand = f"ARRAY[{binding}]" if operator in {"?|", "?&"} else binding
    sql = f"SELECT {column} {operator} {operand} FROM (SELECT '{{\"key\":1}}'::jsonb AS {column}) t"
    value = "$.key" if operator == "@?" else "key"
    native_operand = "ARRAY[%s]" if operator in {"?|", "?&"} else "%s"
    native_sql = f"SELECT {column} {operator} {native_operand} FROM (SELECT '{{\"key\":1}}'::jsonb AS {column}) t"
    with driver.conn.raw_connection.cursor() as raw:
        raw.execute(native_sql, (value,))
        assert raw.fetchone() == (True,)
    with closing(driver.conn.execute(sql, (value,))) as cursor:
        assert cursor.fetchone() == (True,)


def test_native_row_types_survive_dictionary_and_view_reads(driver):
    from datetime import date
    from decimal import Decimal

    driver.conn.execute(
        "CREATE TABLE review_values(review_value_id INTEGER PRIMARY KEY, review_value_binary BYTEA, review_value_decimal NUMERIC, review_value_date DATE, review_value_boolean BOOLEAN)"
    ).close()
    driver.conn.execute(
        "INSERT INTO review_values VALUES (?, ?, ?, ?, ?)",
        (1, b"\x00\xff", Decimal("1.234567890123456789"), date(2026, 11, 6), True),
    ).close()
    driver.conn.execute(
        "CREATE VIEW review_values_view AS SELECT review_value_id AS id, review_values.* FROM review_values"
    ).close()
    driver.conn.commit()
    row = driver.direct_get_row_dict_from_id("review_values", 1)
    view = driver.direct_get_view_row_dict_from_id("review_values_view", 1)
    for data in (row, view):
        assert data["review_value_binary"] == b"\x00\xff"
        assert data["review_value_decimal"] == Decimal("1.234567890123456789")
        assert data["review_value_date"] == date(2026, 11, 6)
        assert data["review_value_boolean"] is True


@pytest.mark.parametrize("binding", ["?", "%s"], ids=["qmark", "native"])
@pytest.mark.parametrize("phrase", ["FIRST", "NEXT"])
def test_contextual_keywords_still_introduce_bindings(driver, binding, phrase):
    """Attribute classification must preserve the phrases that use these words."""
    with closing(
        driver.conn.execute(
            f"SELECT x FROM generate_series(1, 3) x ORDER BY x FETCH /* gap */ {phrase} {binding} ROWS ONLY",
            (2,),
        )
    ) as cursor:
        assert cursor.fetchall() == [(1,), (2,)]
    with closing(
        driver.conn.execute(
            f"SELECT TIMESTAMPTZ '2026-11-06 00:00:00+00' AT /* gap */ TIME ZONE {binding}",
            ("UTC",),
        )
    ) as cursor:
        assert str(cursor.fetchone()[0]) == "2026-11-06 00:00:00"
    with closing(
        driver.conn.execute(
            f"SELECT TIME {binding}, INTERVAL {binding}", ("12:30:00", "2 hours")
        )
    ) as cursor:
        time, interval = cursor.fetchone()
        assert str(time) == "12:30:00"
        assert interval.total_seconds() == 7200


@pytest.fixture
def driver():
    """Own a tiny private schema, keeping runtime tests independent of WEMI DDL."""
    url = os.environ.get("LIUXIN_TEST_POSTGRES_URL", "")
    if not url:
        pytest.skip("LIUXIN_TEST_POSTGRES_URL is not configured")
    metadata = {"postgres_url": url, "schema": "driver_safety_" + uuid4().hex}
    with closing(connect_postgres(metadata, prompt_for_password=False)) as cleanup:
        with cleanup:
            with cleanup.cursor() as cursor:
                cursor.execute(f'CREATE SCHEMA "{metadata["schema"]}"')
        instance = DatabaseDriver(metadata)
        try:
            instance.conn.execute("CREATE TABLE pending (value INTEGER)").close()
            instance.conn.commit()
            yield instance
        finally:
            instance.close()
            with cleanup:
                with cleanup.cursor() as cursor:
                    cursor.execute(f'DROP SCHEMA "{metadata["schema"]}" CASCADE')


@pytest.mark.parametrize("operation", ["execute", "executemany", "refresh"])
def test_aborted_primary_keeps_its_recoverable_savepoint(driver, operation):
    """A failed statement is not permission to close or roll back its connection."""
    import psycopg2

    primary = driver.conn
    primary.execute("INSERT INTO pending VALUES (1)").close()
    primary.execute("SAVEPOINT recovery").close()
    with pytest.raises(psycopg2.errors.DivisionByZero):
        primary.execute("SELECT 1/0")
    actions = {
        "execute": lambda: driver.direct_execute("SELECT count(*) FROM pending"),
        "executemany": lambda: driver.direct_executemany(
            "INSERT INTO pending VALUES (?)", [(2,)]
        ),
        "refresh": driver.refresh,
    }
    with pytest.raises((DatabaseDriverError, psycopg2.errors.InFailedSqlTransaction)):
        actions[operation]()
    assert driver.conn is primary
    assert not primary.raw_connection.closed
    primary.execute("ROLLBACK TO SAVEPOINT recovery").close()
    assert primary.execute("SELECT value FROM pending").fetchall() == [(1,)]
    primary.commit()
    with closing(driver.get_connection()) as reader:
        assert reader.execute("SELECT value FROM pending").fetchall() == [(1,)]


def test_closed_primary_requires_explicit_reopen(driver):
    """A closed handle cannot be mistaken for the caller's original transaction."""
    primary = driver.conn
    primary.close()
    with pytest.raises(DatabaseDriverError, match="reopen"):
        driver.direct_execute("SELECT 1")
    assert driver.conn is primary
    driver.reopen()
    with closing(driver.direct_execute("SELECT 1")) as cursor:
        assert cursor.fetchone() == (1,)


def test_closed_postgres_handles_leave_tracking(driver):
    retained = []
    for _ in range(30):
        connection = driver.get_connection()
        retained.append(connection)
        connection.close()
        connection.close()
        assert tuple(driver._open_connections) == (driver.conn,)


def test_postgres_does_not_advertise_scratch_switching(driver):
    assert not hasattr(driver, "make_scratch")


@pytest.mark.parametrize(
    "sql, bindings, expected",
    [
        ("SELECT '100%', ?", (7,), ("100%", 7)),
        ("SELECT 'literal %s and %%', ?", (7,), ("literal %s and %%", 7)),
        ("SELECT ? -- why? %\n", (7,), (7,)),
        ("SELECT 1 -- why?\r; SELECT ?", (7,), (7,)),
        ("SELECT ? /* outer ? /* nested % */ ? */", (7,), (7,)),
        ("SELECT $$what? `quote` %$$", None, ("what? `quote` %",)),
        ("SELECT $tag$what? %$tag$, ?", (7,), ("what? %", 7)),
        ("SELECT '{\"key\":1}'::jsonb ? 'key'", None, (True,)),
        ("SELECT ?::jsonb ? ?", ('{"key":1}', "key"), (True,)),
        ("SELECT ?::jsonb ?| ARRAY[?]", ('{"key":1}', "key"), (True,)),
        ("SELECT ?::jsonb ?& ARRAY[?]", ('{"key":1}', "key"), (True,)),
        ("SELECT ?::jsonb @? '$.key'", ('{"key":1}',), (True,)),
        ("SELECT %s::jsonb ? %s", ('{"key":1}', "key"), (True,)),
        ("SELECT '100%%', %s", (7,), ("100%", 7)),
        ("SELECT '100%', %(value)s", {"value": 7}, ("100%", 7)),
        ("SELECT 5 % 2, ?", (7,), (1, 7)),
        ("SELECT 5 % 2", (), (1,)),
        ("SELECT 'don''t?', ?", (7,), ("don't?", 7)),
        (r"SELECT 'C:\', ?", (7,), ("C:\\", 7)),
        (r"SELECT E'don\'t?', ?", (7,), ("don't?", 7)),
        ('SELECT ? AS `embedded"quote`', (7,), (7,)),
    ],
    ids=[
        "qmark_percent",
        "qmark_percent_spelling",
        "line_comment",
        "carriage_return_comment",
        "nested_comment",
        "unbound_dollar",
        "bound_dollar",
        "native_json",
        "qmark_json",
        "json_any",
        "json_all",
        "json_path",
        "pyformat_json",
        "pyformat_percent",
        "named_percent",
        "modulo",
        "empty_bindings",
        "doubled_quote",
        "standard_backslash",
        "escaped_quote",
        "quoted_identifier",
    ],
)
def test_sql_adaptation_preserves_values_and_operators(driver, sql, bindings, expected):
    """Check actual server results, not merely the generated SQL spelling."""
    with closing(driver.conn.execute(sql, bindings)) as cursor:
        assert cursor.fetchone() == expected


@pytest.mark.parametrize("column", ["first", "next", "time"])
@pytest.mark.parametrize("binding", ["?", "%s"], ids=["qmark", "native"])
@pytest.mark.parametrize(
    "operator", ["?", "?|", "?&", "@?"], ids=["key", "any", "all", "path"]
)
def test_qualified_keywords_remain_json_operands(driver, column, binding, operator):
    """A keyword after a qualification dot is an identifier, even across comments."""
    operand = f"ARRAY[{binding}]" if operator in {"?|", "?&"} else binding
    sql = (
        f"SELECT t. /* qualifier */ {column} {operator} {operand} "
        f"FROM (SELECT '{{\"key\":1}}'::jsonb AS {column}) t"
    )
    value = "$.key" if operator == "@?" else "key"
    with closing(driver.conn.execute(sql, (value,))) as cursor:
        assert cursor.fetchone() == (True,)


@pytest.mark.parametrize(
    "definition, keys, included, cardinality, typed",
    [
        (
            "UNIQUE INDEX ON review_links(left_id) INCLUDE(right_id, type)",
            ("left_id",),
            ("right_id", "type"),
            "many_to_one",
            False,
        ),
        (
            "UNIQUE INDEX ON review_links(left_id, right_id) INCLUDE(type)",
            ("left_id", "right_id"),
            ("type",),
            "many_to_many",
            False,
        ),
        (
            "UNIQUE INDEX ON review_links(right_id, left_id, type)",
            ("right_id", "left_id", "type"),
            (),
            "many_to_many",
            True,
        ),
        (
            "UNIQUE INDEX ON review_links(left_id) WHERE right_id > 0",
            (),
            (),
            "unknown",
            False,
        ),
        ("UNIQUE INDEX ON review_links((left_id + 1))", (), (), "unknown", False),
        ("INDEX ON review_links(left_id)", (), (), "unknown", False),
    ],
    ids=[
        "include_endpoints",
        "include_type",
        "typed_key",
        "partial",
        "expression",
        "nonunique",
    ],
)
def test_unique_groups_agree_with_physical_keys_and_link_shape(
    driver, definition, keys, included, cardinality, typed
):
    driver.conn.execute(
        "CREATE TABLE review_links(left_id INTEGER, right_id INTEGER, type TEXT)"
    ).close()
    driver.conn.execute("CREATE " + definition).close()
    driver.conn.commit()
    spec = driver.direct_get_physical_table_spec("review_links")
    assert spec.indexes[0].included_columns == included
    if keys:
        assert spec.indexes[0].columns == keys
    driver.conn.execute("INSERT INTO pending VALUES (7)").close()
    primary = driver.conn
    assert driver._get_unique_column_groups("review_links") == ((keys,) if keys else ())
    shape, identity = DriverWrapper._link_constraint_shape(
        SimpleNamespace(driver=driver), "review_links", "left_id", "right_id", "type"
    )
    assert shape.value == cardinality
    assert identity is typed
    assert driver.conn is primary
    assert primary.execute("SELECT value FROM pending").fetchall() == [(7,)]
    with closing(driver.get_connection()) as reader:
        assert reader.execute("SELECT value FROM pending").fetchall() == []
    assert tuple(driver._open_connections) == (primary,)


def test_unique_groups_include_primary_keys_and_exclude_invalid_indexes(driver):
    """A failed concurrent build leaves a real invalid index, not a global key."""
    import psycopg2

    driver.conn.execute(
        "CREATE TABLE review_links(id INTEGER PRIMARY KEY, left_id INTEGER)"
    ).close()
    driver.conn.executemany(
        "INSERT INTO review_links VALUES (?, ?)", [(1, 7), (2, 7)]
    ).close()
    driver.conn.commit()
    with closing(driver.get_connection()) as build:
        build.raw_connection.autocommit = True
        with pytest.raises(psycopg2.errors.UniqueViolation):
            build.execute(
                "CREATE UNIQUE INDEX CONCURRENTLY invalid_key ON review_links(left_id)"
            )
    indexes = driver.direct_get_physical_table_spec("review_links").indexes
    assert any(index.name == "invalid_key" and not index.valid for index in indexes)
    assert driver._get_unique_column_groups("review_links") == (("id",),)


def test_executemany_translates_once_without_consuming_bindings_twice(driver):
    driver.conn.execute("CREATE TABLE labels (value TEXT)").close()
    driver.conn.executemany(
        "INSERT INTO labels VALUES ('100% ' || ?)",
        ((value,) for value in ("one", "two")),
    ).close()
    assert driver.conn.execute(
        "SELECT value FROM labels ORDER BY value"
    ).fetchall() == [
        ("100% one",),
        ("100% two",),
    ]


def test_native_script_preserves_bodies_literals_and_transaction_ownership(driver):
    """The server parses procedural/SQL-standard bodies; adapter cleanup never commits."""
    primary = driver.conn
    primary.execute("INSERT INTO pending VALUES (1)").close()
    primary.executescript(
        "CREATE TABLE script_values (value TEXT);"
        "INSERT INTO script_values VALUES ('a;b');"
        "DO $body$ BEGIN INSERT INTO script_values VALUES ('why? %'); END $body$;"
        "CREATE FUNCTION script_value() RETURNS TEXT LANGUAGE SQL "
        "BEGIN ATOMIC RETURN 'native;?'; END;"
        "INSERT INTO script_values VALUES (script_value());"
    )
    assert primary.execute(
        "SELECT value FROM script_values ORDER BY value"
    ).fetchall() == [
        ("a;b",),
        ("native;?",),
        ("why? %",),
    ]
    with closing(driver.get_connection()) as reader:
        assert reader.execute("SELECT value FROM pending").fetchall() == []
    primary.rollback()
    assert primary.execute("SELECT value FROM pending").fetchall() == []
    assert primary.execute("SELECT to_regclass('script_values')").fetchone() == (None,)


def test_failed_script_keeps_caller_savepoint_recovery(driver):
    import psycopg2

    primary = driver.conn
    primary.execute("INSERT INTO pending VALUES (1)").close()
    primary.execute("SAVEPOINT recovery").close()
    with pytest.raises(psycopg2.errors.DivisionByZero):
        primary.executescript("INSERT INTO pending VALUES (2); SELECT 1/0;")
    primary.execute("ROLLBACK TO SAVEPOINT recovery").close()
    assert primary.execute("SELECT value FROM pending").fetchall() == [(1,)]


def test_script_calls_installed_hooks_before_each_statement(driver):
    """Correct script parsing must retain maintenance's transaction boundaries."""
    statements = []

    def observe(connection, sql):
        statements.append(sql)
        if sql.strip().upper() == "COMMIT;":
            raise RuntimeError("completion rejected")

    vars(driver.conn)["_liuxin_before_statement"] = observe
    with pytest.raises(RuntimeError, match="completion rejected"):
        driver.conn.executescript("INSERT INTO pending VALUES (1);SELECT 'a;b';COMMIT;")
    assert len(statements) == 3
    assert statements[1] == "SELECT 'a;b';"
    driver.conn.rollback()
    assert driver.conn.execute("SELECT value FROM pending").fetchall() == []


def test_mixed_binding_styles_fail_before_any_statement_executes(driver):
    with pytest.raises(ValueError, match="mix"):
        driver.conn.execute("INSERT INTO pending VALUES (? + %s)", (1, 2))
    assert driver.conn.execute("SELECT value FROM pending").fetchall() == []


def test_failed_postgres_connection_setup_closes_the_raw_handle(driver, monkeypatch):
    import importlib

    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import (
        PostgresConnectionAdapter,
    )

    module = importlib.import_module(type(driver).__module__)
    connect = module.connect_postgres
    opened = []

    def capture(*args, **kwargs):
        raw = connect(*args, **kwargs)
        opened.append(raw)
        return raw

    def reject(_):
        raise RuntimeError("setup failed")

    monkeypatch.setattr(module, "connect_postgres", capture)
    monkeypatch.setattr(PostgresConnectionAdapter, "commit", reject)
    with pytest.raises(RuntimeError, match="setup failed"):
        driver.get_connection()
    assert len(opened) == 1 and opened[0].closed
    assert tuple(driver._open_connections) == (driver.conn,)
