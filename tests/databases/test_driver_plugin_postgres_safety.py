"""Exercise driver recovery and SQL adaptation on a real PostgreSQL server."""

import json
import os
import subprocess
import sys
from contextlib import closing
from types import SimpleNamespace
from uuid import uuid4

import pytest

from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL import sql_tokens
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import (
    connect_postgres,
)
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.databasedriver import (
    DatabaseDriver,
)
from LiuXin_alpha.databases.driver_wrapper import DriverWrapper
from LiuXin_alpha.errors import DatabaseDriverError

UNICODE_NAMES = [
    "plain",
    "名",
    "e\u0301",
    "📚",
    "\u00a0",
    "\u0085",
    "\u2003",
    "\ufeff",
    "x\u00a0time",
    "ſelect",
    "ıN",
]
UNICODE_IDS = [
    "ascii",
    "cjk",
    "combining",
    "emoji",
    "nbsp",
    "nel",
    "em_space",
    "bom",
    "keyword_suffix",
    "long_s_keyword",
    "dotless_i_keyword",
]


@pytest.mark.parametrize("name", UNICODE_NAMES, ids=UNICODE_IDS)
@pytest.mark.parametrize("binding", ["?", "%s"], ids=["qmark", "native"])
@pytest.mark.parametrize("quoted", [False, True], ids=["unquoted", "quoted"])
def test_unicode_json_identifiers_match_native_engine(driver, name, binding, quoted):
    identifier = '"' + name + '"' if quoted else name
    suffix = f" FROM (SELECT '{{\"key\":1}}'::jsonb AS {identifier}) t"
    with driver.conn.raw_connection.cursor() as raw:
        raw.execute(f"SELECT {identifier} ? %s" + suffix, ("key",))
        expected = raw.fetchone()
    with closing(
        driver.conn.execute(f"SELECT {identifier} ? {binding}" + suffix, ("key",))
    ) as cursor:
        assert cursor.fetchone() == expected == (True,)


@pytest.mark.parametrize("tag", ["", *UNICODE_NAMES], ids=["empty", *UNICODE_IDS])
@pytest.mark.parametrize(
    "binding", [None, "?", "%s"], ids=["unbound", "qmark", "native"]
)
def test_unicode_dollar_literals_preserve_committed_values(driver, tag, binding):
    payload = "before `quoted`; COMMIT; ?; 100% after"
    driver.conn.execute(
        "CREATE TABLE literal_values(id INTEGER,body TEXT,extra INTEGER)"
    ).close()
    with driver.conn.raw_connection.cursor() as raw:
        raw.execute("INSERT INTO literal_values VALUES(1,%s,7)", (payload,))
    sql = (
        f"INSERT INTO literal_values VALUES(2,${tag}${payload}${tag}$,{binding or '7'})"
    )
    with closing(driver.conn.execute(sql, (7,) if binding else None)):
        pass
    driver.conn.commit()
    with closing(driver.get_connection()) as reader:
        with reader.raw_connection.cursor() as raw:
            raw.execute("SELECT body,extra FROM literal_values ORDER BY id")
            assert raw.fetchall() == [(payload, 7), (payload, 7)]


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
    "column",
    [
        "first",
        "next",
        "time",
        "zone",
        "by",
        "at",
        "interval",
        "between",
        "escape",
        "set",
        "values",
    ],
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


@pytest.mark.parametrize(
    "gap",
    ["\n", "\r", "\r\n", " -- quote '; `name`\n", "\n-- gap\r\t"],
    ids=["lf", "cr", "crlf", "comment", "two_lines"],
)
@pytest.mark.parametrize("batch", [False, True], ids=["execute", "executemany"])
@pytest.mark.parametrize(
    "binding", [None, "?", "%s"], ids=["unbound", "qmark", "native"]
)
@pytest.mark.parametrize(
    "ending",
    [r"'\'`literal`\' ? ; COMMIT; 100% %s'", r"'with\'quote'"],
    ids=["opaque_syntax", "escaped_quote"],
)
def test_continued_escape_strings_preserve_committed_values(
    driver, gap, binding, ending, batch
):
    literal = "E'prefix'" + gap + ending + "\n' tail'"
    with driver.conn.raw_connection.cursor() as raw:
        raw.execute("SELECT " + literal)
        expected = raw.fetchone()[0]
    driver.conn.execute(
        "CREATE TABLE continued_values(body TEXT,extra INTEGER)"
    ).close()
    # Native pyformat callers escape literal percent signs; qmark callers do not.
    supplied_literal = literal.replace("%", "%%") if binding == "%s" else literal
    sql = f"INSERT INTO continued_values VALUES({supplied_literal},{binding or '7'})"
    if batch:
        driver.conn.executemany(sql, [(7,) if binding else ()] * 2).close()
    else:
        driver.conn.execute(sql, (7,) if binding else None).close()
    driver.conn.commit()
    with closing(driver.get_connection()) as reader:
        with reader.raw_connection.cursor() as raw:
            raw.execute("SELECT body,extra FROM continued_values")
            assert raw.fetchall() == [(expected, 7)] * (2 if batch else 1)


@pytest.mark.parametrize("binding", ["?", "%s"], ids=["qmark", "native"])
@pytest.mark.parametrize(
    "sql,values",
    [
        ("SELECT 2 BETWEEN {bind} AND {bind}", (1, 3)),
        ("SELECT 2 NOT BETWEEN {bind} AND {bind}", (1, 3)),
        ("SELECT 'a_b' LIKE {bind} ESCAPE {bind}", ("a!_b", "!")),
        ("SELECT 'a_b' NOT LIKE {bind} ESCAPE {bind}", ("a!_b", "!")),
        ("VALUES ({bind})", (7,)),
        ("SELECT * FROM (VALUES ({bind})) t(value)", (7,)),
        (
            "SELECT NOT between ? {bind} FROM (SELECT '{\"key\":1}'::jsonb AS between) t",
            ("key",),
        ),
        (
            "SELECT (values ? {bind}), 2 BETWEEN {bind} AND {bind} FROM (SELECT '{\"key\":1}'::jsonb AS values) t",
            ("key", 1, 3),
        ),
        ("VALUES {bind}", ((7,),)),
        ("SELECT * FROM (VALUES {bind}) t(value)", ((7,),)),
        ("VALUES {bind} UNION ALL VALUES {bind}", ((7,), (8,))),
        ("SELECT 2 BETWEEN SYMMETRIC {bind} AND {bind}", (3, 1)),
        ("SELECT 2 NOT BETWEEN ASYMMETRIC {bind} AND {bind}", (3, 1)),
    ],
    ids=[
        "between",
        "not_between",
        "escape",
        "not_escape",
        "values",
        "nested_values",
        "unary_not",
        "combined",
        "row_tuple",
        "nested_row_tuple",
        "union_row_tuples",
        "symmetric",
        "not_asymmetric",
    ],
)
def test_keyword_contexts_keep_real_sql_grammar(driver, binding, sql, values):
    with driver.conn.raw_connection.cursor() as raw:
        raw.execute(sql.replace("{bind}", "%s"), values)
        expected = raw.fetchall()
    with closing(driver.conn.execute(sql.replace("{bind}", binding), values)) as cursor:
        assert cursor.fetchall() == expected


@pytest.mark.parametrize("escape", [False, True], ids=["ordinary", "escape"])
@pytest.mark.parametrize("continuation", [False, True], ids=["eof", "continued"])
def test_line_comment_after_string_does_not_stall_translation(
    driver, escape, continuation
):
    sql = f"SELECT {'E' if escape else ''}'prefix' " + "--" * 2048
    if continuation:
        sql += "\n' tail'"
    # A subprocess bounds failures: an exponential regex can otherwise hang
    # the entire gate. Load only this module so the deadline excludes app setup.
    program = """
import importlib.util, json, sys
spec = importlib.util.spec_from_file_location('tokens_probe', sys.argv[1])
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
sql = json.load(sys.stdin)
print(json.dumps([module.adapt_sql(sql, bound=False), list(module.split_sql_statements(sql))]))
"""
    result = subprocess.run(
        [sys.executable, "-c", program, sql_tokens.__file__],
        input=json.dumps(sql),
        capture_output=True,
        text=True,
        timeout=10,
        check=True,
    )
    adapted, statements = json.loads(result.stdout)
    assert adapted == sql and statements == [sql]
    with driver.conn.raw_connection.cursor() as raw:
        raw.execute(sql)
        expected = raw.fetchone()
    with closing(driver.conn.execute(adapted)) as cursor:
        assert (
            cursor.fetchone()
            == expected
            == ("prefix tail" if continuation else "prefix",)
        )


@pytest.mark.parametrize("binding", ["?", "%s"], ids=["qmark", "native"])
def test_keyword_columns_survive_update_set_and_insert_values(driver, binding):
    driver.conn.execute('CREATE TABLE keywords("values" JSONB,"set" TEXT)').close()
    driver.conn.execute(
        f"INSERT INTO keywords VALUES ({binding}::jsonb,{binding})",
        ('{"key":1}', "first"),
    ).close()
    driver.conn.execute(
        f"UPDATE keywords SET set={binding} WHERE values ? {binding}",
        ("updated", "key"),
    ).close()
    driver.conn.commit()
    with closing(driver.get_connection()) as reader:
        with reader.raw_connection.cursor() as raw:
            raw.execute('SELECT "set" FROM keywords')
            assert raw.fetchall() == [("updated",)]


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


@pytest.mark.parametrize("binding", ["?", "%s"], ids=["qmark", "native"])
@pytest.mark.parametrize("batch", [False, True], ids=["execute", "executemany"])
@pytest.mark.parametrize(
    "sql,values",
    [
        ("SELECT SUBSTRING({b} FROM {b} FOR {b})", ("abcdef", 2, 3)),
        ("SELECT TRIM(LEADING {b} FROM {b})", ("x", "xxvalue")),
        ("SELECT TRIM(TRAILING {b} FROM {b})", ("x", "valuexx")),
        ("SELECT TRIM(BOTH {b} FROM {b})", ("x", "xxvaluexx")),
        ("SELECT OVERLAY({b} PLACING {b} FROM {b} FOR {b})", ("abcdef", "X", 2, 3)),
        ("SELECT DISTINCT ON ({b}) {b}", (1, "result")),
        ("SELECT DISTINCT ON (({b} + ({b}))) {b}", (1, 2, "nested")),
        ("SELECT CONCAT(VARIADIC {b}::text[])", (["a", "b"],)),
        (
            "SELECT SUM(n) OVER (ORDER BY n ROWS {b} PRECEDING) FROM generate_series(1,3) n",
            (1,),
        ),
        (
            "SELECT SUM(n) OVER (ORDER BY n RANGE {b} PRECEDING) FROM generate_series(1,3) n",
            (1,),
        ),
        (
            "SELECT SUM(n) OVER (ORDER BY n GROUPS {b} PRECEDING) FROM generate_series(1,3) n",
            (1,),
        ),
        ("SELECT SUM(n) OVER (ROWS {b} PRECEDING) FROM generate_series(1,3) n", (1,)),
        (
            "SELECT SUM(n) OVER w FROM generate_series(1,3) n WINDOW w AS (ROWS {b} PRECEDING)",
            (1,),
        ),
        (
            "SELECT DISTINCT /* gap */ ON (({b})) json ? {b} FROM (SELECT '{\"key\":1}'::jsonb AS json) t",
            (1, "key"),
        ),
        (
            "SELECT SUM(n) OVER (ORDER BY (rows ? {b}) ROWS {b} PRECEDING) FROM (SELECT 1 n, '{\"key\":1}'::jsonb AS rows) t",
            ("key", 1),
        ),
    ],
    ids=[
        "substring",
        "leading",
        "trailing",
        "both",
        "overlay",
        "distinct",
        "nested_distinct",
        "variadic",
        "rows",
        "range",
        "groups",
        "unordered_window",
        "named_window",
        "distinct_json",
        "window_json",
    ],
)
def test_expression_boundaries_preserve_single_and_batch_writes(
    driver, binding, batch, sql, values
):
    """Commit the same native and portable expressions through the public driver."""
    select = "SELECT result::text FROM (" + sql + ") AS t(result)"
    with driver.conn.raw_connection.cursor() as raw:
        raw.execute(select.replace("{b}", "%s"), values)
        expected = raw.fetchall()
    driver.conn.execute("CREATE TABLE grammar_values(body TEXT)").close()
    driver.conn.commit()
    insert = "INSERT INTO grammar_values " + select.replace("{b}", binding)
    if batch:
        driver.direct_executemany(insert, (values for _ in range(2)))
    else:
        driver.direct_execute(insert, values).close()
    with closing(driver.get_connection()) as reader:
        with reader.raw_connection.cursor() as raw:
            raw.execute("SELECT body FROM grammar_values")
            assert raw.fetchall() == expected * (2 if batch else 1)


@pytest.mark.parametrize("column", ["rows", "range", "groups"])
@pytest.mark.parametrize("binding", ["?", "%s"], ids=["qmark", "native"])
def test_window_frame_words_still_name_json_operands(driver, column, binding):
    sql = f"SELECT ({column} ? {binding}), length(({column} ? {binding})::text) FROM (SELECT '{{\"key\":1}}'::jsonb AS {column}) t"
    with closing(driver.conn.execute(sql, ("key", "key"))) as cursor:
        assert cursor.fetchone() == (True, 4)


@pytest.fixture(params=["startup", "session", "local"])
def nonstandard_driver(request, monkeypatch):
    """Select escape-string mode through startup options or a live transaction."""
    if request.param == "startup":
        from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

        url = os.environ.get("LIUXIN_TEST_POSTGRES_URL", "")
        if not url:
            pytest.skip("LIUXIN_TEST_POSTGRES_URL is not configured")
        parts = urlsplit(url)
        query = dict(parse_qsl(parts.query))
        query["options"] = (
            query.get("options", "") + " -c standard_conforming_strings=off"
        )
        monkeypatch.setenv(
            "LIUXIN_TEST_POSTGRES_URL",
            urlunsplit(parts._replace(query=urlencode(query, quote_via=quote))),
        )
    instance = request.getfixturevalue("driver")
    instance.conn.execute("CREATE TABLE mode_values(body TEXT, extra INTEGER)").close()
    instance.conn.commit()
    if request.param != "startup":
        instance.conn.execute(
            f"SET {request.param.upper()} standard_conforming_strings=off"
        ).close()
    assert (
        instance.conn.raw_connection.get_parameter_status("standard_conforming_strings")
        == "off"
    )
    return instance


@pytest.mark.parametrize("batch", [False, True], ids=["execute", "executemany"])
@pytest.mark.parametrize(
    "binding", [None, "?", "%s"], ids=["unbound", "qmark", "native"]
)
@pytest.mark.parametrize(
    "prefix,gap",
    [("", ""), ("E", ""), ("", "\n"), ("E", " -- gap\r\n")],
    ids=["ordinary", "explicit", "continued", "continued_explicit"],
)
def test_current_string_mode_preserves_committed_literals(
    nonstandard_driver, batch, binding, prefix, gap
):
    driver = nonstandard_driver
    literal = (
        prefix
        + ("'prefix'" + gap if gap else "")
        + r"'\'`literal`\' ?; COMMIT; 100% %s'"
    )
    with driver.conn.raw_connection.cursor() as raw:
        raw.execute("SELECT " + literal)
        expected = raw.fetchone()[0]
    supplied = literal.replace("%", "%%") if binding == "%s" else literal
    sql = f"INSERT INTO mode_values VALUES({supplied},{binding or '7'})"
    if batch:
        driver.conn.executemany(sql, [(7,) if binding else ()] * 2).close()
    else:
        driver.conn.execute(sql, (7,) if binding else None).close()
    driver.conn.commit()
    with closing(driver.get_connection()) as reader:
        with reader.raw_connection.cursor() as raw:
            raw.execute("SELECT body,extra FROM mode_values")
            assert raw.fetchall() == [(expected, 7)] * (2 if batch else 1)


@pytest.mark.parametrize(
    "standalone", [False, True], ids=["connection_cursor", "standalone_cursor"]
)
def test_existing_cursor_tracks_set_local_savepoint_and_rollback(driver, standalone):
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import (
        PostgresCursorAdapter,
    )

    driver.conn.execute("SET standard_conforming_strings=on").close()
    driver.conn.commit()
    cursor = (
        PostgresCursorAdapter(driver.conn.raw_connection.cursor())
        if standalone
        else driver.conn.cursor()
    )
    with closing(cursor):
        for control, expected in [
            ("SAVEPOINT mode_before", r"a\nb"),
            ("SET LOCAL standard_conforming_strings=off", "a\nb"),
            ("ROLLBACK TO mode_before", r"a\nb"),
            ("SET standard_conforming_strings=off", "a\nb"),
            ("ROLLBACK", r"a\nb"),
            ("BEGIN", r"a\nb"),
            ("SET LOCAL standard_conforming_strings=off", "a\nb"),
            ("COMMIT", r"a\nb"),
        ]:
            driver.conn.execute(control).close()
            # Escaped quotes expose backticks only when the server's mode is off.
            literal = r"'a\nb'" if expected == r"a\nb" else r"'a\nb\'`literal`\''"
            with driver.conn.raw_connection.cursor() as raw:
                raw.execute("SELECT " + literal)
                native = raw.fetchone()
            cursor.execute("SELECT " + literal)
            assert (
                cursor.fetchone()
                == native
                == ((expected if expected == r"a\nb" else expected + "'`literal`'"),)
            )


def test_hooked_script_reads_mode_changes_between_statements(driver):
    driver.conn.execute("CREATE TABLE script_modes(body TEXT)").close()
    driver.conn.commit()
    calls = []
    driver.conn._liuxin_before_statement = lambda connection, sql: calls.append(sql)
    with driver.conn:
        driver.conn.executescript(
            "SET standard_conforming_strings=off;"
            r"INSERT INTO script_modes VALUES('prefix\'`literal`\' ; COMMIT;');"
            "SET standard_conforming_strings=on;"
            r"INSERT INTO script_modes VALUES('a\nb');"
        )
    assert len(calls) == 4
    with closing(driver.get_connection()) as reader:
        with reader.raw_connection.cursor() as raw:
            raw.execute("SELECT body FROM script_modes")
            assert raw.fetchall() == [("prefix'`literal`' ; COMMIT;",), (r"a\nb",)]


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
