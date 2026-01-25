"""Database contract: search + values-set + chunking + random rows (chunk 07).

This slice focuses on mid-level convenience surfaces on Database:

* Database.search(): exact-match semantics, error surfaces, and "injection-shaped" payload inertness.
* Database.get_values_set(): DISTINCT extraction (set + iterator forms).
* Database.chunk_iterator(): grouping by a unique column (same-table grouping).
* Database.get_random_row(): returns only real rows for non-empty tables; defines behavior for empty tables.

Design notes
------------
- We create a dedicated per-test contract table with UNIQUE column names. This avoids ambiguity in
  driver-side "identify table from column/row" logic.
- We execute DDL/DML via a short-lived driver connection, because some Database introspection calls
  may force-refresh the driver's primary connection.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Iterable, Sequence

import pytest

from LiuXin_alpha.errors import InputIntegrityError
from LiuXin_alpha.databases.row import Row


@dataclass(frozen=True)
class ContractTable:
    name: str
    id_col: str
    scratch_col: str
    text_col: str
    group_col: str
    num_col: str


def _stable_suffix(nodeid: str) -> str:
    # Deterministic across runs (unlike hash()).
    return hashlib.sha1(nodeid.encode("utf-8")).hexdigest()[:10]


def _exec_sql(db, stmt: str, bindings: tuple | None = None) -> None:
    """Execute SQL using a short-lived driver connection (SQLite/APSW tolerant)."""
    driver = getattr(db, "driver", None)
    if driver is None or not hasattr(driver, "get_connection"):
        raise RuntimeError("Database has no driver with get_connection()")

    conn = driver.get_connection()
    try:
        cur = conn.cursor()
        if bindings is None:
            cur.execute(stmt)
        else:
            cur.execute(stmt, bindings)

        try:
            conn.commit()
        except Exception:
            try:
                conn.execute("COMMIT")
            except Exception:
                pass
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _fetch_all(db, stmt: str, bindings: tuple | None = None) -> list[tuple]:
    driver = getattr(db, "driver", None)
    if driver is None or not hasattr(driver, "get_connection"):
        raise RuntimeError("Database has no driver with get_connection()")

    conn = driver.get_connection()
    try:
        cur = conn.cursor()
        if bindings is None:
            cur.execute(stmt)
        else:
            cur.execute(stmt, bindings)
        return list(cur.fetchall())
    finally:
        try:
            conn.close()
        except Exception:
            pass


@pytest.fixture
def contract_table(open_db, request) -> ContractTable:
    """Create a dedicated contract table for this test (unique columns)."""
    suf = _stable_suffix(request.node.nodeid)

    table = ContractTable(
        name=f"db_contract_s7_{suf}",
        id_col="id",
        scratch_col=f"scratch_s7_{suf}",
        text_col=f"text_s7_{suf}",
        group_col=f"group_s7_{suf}",
        num_col=f"num_s7_{suf}",
    )

    _exec_sql(
        open_db,
        f"""
        CREATE TABLE IF NOT EXISTS {table.name} (
            {table.id_col} INTEGER PRIMARY KEY,
            {table.scratch_col} TEXT UNIQUE,
            {table.text_col} TEXT,
            {table.group_col} TEXT,
            {table.num_col} INTEGER
        );
        """.strip(),
    )
    
    # Driver/wrapper caches may have memoized table lists/columns.
    try:
        open_db.driver.call_after_table_changes()
    except Exception:
        pass
    try:
        open_db.refresh_db_metadata()
    except Exception:
        pass

    return table


def _insert_rows(
    db,
    table: ContractTable,
    rows: Sequence[tuple[str, str | None, str | None, int | None]],
) -> None:
    """Insert rows as (scratch, text, group, num)."""
    for scratch, txt, grp, num in rows:
        _exec_sql(
            db,
            f"INSERT INTO {table.name} ({table.scratch_col}, {table.text_col}, {table.group_col}, {table.num_col}) "
            f"VALUES (?, ?, ?, ?);",
            (scratch, txt, grp, num),
        )


def _all_row_ids(db, table: ContractTable) -> set[int]:
    got = _fetch_all(db, f"SELECT {table.id_col} FROM {table.name};")
    return {int(r[0]) for r in got}


def _find_id_by_scratch(db, table: ContractTable, scratch: str) -> int:
    got = _fetch_all(
        db,
        f"SELECT {table.id_col} FROM {table.name} WHERE {table.scratch_col} = ?;",
        (scratch,),
    )
    assert got and got[0] and got[0][0] is not None
    return int(got[0][0])


def _pick_non_nul_payloads(pick_payload, n: int = 32) -> list[str]:
    payloads: list[str] = []
    for i in range(n):
        p = pick_payload(i)
        if "\x00" in p:
            continue
        payloads.append(p)
    # Ensure we always have enough variety even if the corpus changes.
    assert payloads, "No usable payloads found (all contained NUL?)"
    return payloads


# ------------------------------------------------------------------------------
# Database.search
# ------------------------------------------------------------------------------


def test_search_returns_rows_and_is_exact_match(open_db, contract_table: ContractTable, pick_payload):
    payloads = _pick_non_nul_payloads(pick_payload, n=48)
    needle = payloads[7]

    rows = [
        ("s7_a", needle, "G1", 10),
        ("s7_b", payloads[9], "G1", 11),
        ("s7_c", payloads[11], "G2", 12),
    ]
    _insert_rows(open_db, contract_table, rows)

    got = open_db.search(table=contract_table.name, column=contract_table.text_col, search_term=needle)
    assert isinstance(got, list)
    assert len(got) == 1
    assert isinstance(got[0], Row)
    assert got[0].table == contract_table.name
    assert got[0][contract_table.text_col] == needle

    # Exact-match (not substring)
    if len(needle) >= 3:
        sub = needle[:3]
        got2 = open_db.search(table=contract_table.name, column=contract_table.text_col, search_term=sub)
        assert got2 == []


def test_search_empty_on_no_match(open_db, contract_table: ContractTable):
    _insert_rows(open_db, contract_table, [("s7_x", "alpha", "G0", 1)])
    got = open_db.search(table=contract_table.name, column=contract_table.text_col, search_term="beta")
    assert got == []


def test_search_non_string_terms_do_not_crash(open_db, contract_table: ContractTable):
    _insert_rows(open_db, contract_table, [("s7_x", "alpha", "G0", 1)])

    weird_terms = [
        b"alpha",  # bytes -> driver coerces via force_unicode=str
        1,
        1.0,
        True,
        {"k": "v"},
        object(),
    ]
    for term in weird_terms:
        got = open_db.search(table=contract_table.name, column=contract_table.text_col, search_term=term)
        assert isinstance(got, list)
        assert all(isinstance(r, Row) for r in got)


def test_search_invalid_table_raises(open_db):
    with pytest.raises(InputIntegrityError):
        open_db.search(table="definitely_not_a_table", column="nope", search_term="x")


def test_search_invalid_column_raises(open_db, contract_table: ContractTable):
    _insert_rows(open_db, contract_table, [("s7_x", "alpha", "G0", 1)])
    with pytest.raises(InputIntegrityError):
        open_db.search(table=contract_table.name, column="definitely_not_a_column", search_term="alpha")


@pytest.mark.xfail(strict=True, reason="Current driver uses '=' so NULL is not matched; consider 'IS NULL' semantics.")
def test_search_none_should_match_null_rows_desired_behavior(open_db, contract_table: ContractTable):
    _insert_rows(open_db, contract_table, [("s7_n", None, "G0", 1)])
    got = open_db.search(table=contract_table.name, column=contract_table.text_col, search_term=None)
    assert len(got) == 1


# ------------------------------------------------------------------------------
# Database.get_values_set
# ------------------------------------------------------------------------------


def test_get_values_set_returns_unique_values(open_db, contract_table: ContractTable):
    _insert_rows(
        open_db,
        contract_table,
        [
            ("s7_a", "alpha", "A", 1),
            ("s7_b", "beta", "A", 2),
            ("s7_c", "gamma", "B", 3),
            ("s7_d", "delta", None, 4),
        ],
    )

    got = open_db.get_values_set(target_column=contract_table.group_col, iterator_return=False)
    assert isinstance(got, set)
    assert got == {"A", "B", None}


def test_get_values_set_iterator_matches_set(open_db, contract_table: ContractTable):
    _insert_rows(
        open_db,
        contract_table,
        [
            ("s7_a", "alpha", "A", 1),
            ("s7_b", "beta", "A", 2),
            ("s7_c", "gamma", "B", 3),
        ],
    )

    as_set = open_db.get_values_set(target_column=contract_table.group_col, iterator_return=False)
    it = open_db.get_values_set(target_column=contract_table.group_col, iterator_return=True)
    got_from_iter = set(it)
    assert got_from_iter == as_set


def test_get_values_set_unknown_column_raises(open_db):
    with pytest.raises(InputIntegrityError):
        open_db.get_values_set(target_column="column_that_does_not_exist_anywhere", iterator_return=False)


# ------------------------------------------------------------------------------
# Database.chunk_iterator
# ------------------------------------------------------------------------------


def test_chunk_iterator_groups_rows_by_column(open_db, contract_table: ContractTable):
    # Avoid NULL group values here; see xfail test below.
    _insert_rows(
        open_db,
        contract_table,
        [
            ("s7_1", "one", "G1", 1),
            ("s7_2", "two", "G1", 2),
            ("s7_3", "three", "G2", 3),
            ("s7_4", "four", "G3", 4),
            ("s7_5", "five", "G3", 5),
        ],
    )
    expected_ids = _all_row_ids(open_db, contract_table)

    chunks = list(open_db.chunk_iterator(column=contract_table.group_col, target_table=None))
    assert chunks, "Expected at least one chunk"

    seen_ids: set[int] = set()
    for chunk in chunks:
        assert isinstance(chunk, list)
        assert chunk, "No empty chunks expected for non-NULL group values"
        assert all(isinstance(r, Row) for r in chunk)
        # All rows within a chunk share the same group value.
        grp_vals = {r[contract_table.group_col] for r in chunk}
        assert len(grp_vals) == 1
        for r in chunk:
            assert r.table == contract_table.name
            assert r.row_id is not None
            seen_ids.add(int(r.row_id))

    assert seen_ids == expected_ids
    # Chunk count equals number of unique group values (order is not defined).
    assert len(chunks) == len({"G1", "G2", "G3"})


@pytest.mark.xfail(strict=True, reason="chunk_iterator relies on '=' search; NULL group values yield empty chunks today.")
def test_chunk_iterator_should_group_null_values_desired_behavior(open_db, contract_table: ContractTable):
    _insert_rows(
        open_db,
        contract_table,
        [
            ("s7_1", "one", None, 1),
            ("s7_2", "two", None, 2),
        ],
    )
    chunks = list(open_db.chunk_iterator(column=contract_table.group_col, target_table=None))
    # Desired: one chunk containing both rows.
    assert any(len(chunk) == 2 for chunk in chunks)


def test_chunk_iterator_unknown_column_raises(open_db):
    with pytest.raises(InputIntegrityError):
        list(open_db.chunk_iterator(column="definitely_not_a_column", target_table=None))


# ------------------------------------------------------------------------------
# Database.get_random_row
# ------------------------------------------------------------------------------


def test_get_random_row_returns_existing_row(open_db, contract_table: ContractTable):
    _insert_rows(
        open_db,
        contract_table,
        [
            ("s7_a", "alpha", "G1", 1),
            ("s7_b", "beta", "G1", 2),
            ("s7_c", "gamma", "G2", 3),
            ("s7_d", "delta", "G3", 4),
        ],
    )
    ids = _all_row_ids(open_db, contract_table)
    assert ids

    # Repeated calls should always return a real row from the table.
    for _ in range(25):
        row = open_db.get_random_row(table=contract_table.name)
        assert isinstance(row, Row)
        assert row.table == contract_table.name
        assert row.row_id is not None
        assert int(row.row_id) in ids


def test_get_random_row_empty_table_current_behavior(open_db, contract_table: ContractTable):
    # No inserts. Current driver returns None row_dict; Database wraps it in a Row with no id.
    row = open_db.get_random_row(table=contract_table.name)
    assert isinstance(row, Row)
    assert row.row_id is None


def test_get_random_row_invalid_table_raises(open_db):
    with pytest.raises(InputIntegrityError):
        open_db.get_random_row(table="definitely_not_a_table")
