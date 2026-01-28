"""Database contract: CRUD + search + unicode nightmares (chunk 03).

This slice focuses on *behavioral* correctness of the high-level
:class:`~LiuXin_alpha.databases.database.Database` API:

* Creating a writable blank row via Database.get_blank_row().
* Updating rows via Row.sync() and round-tripping via Database.get_row_from_id().
* Searching via Database.search() across a torture corpus of multilingual strings.
* Ensuring SQL-injection-shaped inputs remain inert data.
* Delete and duplicate semantics (including unique-constraint cleanup).
* Light sanity checks for iterator surfaces (get_all_rows, get_values_set, chunk_iterator).

These tests intentionally create a dedicated per-test contract table with
*unique* column names to avoid ambiguity in driver-side table detection.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

import pytest

from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError


@dataclass(frozen=True)
class ContractTable:
    name: str
    id_col: str
    scratch_col: str
    text_col: str
    group_col: str
    unique_col: str
    blob_col: str
    num_col: str


def _exec_sql(db, stmt: str, bindings: tuple | None = None) -> None:
    """Execute SQL in a backend-tolerant way.

    Important: Database.get_tables()/introspection may force-refresh the driver's primary
    connection. To avoid stale connection aliases on Database objects, this helper prefers a
    short-lived *new* driver connection for DDL/DML.
    """

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

        # sqlite3 requires commit for cross-connection visibility; APSW often autocommits.
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


@pytest.fixture
def contract_table(open_db) -> ContractTable:
    """Create a dedicated contract table for this test DB instance."""

    t = ContractTable(
        name="db_contract_l3",
        id_col="db_contract_l3_id",
        scratch_col="db_contract_l3_scratch",
        text_col="db_contract_l3_text",
        group_col="db_contract_l3_group",
        unique_col="db_contract_l3_unique",
        blob_col="db_contract_l3_blob",
        num_col="db_contract_l3_num",
    )

    # Keep schema intentionally permissive: get_blank_row() inserts using only the scratch column.
    _exec_sql(
        open_db,
        (
            "CREATE TABLE IF NOT EXISTS db_contract_l3 ("
            "db_contract_l3_id INTEGER PRIMARY KEY,"
            "db_contract_l3_scratch TEXT NOT NULL DEFAULT '',"
            "db_contract_l3_text TEXT,"
            "db_contract_l3_group TEXT,"
            "db_contract_l3_unique TEXT UNIQUE,"
            "db_contract_l3_blob BLOB,"
            "db_contract_l3_num REAL,"
            "db_contract_l3_bool INTEGER,"
            "db_contract_l3_created TEXT"
            ");"
        ),
    )

    # Driver caches may have memoized table lists/columns.
    try:
        open_db.driver.call_after_table_changes()
    except Exception:
        pass
    try:
        open_db.refresh_db_metadata()
    except Exception:
        # refresh_db_metadata is best-effort; some tests don't require it.
        pass

    return t


def _subset(payloads: Sequence[str], *, take: int) -> list[str]:
    # Deterministically sample from the start; the corpus is already curated.
    return list(payloads[:take])


def test_contract_table_visible_in_introspection(open_db, contract_table: ContractTable):
    tables = set(open_db.get_tables())
    assert contract_table.name in tables

    cols = open_db.get_column_headings(contract_table.name)
    assert contract_table.id_col in cols
    assert contract_table.scratch_col in cols
    assert contract_table.text_col in cols


def test_get_blank_row_creates_real_row(open_db, contract_table: ContractTable, assert_integrity):
    before = open_db.get_record_count(contract_table.name)

    row = open_db.get_blank_row(contract_table.name)
    assert row is not None
    assert row.table == contract_table.name
    assert row.row_id is not None
    assert contract_table.scratch_col in row
    assert contract_table.id_col in row

    after = open_db.get_record_count(contract_table.name)
    assert after == before + 1
    assert_integrity(open_db.driver)


@pytest.mark.parametrize(
    "payload",
    [
        # A stable, representative slice; NUL is handled in a dedicated test.
        "plain-ascii",
        "quotes 'single' and \"double\"",
        "emoji 😀🤖🧠",
        "combining e\u0301cole",
        "rtl עברית العربية",
        "cjk 漢字かなカナ",
        "zero-width \u200b\u200d join",
        "mixed ßøđ€ symbols",
        "with\nnewlines\r\nwindows",
        "semi;colon;party",
        "sql-comment -- not actually a comment",
        "c-style /* comment */ markers",
        "backticks `like` these",
        "path/like/thing/..//../",
        "x" * 4096,
    ],
)
def test_row_sync_roundtrips_text_payloads(open_db, contract_table: ContractTable, payload: str, assert_integrity):
    row = open_db.get_blank_row(contract_table.name)
    row[contract_table.text_col] = payload
    row[contract_table.group_col] = "grp"
    row.sync()

    got = open_db.get_row_from_id(contract_table.name, row.row_id)
    assert got is not None
    assert got[contract_table.text_col] == payload
    assert got[contract_table.group_col] == "grp"
    assert_integrity(open_db.driver)


def test_row_sync_nul_payload_is_handled_safely(open_db, contract_table: ContractTable, assert_integrity):
    """NUL bytes are a common SQLite edge case.

    Some stacks accept them, others reject them. Either is fine, but we must
    not corrupt the DB or leak half-written rows.
    """

    payload = "nul\x00byte\x00inside"
    before = open_db.get_record_count(contract_table.name)

    row = open_db.get_blank_row(contract_table.name)
    row[contract_table.text_col] = payload

    try:
        row.sync()
    except Exception:
        # Ensure we can safely clean up the blank row.
        try:
            open_db.delete(row)
        except Exception:
            pass

        assert open_db.get_record_count(contract_table.name) == before
        assert contract_table.name in set(open_db.get_tables())
        assert_integrity(open_db.driver)
        return

    # If accepted, it must roundtrip exactly.
    got = open_db.get_row_from_id(contract_table.name, row.row_id)
    assert got is not None
    assert got[contract_table.text_col] == payload
    assert open_db.get_record_count(contract_table.name) == before + 1
    assert_integrity(open_db.driver)


def test_search_finds_inserted_rows_for_multilingual_terms(
    open_db,
    contract_table: ContractTable,
    torture_strings: Sequence[str],
    assert_integrity,
):
    # Insert a small set and ensure each can be retrieved using Database.search().
    values = _subset(torture_strings, take=10)
    ids: list[int] = []
    for v in values:
        r = open_db.get_blank_row(contract_table.name)
        r[contract_table.text_col] = v
        r.sync()
        assert r.row_id is not None
        ids.append(int(r.row_id))

    for v in values:
        found = open_db.search(contract_table.name, contract_table.text_col, v)
        assert found, f"Expected to find payload via search: {v!r}"
        # At least one of the hits must match an inserted id.
        assert any(int(fr.row_id) in ids for fr in found if fr.row_id is not None)

    assert_integrity(open_db.driver)


@pytest.mark.parametrize(
    "payload",
    [
        "' OR '1'='1",
        "' OR 1=1 --",
        "\" OR \"1\"=\"1\" --",
        "'); DROP TABLE titles; --",
        "'); DROP TABLE creators; --",
        "'; ATTACH DATABASE ':memory:' AS evil; --",
        "'; PRAGMA foreign_keys=OFF; --",
        "'||(SELECT name FROM sqlite_master LIMIT 1)||'",
        "%'; UPDATE titles SET title='pwned' WHERE 1=1; --",
        '\"; VACUUM; --',
        "'); SELECT randomblob(1024); --",
    ],
)
def test_sql_injection_shaped_payloads_are_inert_data(
    open_db,
    contract_table: ContractTable,
    payload: str,
    assert_integrity,
):
    before_tables = set(open_db.get_tables())
    before_count = open_db.get_record_count(contract_table.name)

    r = open_db.get_blank_row(contract_table.name)
    r[contract_table.text_col] = payload
    r.sync()

    found = open_db.search(contract_table.name, contract_table.text_col, payload)
    assert found
    assert any(fr.row_id == r.row_id for fr in found)

    after_tables = set(open_db.get_tables())
    assert after_tables == before_tables
    assert open_db.get_record_count(contract_table.name) == before_count + 1
    assert_integrity(open_db.driver)


def test_get_values_set_and_iterator_agree(open_db, contract_table: ContractTable):
    values = ["alpha", "beta", "gamma", "alpha", None]
    for v in values:
        r = open_db.get_blank_row(contract_table.name)
        r[contract_table.group_col] = v
        r.sync()

    s = open_db.get_values_set(target_column=contract_table.group_col, iterator_return=False)
    it = set(open_db.get_values_set(target_column=contract_table.group_col, iterator_return=True))
    assert isinstance(s, set)
    assert s == it
    assert "alpha" in s
    assert "beta" in s
    assert "gamma" in s


def test_get_all_rows_list_and_iterator_surfaces(open_db, contract_table: ContractTable):
    # Seed a few rows.
    ids = []
    for i in range(15):
        r = open_db.get_blank_row(contract_table.name)
        r[contract_table.text_col] = f"row-{i}"
        r[contract_table.num_col] = float(i) / 10.0
        r.sync()
        ids.append(r.row_id)

    all_list = open_db.get_all_rows(contract_table.name, iterator_return=False)
    assert isinstance(all_list, list)
    assert any(rr.row_id in ids for rr in all_list)

    all_iter = list(open_db.get_all_rows(contract_table.name, iterator_return=True))
    assert all_iter
    assert any(rr.row_id in ids for rr in all_iter)


def test_get_random_row_returns_existing_id(open_db, contract_table: ContractTable):
    ids = set()
    for i in range(10):
        r = open_db.get_blank_row(contract_table.name)
        r[contract_table.text_col] = f"seed-{i}"
        r.sync()
        ids.add(r.row_id)

    # Probe a few times; not asserting distribution, only membership.
    for _ in range(5):
        rr = open_db.get_random_row(contract_table.name)
        assert rr is not None
        assert rr.row_id in ids
        assert rr.table == contract_table.name


def test_delete_removes_row_and_get_row_from_id_returns_none(open_db, contract_table: ContractTable):
    r = open_db.get_blank_row(contract_table.name)
    r[contract_table.text_col] = "to-delete"
    r.sync()
    rid = r.row_id
    assert rid is not None

    assert open_db.get_row_from_id(contract_table.name, rid) is not None

    open_db.delete(r)
    assert open_db.get_row_from_id(contract_table.name, rid) is None


def test_delete_errors_on_row_without_id(open_db):
    from LiuXin_alpha.databases.row import Row

    bad = Row(database=open_db, row_dict={})
    with pytest.raises(InputIntegrityError):
        open_db.delete(bad)


def test_dupe_row_success_allows_duplicate_when_unique_is_null(open_db, contract_table: ContractTable):
    base = open_db.get_blank_row(contract_table.name)
    base[contract_table.text_col] = "dupe-me"
    base[contract_table.unique_col] = None  # UNIQUE permits multiple NULLs
    base.sync()

    dup = open_db.dupe_row(base)
    assert dup is not None
    assert dup.row_id is not None
    assert dup.row_id != base.row_id
    assert dup[contract_table.text_col] == base[contract_table.text_col]
    assert dup[contract_table.unique_col] is None


def test_dupe_row_unique_violation_cleans_up_blank_row(open_db, contract_table: ContractTable):
    from LiuXin_alpha.errors import DatabaseIntegrityError

    base = open_db.get_blank_row(contract_table.name)
    base[contract_table.text_col] = "unique-base"
    base[contract_table.unique_col] = "only-once"
    base.sync()

    before = open_db.get_record_count(contract_table.name)

    with pytest.raises(DatabaseIntegrityError):
        open_db.dupe_row(base)

    after = open_db.get_record_count(contract_table.name)
    assert after == before


def test_chunk_iterator_groups_by_unique_values(open_db, contract_table: ContractTable):
    # Create rows with 3 distinct group values (including unicode).
    groups = ["A", "B", "漢字"]
    expected: dict[str, set[str]] = {g: set() for g in groups}

    for g in groups:
        for i in range(3):
            r = open_db.get_blank_row(contract_table.name)
            val = f"{g}-v{i}"
            r[contract_table.group_col] = g
            r[contract_table.text_col] = val
            r.sync()
            expected[g].add(val)

    chunks = list(open_db.chunk_iterator(column=contract_table.group_col, target_table=contract_table.name))
    assert chunks

    # Normalize into group -> set(text values)
    got: dict[str, set[str]] = {}
    for chunk in chunks:
        assert chunk
        g = chunk[0][contract_table.group_col]
        got.setdefault(g, set()).update({r[contract_table.text_col] for r in chunk})

    # Chunk order is unspecified.
    assert set(got.keys()) == set(expected.keys())
    for g in groups:
        assert got[g] == expected[g]


def test_row_hash_and_equality_use_db_uuid_and_id(open_db, contract_table: ContractTable):
    r1 = open_db.get_blank_row(contract_table.name)
    r1[contract_table.text_col] = "hash-me"
    r1.sync()

    r2 = open_db.get_row_from_id(contract_table.name, r1.row_id)
    assert r2 is not None

    assert r1 == r2
    assert hash(r1) == hash(r2)
    s = {r1}
    assert r2 in s


def test_row_setitem_rejects_column_from_other_table(open_db, contract_table: ContractTable):
    row = open_db.get_blank_row(contract_table.name)

    # Pick a column from *some other* table if possible.
    other_col = None
    cols_by_table = open_db.get_tables_and_columns()
    for t, cols in cols_by_table.items():
        if t == contract_table.name:
            continue
        for c in cols:
            if c not in row:
                other_col = c
                break
        if other_col is not None:
            break

    if other_col is None:
        pytest.skip("No other-table column available to validate Row.__setitem__ rejection")

    with pytest.raises(KeyError):
        row[other_col] = "nope"


def test_get_blank_row_errors_if_table_has_no_scratch_column(open_db):
    # Create a minimal table with an id but no scratch column.
    _exec_sql(open_db, "CREATE TABLE IF NOT EXISTS db_contract_l3_noscratch (db_contract_l3_noscratch_id INTEGER);")
    try:
        open_db.driver.call_after_table_changes()
    except Exception:
        pass

    with pytest.raises(DatabaseIntegrityError):
        open_db.get_blank_row("db_contract_l3_noscratch")
