"""
Check Database exact searches, distinct values, grouped iteration, and random-row behavior.

Dedicated per-test tables use hashed column names to reduce table-inference
ambiguity. Short-lived SQL connections have best-effort commit/close handling.
Strict xfail cases retain desired NULL search and grouping behavior.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database/database_contract/test_db_search_and_sets.py
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
    """
    Hold immutable table and column names for one search/set contract fixture.

    Example:
        >>> table = ContractTable('sample', 'id', 'scratch', 'text', 'group', 'num')
        >>> table.text_col
        'text'
    """
    name: str
    id_col: str
    scratch_col: str
    text_col: str
    group_col: str
    num_col: str


def _stable_suffix(nodeid: str) -> str:
    # Deterministic across runs (unlike hash()).
    """
    Hash a strict UTF-8 node ID to a ten-character SHA-1 suffix.

    Example:
        >>> _stable_suffix('abc')
        'a9993e3647'


    :param nodeid: Pytest node ID used to distinguish test objects.
    :return: First ten lowercase hexadecimal digest characters; encoding errors
        propagate.
    """
    return hashlib.sha1(nodeid.encode("utf-8")).hexdigest()[:10]


def _exec_sql(db, stmt: str, bindings: tuple | None = None) -> None:
    """
    Execute a statement on a fresh driver connection and attempt to commit.

    Require get_connection. If commit fails, try SQL COMMIT and suppress its ordinary
    errors. Always attempt close, suppressing ordinary close errors; execution errors
    propagate.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_search_and_sets.py


    :param db: Caller-owned Database used for schema inspection or SQL operations.
    :param stmt: SQL statement executed on a fresh driver connection.
    :param bindings: Optional bound parameters; None calls execute without a bindings
        argument.
    :return: None; successful return does not independently establish commit success.
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
    """
    Execute a query on a fresh driver connection, materialize its rows, and attempt close in finally.

    Require get_connection; query errors propagate and ordinary close errors are
    suppressed.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_search_and_sets.py


    :param db: Caller-owned Database used for schema inspection or SQL operations.
    :param stmt: SQL statement executed on a fresh driver connection.
    :param bindings: Optional bound parameters; None calls execute without a bindings
        argument.
    :return: List of fetched rows; no explicit commit occurs.
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
        return list(cur.fetchall())
    finally:
        try:
            conn.close()
        except Exception:
            pass


@pytest.fixture
def contract_table(open_db, request) -> ContractTable:
    """
    Create a node-specific contract table and attempt driver and Database metadata refreshes.

    Suppress ordinary refresh errors. The database fixture owns the table’s lifetime;
    this fixture performs no explicit drop.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_search_and_sets.py


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :param request: Pytest request whose node ID supplies the naming suffix.
    :return: ContractTable with a generic id column and hashed scratch/text/group/number
        names.
    """
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
    """
    Insert each scratch/text/group/number tuple through a separate fresh-connection SQL call.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_search_and_sets.py


    :param db: Caller-owned Database used for schema inspection or SQL operations.
    :param table: ContractTable descriptor supplying trusted SQL identifiers.
    :param rows: Ordered sequence of four-value tuples, bound separately for each
        insert.
    :return: None; earlier inserts can remain if a later insert fails.
    """
    for scratch, txt, grp, num in rows:
        _exec_sql(
            db,
            f"INSERT INTO {table.name} ({table.scratch_col}, {table.text_col}, {table.group_col}, {table.num_col}) "
            f"VALUES (?, ?, ?, ?);",
            (scratch, txt, grp, num),
        )


def _all_row_ids(db, table: ContractTable) -> set[int]:
    """
    Fetch every ID from the dedicated table and convert the values to integers.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_search_and_sets.py


    :param db: Caller-owned Database used for schema inspection or SQL operations.
    :param table: ContractTable descriptor supplying trusted table and ID-column names.
    :return: Set of persisted integer IDs.
    """
    got = _fetch_all(db, f"SELECT {table.id_col} FROM {table.name};")
    return {int(r[0]) for r in got}


def _find_id_by_scratch(db, table: ContractTable, scratch: str) -> int:
    """
    Query the scratch key and require a non-None ID in the first result.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_search_and_sets.py


    :param db: Caller-owned Database used for schema inspection or SQL operations.
    :param table: ContractTable descriptor supplying trusted SQL identifiers.
    :param scratch: Exact scratch key passed as a bound value.
    :return: First matching ID converted to int; missing or malformed results raise
        AssertionError.
    """
    got = _fetch_all(
        db,
        f"SELECT {table.id_col} FROM {table.name} WHERE {table.scratch_col} = ?;",
        (scratch,),
    )
    assert got and got[0] and got[0][0] is not None
    return int(got[0][0])


def _pick_non_nul_payloads(pick_payload, n: int = 32) -> list[str]:
    """
    Collect the first n indexed corpus values without embedded NUL, preserving duplicates and order.

    Require at least one retained value; nonpositive n therefore raises AssertionError.

    Example:
        >>> _pick_non_nul_payloads(lambda i: ['a', chr(0), 'b'][i], n=3)
        ['a', 'b']


    :param pick_payload: Fixture callable selecting a Unicode corpus payload by index.
    :param n: Number of payload indices to inspect.
    :return: Nonempty list of retained strings.
    """
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
    """
    Check searching a seeded payload returns exactly its Row and that a three-character prefix finds nothing when the payload is long enough.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_search_and_sets.py::test_search_returns_rows_and_is_exact_match


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :param contract_table: Fixture descriptor of the test-specific table and its
        columns.
    :param pick_payload: Fixture callable selecting a Unicode corpus payload by index.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Seed alpha and check searching beta returns an empty list.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_search_and_sets.py::test_search_empty_on_no_match


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :param contract_table: Fixture descriptor of the test-specific table and its
        columns.
    :return: None; failed expectations raise AssertionError.
    """
    _insert_rows(open_db, contract_table, [("s7_x", "alpha", "G0", 1)])
    got = open_db.search(table=contract_table.name, column=contract_table.text_col, search_term="beta")
    assert got == []


def test_search_non_string_terms_do_not_crash(open_db, contract_table: ContractTable):
    """
    Search with bytes, numbers, a boolean, a mapping, and an object, requiring lists containing only Rows.

    No particular match count or coercion result is asserted.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_search_and_sets.py::test_search_non_string_terms_do_not_crash


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :param contract_table: Fixture descriptor of the test-specific table and its
        columns.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Check searching a nonexistent table raises InputIntegrityError.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_search_and_sets.py::test_search_invalid_table_raises


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :return: None; failed expectations raise AssertionError.
    """
    with pytest.raises(InputIntegrityError):
        open_db.search(table="definitely_not_a_table", column="nope", search_term="x")


def test_search_invalid_column_raises(open_db, contract_table: ContractTable):
    """
    Check searching an unknown column of an existing table raises InputIntegrityError.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_search_and_sets.py::test_search_invalid_column_raises


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :param contract_table: Fixture descriptor of the test-specific table and its
        columns.
    :return: None; failed expectations raise AssertionError.
    """
    _insert_rows(open_db, contract_table, [("s7_x", "alpha", "G0", 1)])
    with pytest.raises(InputIntegrityError):
        open_db.search(table=contract_table.name, column="definitely_not_a_column", search_term="alpha")


@pytest.mark.xfail(strict=True, reason="Current driver uses '=' so NULL is not matched; consider 'IS NULL' semantics.")
def test_search_none_should_match_null_rows_desired_behavior(open_db, contract_table: ContractTable):
    """
    Express a one-row NULL search result under the existing strict xfail marker.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_search_and_sets.py::test_search_none_should_match_null_rows_desired_behavior


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :param contract_table: Fixture descriptor of the test-specific table and its
        columns.
    :return: None; failed expectations raise AssertionError.
    """
    _insert_rows(open_db, contract_table, [("s7_n", None, "G0", 1)])
    got = open_db.search(table=contract_table.name, column=contract_table.text_col, search_term=None)
    assert len(got) == 1


# ------------------------------------------------------------------------------
# Database.get_values_set
# ------------------------------------------------------------------------------


def test_get_values_set_returns_unique_values(open_db, contract_table: ContractTable):
    """
    Check set retrieval deduplicates group values while retaining None.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_search_and_sets.py::test_get_values_set_returns_unique_values


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :param contract_table: Fixture descriptor of the test-specific table and its
        columns.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Materialize the distinct-value iterator and check it equals the set-returning form.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_search_and_sets.py::test_get_values_set_iterator_matches_set


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :param contract_table: Fixture descriptor of the test-specific table and its
        columns.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Check distinct-value retrieval for an unknown column raises InputIntegrityError.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_search_and_sets.py::test_get_values_set_unknown_column_raises


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :return: None; failed expectations raise AssertionError.
    """
    with pytest.raises(InputIntegrityError):
        open_db.get_values_set(target_column="column_that_does_not_exist_anywhere", iterator_return=False)


# ------------------------------------------------------------------------------
# Database.chunk_iterator
# ------------------------------------------------------------------------------


def test_chunk_iterator_groups_rows_by_column(open_db, contract_table: ContractTable):
    # Avoid NULL group values here; see xfail test below.
    """
    Check three non-NULL groups produce nonempty Row lists with uniform group values and complete ID coverage.

    Assert the chunk count but do not require a particular chunk order.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_search_and_sets.py::test_chunk_iterator_groups_rows_by_column


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :param contract_table: Fixture descriptor of the test-specific table and its
        columns.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Express the desired presence of a two-row NULL group under the existing strict xfail marker.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_search_and_sets.py::test_chunk_iterator_should_group_null_values_desired_behavior


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :param contract_table: Fixture descriptor of the test-specific table and its
        columns.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Consume the chunk iterator and check an unknown column raises InputIntegrityError.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_search_and_sets.py::test_chunk_iterator_unknown_column_raises


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :return: None; failed expectations raise AssertionError.
    """
    with pytest.raises(InputIntegrityError):
        list(open_db.chunk_iterator(column="definitely_not_a_column", target_table=None))


# ------------------------------------------------------------------------------
# Database.get_random_row
# ------------------------------------------------------------------------------


def test_get_random_row_returns_existing_row(open_db, contract_table: ContractTable):
    """
    Request twenty-five random Rows and check each belongs to the seeded table and its known ID set.

    The test does not measure distribution or require every seeded ID to appear.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_search_and_sets.py::test_get_random_row_returns_existing_row


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :param contract_table: Fixture descriptor of the test-specific table and its
        columns.
    :return: None; failed expectations raise AssertionError.
    """
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
    # No inserts. Current driver returns an empty Row shell with no resolved table.
    """
    Check an empty-table random lookup returns an unresolved Row shell whose row_id access raises InputIntegrityError.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_search_and_sets.py::test_get_random_row_empty_table_current_behavior


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :param contract_table: Fixture descriptor of the test-specific table and its
        columns.
    :return: None; failed expectations raise AssertionError.
    """
    row = open_db.get_random_row(table=contract_table.name)
    assert isinstance(row, Row)
    assert row.table is None
    with pytest.raises(InputIntegrityError):
        _ = row.row_id


def test_get_random_row_invalid_table_raises(open_db):
    """
    Check a random-row lookup for a nonexistent table raises InputIntegrityError.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_search_and_sets.py::test_get_random_row_invalid_table_raises


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :return: None; failed expectations raise AssertionError.
    """
    with pytest.raises(InputIntegrityError):
        open_db.get_random_row(table="definitely_not_a_table")
