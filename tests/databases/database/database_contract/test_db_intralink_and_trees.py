"""Database contract: intralinks + tree helpers (Chunk 10).

This chunk exercises the Database-level APIs for:

* Intralinks (self-links) within a single table.
* Tree helpers based on a *_parent column (root/children/linear walk).

The tests discover suitable tables dynamically from the provisioned test DB schema,
so they continue to work as the schema evolves.

Notes:
* We prefer writing payloads into each table's scratch column ("...scratch") because it
  exists on every table and is intended to tolerate arbitrary text.
* A couple of tests are marked xfail to capture known bugs / spec mismatches, so you
  get a hard signal during refactors without forcing the whole suite to red by default.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest


@dataclass(frozen=True)
class _IntralinkTarget:
    table: str
    link_table: str
    primary_col: str
    secondary_col: str
    type_col: str


def _pick_intralink_target(open_db) -> _IntralinkTarget:
    """Find a table that has an intralink table."""

    wrapper = open_db.driver_wrapper
    tables_and_cols = wrapper.get_tables_and_columns()
    for table in sorted(tables_and_cols.keys()):
        if table.startswith("sqlite_"):
            continue
        link_table = wrapper.check_for_intralink_table(table)
        if not link_table:
            continue
        if link_table not in tables_and_cols:
            continue

        # Derive intralink columns.
        try:
            primary_col = wrapper.get_intralink_column(table, "primary_id")
            secondary_col = wrapper.get_intralink_column(table, "secondary_id")
            type_col = wrapper.get_intralink_column(table, "type")
        except Exception:
            continue

        return _IntralinkTarget(
            table=table,
            link_table=link_table,
            primary_col=primary_col,
            secondary_col=secondary_col,
            type_col=type_col,
        )

    pytest.skip("No intralink-capable tables found in this test DB schema")


def _pick_tree_table(open_db) -> tuple[str, str]:
    """Return (table, parent_col) for any table with a *_parent column."""

    wrapper = open_db.driver_wrapper
    for table in sorted(wrapper.get_tables_and_columns().keys()):
        if table.startswith("sqlite_"):
            continue
        try:
            parent_col = wrapper.get_parent_column(table)
        except Exception:
            continue
        if parent_col:
            return table, parent_col
    pytest.skip("No *_parent tree tables found in this test DB schema")


def _scratch_col(open_db, table: str) -> str:
    return open_db.driver_wrapper.get_scratch_column(table)


def _make_row(open_db, table: str, payload: str, *, parent_col: str | None = None, parent_id=None):
    """Create + sync a row with scratch payload (and optional parent pointer)."""

    row = open_db.get_blank_row(table)
    row[_scratch_col(open_db, table)] = payload
    if parent_col is not None:
        row[parent_col] = parent_id
    row.sync()
    assert row.row_id is not None
    return row


def _pick_link_type(open_db, table: str) -> str:
    """Pick a link type that will satisfy preference restrictions if present."""

    key = f"allowed_{table}_intralink_types"
    prefs = getattr(open_db, "preferences", {})
    if isinstance(prefs, dict) and key in prefs:
        allowed = prefs[key]
        if allowed:
            # Database lower()s and strip()s link types.
            return str(allowed[0]).strip().lower()
    return "related"


# ---------------------------------------------------------------------------
# Intralinks
# ---------------------------------------------------------------------------


def test_intralink_target_discovery_is_consistent(open_db):
    t = _pick_intralink_target(open_db)
    # check_for_intralink_table and get_link_table_name should agree.
    assert open_db.driver_wrapper.get_link_table_name(t.table, t.table) == t.link_table
    assert t.link_table in open_db.driver_wrapper.get_tables()


def test_intralink_rows_requires_same_table(open_db, pick_payload):
    """Calling the intralink API with two different tables should error."""

    from LiuXin_alpha.errors import InputIntegrityError

    t = _pick_intralink_target(open_db)
    # Find any other main table to create a second row.
    other_table = None
    for cand in sorted(open_db.driver_wrapper.get_tables_and_columns().keys()):
        if cand.startswith("sqlite_"):
            continue
        if cand != t.table and cand in open_db.driver_wrapper.main_tables:
            other_table = cand
            break
    if other_table is None:
        pytest.skip("No second main table found to test cross-table intralink misuse")

    r1 = _make_row(open_db, t.table, pick_payload(3))
    r2 = _make_row(open_db, other_table, pick_payload(4))

    with pytest.raises(InputIntegrityError):
        open_db.intralink_rows(primary_row=r1, secondary_row=r2, link_type=_pick_link_type(open_db, t.table))


def test_intralink_rows_requires_ids(open_db, pick_payload):
    """Rows must have ids before they can be intralinked."""

    from LiuXin_alpha.errors import InputIntegrityError

    t = _pick_intralink_target(open_db)
    # Create rows but deliberately do not sync.
    r1 = open_db.get_blank_row(t.table)
    r2 = open_db.get_blank_row(t.table)
    r1[_scratch_col(open_db, t.table)] = pick_payload(1)
    r2[_scratch_col(open_db, t.table)] = pick_payload(2)
    assert r1.row_id is None
    assert r2.row_id is None

    with pytest.raises(InputIntegrityError):
        open_db.intralink_rows(primary_row=r1, secondary_row=r2, link_type=_pick_link_type(open_db, t.table))


@pytest.mark.parametrize("payload_ix", [0, 5, 9, 13, 21])
def test_intralink_rows_creates_link_row(open_db, pick_payload, payload_ix: int):
    t = _pick_intralink_target(open_db)
    r1 = _make_row(open_db, t.table, pick_payload(payload_ix))
    r2 = _make_row(open_db, t.table, pick_payload(payload_ix + 1))
    link_type = _pick_link_type(open_db, t.table)

    link_row = open_db.intralink_rows(primary_row=r1, secondary_row=r2, link_type=link_type)
    assert link_row.table == t.link_table
    assert link_row[t.primary_col] == r1.row_id
    assert link_row[t.secondary_col] == r2.row_id
    assert str(link_row[t.type_col]).strip().lower() == link_type


def test_get_intralink_row_none_when_absent(open_db, pick_payload):
    t = _pick_intralink_target(open_db)
    r1 = _make_row(open_db, t.table, pick_payload(7))
    r2 = _make_row(open_db, t.table, pick_payload(8))

    got = open_db.get_intralink_row(primary_row=r1, secondary_row=r2)
    assert got is None


def test_get_intralink_row_returns_row_when_present(open_db, pick_payload):
    t = _pick_intralink_target(open_db)
    r1 = _make_row(open_db, t.table, pick_payload(10))
    r2 = _make_row(open_db, t.table, pick_payload(11))
    open_db.intralink_rows(primary_row=r1, secondary_row=r2, link_type=_pick_link_type(open_db, t.table))

    got = open_db.get_intralink_row(primary_row=r1, secondary_row=r2)
    assert got is not None
    assert got.table == t.link_table
    assert got[t.primary_col] == r1.row_id
    assert got[t.secondary_col] == r2.row_id


def test_get_intralink_row_errors_if_multiple_links_between_pair(open_db, pick_payload):
    """If multiple intralink rows exist for the same pair, Database should complain."""

    from LiuXin_alpha.errors import DatabaseIntegrityError

    t = _pick_intralink_target(open_db)
    r1 = _make_row(open_db, t.table, pick_payload(12))
    r2 = _make_row(open_db, t.table, pick_payload(14))
    link_type = _pick_link_type(open_db, t.table)
    open_db.intralink_rows(primary_row=r1, secondary_row=r2, link_type=link_type)

    # Attempt to create a second link row with same ids but a different type.
    # Some schemas enforce uniqueness here; if so, we skip.
    dupe = open_db.get_blank_row(t.link_table)
    dupe[t.primary_col] = r1.row_id
    dupe[t.secondary_col] = r2.row_id
    dupe[t.type_col] = link_type + "_alt"
    try:
        dupe.sync()
    except Exception:
        pytest.skip("Schema enforces uniqueness for intralink pair; cannot create duplicate link rows")

    with pytest.raises(DatabaseIntegrityError):
        open_db.get_intralink_row(primary_row=r1, secondary_row=r2)


def test_get_intralink_rows_primary_secondary_filters(open_db, pick_payload):
    t = _pick_intralink_target(open_db)
    link_type = _pick_link_type(open_db, t.table)

    a = _make_row(open_db, t.table, pick_payload(1))
    b = _make_row(open_db, t.table, pick_payload(2))
    c = _make_row(open_db, t.table, pick_payload(3))

    open_db.intralink_rows(primary_row=a, secondary_row=b, link_type=link_type)
    open_db.intralink_rows(primary_row=a, secondary_row=c, link_type=link_type)
    open_db.intralink_rows(primary_row=b, secondary_row=a, link_type=link_type)

    only_primary = open_db.get_intralink_rows(row=a, primary=True, secondary=False)
    assert {r[t.secondary_col] for r in only_primary} == {b.row_id, c.row_id}

    only_secondary = open_db.get_intralink_rows(row=a, primary=False, secondary=True)
    assert {r[t.primary_col] for r in only_secondary} == {b.row_id}

    both = open_db.get_intralink_rows(row=a, primary=True, secondary=True)
    # Should be 3 link rows total.
    assert len(both) == 3


def test_get_intralink_rows_type_filter(open_db, pick_payload):
    t = _pick_intralink_target(open_db)
    a = _make_row(open_db, t.table, pick_payload(30))
    b = _make_row(open_db, t.table, pick_payload(31))
    c = _make_row(open_db, t.table, pick_payload(32))

    open_db.intralink_rows(primary_row=a, secondary_row=b, link_type="alpha")
    open_db.intralink_rows(primary_row=a, secondary_row=c, link_type="beta")

    filtered = open_db.get_intralink_rows(row=a, primary=True, secondary=True, link_type_filter="alpha")
    assert filtered
    assert all(str(r[t.type_col]).strip().lower() == "alpha" for r in filtered)


def test_get_intralinked_rows_argument_validation(open_db, pick_payload):
    from LiuXin_alpha.errors import InputIntegrityError

    t = _pick_intralink_target(open_db)
    a = _make_row(open_db, t.table, pick_payload(40))
    b = _make_row(open_db, t.table, pick_payload(41))

    with pytest.raises(InputIntegrityError):
        open_db.get_intralinked_rows(primary_row=a, secondary_row=b)
    with pytest.raises(InputIntegrityError):
        open_db.get_intralinked_rows(primary_row=None, secondary_row=None)


@pytest.mark.xfail(reason="Bug: get_intralinked_rows returns link rows, not intralinked rows")
def test_get_intralinked_rows_primary_returns_secondary_rows(open_db, pick_payload):
    """Spec intent: when primary_row is set, return the secondary rows linked from it."""

    t = _pick_intralink_target(open_db)
    a = _make_row(open_db, t.table, pick_payload(50))
    b = _make_row(open_db, t.table, pick_payload(51))

    open_db.intralink_rows(primary_row=a, secondary_row=b, link_type=_pick_link_type(open_db, t.table))

    got = open_db.get_intralinked_rows(primary_row=a, secondary_row=None)
    assert got and all(r.table == t.table for r in got)
    assert {r.row_id for r in got} == {b.row_id}


def test_unlinked_intralink_pair_deletes_link(open_db, pick_payload):
    t = _pick_intralink_target(open_db)
    a = _make_row(open_db, t.table, pick_payload(60))
    b = _make_row(open_db, t.table, pick_payload(61))

    open_db.intralink_rows(primary_row=a, secondary_row=b, link_type=_pick_link_type(open_db, t.table))
    assert open_db.get_intralink_row(primary_row=a, secondary_row=b) is not None

    open_db.unlinked_intralink(primary_row=a, secondary_row=b)
    assert open_db.get_intralink_row(primary_row=a, secondary_row=b) is None


def test_unlinked_intralink_by_primary_deletes_all(open_db, pick_payload):
    t = _pick_intralink_target(open_db)
    a = _make_row(open_db, t.table, pick_payload(70))
    b = _make_row(open_db, t.table, pick_payload(71))
    c = _make_row(open_db, t.table, pick_payload(72))
    link_type = _pick_link_type(open_db, t.table)

    open_db.intralink_rows(primary_row=a, secondary_row=b, link_type=link_type)
    open_db.intralink_rows(primary_row=a, secondary_row=c, link_type=link_type)
    assert len(open_db.get_intralink_rows(row=a, primary=True, secondary=False)) == 2

    open_db.unlinked_intralink(primary_row=a, secondary_row=None)
    assert len(open_db.get_intralink_rows(row=a, primary=True, secondary=False)) == 0


@pytest.mark.xfail(reason="Bug: unlinked_intralink uses primary_row.table even when primary_row is None")
def test_unlinked_intralink_by_secondary_deletes_all(open_db, pick_payload):
    t = _pick_intralink_target(open_db)
    a = _make_row(open_db, t.table, pick_payload(80))
    b = _make_row(open_db, t.table, pick_payload(81))
    c = _make_row(open_db, t.table, pick_payload(82))
    link_type = _pick_link_type(open_db, t.table)

    open_db.intralink_rows(primary_row=a, secondary_row=b, link_type=link_type)
    open_db.intralink_rows(primary_row=c, secondary_row=b, link_type=link_type)
    assert len(open_db.get_intralink_rows(row=b, primary=False, secondary=True)) == 2

    open_db.unlinked_intralink(primary_row=None, secondary_row=b)
    assert len(open_db.get_intralink_rows(row=b, primary=False, secondary=True)) == 0


# ---------------------------------------------------------------------------
# Tree helpers
# ---------------------------------------------------------------------------


def test_get_parent_column_returns_false_on_non_tree_table(open_db):
    wrapper = open_db.driver_wrapper
    for table in sorted(wrapper.get_tables_and_columns().keys()):
        if table.startswith("sqlite_"):
            continue
        try:
            parent_col = wrapper.get_parent_column(table)
        except Exception:
            continue
        if not parent_col:
            # Found a non-tree table.
            assert parent_col is False
            return
    pytest.skip("No non-tree tables found; cannot exercise get_parent_column False path")


@pytest.mark.parametrize("payload_ix", [0, 6, 12])
def test_tree_root_children_and_linear_list(open_db, pick_payload, payload_ix: int):
    table, parent_col = _pick_tree_table(open_db)

    root = _make_row(open_db, table, pick_payload(payload_ix), parent_col=parent_col, parent_id=None)
    child = _make_row(open_db, table, pick_payload(payload_ix + 1), parent_col=parent_col, parent_id=root.row_id)
    grandchild = _make_row(
        open_db, table, pick_payload(payload_ix + 2), parent_col=parent_col, parent_id=child.row_id
    )

    # Children: root should have 1, leaf should have 0.
    root_children = open_db.get_children(root)
    assert {r.row_id for r in root_children} == {child.row_id}
    assert open_db.get_children(grandchild) == []

    # Root lookup.
    got_root = open_db.get_root_row(grandchild)
    assert got_root.row_id == root.row_id

    # Linear list ordering.
    chain = open_db.get_linear_row_list(grandchild)
    assert [r.row_id for r in chain] == [root.row_id, child.row_id, grandchild.row_id]


def test_tree_linear_list_root_is_trivial(open_db, pick_payload):
    table, parent_col = _pick_tree_table(open_db)
    root = _make_row(open_db, table, pick_payload(90), parent_col=parent_col, parent_id=None)
    chain = open_db.get_linear_row_list(root)
    assert [r.row_id for r in chain] == [root.row_id]
