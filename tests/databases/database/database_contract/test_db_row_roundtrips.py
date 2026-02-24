"""Database contract: Row round-trips + identity semantics (chunk 05).

This slice focuses on the core Row workflow exposed by
:class:`~LiuXin_alpha.databases.database.Database`:

* Creating writable rows via Database.get_blank_row().
* Updating via Row.__setitem__ + Row.sync().
* Reading back via Database.get_row_from_id().
* Deleting via Database.delete().
* Duplicating via Database.dupe_row() (including the cleanup path).
* Row identity semantics: hash/equality/deepcopy and read-only mode.

These tests intentionally exercise *real* schema tables (e.g. titles/creators)
to validate the conventions used by DriverWrapper (id/scratch/base columns).
"""

from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Iterable

import pytest

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.errors import (
    DatabaseIntegrityError,
    InputIntegrityError,
    RowReadOnlyError,
)


@dataclass(frozen=True)
class TableShape:
    table: str
    id_col: str
    scratch_col: str
    base_col: str
    text_col: str


PREFERRED_MAIN_TABLES: tuple[str, ...] = (
    # Strongly expected to exist in test_db_13 (see TestDB13Properties).
    "titles",
    "creators",
    "series",
    "publishers",
    "tags",
    "subjects",
    "genres",
    # NOTE: `languages` is a locked constant table in the FRBR schema.
    # Useful fallbacks if schema evolves:
    "notes",
    "comments",
    "synopses",
    "feeds",
    "folders",
    "files",
)



def _pick_text_like_column(cols: Iterable[str], *, base: str, exclude: set[str]) -> str:
    """Pick a column suitable for stuffing an arbitrary unicode payload.

    Contract tests need to be able to create "distinct" rows in arbitrary tables.
    Some tables begin with FK/id columns (e.g. folder_store_id), so a naive "first
    non-excluded" choice will violate foreign keys when we write text into it.

    Heuristics:
    - never pick *_id / *_fk columns unless there is no alternative
    - avoid timestamp-ish columns
    - prefer name/title/text/payload/comment/json/path/value-like columns
    """
    cols_list = [c for c in cols if c not in exclude]
    if not cols_list:
        # Fall back to whatever we were given.
        return list(cols)[0]

    def is_id_like(c: str) -> bool:
        cl = c.lower()
        return cl.endswith('_id') or cl.endswith('_fk') or cl == 'id'

    def is_time_like(c: str) -> bool:
        cl = c.lower()
        return (
            'timestamp' in cl
            or 'datestamp' in cl
            or cl.endswith('_ep_k')
            or cl.endswith('_epoch')
            or cl.endswith('_epoch_ms')
        )

    keywords = (
        'payload', 'name', 'title', 'text', 'comment', 'note', 'label', 'key', 'path', 'relpath', 'json', 'value'
    )

    candidates = [c for c in cols_list if not is_id_like(c) and not is_time_like(c)]

    for kw in keywords:
        for c in candidates:
            if kw in c.lower():
                return c

    for suf in ('name', 'title', 'text', 'payload', 'value'):
        cand = f"{base}_{suf}"
        if cand in candidates:
            return cand

    if candidates:
        return candidates[0]

    non_id = [c for c in cols_list if not is_id_like(c)]
    if non_id:
        return non_id[0]

    return cols_list[0]


def _shape_for_table(db, table: str) -> TableShape:
    id_col = db.driver_wrapper.get_id_column(table)
    scratch_col = db.driver_wrapper.get_scratch_column(table)
    base_col = db.driver_wrapper.get_column_base(table)

    cols = db.get_column_headings(table)
    exclude = {id_col, scratch_col}
    # Common 'maintenance' columns, if present:
    exclude.add(f"{base_col}_datestamp")
    exclude.add(f"{base_col}_phash")

    text_col = _pick_text_like_column(cols, base=base_col, exclude=exclude)
    return TableShape(table=table, id_col=id_col, scratch_col=scratch_col, base_col=base_col, text_col=text_col)


@pytest.fixture(scope="session")
def _table_names_expected() -> set[str]:
    # Keep in sync with TestDB13Properties.theo_main_tables (but this is a soft expectation).
    return {
        "files",
        "publishers",
        "genres",
        "custom_columns",
        "folder_stores",
        "covers",
        "tags",
        "series",
        "notes",
        "identifiers",
        "devices",
        "folders",
        "languages",
        "last_read_positions",
        "books",
        "comments",
        "synopses",
        "titles",
        "feeds",
        "creators",
        "subjects",
    }


@pytest.fixture
def primary_table_shape(open_db, _table_names_expected) -> TableShape:
    """Select a stable main table to run row-roundtrip tests against."""
    tables = set(open_db.get_tables())

    # Soft sanity: if these disappear, schema changed dramatically.
    missing = sorted(list(_table_names_expected - tables))
    if len(missing) > 10:
        pytest.fail(f"Unexpected schema: too many expected main tables missing: {missing!r}")

    for name in PREFERRED_MAIN_TABLES:
        if name in tables and not open_db.driver_wrapper.is_view(name):
            return _shape_for_table(open_db, name)

    # Fall back: pick any non-view relation so row-roundtrip tests can write.
    non_views = [t for t in sorted(tables) if not open_db.driver_wrapper.is_view(t)]
    if non_views:
        return _shape_for_table(open_db, non_views[0])

    pytest.fail(f"No writable table found (all relations appear to be views). Have: {sorted(tables)!r}")


@pytest.fixture
def secondary_table_shape(open_db) -> TableShape:
    """Pick a second table (different from the primary) to avoid single-table blind spots."""
    tables = list(open_db.get_tables())
    for name in ("creators", "series", "publishers", "tags", "subjects", "genres"):
        if name in tables:
            return _shape_for_table(open_db, name)
    return _shape_for_table(open_db, tables[0])


# ---------------------------------------------------------------------------
# get_blank_row() + get_row_from_id() + delete()
# ---------------------------------------------------------------------------


def test_get_blank_row_returns_row_with_table_and_id(open_db, primary_table_shape: TableShape):
    t = primary_table_shape
    row = open_db.get_blank_row(t.table)

    assert isinstance(row, Row)
    assert row.table == t.table
    assert row.row_id is not None

    # The id column should exist and match row_id
    assert t.id_col in row.row_dict
    assert row[t.id_col] == row.row_id

    # The scratch column is used internally to create the blank row; it should be cleared.
    assert t.scratch_col in row.row_dict
    assert row[t.scratch_col] in ("", None)

    reread = open_db.get_row_from_id(t.table, row.row_id)
    assert reread is not None
    assert isinstance(reread, Row)
    assert reread.row_id == row.row_id
    assert reread.table == t.table


def test_get_blank_row_ids_are_distinct(open_db, primary_table_shape: TableShape):
    t = primary_table_shape
    r1 = open_db.get_blank_row(t.table)
    r2 = open_db.get_blank_row(t.table)

    assert r1.row_id != r2.row_id
    assert r1[t.id_col] != r2[t.id_col]


def test_delete_requires_row_id(open_db, primary_table_shape: TableShape, pick_payload):
    t = primary_table_shape
    payload = pick_payload(3)

    # Build a row that can be identified to a table, but do not sync/insert it.
    row = Row(database=open_db, row_dict={t.text_col: payload})
    assert row.row_id is None

    with pytest.raises(InputIntegrityError):
        open_db.delete(row)


def test_delete_removes_row_from_database(open_db, primary_table_shape: TableShape, pick_payload):
    t = primary_table_shape
    payload = pick_payload(10)  # avoid embedded NUL

    row = open_db.get_blank_row(t.table)
    row[t.text_col] = payload
    row.sync()

    rid = row.row_id
    assert rid is not None

    before = open_db.get_record_count(t.table)
    open_db.delete(row)
    after = open_db.get_record_count(t.table)

    assert after == before - 1
    assert open_db.get_row_from_id(t.table, rid) is None


def test_get_row_from_id_missing_returns_none(open_db, primary_table_shape: TableShape):
    t = primary_table_shape
    # Choose an ID far above the current high-water mark.
    hi = open_db.driver_wrapper.get_highest_id(t.table) or 0
    missing = int(hi) + 100_000

    assert open_db.get_row_from_id(t.table, missing) is None


# ---------------------------------------------------------------------------
# Row.sync() round-trips (including the "no-id" path)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("payload_i", [0, 1, 2, 3, 4, 10, 11, 12, 13, 14, 15, 16])
def test_blank_row_update_roundtrips_unicode_payloads(
    open_db,
    primary_table_shape: TableShape,
    pick_payload,
    payload_i: int,
):
    t = primary_table_shape
    payload = pick_payload(payload_i)

    # Some backends or drivers treat NUL badly; skip that payload explicitly.
    if "\x00" in payload:
        pytest.skip("Payload contains embedded NUL")

    row = open_db.get_blank_row(t.table)
    row[t.text_col] = payload
    row.sync()

    reread = open_db.get_row_from_id(t.table, row.row_id)
    assert reread is not None
    assert reread[t.text_col] == payload


def test_row_sync_inserts_when_missing_id(open_db, primary_table_shape: TableShape, pick_payload):
    t = primary_table_shape
    payload = pick_payload(12)  # rtl/hebrew/arabic

    row = Row(database=open_db, row_dict={t.text_col: payload})
    assert row.row_id is None

    row.sync()
    assert row.row_id is not None
    assert row.table == t.table

    reread = open_db.get_row_from_id(t.table, row.row_id)
    assert reread is not None
    assert reread[t.text_col] == payload


def test_row_properties_refresh_for_empty_row_dict(open_db):
    row = Row(database=open_db, row_dict={})
    assert row.row_id is None
    assert row.table is None
    assert row.allowed_tables is not None
    assert isinstance(row.allowed_tables, set)


def test_row_setitem_on_empty_row_dict_infers_table(open_db, primary_table_shape: TableShape, pick_payload):
    t = primary_table_shape
    payload = pick_payload(0)

    row = Row(database=open_db, row_dict={})
    assert row.table is None

    row[t.text_col] = payload
    assert row.table == t.table


def test_row_make_read_only_disallows_sync(open_db, primary_table_shape: TableShape, pick_payload):
    t = primary_table_shape
    payload = pick_payload(1)

    row = open_db.get_blank_row(t.table)
    row[t.text_col] = payload
    row.make_read_only()

    with pytest.raises(RowReadOnlyError):
        row.sync()


# ---------------------------------------------------------------------------
# dupe_row() semantics (success OR cleanup on unique-constraint failure)
# ---------------------------------------------------------------------------


def test_dupe_row_creates_copy_or_cleans_up(open_db, secondary_table_shape: TableShape, pick_payload):
    t = secondary_table_shape
    payload = pick_payload(4)

    # Create a row that is as "normal" as possible.
    row = open_db.get_blank_row(t.table)
    row[t.text_col] = payload
    row.sync()

    rid = row.row_id
    assert rid is not None

    count_before = open_db.get_record_count(t.table)

    try:
        dupe = open_db.dupe_row(row)
    except DatabaseIntegrityError:
        # Unique constraint (or similar) prevented duplication.
        # The helper should have cleaned up the newly-created blank row.
        assert open_db.get_record_count(t.table) == count_before
        assert open_db.get_row_from_id(t.table, rid) is not None
        return

    assert dupe is not None
    assert isinstance(dupe, Row)
    assert dupe.table == row.table
    assert dupe.row_id is not None
    assert dupe.row_id != rid
    assert open_db.get_record_count(t.table) == count_before + 1

    reread_dupe = open_db.get_row_from_id(t.table, dupe.row_id)
    assert reread_dupe is not None
    assert reread_dupe[t.text_col] == payload


def test_dupe_row_does_not_mutate_original(open_db, secondary_table_shape: TableShape, pick_payload):
    t = secondary_table_shape
    payload = pick_payload(13)  # cjk

    row = open_db.get_blank_row(t.table)
    row[t.text_col] = payload
    row.sync()

    orig = copy.deepcopy(row.row_dict)

    try:
        _ = open_db.dupe_row(row)
    except DatabaseIntegrityError:
        pass

    assert row.row_dict == orig


# ---------------------------------------------------------------------------
# Row identity semantics: equality/hash/deepcopy
# ---------------------------------------------------------------------------


def test_rows_with_same_db_table_id_compare_equal(open_db, primary_table_shape: TableShape, pick_payload):
    t = primary_table_shape
    payload = pick_payload(2)

    row = open_db.get_blank_row(t.table)
    row[t.text_col] = payload
    row.sync()

    reread = open_db.get_row_from_id(t.table, row.row_id)
    assert reread is not None

    assert row == reread
    assert hash(row) == hash(reread)

    # But their row_dict instances are independent snapshots.
    assert row.row_dict is not reread.row_dict


def test_row_can_be_dict_key(open_db, primary_table_shape: TableShape, pick_payload):
    t = primary_table_shape
    payload = pick_payload(15)  # mixed symbols / zero-width joiners etc

    row = open_db.get_blank_row(t.table)
    row[t.text_col] = payload
    row.sync()

    reread = open_db.get_row_from_id(t.table, row.row_id)
    assert reread is not None

    d = {row: "a"}
    assert d[reread] == "a"


def test_row_deepcopy_preserves_identity(open_db, primary_table_shape: TableShape, pick_payload):
    t = primary_table_shape
    payload = pick_payload(3)

    row = open_db.get_blank_row(t.table)
    row[t.text_col] = payload
    row.sync()

    row_copy = copy.deepcopy(row)
    assert isinstance(row_copy, Row)
    assert row_copy.table == row.table
    assert row_copy.row_id == row.row_id
    assert row_copy == row

    # Mutating the deepcopy should mutate the underlying DB row (same id).
    new_payload = pick_payload(4)
    row_copy[t.text_col] = new_payload
    row_copy.sync()

    reread = open_db.get_row_from_id(t.table, row.row_id)
    assert reread is not None
    assert reread[t.text_col] == new_payload


def test_row_contains_and_getitem_semantics(open_db, primary_table_shape: TableShape, pick_payload):
    t = primary_table_shape
    payload = pick_payload(0)

    row = open_db.get_blank_row(t.table)
    assert t.text_col in row  # should already be present or materializable via __getitem__
    row[t.text_col] = payload
    row.sync()

    reread = open_db.get_row_from_id(t.table, row.row_id)
    assert reread is not None
    assert t.text_col in reread
    assert reread[t.text_col] == payload
