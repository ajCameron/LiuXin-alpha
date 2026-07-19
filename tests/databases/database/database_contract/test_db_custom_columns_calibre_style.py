"""Database contract: Calibre-style custom columns (custom_column_N tables).

This suite targets the Database-level custom column APIs implemented via:

* ``CustomColumnsDriverWrapperMixin`` (create/update/mark-delete) accessed through
  ``Database.driver_wrapper``.
* ``CustomColumnDatabaseMixin.get_interlinked_rows_cc`` on ``Database``.

These tests deliberately use unicode + SQL-injection-shaped *data* payloads to ensure
parameter binding is consistently used and that dangerous-looking strings remain inert.

Notes
-----
The driver wrapper's Calibre-style custom columns create tables named:

* ``custom_column_<id>``
* ``<in_table>_custom_column_<id>_link`` (only for normalized types)

The Database keeps its own ``Database.custom_tables`` cache, which is refreshed via
``Database.refresh_db_metadata()``. The contract below therefore refreshes metadata
after creating columns.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pytest

from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError
from LiuXin_alpha.utils.language_tools.pluralizers import plural_singular_mapper


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _require_table(db, table: str) -> None:
    if table not in db.get_tables(force_refresh=True):
        pytest.skip(f"Table {table!r} not present in provisioned contract DB")


def _pick_alt_main_table(db, *, exclude: set[str]) -> str | None:
    """Pick a usable non-excluded main table that supports get_blank_row (has a scratch column).

    We deliberately avoid views: contract tests should only write to real tables.
    """

    for t in sorted(db.main_tables):
        if t in exclude:
            continue
        if not _is_insertable_table(db, t):
            continue
        try:
            _ = db.driver_wrapper.get_scratch_column(t)
        except Exception:
            continue
        return t
    return None



def _sqlite_master_type(db, name: str) -> str | None:
    """Return sqlite_master.type for name (table/view/index/trigger) if available."""
    try:
        cur = db.driver_wrapper.execute("SELECT type FROM sqlite_master WHERE name=?;", (name,))
        row = cur.fetchone() if hasattr(cur, "fetchone") else (next(cur, None) if cur is not None else None)
        return str(row[0]) if row else None
    except Exception:
        return None


def _is_insertable_table(db, name: str) -> bool:
    """True if `name` is a concrete table (not a view) in SQLite."""
    t = _sqlite_master_type(db, name)
    if t is None:
        # Unknown backend or table not found; be conservative and allow callers to handle.
        return False
    return t == "table"

def _cc_default_table(db) -> str:
    """Pick the default main table for Calibre-style custom columns.

    Historically this was 'books'. In the FRBR/WEMI schema the closest analogue is
    usually 'manifestations'. We therefore prefer insertable FRBR tables and only
    fall back to 'books' if it is a *real* table (not a view).
    """

    for cand in ("manifestations", "items", "works", "books"):
        if cand not in db.get_tables(force_refresh=True):
            continue
        if not _is_insertable_table(db, cand):
            continue
        try:
            _ = db.driver_wrapper.get_scratch_column(cand)
        except Exception:
            continue
        return cand

    alt = _pick_alt_main_table(db, exclude={"custom_columns"})
    if alt is None:
        pytest.skip("No suitable main table available for custom column contract tests")
    return alt


def _cc_canonical_table(db, table: str) -> str:
    """Canonicalise Calibre-era table choices to an insertable FRBR/WEMI analogue."""

    if table == "books":
        existing = set(db.get_tables(force_refresh=True))
        if table not in existing:
            return _cc_default_table(db)
        if not _is_insertable_table(db, table):
            return _cc_default_table(db)
    return table




def _create_target_row(db, table: str):
    """Create a blank Row in a table (requires that table has a scratch column)."""

    table = _cc_canonical_table(db, table)
    _require_table(db, table)
    row = db.get_blank_row(table)
    assert row.row_id is not None
    return row


def _headings(db, table: str) -> list[str]:
    return list(db.driver_wrapper.get_column_headings(table))


def _find_one(headings: Iterable[str], predicate, *, label: str) -> str:
    matches = [h for h in headings if predicate(h)]
    assert matches, f"Could not find {label} in headings: {list(headings)!r}"
    assert len(matches) == 1, f"Multiple candidates for {label}: {matches!r}"
    return str(matches[0])


def _detect_fk_from_col(db, *, table: str, target_table: str, headings: Iterable[str]) -> str | None:
    """Find the column in `table` that FK-references `target_table`.

    This keeps tests robust across legacy naming (e.g. *_book referencing a non-books table)
    and FRBR/WEMI-aware naming.
    """

    try:
        rows = db.driver_wrapper.execute(
            f"PRAGMA foreign_key_list('{table}');"
        ).fetchall()
    except Exception:
        return None

    heading_set = {str(h) for h in headings}

    # SQLite PRAGMA foreign_key_list columns:
    # (id, seq, table, from, to, on_update, on_delete, match)
    for r in rows or []:
        try:
            ref_table = r[2]
            from_col = r[3]
        except Exception:
            continue
        if str(ref_table) == str(target_table) and str(from_col) in heading_set:
            return str(from_col)
    return None


@dataclass(frozen=True)
class _CCTables:
    cc_table: str
    link_table: str | None
    in_table: str



def _create_cc(db, *, name: str, datatype: str, in_table: str = "books", is_multiple: bool = False) -> tuple[int, _CCTables]:
    """Create a custom column, refresh Database metadata, and return (num, tables)."""

    in_table = _cc_canonical_table(db, in_table)

    num = int(
        db.driver_wrapper.create_custom_column(
            name=name,
            datatype=datatype,
            is_multiple=is_multiple,
            in_table=in_table,
        )
    )
    cc_table, link_table = db.driver_wrapper.custom_table_names(num, in_table=in_table)

    # Database maintains its own cache of custom tables.
    db.refresh_db_metadata()

    # For non-normalized types the link table isn't created.
    existing = set(db.get_tables(force_refresh=True))
    lt = link_table if link_table in existing else None

    return num, _CCTables(str(cc_table), str(lt) if lt else None, str(in_table))

def _insert_custom_value_row(db, cc_table: str, value) -> int:
    """Insert a row in a custom column value table and return its id."""

    headings = _headings(db, cc_table)
    value_col = _find_one(headings, lambda h: h.endswith("_value"), label="custom value column")

    db.driver_wrapper.add_row({value_col: value})
    return int(db.driver.direct_get_highest_id(cc_table))



def _insert_link_row(db, link_table: str, *, target_table: str, target_id: int, value_id: int, extra=None) -> int:
    """Insert a row in a custom-column link table and return its id.

    Link-table naming is not fully stable across schemas (e.g. some Calibre-derived paths
    keep the historical *_book column name even when the actual target table is not books).
    This helper therefore prefers FK-introspection and only falls back to name heuristics.
    """

    headings = _headings(db, link_table)

    value_col = _find_one(headings, lambda h: h.endswith("_value"), label="link value column")

    def _detect_target_col_via_fk() -> str | None:
        """Use FK metadata to find the link table column that points at target_table."""

        try:
            rows = db.driver_wrapper.execute(
                f"PRAGMA foreign_key_list('{link_table}');"
            ).fetchall()
        except Exception:
            return None

        # SQLite PRAGMA foreign_key_list columns:
        # (id, seq, table, from, to, on_update, on_delete, match)
        for r in rows or []:
            try:
                ref_table = r[2]
                from_col = r[3]
            except Exception:
                continue
            if str(ref_table) == str(target_table) and str(from_col) in headings:
                return str(from_col)
        return None

    # Prefer FK-introspection (robust against naming differences like *_book vs *_manifestation).
    target_col = _detect_target_col_via_fk()

    if target_col is None:
        # Heuristic: target column is typically the only non-id, non-value, non-extra column.
        candidates = [
            h
            for h in headings
            if not h.endswith("_id") and h != value_col and not h.endswith("_extra")
        ]
        if len(candidates) == 1:
            target_col = str(candidates[0])
        else:
            # Legacy Calibre naming sometimes hard-codes *_book regardless of the real target table.
            bookish = [h for h in candidates if str(h).endswith("_book")]
            if len(bookish) == 1:
                target_col = str(bookish[0])
            else:
                target_suffix = f"_{plural_singular_mapper(target_table)}"
                suffixed = [h for h in candidates if str(h).endswith(target_suffix)]
                if len(suffixed) == 1:
                    target_col = str(suffixed[0])
                else:
                    assert False, f"Could not find link target column in headings: {headings!r}"

    payload = {target_col: int(target_id), value_col: int(value_id)}
    extra_cols = [h for h in headings if h.endswith("_extra")]
    if extra_cols:
        # Extra is optional, but if caller provides it, write it to the single _extra column.
        if extra is not None:
            payload[str(extra_cols[0])] = extra
    db.driver_wrapper.add_row(payload)
    return int(db.driver.direct_get_highest_id(link_table))


# ---------------------------------------------------------------------------
# Creation + metadata
# ---------------------------------------------------------------------------


def test_create_custom_column_rejects_unknown_datatype(db) -> None:
    in_table = _cc_default_table(db)
    with pytest.raises(ValueError):
        db.driver_wrapper.create_custom_column(name="cc_badtype", datatype="giraffe", in_table=in_table)



@pytest.mark.parametrize("datatype", ["rating", "int", "float", "datetime", "bool"])
def test_create_custom_column_rejects_multiple_for_scalar_types(db, datatype: str) -> None:
    in_table = _cc_default_table(db)
    with pytest.raises(NotImplementedError):
        db.driver_wrapper.create_custom_column(
            name=f"cc_multi_{datatype}",
            datatype=datatype,
            is_multiple=True,
            in_table=in_table,
        )



@pytest.mark.parametrize(
    "label",
    [
        "Books__bad",  # uppercase
        "1bad",  # leading digit
        "bad-label",  # dash
        "bad label",  # space
        "bad#label",  # forbidden char
        "",  # empty
    ],
)
def test_create_custom_column_rejects_invalid_label(db, label: str) -> None:
    in_table = _cc_default_table(db)
    with pytest.raises((AssertionError, ValueError)):
        db.driver_wrapper.create_custom_column(name="cc_label", datatype="text", label=label, in_table=in_table)


def test_create_custom_column_requires_valid_in_table(db) -> None:
    with pytest.raises(AssertionError):
        db.driver_wrapper.create_custom_column(name="cc_tab", datatype="text", in_table="no_such_table")


@pytest.mark.parametrize("datatype", ["rating", "text", "series", "enumeration"])
def test_create_custom_column_creates_normalized_tables_and_is_discoverable(db, datatype: str) -> None:
    _require_table(db, "custom_columns")
    _, tables = _create_cc(db, name=f"cc_{datatype}", datatype=datatype, in_table="books")

    assert tables.cc_table.startswith("custom_column_")
    assert tables.link_table is not None and tables.link_table.endswith("_link")

    existing = set(db.get_tables(force_refresh=True))
    assert tables.cc_table in existing
    assert tables.link_table in existing

    # Database-level categorization
    assert db.categorize_table(tables.cc_table) == "custom"
    assert db.categorize_table(tables.link_table) == "custom"

    # Macro-level discovery
    direct = set(db.driver_wrapper.get_custom_tables())
    assert tables.cc_table in direct
    assert tables.link_table in direct


@pytest.mark.parametrize("datatype", ["comments", "datetime", "int", "float", "bool", "composite"])
def test_create_custom_column_creates_unnormalized_table_only(db, datatype: str) -> None:
    _require_table(db, "custom_columns")
    _, tables = _create_cc(db, name=f"cc_{datatype}", datatype=datatype, in_table="books")

    existing = set(db.get_tables(force_refresh=True))
    assert tables.cc_table in existing

    # Unnormalized custom columns do not create a link table.
    assert tables.link_table is None or tables.link_table not in existing

    assert db.categorize_table(tables.cc_table) == "custom"


def test_set_custom_column_metadata_updates_custom_columns_row(db) -> None:
    num, _tables = _create_cc(db, name="cc_meta", datatype="text")
    base = _tables.in_table

    changed = db.driver_wrapper.set_custom_column_metadata(
        num=num,
        name="cc_meta_renamed",
        label=f"{base}__cc_meta_label",
        is_editable=False,
        display={"heading": "🚀", "enum_values": ["a", "b"]},
        in_table=base,
    )
    assert changed is True

    row = db.get_row_from_id("custom_columns", num)
    assert row is not None
    assert row["custom_column_name"] == "cc_meta_renamed"
    assert row["custom_column_label"] == f"{base}__cc_meta_label"
    assert bool(row["custom_column_editable"]) is False


# ---------------------------------------------------------------------------
# get_interlinked_rows_cc (normalized: via link table)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "plain-ascii",
        "emoji 😀🤖🧠",
        "combining e\u0301cole",
        "rtl עברית العربية",
        "cjk 漢字かなカナ",
        "quotes 'single' and \"double\"",
        "sql-comment -- not actually a comment",
        "'); DROP TABLE titles; --",
    ],
)
def test_get_interlinked_rows_cc_normalized_roundtrip_unicode_and_injection(db, value: str) -> None:
    # Normalized TEXT custom column on books.
    _num, tables = _create_cc(db, name="cc_text", datatype="text", in_table="books")
    assert tables.link_table is not None

    book = _create_target_row(db, tables.in_table)
    cc_id = _insert_custom_value_row(db, tables.cc_table, value)
    _insert_link_row(db, tables.link_table, target_table=tables.in_table, target_id=int(book.row_id), value_id=cc_id)

    got = db.get_interlinked_rows_cc(book, tables.cc_table, link_table=True)
    assert isinstance(got, list)
    assert len(got) == 1
    assert got[0].table == tables.cc_table

    # Value column name is deterministic: singular(table) + _value
    value_col = f"{plural_singular_mapper(tables.cc_table)}_value"
    assert got[0][value_col] == value


def test_get_interlinked_rows_cc_normalized_empty_when_no_links(db) -> None:
    _num, tables = _create_cc(db, name="cc_empty", datatype="text", in_table="books")
    assert tables.link_table is not None

    book = _create_target_row(db, tables.in_table)
    assert db.get_interlinked_rows_cc(book, tables.cc_table, link_table=True) == []


def test_get_interlinked_rows_cc_normalized_orders_by_link_row_id(db, pick_payload) -> None:
    _num, tables = _create_cc(db, name="cc_order", datatype="text", in_table="books")
    assert tables.link_table is not None

    book = _create_target_row(db, tables.in_table)

    # Insert two distinct custom values.
    v1 = pick_payload(0)
    v2 = pick_payload(1)
    if "\x00" in v1 or "\x00" in v2:
        pytest.skip("Fixture provided payload with embedded NUL; skip ordering test")

    id1 = _insert_custom_value_row(db, tables.cc_table, v1)
    id2 = _insert_custom_value_row(db, tables.cc_table, v2)

    # Link them in a specific order (id2 first, then id1).
    _insert_link_row(db, tables.link_table, target_table=tables.in_table, target_id=int(book.row_id), value_id=id2)
    _insert_link_row(db, tables.link_table, target_table=tables.in_table, target_id=int(book.row_id), value_id=id1)

    got = db.get_interlinked_rows_cc(book, tables.cc_table, link_table=True)
    value_col = f"{plural_singular_mapper(tables.cc_table)}_value"
    assert [r[value_col] for r in got] == [v2, v1]


def test_get_interlinked_rows_cc_normalized_errors_when_link_table_not_registered(db) -> None:
    # Use the schema-appropriate default main table (historically 'books').
    book = _create_target_row(db, _cc_default_table(db))
    with pytest.raises(InputIntegrityError):
        db.get_interlinked_rows_cc(book, "custom_column_999999", link_table=True)


def test_get_interlinked_rows_cc_normalized_errors_on_target_table_mismatch(db) -> None:
    _num, tables = _create_cc(db, name="cc_mismatch", datatype="text", in_table="books")
    assert tables.link_table is not None

    alt = _pick_alt_main_table(db, exclude={tables.in_table, "custom_columns"})
    if alt is None:
        pytest.skip("No alternate main table with scratch column available")

    other_row = _create_target_row(db, alt)
    with pytest.raises(InputIntegrityError):
        db.get_interlinked_rows_cc(other_row, tables.cc_table, link_table=True)


def test_link_table_enforces_unique_pairs(db) -> None:
    _num, tables = _create_cc(db, name="cc_pairs", datatype="text", in_table="books")
    assert tables.link_table is not None

    book = _create_target_row(db, tables.in_table)
    cc_id = _insert_custom_value_row(db, tables.cc_table, "unique-pair")
    _insert_link_row(db, tables.link_table, target_table=tables.in_table, target_id=int(book.row_id), value_id=cc_id)

    with pytest.raises(DatabaseIntegrityError):
        _insert_link_row(db, tables.link_table, target_table=tables.in_table, target_id=int(book.row_id), value_id=cc_id)


def test_link_table_rejects_missing_foreign_keys(db) -> None:
    _num, tables = _create_cc(db, name="cc_fk", datatype="text", in_table="books")
    assert tables.link_table is not None

    book = _create_target_row(db, tables.in_table)
    cc_id = _insert_custom_value_row(db, tables.cc_table, "fk-value")

    # Bad book id.
    with pytest.raises(DatabaseIntegrityError):
        _insert_link_row(db, tables.link_table, target_table=tables.in_table, target_id=int(book.row_id) + 10_000_000, value_id=cc_id)

    # Bad value id.
    with pytest.raises(DatabaseIntegrityError):
        _insert_link_row(db, tables.link_table, target_table=tables.in_table, target_id=int(book.row_id), value_id=cc_id + 10_000_000)


def test_custom_value_table_enforces_unique_value_for_normalized_types(db) -> None:
    _num, tables = _create_cc(db, name="cc_unique", datatype="text", in_table="books")
    _insert_custom_value_row(db, tables.cc_table, "dup")
    with pytest.raises(DatabaseIntegrityError):
        _insert_custom_value_row(db, tables.cc_table, "dup")


def test_custom_value_table_sanitizes_embedded_nul(db) -> None:
    """Embedded NUL bytes are normalized to a visible placeholder.

    Historically we avoid hard-rejecting these because they can appear in imported
    metadata. Instead we keep stored TEXT values safe for common tooling.
    """

    _num, tables = _create_cc(db, name="cc_nul", datatype="text", in_table="books")

    cc_id = _insert_custom_value_row(db, tables.cc_table, "nul\x00byte")

    row = db.driver_wrapper.get_row_from_id(tables.cc_table, cc_id)
    assert row is not None

    headings = _headings(db, tables.cc_table)
    value_col = _find_one(headings, lambda h: h.endswith("_value"), label="custom value column")

    assert row[value_col] == "nul<NUL>byte"
    assert "\x00" not in row[value_col]


# ---------------------------------------------------------------------------
# get_interlinked_rows_cc (unnormalized: direct book/value table)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("datatype", ["comments", "int", "float", "bool", "datetime"])
def test_get_interlinked_rows_cc_unnormalized_roundtrip_via_direct_table(db, datatype: str) -> None:
    _num, tables = _create_cc(db, name=f"cc_un_{datatype}", datatype=datatype, in_table="books")

    book = _create_target_row(db, tables.in_table)

    headings = _headings(db, tables.cc_table)

    # Prefer FK-introspection (robust against naming differences like *_book vs *_manifestation).
    book_col = _detect_fk_from_col(db, table=tables.cc_table, target_table=tables.in_table, headings=headings)
    if book_col is None:
        target_suffix = f"_{plural_singular_mapper(tables.in_table)}"
        book_col = _find_one(
            headings,
            lambda h: h.endswith(target_suffix) or h.endswith("_book"),
            label="custom unnormalized target column",
        )

    value_col = _find_one(headings, lambda h: h.endswith("_value"), label="custom unnormalized value column")

    if datatype == "int":
        v = 42
    elif datatype == "float":
        v = 3.14159
    elif datatype == "bool":
        v = True
    elif datatype == "datetime":
        # Stored as timestamp by SQLite driver; accept string or int-ish.
        v = "2026-01-23 00:00:00"
    else:
        v = "freeform comment 📝"

    db.driver_wrapper.add_row({book_col: int(book.row_id), value_col: v})

    got = db.get_interlinked_rows_cc(book, tables.cc_table, link_table=False)
    assert isinstance(got, list)
    assert got and isinstance(got[0], dict)
    assert got[0][book_col] == int(book.row_id)


def test_get_interlinked_rows_cc_unnormalized_raises_if_link_table_true(db) -> None:
    _num, tables = _create_cc(db, name="cc_un_comments", datatype="comments", in_table="books")
    book = _create_target_row(db, tables.in_table)

    with pytest.raises(InputIntegrityError):
        db.get_interlinked_rows_cc(book, tables.cc_table, link_table=True)


def test_unnormalized_table_enforces_one_value_per_book(db) -> None:
    _num, tables = _create_cc(db, name="cc_un_one", datatype="int", in_table="books")
    book = _create_target_row(db, tables.in_table)

    headings = _headings(db, tables.cc_table)

    # Prefer FK-introspection (robust against naming differences like *_book vs *_manifestation).
    book_col = _detect_fk_from_col(db, table=tables.cc_table, target_table=tables.in_table, headings=headings)
    if book_col is None:
        target_suffix = f"_{plural_singular_mapper(tables.in_table)}"
        book_col = _find_one(
            headings,
            lambda h: h.endswith(target_suffix) or h.endswith("_book"),
            label="custom unnormalized target column",
        )

    value_col = _find_one(headings, lambda h: h.endswith("_value"), label="custom unnormalized value column")

    db.driver_wrapper.add_row({book_col: int(book.row_id), value_col: 1})
    with pytest.raises(DatabaseIntegrityError):
        db.driver_wrapper.add_row({book_col: int(book.row_id), value_col: 2})


# ---------------------------------------------------------------------------
# Mark-delete lifecycle
# ---------------------------------------------------------------------------


def test_delete_custom_column_marks_for_delete(db) -> None:
    num, _tables = _create_cc(db, name="cc_todelete", datatype="text", in_table="books")
    db.driver_wrapper.delete_custom_column(num)

    row = db.get_row_from_id("custom_columns", num)
    assert row is not None
    assert int(row["custom_column_mark_for_delete"]) == 1


def test_marked_custom_column_is_removed_on_customcolumns_load(db) -> None:
    # CustomColumns' deletion routine should drop tables for the attachment table (books/manifestations/etc.).
    num, tables = _create_cc(db, name="cc_del_apply", datatype="text", in_table="books")
    assert tables.link_table is not None

    db.driver_wrapper.delete_custom_column(num)

    # Simulate "restart" behaviour by instantiating the Calibre-style CustomColumns loader.
    from LiuXin_alpha.databases.custom_columns import CustomColumns

    _ = CustomColumns(db=db, table=tables.in_table)

    existing = set(db.get_tables(force_refresh=True))
    assert tables.cc_table not in existing
    assert tables.link_table not in existing

    # The marked row should also be gone.
    assert db.get_row_from_id("custom_columns", num) is None




def test_mark_delete_for_non_default_table(db) -> None:
    base = _cc_default_table(db)
    alt = _pick_alt_main_table(db, exclude={base, "custom_columns"})
    if alt is None:
        pytest.skip("No alternate main table with scratch column available")

    num, tables = _create_cc(db, name="cc_alt_del", datatype="text", in_table=alt)
    assert tables.link_table is not None
    db.driver_wrapper.delete_custom_column(num)

    from LiuXin_alpha.databases.custom_columns import CustomColumns

    _ = CustomColumns(db=db, table=alt)

    existing = set(db.get_tables(force_refresh=True))
    assert tables.cc_table not in existing
    assert tables.link_table not in existing
