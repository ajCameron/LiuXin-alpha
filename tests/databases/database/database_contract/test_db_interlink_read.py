"""Database contract: interlink read methods (chunk 08).

This slice focuses on the *read* surface for many-to-many (interlink) tables:

* Database.get_interlink_row()
* Database.get_interlink_rows()
* Database.get_interlinked_rows()
* Database.get_interlink_values()

The contract DB used for these tests is intentionally sparse (test_db_13). We
therefore create fresh rows + links during each test, so the behaviour is
deterministic and works across different driver backends.

These tests intentionally exercise DriverWrapper conventions around:

* link table naming / column naming
* optional priority columns (some links have them, some do not)
* optional type columns (ditto)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional, Tuple

import pytest

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError


PREFERRED_INTERLINK_PAIRS: tuple[tuple[str, str], ...] = (
    # Calibre-style classics (very likely to exist in base schema).
    ("titles", "creators"),
    ("titles", "tags"),
    ("titles", "series"),
    ("titles", "publishers"),
    ("titles", "subjects"),
    ("titles", "genres"),
    ("titles", "languages"),
    # Fallbacks that still often exist:
    ("files", "folders"),
    ("titles", "comments"),
)


@dataclass(frozen=True)
class InterlinkShape:
    primary_table: str
    secondary_table: str
    link_table: str
    primary_id_col: str
    secondary_id_col: str
    primary_link_col: str
    secondary_link_col: str
    priority_link_col: Optional[str]
    type_link_col: Optional[str]


def _pick_text_like_column(cols: Iterable[str], *, base: str, exclude: set[str]) -> str:
    cols_list = list(cols)
    if base in cols_list and base not in exclude:
        return base
    pref = [c for c in cols_list if c.startswith(base) and c not in exclude]
    if pref:
        return pref[0]
    for c in cols_list:
        if c not in exclude:
            return c
    # last resort
    return cols_list[0]


def _pick_interlink_shape(open_db) -> InterlinkShape:
    """Pick an interlinkable (primary, secondary) table pair that exists.

    Prefer a shape whose link table supports a `type` column if possible, since many contract
    tests exercise type-aware paths.
    """
    dw = open_db.driver_wrapper

    def resolve_pair(a: str, b: str) -> Optional[InterlinkShape]:
        link_table = dw.get_link_table_name(a, b)
        if not link_table:
            return None
        # Ensure this is categorised as an interlink table (if metadata is up to date).
        if hasattr(open_db, "interlink_tables") and link_table not in set(open_db.interlink_tables):
            # Some schemas may still link but not be categorised; allow as fallback.
            pass

        primary_id_col = dw.get_id_column(a)
        secondary_id_col = dw.get_id_column(b)

        primary_link_col = dw.get_link_column(a, b, primary_id_col)
        secondary_link_col = dw.get_link_column(a, b, secondary_id_col)

        # Optional columns.
        try:
            priority_link_col = dw.get_link_column(a, b, "priority")
        except Exception:
            priority_link_col = None
        try:
            type_link_col = dw.get_link_column(a, b, "type")
        except Exception:
            type_link_col = None

        return InterlinkShape(
            primary_table=a,
            secondary_table=b,
            link_table=link_table,
            primary_id_col=primary_id_col,
            secondary_id_col=secondary_id_col,
            primary_link_col=primary_link_col,
            secondary_link_col=secondary_link_col,
            priority_link_col=priority_link_col,
            type_link_col=type_link_col,
        )

    best: Optional[InterlinkShape] = None

    # Try preferred pairs first.
    for a, b in PREFERRED_INTERLINK_PAIRS:
        sh = resolve_pair(a, b) or resolve_pair(b, a)
        if sh is None:
            continue
        if sh.type_link_col is not None:
            return sh
        if best is None:
            best = sh

    # Otherwise, scan all main-table pairs.
    mains = list(open_db.main_tables)
    for i, a in enumerate(mains):
        for b in mains[i + 1 :]:
            sh = resolve_pair(a, b) or resolve_pair(b, a)
            if sh is None:
                continue
            if sh.type_link_col is not None:
                return sh
            if best is None:
                best = sh

    if best is not None:
        return best

    raise pytest.SkipTest("No interlinkable table pair found in this schema")  # pragma: no cover



def _pick_allowed_type_for_shape(open_db, sh: "InterlinkShape", *, preferred: str = "authors") -> Optional[str]:
    """Pick an allowed link `type` for the chosen interlink shape.

    The FRBR generator may enforce allowed types via either:

    * `{link_table}__types` (single column: `type`)
    * `allowed_types__{link_table}` (legacy pattern; type column ends with `_type`)

    If the shape has no type column, return None.
    """

    if sh.type_link_col is None:
        return None

    existing = set(open_db.get_tables(force_refresh=True))

    def fresh_get(stmt: str):
        conn = open_db.driver.get_connection()
        try:
            return conn.get(stmt, all=True)
        finally:
            try:
                conn.close()
            except Exception:
                pass

    types_table = f"{sh.link_table}__types"
    if types_table in existing:
        rows = fresh_get(f"SELECT `type` FROM `{types_table}` ORDER BY `type`;")
        types = [r[0] for r in rows if r and r[0] is not None]
        if preferred in types:
            return preferred
        return types[0] if types else preferred

    legacy_table = f"allowed_types__{sh.link_table}"
    if legacy_table in existing:
        headings = list(open_db.driver_wrapper.get_column_headings(legacy_table))
        if "type" in headings:
            col = "type"
        else:
            type_cols = [h for h in headings if h.endswith("_type") and not h.endswith("_id")]
            col = type_cols[0] if type_cols else headings[0]
        rows = fresh_get(
            f"SELECT `{col}` FROM `{legacy_table}` WHERE `{col}` IS NOT NULL ORDER BY `{col}`;"
        )
        types = [r[0] for r in rows if r and r[0] is not None]
        if preferred in types:
            return preferred
        return types[0] if types else preferred

    # If the schema provides a type column but no registry table exists, just return the preferred.
    return preferred



def _create_distinct_row(open_db, table: str, *, payload: str) -> Row:
    """Create a writable row and set a stable 'text-like' column to payload."""
    dw = open_db.driver_wrapper
    row = open_db.get_blank_row(table)
    assert isinstance(row, Row)
    cols = list(dw.get_column_headings(table))

    # Prefer the scratch column if it actually exists on the table.
    # Many FRBR/WEMI tables have required foreign keys; writing payload into an FK column
    # will fail. The scratch column exists specifically to make a row distinct.
    try:
        scratch_col = dw.get_scratch_column(table)
    except Exception:
        scratch_col = None
    if scratch_col and scratch_col in cols:
        row[scratch_col] = payload
        row.sync()
        return row

    base = dw.get_column_base(table)

    # If there's no scratch column, avoid id + any foreign-key columns when picking
    # a payload target.
    exclude = {dw.get_id_column(table)}
    try:
        conn = open_db.driver.get_connection()
        try:
            fk_rows = conn.get(f"PRAGMA foreign_key_list(`{table}`);", all=True)
        finally:
            try:
                conn.close()
            except Exception:
                pass
        # SQLite PRAGMA foreign_key_list columns: (id, seq, table, from, to, on_update, on_delete, match)
        exclude |= {r[3] for r in fk_rows if r and len(r) > 3}
    except Exception:
        pass

    text_col = _pick_text_like_column(cols, base=base, exclude=exclude)
    row[text_col] = payload
    row.sync()
    return row


def _interlink(open_db, sh: InterlinkShape, *, primary: Row, secondary: Row, priority, link_type: Optional[str] = None) -> Row:
    """Create an interlink row, using type only if the schema supports it."""
    kwargs = {"priority": priority}
    if sh.type_link_col is not None and link_type is not None:
        kwargs["type"] = link_type
    return open_db.interlink_rows(primary_row=primary, secondary_row=secondary, **kwargs)


def _pick_unique_column_for_table(open_db, table: str) -> Optional[str]:
    """Pick a column name that occurs only in the given table (helps identify_table_from_column)."""
    toc = open_db.driver.direct_get_tables_and_columns()
    occurrences: dict[str, int] = {}
    for cols in toc.values():
        for c in cols:
            occurrences[c] = occurrences.get(c, 0) + 1

    cols = toc.get(table) or []
    # Prefer columns that look value-ish and aren't too generic.
    avoid = {"id", "uuid", "path", "sort", "timestamp", "last_modified", "date", "created"}
    candidates = [_ for _ in cols if occurrences.get(_, 0) == 1]

    final_cands = []
    for c in candidates:

        if c in avoid:
            continue

        for avoid_cand in avoid:
            if c.endswith(avoid_cand):
                break
        else:
            final_cands.append(c)

    if final_cands:
        return final_cands[0]

    # If no truly-unique column exists, the method is ambiguous; skip.
    return None


UNICODE_TORTURE_PAYLOADS: tuple[str, ...] = (
    "Hello",
    "Français: naïve café déjà vu",
    "العربية‎ (Arabic RTL)",
    "עברית (Hebrew RTL)",
    "中文測試 (CJK)",
    "हिन्दी (Devanagari)",
    "ไทย (Thai)",
    "emoji 😺🚀✨",
    "combining: e\u0301 vs é",
    "Zalgo: Z̷a̷l̷g̷o̷",
    "zero-width: a\u200bb\u200dc\u2066X\u2069",
    "injection-ish: ' ; DROP TABLE titles; --",
)


def test_get_interlink_row_returns_none_if_unlinked(open_db):
    sh = _pick_interlink_shape(open_db)

    p = _create_distinct_row(open_db, sh.primary_table, payload="p-unlinked")
    s = _create_distinct_row(open_db, sh.secondary_table, payload="s-unlinked")

    got = open_db.get_interlink_row(primary_row=p, secondary_row=s)
    assert got is None


@pytest.mark.parametrize("payload", UNICODE_TORTURE_PAYLOADS)
def test_get_interlink_row_roundtrips_one_link(open_db, payload: str):
    sh = _pick_interlink_shape(open_db)

    p = _create_distinct_row(open_db, sh.primary_table, payload=f"p:{payload}")
    s = _create_distinct_row(open_db, sh.secondary_table, payload=f"s:{payload}")

    link_type = _pick_allowed_type_for_shape(open_db, sh, preferred="authors")
    link = _interlink(open_db, sh, primary=p, secondary=s, priority=1, link_type=link_type)
    assert isinstance(link, Row)
    assert link.table == sh.link_table

    got = open_db.get_interlink_row(primary_row=p, secondary_row=s, onelink=True)
    assert isinstance(got, Row)
    assert got.table == sh.link_table

    # Verify the link actually references the correct ids.
    assert got[sh.primary_link_col] == p.row_id
    assert got[sh.secondary_link_col] == s.row_id

    # onelink=False with a single link should return a list
    got_many = open_db.get_interlink_row(primary_row=p, secondary_row=s, onelink=False)
    assert isinstance(got_many, list)
    assert len(got_many) == 1
    assert got_many[0][sh.primary_link_col] == p.row_id
    assert got_many[0][sh.secondary_link_col] == s.row_id

    # Sanity: interlink_rows may set type/priority if available.
    if sh.type_link_col is not None:
        assert got[sh.type_link_col] == "authors"
    if sh.priority_link_col is not None:
        assert isinstance(got[sh.priority_link_col], (int, float))


def test_get_interlink_row_errors_on_multiple_links_when_possible(open_db):
    sh = _pick_interlink_shape(open_db)

    p = _create_distinct_row(open_db, sh.primary_table, payload="p-multi")
    s = _create_distinct_row(open_db, sh.secondary_table, payload="s-multi")

    # We can only create multiple logical links if the link table supports a distinguishing column.
    if sh.type_link_col is None:
        pytest.skip("Link table has no type column; cannot create multiple links for the same row-pair")

    types = _pick_two_allowed_types_for_shape(open_db, sh, preferred="authors")
    if types is None:
        pytest.skip("Could not find two distinct allowed types for this link table")

    t1, t2 = types

    _interlink(open_db, sh, primary=p, secondary=s, priority=1, link_type=t1)

    try:
        _interlink(open_db, sh, primary=p, secondary=s, priority=2, link_type=t2)
    except DatabaseIntegrityError:
        pytest.skip("Link table enforces uniqueness across row-pair even with differing type; cannot create multi-link")

    with pytest.raises(DatabaseIntegrityError):
        open_db.get_interlink_row(primary_row=p, secondary_row=s, onelink=True)

    got = open_db.get_interlink_row(primary_row=p, secondary_row=s, onelink=False)
    assert isinstance(got, list)
    assert len(got) >= 2



def test_get_interlink_rows_returns_all_links_and_sorts_by_priority_if_present(open_db):
    sh = _pick_interlink_shape(open_db)
    link_type = _pick_allowed_type_for_shape(open_db, sh, preferred="authors")

    p = _create_distinct_row(open_db, sh.primary_table, payload="p-pri")

    s1 = _create_distinct_row(open_db, sh.secondary_table, payload="s-pri-1")
    s2 = _create_distinct_row(open_db, sh.secondary_table, payload="s-pri-2")
    s3 = _create_distinct_row(open_db, sh.secondary_table, payload="s-pri-3")

    # Create links with explicit priority numbers (ignored if table lacks a priority column).
    _interlink(open_db, sh, primary=p, secondary=s1, priority=10, link_type=link_type)
    _interlink(open_db, sh, primary=p, secondary=s2, priority=-5, link_type=link_type)
    _interlink(open_db, sh, primary=p, secondary=s3, priority=3, link_type=link_type)

    link_rows = open_db.get_interlink_rows(primary_row=p, secondary_table=sh.secondary_table)
    assert isinstance(link_rows, list)
    assert len(link_rows) == 3
    assert all(isinstance(r, Row) for r in link_rows)
    assert all(r.table == sh.link_table for r in link_rows)

    # They should all reference the primary id.
    assert all(r[sh.primary_link_col] == p.row_id for r in link_rows)

    if sh.priority_link_col is not None and all(sh.priority_link_col in r for r in link_rows):
        priorities = [r[sh.priority_link_col] for r in link_rows]
        assert priorities == sorted(priorities)


def test_get_interlinked_rows_returns_secondary_rows_in_priority_order_when_present(open_db):
    sh = _pick_interlink_shape(open_db)
    link_type = _pick_allowed_type_for_shape(open_db, sh, preferred="authors")

    p = _create_distinct_row(open_db, sh.primary_table, payload="p-linked")

    s_hi = _create_distinct_row(open_db, sh.secondary_table, payload="s-hi")
    s_mid = _create_distinct_row(open_db, sh.secondary_table, payload="s-mid")
    s_lo = _create_distinct_row(open_db, sh.secondary_table, payload="s-lo")

    _interlink(open_db, sh, primary=p, secondary=s_lo, priority=1, link_type=link_type)
    _interlink(open_db, sh, primary=p, secondary=s_mid, priority=5, link_type=link_type)
    _interlink(open_db, sh, primary=p, secondary=s_hi, priority=9, link_type=link_type)

    linked = open_db.get_interlinked_rows(target_row=p, secondary_table=sh.secondary_table)
    assert isinstance(linked, list)
    assert len(linked) == 3
    assert all(isinstance(r, Row) for r in linked)
    assert all(r.table == sh.secondary_table for r in linked)

    # If the schema has a priority column, we expect descending priority order (highest first).
    if sh.priority_link_col is not None:
        # Compare using ids - stable even if other columns are defaulted.
        got_ids = [r.row_id for r in linked]
        expected_ids = [s_hi.row_id, s_mid.row_id, s_lo.row_id]
        if got_ids != expected_ids:
            # Some schemas lack priority semantics; accept any permutation if priorities aren't actually stored.
            # We check this by reading back priorities via get_interlink_rows.
            link_rows = open_db.get_interlink_rows(primary_row=p, secondary_table=sh.secondary_table)
            if sh.priority_link_col in link_rows[0]:
                # priorities stored -> ordering must match
                assert got_ids == expected_ids


def test_get_interlinked_rows_type_filter_when_available(open_db):
    sh = _pick_interlink_shape(open_db)
    if sh.type_link_col is None:
        pytest.skip("Link table has no type column; cannot test type_filter")

    types = _pick_two_allowed_types_for_shape(open_db, sh, preferred="authors")
    if types is None:
        pytest.skip("Could not find two distinct allowed types for this link table")
    t_a, t_b = types

    p = _create_distinct_row(open_db, sh.primary_table, payload="p-type")
    s_a1 = _create_distinct_row(open_db, sh.secondary_table, payload="s-a1")
    s_a2 = _create_distinct_row(open_db, sh.secondary_table, payload="s-a2")
    s_b = _create_distinct_row(open_db, sh.secondary_table, payload="s-b")

    _interlink(open_db, sh, primary=p, secondary=s_a1, priority=1, link_type=t_a)
    _interlink(open_db, sh, primary=p, secondary=s_a2, priority=1, link_type=t_a)
    _interlink(open_db, sh, primary=p, secondary=s_b, priority=1, link_type=t_b)

    got_a = open_db.get_interlinked_rows(primary_row=p, secondary_table=sh.secondary_table, type_filter=t_a)
    got_b = open_db.get_interlinked_rows(primary_row=p, secondary_table=sh.secondary_table, type_filter=t_b)

    assert {r.row_id for r in got_a} == {s_a1.row_id, s_a2.row_id}
    assert {r.row_id for r in got_b} == {s_b.row_id}



def test_get_interlink_values_returns_set_when_unique_column_available(open_db):
    sh = _pick_interlink_shape(open_db)
    link_type = _pick_allowed_type_for_shape(open_db, sh, preferred="authors")

    unique_col = _pick_unique_column_for_table(open_db, sh.secondary_table)

    if unique_col is None:
        pytest.skip("No unique column found for secondary table; identify_table_from_column would be ambiguous")

    p = _create_distinct_row(open_db, sh.primary_table, payload="p-values")

    s1 = _create_distinct_row(open_db, sh.secondary_table, payload="s-v1")
    s2 = _create_distinct_row(open_db, sh.secondary_table, payload="s-v2")

    # Set a distinct value in the unique column so we can assert via get_interlink_values.
    s1[unique_col] = "val-α"  # Greek alpha
    s2[unique_col] = "val-β"  # Greek beta
    s1.sync()
    s2.sync()

    _interlink(open_db, sh, primary=p, secondary=s1, priority=1, link_type=link_type)
    _interlink(open_db, sh, primary=p, secondary=s2, priority=2, link_type="editors")

    got = open_db.get_interlink_values(target_row=p, secondary_column=unique_col)
    assert got == {"val-α", "val-β"}

#
# def test_get_interlinked_rows_rejects_invalid_inputs(open_db):
#     sh = _pick_interlink_shape(open_db)
    link_type = _pick_allowed_type_for_shape(open_db, sh, preferred="authors")
#     p = _create_distinct_row(open_db, sh.primary_table, payload="p-invalid")
#
#     with pytest.raises(InputIntegrityError):
#         open_db.get_interlinked_rows(target_row="not-a-row", secondary_table=sh.secondary_table)
#
#     with pytest.raises(InputIntegrityError):
#         open_db.get_interlinked_rows(target_row=p, secondary_table="definitely_not_a_table")
#
#     with pytest.raises(InputIntegrityError):
#         open_db.get_interlinked_rows(target_row=p, secondary_table=sh.primary_table)
#
#
# def test_get_interlinked_rows_returns_empty_list_when_no_link_table_exists(open_db):
#     sh = _pick_interlink_shape(open_db)
    link_type = _pick_allowed_type_for_shape(open_db, sh, preferred="authors")
#     p = _create_distinct_row(open_db, sh.primary_table, payload="p-nolink")
#
#     # Find a secondary main table that does NOT have a link table with primary.
#     dw = open_db.driver_wrapper
#     for sec in open_db.main_tables:
#         if sec == sh.primary_table:
#             continue
#         if dw.get_link_table_name(sh.primary_table, sec) is None and dw.get_link_table_name(sec, sh.primary_table) is None:
#             got = open_db.get_interlinked_rows(target_row=p, secondary_table=sec)
#             assert got == []
#             return
#
#     pytest.skip("All main tables appear linkable to the chosen primary; cannot test missing link-table path")
