"""Database contract: interlink write/update/unlink methods (chunk 09).

This slice focuses on the *write* surface for many-to-many (interlink) tables:

* Database.interlink_rows()
* Database.update_interlink()
* Database.update_interlink_priority()
* Database.unlink_interlink()
* Database.unlink_all()

These tests intentionally exercise:
* priority behaviours (highest/lowest/numeric/None/not_set/invalid)
* optional type column behaviours
* duplicate-link cleanup on uniqueness errors
* link-row extra column writing via **col_value_pairs
* unlink behaviours, including type_filter

The contract DB used for these tests is intentionally sparse (test_db_13). We
therefore create fresh rows + links during each test, so behaviour is
deterministic across driver backends.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional, Tuple

import pytest

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError


PREFERRED_INTERLINK_PAIRS: tuple[tuple[str, str], ...] = (
    ("titles", "creators"),
    ("titles", "tags"),
    ("titles", "series"),
    ("titles", "publishers"),
    ("titles", "subjects"),
    ("titles", "genres"),
    ("titles", "languages"),
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


def _pick_interlink_shape(open_db) -> InterlinkShape:
    """Pick an interlinkable (primary, secondary) table pair that exists."""
    dw = open_db.driver_wrapper

    def resolve_pair(a: str, b: str) -> Optional[InterlinkShape]:
        link_table = dw.get_link_table_name(a, b)
        if not link_table:
            return None

        primary_id_col = dw.get_id_column(a)
        secondary_id_col = dw.get_id_column(b)

        primary_link_col = dw.get_link_column(a, b, primary_id_col)
        secondary_link_col = dw.get_link_column(a, b, secondary_id_col)

        # Optional columns (may raise in some wrappers).
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

    for a, b in PREFERRED_INTERLINK_PAIRS:
        sh = resolve_pair(a, b) or resolve_pair(b, a)
        if sh is not None:
            return sh

    mains = list(getattr(open_db, "main_tables", []))
    if not mains:
        raise pytest.SkipTest("Database has no main_tables metadata available")  # pragma: no cover
    for i, a in enumerate(mains):
        for b in mains[i + 1 :]:
            sh = resolve_pair(a, b) or resolve_pair(b, a)
            if sh is not None:
                return sh

    raise pytest.SkipTest("No interlinkable table pair found in this schema")  # pragma: no cover


def _create_distinct_row(open_db, table: str, *, payload: str) -> Row:
    """Create a writable row and set a stable 'text-like' column to payload."""
    dw = open_db.driver_wrapper

    # Some tables are intentionally read-only constants (e.g. `languages`).
    # For contract tests, pick an existing row deterministically.
    if table in {"languages"}:
        import zlib

        pk = dw.get_id_column(table)
        count = int(dw.get_record_count(table) or 0)
        assert count > 0, f"Expected seeded rows in read-only table {table!r}"

        offset = (zlib.crc32(payload.encode("utf-8")) & 0xFFFFFFFF) % count
        cur = dw.execute(f"SELECT {pk} FROM {table} ORDER BY {pk} LIMIT 1 OFFSET ?;", (int(offset),))
        got = cur.fetchone()
        assert got is not None
        row_id = got[0]
        row = open_db.get_row_from_id(table, row_id)
        assert isinstance(row, Row)
        return row

    row = open_db.get_blank_row(table)
    assert isinstance(row, Row)

    cols = list(dw.get_column_headings(table))
    base = dw.get_column_base(table)
    exclude = {dw.get_id_column(table), dw.get_scratch_column(table)}

    text_col = _pick_text_like_column(cols, base=base, exclude=exclude)
    row[text_col] = payload
    row.sync()
    return row


def _has_priority_support(open_db, sh: InterlinkShape) -> bool:
    if sh.priority_link_col is None:
        return False
    # Some wrappers return a string but the column may not exist on the link table (defensive).
    try:
        headings = set(open_db.driver_wrapper.get_column_headings(sh.link_table))
    except Exception:
        return False
    return sh.priority_link_col in headings


def _has_type_support(open_db, sh: InterlinkShape) -> bool:
    if sh.type_link_col is None:
        return False
    try:
        headings = set(open_db.driver_wrapper.get_column_headings(sh.link_table))
    except Exception:
        return False
    return sh.type_link_col in headings


def _pick_type_registry_column(open_db, registry_table: str) -> str:
    """Return the column name that stores the link type in a registry table."""
    headings = list(open_db.driver_wrapper.get_column_headings(registry_table))
    if "type" in headings:
        return "type"
    type_cols = [h for h in headings if h.endswith("_type") and not h.endswith("_id")]
    if type_cols:
        return type_cols[0]
    non_id = [h for h in headings if not h.endswith("_id") and h != "id"]
    return non_id[0] if non_id else headings[0]


def _type_registry_for_interlink(open_db, link_table: str) -> tuple[str, str] | None:
    """Return (registry_table, type_col) for this link table, or None if absent."""
    dw = open_db.driver_wrapper
    tables = set(dw.get_tables(force_refresh=True) or [])
    types_table = f"{link_table}__types"
    if types_table in tables:
        return types_table, _pick_type_registry_column(open_db, types_table)
    legacy_table = f"allowed_types__{link_table}"
    if legacy_table in tables:
        return legacy_table, _pick_type_registry_column(open_db, legacy_table)
    return None


def _ensure_interlink_type_registered(open_db, sh: InterlinkShape, link_type: str) -> None:
    """If schema enforces allowed interlink types, ensure `link_type` is registered.

    Some generated schemas use triggers checking membership in `{link_table}__types`
    (or legacy `allowed_types__{link_table}`) tables. Production code may not seed
    those registries, so contract tests do so explicitly.
    """
    if sh.type_link_col is None:
        return

    reg = _type_registry_for_interlink(open_db, sh.link_table)
    if not reg:
        return
    registry_table, type_col = reg

    canonical = str(link_type).strip()
    open_db.driver_wrapper.execute(
        f"INSERT OR IGNORE INTO `{registry_table}` (`{type_col}`) VALUES (?);",
        (canonical,),
    )


def _pick_extra_link_column_base(open_db, sh: InterlinkShape) -> Optional[tuple[str, str]]:
    """Pick a link-table column we can set via **col_value_pairs.

    Returns (base_name_to_pass, resolved_column_name) or None if none is suitable.
    """
    dw = open_db.driver_wrapper
    headings = list(dw.get_column_headings(sh.link_table))

    exclude = {
        dw.get_id_column(sh.link_table),
        dw.get_scratch_column(sh.link_table),
        sh.primary_link_col,
        sh.secondary_link_col,
    }
    if sh.priority_link_col:
        exclude.add(sh.priority_link_col)
    if sh.type_link_col:
        exclude.add(sh.type_link_col)

    # Prefer columns where get_link_column(col) roundtrips to itself.
    for col in headings:
        if col in exclude:
            continue
        try:
            resolved = dw.get_link_column(sh.primary_table, sh.secondary_table, col)
        except Exception:
            continue
        if resolved == col:
            return col, resolved

    # Otherwise, try a few common bases and accept if they resolve to a real non-excluded heading.
    common_bases = ("index", "idx", "sort", "position", "note", "data", "extra", "flags", "value")
    heading_set = set(headings)
    for base in common_bases:
        try:
            resolved = dw.get_link_column(sh.primary_table, sh.secondary_table, base)
        except Exception:
            continue
        if resolved in heading_set and resolved not in exclude:
            return base, resolved

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


def test_interlink_rows_rejects_rows_without_ids(open_db):
    sh = _pick_interlink_shape(open_db)

    # Construct a Row-like object that *identifies* as the primary table but has no id column.
    # (Using get_blank_row() would insert a real row and therefore have an id.)
    dw = open_db.driver_wrapper
    scratch_col = dw.get_scratch_column(sh.primary_table)
    p = Row(database=open_db, row_dict={scratch_col: 'contract-no-id'})

    s = _create_distinct_row(open_db, sh.secondary_table, payload='secondary-has-id')
    assert p.row_id is None
    assert s.row_id is not None

    with pytest.raises(InputIntegrityError):
        open_db.interlink_rows(primary_row=p, secondary_row=s, priority='not_set')



def test_interlink_rows_rejects_unlinkable_tables_when_possible(open_db):
    dw = open_db.driver_wrapper
    mains = list(getattr(open_db, "main_tables", []))
    if not mains:
        raise pytest.SkipTest("Database has no main_tables metadata available")  # pragma: no cover

    # Find a pair with no link table in either direction.
    pair: Optional[tuple[str, str]] = None
    for i, a in enumerate(mains):
        for b in mains[i + 1 :]:
            if dw.get_link_table_name(a, b) is None and dw.get_link_table_name(b, a) is None:
                pair = (a, b)
                break
        if pair:
            break

    if pair is None:
        pytest.skip("All main-table pairs appear linkable in this schema; cannot test missing link-table path")

    a, b = pair
    ra = _create_distinct_row(open_db, a, payload="unl-a")
    rb = _create_distinct_row(open_db, b, payload="unl-b")

    with pytest.raises(InputIntegrityError):
        open_db.interlink_rows(primary_row=ra, secondary_row=rb, priority="not_set")


@pytest.mark.parametrize("payload", UNICODE_TORTURE_PAYLOADS[:6])
def test_interlink_rows_creates_link_row_basic(open_db, payload: str):
    sh = _pick_interlink_shape(open_db)
    p = _create_distinct_row(open_db, sh.primary_table, payload=f"p:{payload}")
    s = _create_distinct_row(open_db, sh.secondary_table, payload=f"s:{payload}")

    link = open_db.interlink_rows(primary_row=p, secondary_row=s, priority="not_set")
    assert isinstance(link, Row)
    assert link.table == sh.link_table
    assert link[sh.primary_link_col] == p.row_id
    assert link[sh.secondary_link_col] == s.row_id


def test_interlink_rows_duplicate_link_cleanup_on_uniqueness_error(open_db):
    sh = _pick_interlink_shape(open_db)
    p = _create_distinct_row(open_db, sh.primary_table, payload="p-dup")
    s = _create_distinct_row(open_db, sh.secondary_table, payload="s-dup")

    before = open_db.driver_wrapper.get_record_count(target_table=sh.link_table)
    open_db.interlink_rows(primary_row=p, secondary_row=s, priority="not_set")

    mid = open_db.driver_wrapper.get_record_count(target_table=sh.link_table)
    assert mid == before + 1

    # Attempt a duplicate link with the same (row, row) pair and (optional) same type.
    # Many schemas enforce uniqueness here.
    with pytest.raises(DatabaseIntegrityError):
        open_db.interlink_rows(primary_row=p, secondary_row=s, priority="not_set")

    after = open_db.driver_wrapper.get_record_count(target_table=sh.link_table)
    assert after == mid  # cleanup should leave the count unchanged


def test_interlink_rows_type_column_roundtrips_when_supported(open_db):
    sh = _pick_interlink_shape(open_db)
    if not _has_type_support(open_db, sh):
        pytest.skip("Link table has no usable type column")

    p = _create_distinct_row(open_db, sh.primary_table, payload="p-type")
    s = _create_distinct_row(open_db, sh.secondary_table, payload="s-type")
    link_type = "βeta-نوع"  # Greek beta + Arabic

    _ensure_interlink_type_registered(open_db, sh, link_type)

    link = open_db.interlink_rows(primary_row=p, secondary_row=s, priority="not_set", type=link_type)
    assert link[sh.type_link_col] == link_type


def test_interlink_rows_sets_extra_columns_via_col_value_pairs_when_available(open_db):
    sh = _pick_interlink_shape(open_db)
    picked = _pick_extra_link_column_base(open_db, sh)
    if picked is None:
        pytest.skip("No suitable extra link-table column found to test **col_value_pairs")
    base_name, resolved_col = picked

    p = _create_distinct_row(open_db, sh.primary_table, payload="p-extra")
    s = _create_distinct_row(open_db, sh.secondary_table, payload="s-extra")

    value = "val-🧪-テスト"
    link = open_db.interlink_rows(primary_row=p, secondary_row=s, priority="not_set", **{base_name: value})
    assert link[resolved_col] == value


@pytest.mark.parametrize("priority", [None, 0, 7, -3, 2.25])
def test_interlink_rows_priority_numeric_and_none_when_supported(open_db, priority):
    sh = _pick_interlink_shape(open_db)
    if not _has_priority_support(open_db, sh):
        pytest.skip("Link table has no usable priority column")

    p = _create_distinct_row(open_db, sh.primary_table, payload=f"p-pri-{priority}")
    s = _create_distinct_row(open_db, sh.secondary_table, payload=f"s-pri-{priority}")

    link = open_db.interlink_rows(primary_row=p, secondary_row=s, priority=priority)
    if priority is None:
        assert link[sh.priority_link_col] == 0
    else:
        assert link[sh.priority_link_col] == priority


@pytest.mark.parametrize("mode", ["highest", "lowest"])
def test_interlink_rows_priority_highest_lowest_follows_get_max_min_contract(open_db, mode: str):
    sh = _pick_interlink_shape(open_db)
    if not _has_priority_support(open_db, sh):
        pytest.skip("Link table has no usable priority column")

    # Capture the current extrema using the same surface Database uses internally.
    before = open_db.get_max(sh.priority_link_col) if mode == "highest" else open_db.get_min(sh.priority_link_col)
    before_count = open_db.driver_wrapper.get_record_count(target_table=sh.link_table)

    p = _create_distinct_row(open_db, sh.primary_table, payload=f"p-{mode}")
    s = _create_distinct_row(open_db, sh.secondary_table, payload=f"s-{mode}")

    link = open_db.interlink_rows(primary_row=p, secondary_row=s, priority=mode)

    # Mirror the algorithm in Database.interlink_rows.
    expected: Optional[int]
    try:
        base = int(before)  # may raise if None / not numeric
    except Exception:
        # If the link table is empty, the code forces 1; otherwise it errors earlier.
        if before_count == 0:
            expected = 1
        else:
            # If the column is non-numeric and table non-empty, the implementation raises DatabaseIntegrityError.
            # But since we got here, treat as weakly-defined.
            pytest.skip("Priority extrema was non-numeric with non-empty link table; behaviour is driver/schema-specific")
    else:
        expected = base + 1 if mode == "highest" else base - 1

    assert link[sh.priority_link_col] == expected


def test_interlink_rows_priority_invalid_string_raises(open_db):
    sh = _pick_interlink_shape(open_db)
    if not _has_priority_support(open_db, sh):
        pytest.skip("Link table has no usable priority column")

    p = _create_distinct_row(open_db, sh.primary_table, payload="p-badpri")
    s = _create_distinct_row(open_db, sh.secondary_table, payload="s-badpri")

    with pytest.raises(InputIntegrityError):
        open_db.interlink_rows(primary_row=p, secondary_row=s, priority="banana")


def test_interlink_rows_priority_not_set_leaves_default_when_supported(open_db):
    sh = _pick_interlink_shape(open_db)
    if not _has_priority_support(open_db, sh):
        pytest.skip("Link table has no usable priority column")

    p = _create_distinct_row(open_db, sh.primary_table, payload="p-notset")
    s = _create_distinct_row(open_db, sh.secondary_table, payload="s-notset")

    blank = open_db.driver_wrapper.get_blank_row(sh.link_table)
    default_val = blank.get(sh.priority_link_col)

    link = open_db.interlink_rows(primary_row=p, secondary_row=s, priority="not_set")
    assert link[sh.priority_link_col] == default_val


def test_update_interlink_updates_priority_numeric_when_supported(open_db):
    sh = _pick_interlink_shape(open_db)
    if not _has_priority_support(open_db, sh):
        pytest.skip("Link table has no usable priority column")

    p = _create_distinct_row(open_db, sh.primary_table, payload="p-updpri")
    s = _create_distinct_row(open_db, sh.secondary_table, payload="s-updpri")

    link = open_db.interlink_rows(primary_row=p, secondary_row=s, priority=1)
    assert link[sh.priority_link_col] == 1

    updated = open_db.update_interlink(primary_row=p, secondary_row=s, priority=42)
    assert updated[sh.priority_link_col] == 42

    unchanged = open_db.update_interlink(primary_row=p, secondary_row=s, priority="unchanged")
    assert unchanged[sh.priority_link_col] == 42


def test_update_interlink_priority_highest_lowest_when_supported(open_db):
    sh = _pick_interlink_shape(open_db)
    if not _has_priority_support(open_db, sh):
        pytest.skip("Link table has no usable priority column")

    p = _create_distinct_row(open_db, sh.primary_table, payload="p-upd-hi-lo")

    s1 = _create_distinct_row(open_db, sh.secondary_table, payload="s1-upd")
    s2 = _create_distinct_row(open_db, sh.secondary_table, payload="s2-upd")

    l1 = open_db.interlink_rows(primary_row=p, secondary_row=s1, priority=5)
    l2 = open_db.interlink_rows(primary_row=p, secondary_row=s2, priority=10)

    max_before = open_db.get_max(sh.priority_link_col)
    updated = open_db.update_interlink(primary_row=p, secondary_row=s1, priority="highest")
    assert updated[sh.priority_link_col] == int(max_before) + 1

    min_before = open_db.get_min(sh.priority_link_col)
    updated2 = open_db.update_interlink(primary_row=p, secondary_row=s2, priority="lowest")
    assert updated2[sh.priority_link_col] == int(min_before) - 1


def test_update_interlink_updates_extra_cols_when_available(open_db):
    sh = _pick_interlink_shape(open_db)
    picked = _pick_extra_link_column_base(open_db, sh)
    if picked is None:
        pytest.skip("No suitable extra link-table column found to test update_interlink col_value_pairs")
    base_name, resolved_col = picked

    p = _create_distinct_row(open_db, sh.primary_table, payload="p-updextra")
    s = _create_distinct_row(open_db, sh.secondary_table, payload="s-updextra")

    open_db.interlink_rows(primary_row=p, secondary_row=s, priority="not_set", **{base_name: "old"})
    updated = open_db.update_interlink(primary_row=p, secondary_row=s, priority="unchanged", **{base_name: "new"})
    assert updated[resolved_col] == "new"


def test_update_interlink_errors_if_no_link_exists(open_db):
    sh = _pick_interlink_shape(open_db)
    p = _create_distinct_row(open_db, sh.primary_table, payload="p-nolink")
    s = _create_distinct_row(open_db, sh.secondary_table, payload="s-nolink")

    with pytest.raises(Exception):
        open_db.update_interlink(primary_row=p, secondary_row=s, priority="highest")


def test_update_interlink_rejects_invalid_priority_type(open_db):
    sh = _pick_interlink_shape(open_db)
    if not _has_priority_support(open_db, sh):
        pytest.skip("Link table has no usable priority column")

    p = _create_distinct_row(open_db, sh.primary_table, payload="p-badpri2")
    s = _create_distinct_row(open_db, sh.secondary_table, payload="s-badpri2")

    open_db.interlink_rows(primary_row=p, secondary_row=s, priority=1)

    with pytest.raises(InputIntegrityError):
        open_db.update_interlink(primary_row=p, secondary_row=s, priority={"nope": 1})


def test_update_interlink_priority_reorders_ids_list_and_tuple_when_supported(open_db):
    sh = _pick_interlink_shape(open_db)
    if not _has_priority_support(open_db, sh):
        pytest.skip("Link table has no usable priority column")

    p = _create_distinct_row(open_db, sh.primary_table, payload="p-reorder")

    s1 = _create_distinct_row(open_db, sh.secondary_table, payload="s-re-1")
    s2 = _create_distinct_row(open_db, sh.secondary_table, payload="s-re-2")
    s3 = _create_distinct_row(open_db, sh.secondary_table, payload="s-re-3")

    open_db.interlink_rows(primary_row=p, secondary_row=s1, priority=1)
    open_db.interlink_rows(primary_row=p, secondary_row=s2, priority=2)
    open_db.interlink_rows(primary_row=p, secondary_row=s3, priority=3)

    ordered = [s2.row_id, s3.row_id, s1.row_id]  # desired order (highest->lowest after update)
    open_db.update_interlink_priority(primary_row=p, secondary_table=sh.secondary_table, ordered_ids=ordered)

    got = open_db.get_interlinked_rows(primary_row=p, secondary_table=sh.secondary_table)
    got_ids = [r.row_id for r in got]
    assert got_ids[:3] == ordered

    # Same but with tuple, different order.
    ordered2 = (s1.row_id, s2.row_id, s3.row_id)
    open_db.update_interlink_priority(primary_row=p, secondary_table=sh.secondary_table, ordered_ids=ordered2)

    got2 = open_db.get_interlinked_rows(primary_row=p, secondary_table=sh.secondary_table)
    got2_ids = [r.row_id for r in got2]
    assert got2_ids[:3] == list(ordered2)


def test_update_interlink_priority_length_mismatch_asserts(open_db):
    sh = _pick_interlink_shape(open_db)
    if not _has_priority_support(open_db, sh):
        pytest.skip("Link table has no usable priority column")

    p = _create_distinct_row(open_db, sh.primary_table, payload="p-assert")
    s1 = _create_distinct_row(open_db, sh.secondary_table, payload="s-a1")
    s2 = _create_distinct_row(open_db, sh.secondary_table, payload="s-a2")

    open_db.interlink_rows(primary_row=p, secondary_row=s1, priority=1)
    open_db.interlink_rows(primary_row=p, secondary_row=s2, priority=2)

    with pytest.raises(AssertionError):
        open_db.update_interlink_priority(primary_row=p, secondary_table=sh.secondary_table, ordered_ids=[s1.row_id])


def test_unlink_interlink_removes_link(open_db):
    sh = _pick_interlink_shape(open_db)
    p = _create_distinct_row(open_db, sh.primary_table, payload="p-unlink")
    s = _create_distinct_row(open_db, sh.secondary_table, payload="s-unlink")

    open_db.interlink_rows(primary_row=p, secondary_row=s, priority="not_set")
    assert open_db.get_interlink_row(primary_row=p, secondary_row=s) is not None

    open_db.unlink_interlink(primary_row=p, secondary_row=s)
    assert open_db.get_interlink_row(primary_row=p, secondary_row=s) is None


def test_unlink_interlink_errors_on_missing_link(open_db):
    sh = _pick_interlink_shape(open_db)
    p = _create_distinct_row(open_db, sh.primary_table, payload="p-unlink-miss")
    s = _create_distinct_row(open_db, sh.secondary_table, payload="s-unlink-miss")

    with pytest.raises(Exception):
        open_db.unlink_interlink(primary_row=p, secondary_row=s)


def test_unlink_all_removes_all_links(open_db):
    sh = _pick_interlink_shape(open_db)
    p = _create_distinct_row(open_db, sh.primary_table, payload="p-unlink-all")

    s1 = _create_distinct_row(open_db, sh.secondary_table, payload="s-u1")
    s2 = _create_distinct_row(open_db, sh.secondary_table, payload="s-u2")
    s3 = _create_distinct_row(open_db, sh.secondary_table, payload="s-u3")

    open_db.interlink_rows(primary_row=p, secondary_row=s1, priority="not_set")
    open_db.interlink_rows(primary_row=p, secondary_row=s2, priority="not_set")
    open_db.interlink_rows(primary_row=p, secondary_row=s3, priority="not_set")

    assert len(open_db.get_interlinked_rows(primary_row=p, secondary_table=sh.secondary_table)) == 3

    open_db.unlink_all(primary_row=p, secondary_table=sh.secondary_table)
    assert open_db.get_interlinked_rows(primary_row=p, secondary_table=sh.secondary_table) == []


def test_unlink_all_type_filter_removes_only_matching_type_when_supported(open_db):
    sh = _pick_interlink_shape(open_db)
    if not _has_type_support(open_db, sh):
        pytest.skip("Link table has no usable type column")

    p = _create_distinct_row(open_db, sh.primary_table, payload="p-unlink-type")

    s_a1 = _create_distinct_row(open_db, sh.secondary_table, payload="s-a1")
    s_a2 = _create_distinct_row(open_db, sh.secondary_table, payload="s-a2")
    s_b = _create_distinct_row(open_db, sh.secondary_table, payload="s-b")

    _ensure_interlink_type_registered(open_db, sh, "alpha")
    _ensure_interlink_type_registered(open_db, sh, "beta")

    open_db.interlink_rows(primary_row=p, secondary_row=s_a1, priority="not_set", type="alpha")
    open_db.interlink_rows(primary_row=p, secondary_row=s_a2, priority="not_set", type="alpha")
    open_db.interlink_rows(primary_row=p, secondary_row=s_b, priority="not_set", type="beta")

    open_db.unlink_all(primary_row=p, secondary_table=sh.secondary_table, type_filter="alpha")

    remaining = open_db.get_interlinked_rows(primary_row=p, secondary_table=sh.secondary_table)
    assert {r.row_id for r in remaining} == {s_b.row_id}


def test_unlink_all_type_filter_can_handle_multiple_links_per_pair_when_possible(open_db):
    sh = _pick_interlink_shape(open_db)
    if not _has_type_support(open_db, sh):
        pytest.skip("Link table has no usable type column")

    p = _create_distinct_row(open_db, sh.primary_table, payload="p-multi-pair")
    s = _create_distinct_row(open_db, sh.secondary_table, payload="s-multi-pair")

    _ensure_interlink_type_registered(open_db, sh, "alpha")
    _ensure_interlink_type_registered(open_db, sh, "beta")

    open_db.interlink_rows(primary_row=p, secondary_row=s, priority="not_set", type="alpha")

    try:
        open_db.interlink_rows(primary_row=p, secondary_row=s, priority="not_set", type="beta")
    except DatabaseIntegrityError:
        pytest.skip("Schema enforces uniqueness across row-pair even with differing type; cannot create multi-link pair")

    # Now unlink only one type; ensure the other remains.
    open_db.unlink_all(primary_row=p, secondary_table=sh.secondary_table, type_filter="alpha")

    # get_interlinked_rows returns secondary rows, so the secondary still appears linked.
    remaining = open_db.get_interlink_row(primary_row=p, secondary_row=s, onelink=False)
    assert isinstance(remaining, list)
    assert len(remaining) == 1
    assert remaining[0][sh.type_link_col] == "beta"
