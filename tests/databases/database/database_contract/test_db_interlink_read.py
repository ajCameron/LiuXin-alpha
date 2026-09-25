"""
Check interlink retrieval, type filtering, priority ordering, and projected values across database backends.

Discover schema-dependent shapes and skip unsupported cases. Helper fallbacks may
choose conventional type labels absent from an empty registry; their names do not
establish schema validity.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database/database_contract/test_db_interlink_read.py
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
    """
    Hold immutable table, endpoint, and optional priority/type column names for an interlink.

    Example:
        >>> sh = InterlinkShape('a', 'b', 'a_b', 'a_id', 'b_id', 'a_fk', 'b_fk', None, None)
        >>> (sh.primary_table, sh.type_link_col)
        ('a', None)
    """
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
    """
    Choose a payload column using name heuristics, without validating SQL types or constraints.

    Prefer keyword-bearing non-ID, non-time names, then base-name candidates, other safe
    names, non-ID names, and finally any remaining column. If exclusion removes
    everything, re-read cols and take its first element: this can select an excluded
    column or raise IndexError for an empty or exhausted iterable.

    Example:
        >>> _pick_text_like_column(['book_id', 'book_timestamp', 'book_title'], base='book', exclude=set())
        'book_title'
        >>> _pick_text_like_column(['book_id'], base='book', exclude={'book_id'})
        'book_id'


    :param cols: Column names in schema order; a reusable sequence is needed for the
        all-excluded fallback.
    :param base: Table base name used to construct candidate payload column names.
    :param exclude: Column names to exclude from the main candidate list.
    :return: Chosen column name.
    """
    cols_list = [c for c in cols if c not in exclude]
    if not cols_list:
        # Fall back to whatever we were given.
        return list(cols)[0]

    def is_id_like(c: str) -> bool:
        """
        Recognize id itself and names ending in _id or _fk.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database/database_contract/test_db_interlink_read.py


        :param c: Column name to classify case-insensitively.
        :return: True when the lowercased name matches one of the ID conventions.
        """
        cl = c.lower()
        return cl.endswith('_id') or cl.endswith('_fk') or cl == 'id'

    def is_time_like(c: str) -> bool:
        """
        Recognize timestamp/datestamp substrings and the supported epoch suffixes.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database/database_contract/test_db_interlink_read.py


        :param c: Column name to classify case-insensitively.
        :return: True when the lowercased name resembles a timestamp column.
        """
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


def _pick_interlink_shape(
    open_db,
    *,
    writable_secondary: bool = False,
) -> InterlinkShape:
    """
    Find a preferred or discovered interlink shape, favoring typed links and a permissive uniqueness heuristic.

    Try both directions of preferred pairs, then main-table pairs, retaining the first
    untyped fallback. With writable_secondary, reject languages as the secondary table.
    If no shape survives, attempt the legacy pytest.SkipTest fallback.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_interlink_read.py


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :param writable_secondary: Whether to exclude the read-only languages table from the
        secondary role.
    :return: First accepted typed shape, otherwise the saved untyped shape.
    """
    dw = open_db.driver_wrapper

    def supports_multiple_links_per_primary(sh: InterlinkShape) -> bool:
        """
        Inspect SQLite unique indexes for restrictions on multiple secondaries per primary.

        Reject an index containing the primary endpoint but not the secondary when its
        columns are limited to primary, type, and priority. Missing query support or
        index-list errors allow the shape; individual index-info failures are ignored.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database/database_contract/test_db_interlink_read.py


        :param sh: Discovered table pair and its interlink column names.
        :return: Whether this heuristic permits the shape; not a proof of link cardinality.
        """

        # If direct SQL isn't available, fail open (other backends may not expose PRAGMA).
        if getattr(open_db, "get", None) is None:
            return True

        try:
            idx_rows = open_db.get(f"PRAGMA index_list(`{sh.link_table}`);")
        except Exception:
            return True

        # A UNIQUE index that includes the primary link column but *not* the secondary link
        # column indicates a many-one / one-one style constraint on the primary side.
        permitted = {sh.primary_link_col}
        if sh.type_link_col is not None:
            permitted.add(sh.type_link_col)
        if sh.priority_link_col is not None:
            permitted.add(sh.priority_link_col)

        for row in idx_rows:
            # sqlite returns: (seq, name, unique, origin, partial)
            if len(row) < 3:
                continue
            idx_name = row[1]
            is_unique = int(row[2]) == 1
            if not is_unique:
                continue
            try:
                col_rows = open_db.get(f"PRAGMA index_info(`{idx_name}`);")
            except Exception:
                continue
            cols = {r[2] for r in col_rows if len(r) >= 3}

            if sh.primary_link_col in cols and sh.secondary_link_col not in cols:
                # Only treat it as constraining if it's a "simple" uniqueness restriction.
                # (If other columns are included, it may still allow multiple links.)
                if cols.issubset(permitted):
                    return False

        return True

    def resolve_pair(a: str, b: str) -> Optional[InterlinkShape]:
        """
        Resolve one directed table pair into endpoint and link columns.

        Return None without a link-table name. Required ID/endpoint lookup errors propagate;
        optional priority/type lookup errors become None.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database/database_contract/test_db_interlink_read.py


        :param a: Candidate primary table name.
        :param b: Candidate secondary table name.
        :return: InterlinkShape for the pair, or None when no link table is found.
        """
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
        if writable_secondary and sh.secondary_table in {"languages"}:
            continue
        if not supports_multiple_links_per_primary(sh):
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
            if writable_secondary and sh.secondary_table in {"languages"}:
                continue
            if not supports_multiple_links_per_primary(sh):
                continue
            if sh.type_link_col is not None:
                return sh
            if best is None:
                best = sh

    if best is not None:
        return best

    raise pytest.SkipTest("No interlinkable table pair found in this schema")  # pragma: no cover



def _pick_allowed_type_for_shape(open_db, sh: "InterlinkShape", *, preferred: str = "authors") -> Optional[str]:
    """
    Choose a registered type, preferring the requested label and modern registry.

    Use the preferred label if the chosen registry is empty or absent; this does not
    ensure it satisfies an empty registry’s restrictions.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_interlink_read.py


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :param sh: Discovered table pair and its interlink column names.
    :param preferred: Preferred type label, also used when no nonempty registry supplies
        a choice.
    :return: None for an untyped shape, otherwise the preferred or first sorted registry
        value.
    """

    if sh.type_link_col is None:
        return None

    existing = set(open_db.get_tables(force_refresh=True))

    def fresh_get(stmt: str):
        """
        Query through a fresh connection and attempt to close it in finally.

        Query errors propagate; ordinary close errors are suppressed.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database/database_contract/test_db_interlink_read.py


        :param stmt: Read statement executed on a fresh driver connection.
        :return: Driver get result requested with all=True.
        """
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



def _pick_two_allowed_types_for_shape(
    open_db,
    sh: "InterlinkShape",
    *,
    preferred: str = "authors",
) -> Optional[Tuple[str, str]]:
    """
    Choose two distinct registry labels, trying the legacy registry if modern values are empty.

    If neither registry supplies values, return preferred plus editors or roles, even
    when an empty registry exists. Otherwise deduplicate in order, prefer the requested
    first label, and require a different second label.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_interlink_read.py


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :param sh: Discovered table pair and its interlink column names.
    :param preferred: Preferred type label, also used when no nonempty registry supplies
        a choice.
    :return: Two labels, or None for an untyped shape or a registry with only one
        distinct value.
    """

    if sh.type_link_col is None:
        return None

    existing = set(open_db.get_tables(force_refresh=True))

    def fresh_get(stmt: str):
        """
        Query through a fresh connection and attempt to close it in finally.

        Query errors propagate; ordinary close errors are suppressed.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database/database_contract/test_db_interlink_read.py


        :param stmt: Read statement executed on a fresh driver connection.
        :return: Driver get result requested with all=True.
        """
        conn = open_db.driver.get_connection()
        try:
            return conn.get(stmt, all=True)
        finally:
            try:
                conn.close()
            except Exception:
                pass

    types: list[str] = []

    types_table = f"{sh.link_table}__types"
    if types_table in existing:
        rows = fresh_get(f"SELECT `type` FROM `{types_table}` ORDER BY `type`;")
        types = [r[0] for r in rows if r and r[0] is not None]

    legacy_table = f"allowed_types__{sh.link_table}"
    if not types and legacy_table in existing:
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

    # No registry present -> use two conventional strings.
    if not types:
        alt = "editors" if preferred != "editors" else "roles"
        return (preferred, alt)

    # De-dupe while preserving ordering.
    seen: set[str] = set()
    deduped: list[str] = []
    for t in types:
        if t in seen:
            continue
        seen.add(t)
        deduped.append(t)

    if not deduped:
        return None

    first = preferred if preferred in deduped else deduped[0]
    second = next((t for t in deduped if t != first), None)
    if second is None:
        return None
    return (first, second)



def _create_distinct_row(open_db, table: str, *, payload: str) -> Row:
    """
    Create a payload-bearing row, or select a seeded languages row by CRC32 offset.

    For writable tables, prefer an available scratch column; otherwise exclude the ID
    and any discoverable foreign-key columns and use a name heuristic. Scratch and
    foreign-key discovery errors are tolerated. The languages branch performs no write,
    and different payloads can select the same row.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_interlink_read.py


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :param table: Trusted test table name, interpolated into SQL where needed.
    :param payload: Unicode text used to distinguish test rows; languages uses it as a
        deterministic selection key.
    :return: Synced Row, or the existing Row selected from the nonempty languages table.
    """
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
    """
    Create an interlink with priority and, when supported and supplied, a type.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_interlink_read.py


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :param sh: Discovered table pair and its interlink column names.
    :param primary: Saved primary Row.
    :param secondary: Saved secondary Row.
    :param priority: Priority value forwarded unchanged to Database.interlink_rows.
    :param link_type: Type label to register or pass to the link operation.
    :return: Result returned by Database.interlink_rows.
    """
    kwargs = {"priority": priority}
    if sh.type_link_col is not None and link_type is not None:
        kwargs["type"] = link_type
    return open_db.interlink_rows(primary_row=primary, secondary_row=secondary, **kwargs)


def _pick_unique_column_for_table(open_db, table: str) -> Optional[str]:
    """
    Find a non-generic column name that occurs in only one table’s headings.

    Exclude ID, UUID, path, sort, and date/time-style suffixes. Uniqueness concerns
    column names across tables, not SQL UNIQUE constraints.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_interlink_read.py


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :param table: Trusted test table name, interpolated into SQL where needed.
    :return: First acceptable column name in table order, or None.
    """
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
    """
    Check an unlinked pair returns None.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_interlink_read.py::test_get_interlink_row_returns_none_if_unlinked


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :return: None; failed expectations raise AssertionError.
    """
    sh = _pick_interlink_shape(open_db)

    p = _create_distinct_row(open_db, sh.primary_table, payload="p-unlinked")
    s = _create_distinct_row(open_db, sh.secondary_table, payload="s-unlinked")

    got = open_db.get_interlink_row(primary_row=p, secondary_row=s)
    assert got is None


@pytest.mark.parametrize("payload", UNICODE_TORTURE_PAYLOADS)
def test_get_interlink_row_roundtrips_one_link(open_db, payload: str):
    """
    Create one link and check its table, endpoint IDs, singleton-list form, and available type/priority fields.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_interlink_read.py::test_get_interlink_row_roundtrips_one_link


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :param payload: Unicode text used to distinguish test rows; languages uses it as a
        deterministic selection key.
    :return: None; failed expectations raise AssertionError.
    """
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
        assert got[sh.type_link_col] == link_type
    if sh.priority_link_col is not None:
        assert isinstance(got[sh.priority_link_col], (int, float))


def test_get_interlink_row_errors_on_multiple_links_when_possible(open_db):
    """
    Check singular retrieval rejects multiple typed links while list retrieval retains them.

    Skip absent type support, insufficient labels, or a second insertion rejected by
    uniqueness constraints.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_interlink_read.py::test_get_interlink_row_errors_on_multiple_links_when_possible


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Create three links, check their count and primary IDs, and check ascending priorities when all returned links expose that field.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_interlink_read.py::test_get_interlink_rows_returns_all_links_and_sorts_by_priority_if_present


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Check three retrieved Rows belong to the secondary table and exercise descending priority order when available.

    If secondary IDs are not in the expected order, inspect link-row priorities and
    assert their descending order when that field is exposed.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_interlink_read.py::test_get_interlinked_rows_returns_secondary_rows_in_priority_order_when_present


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :return: None; failed expectations raise AssertionError.
    """
    sh = _pick_interlink_shape(open_db)
    link_type = _pick_allowed_type_for_shape(open_db, sh, preferred="authors")

    p = _create_distinct_row(open_db, sh.primary_table, payload="p-linked")

    s_hi = _create_distinct_row(open_db, sh.secondary_table, payload="s-hi")
    s_mid = _create_distinct_row(open_db, sh.secondary_table, payload="s-mid")
    s_lo = _create_distinct_row(open_db, sh.secondary_table, payload="s-lo")

    _interlink(open_db, sh, primary=p, secondary=s_lo, priority=1, link_type=link_type)
    _interlink(open_db, sh, primary=p, secondary=s_mid, priority=5, link_type=link_type)
    _interlink(open_db, sh, primary=p, secondary=s_hi, priority=9, link_type=link_type)

    linked = open_db.get_interlinked_rows(primary_row=p, secondary_table=sh.secondary_table)
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
    """
    Check two type filters return the exact secondary-ID sets assigned to their labels; skip missing type support or labels.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_interlink_read.py::test_get_interlinked_rows_type_filter_when_available


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :return: None; failed expectations raise AssertionError.
    """
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
    # This contract writes distinct sentinel values into the secondary rows, so
    # seeded reference tables such as ``languages`` are not valid candidates.
    """
    Write two Greek values and check projection returns their exact set.

    Skip when no eligible column name exists; select a shape whose secondary table is
    not languages.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_interlink_read.py::test_get_interlink_values_returns_set_when_unique_column_available


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :return: None; failed expectations raise AssertionError.
    """
    sh = _pick_interlink_shape(open_db, writable_secondary=True)
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

    # Use a second allowed type if possible; otherwise just re-use the first.
    if sh.type_link_col is not None:
        two = _pick_two_allowed_types_for_shape(open_db, sh, preferred=link_type or "authors")
        link_type_2 = two[1] if two is not None else link_type
    else:
        link_type_2 = None

    _interlink(open_db, sh, primary=p, secondary=s2, priority=2, link_type=link_type_2)

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
