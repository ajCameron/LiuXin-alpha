"""
Probe representative generated link tables for cardinality and ordering constraints.

Uses one mutable in-memory schema and requests foreign keys off to avoid creating
referenced rows. Allowed-type tables supply valid probe values. Tests cover
representative entries rather than every TOML pair, and do not reset shared state
between cases.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/test_frbr_schema_conformance_02_cardinality_constraints.py
"""

from __future__ import annotations

import pathlib
import sqlite3
from dataclasses import dataclass
from typing import Any, Iterable, Optional

import pytest

try:
    import tomllib  # py3.11+
except ImportError:  # pragma: no cover
    import tomli as tomllib  # type: ignore

from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import (
    database_generator as frbr_gen,
)
from LiuXin_alpha.databases.database_driver_plugins.SQL.utility_mixins import ColumnNameMixin
from LiuXin_alpha.utils.language_tools import plural_singular_mapper, singular_plural_mapper


# ---------------------------------------------------------------------------
# TOML parsing helpers
# ---------------------------------------------------------------------------


def _frbr_pkg_root() -> pathlib.Path:
    """
    Resolve the directory containing the imported FRBR generator module.

    Example:
        >>> _frbr_pkg_root().is_dir()
        True


    :return: Resolved generator package Path.
    """
    return pathlib.Path(frbr_gen.__file__).resolve().parent


def _load_toml(name: str) -> dict[str, Any]:
    """
    Read one package-relative TOML resource as UTF-8 with replacement decoding.

    Filesystem and TOML parsing errors propagate; no schema validation is performed
    here.

    Example:
        >>> isinstance(_load_toml('interlink_table_requests.toml'), dict)
        True


    :param name: Resource path joined to the generator package directory.
    :return: Parsed TOML mapping.
    """
    path = _frbr_pkg_root() / name
    return tomllib.loads(path.read_text(encoding="utf-8", errors="replace"))


def _normalize_requested_columns(entry: dict[str, Any]) -> Any:
    """
    Normalize requested-column aliases into all or a lowercase set.

    Absent requests default to priority; lists stringify and trim values, remove
    nullable and collapse to all if present. Other shapes and non-all strings raise
    TypeError. This helper does not validate individual column names or add type from
    allowed types.

    Example:
        >>> _normalize_requested_columns({'requested_columns': [' Type ', 'nullable']})
        {'type'}
        >>> _normalize_requested_columns({})
        {'priority'}


    :param entry: TOML entry mapping to interpret.
    :return: The string all or a normalized set of requested names.
    """

    requested = entry.get("requested_columns")
    if requested is None:
        requested = entry.get("requested_cols") or entry.get("columns")

    if requested is None:
        return {"priority"}
    if isinstance(requested, str):
        if requested.strip().lower() != "all":
            raise TypeError(f"requested_columns must be 'all' or a list; got: {requested!r}")
        return "all"
    if isinstance(requested, list):
        lowered = {str(x).strip().lower() for x in requested}
        if "nullable" in lowered:
            lowered.remove("nullable")
        if "all" in lowered:
            return "all"
        return lowered
    raise TypeError(f"requested_columns must be 'all' or a list; got: {type(requested)}")


@dataclass(frozen=True)
class _InterlinkPick:
    """
    Record an interlink candidate with source index, endpoints, cardinality and raw settings.

    Requested columns are normalized; allowed types and raw mapping are retained without
    deep copying. Frozen attributes do not freeze nested values.

    Example:
        >>> pick = _InterlinkPick(0, 'works', 'expressions', 'many_to_many', {'priority'}, None, {})
        >>> pick.link_type
        'many_to_many'
    """
    idx: int
    left: str
    right: str
    link_type: str
    requested_cols: Any
    allowed_types: Optional[Any]
    raw: dict[str, Any]


@dataclass(frozen=True)
class _IntralinkPick:
    """
    Record an intralink candidate with table, requested columns and symmetry settings.

    Keeps source index, raw mapping and allowed types; nested data remains mutable.

    Example:
        >>> _IntralinkPick(0, 'works', {'type'}, ['same_as'], False, {}).table
        'works'
    """
    idx: int
    table: str
    requested_cols: Any
    allowed_types: Optional[Any]
    symmetric: bool
    raw: dict[str, Any]


def _iter_interlinks() -> list[_InterlinkPick]:
    """
    Read usable interlink candidates from the shipped TOML in source order.

    Requires a list root; skips non-dicts and entries without left_table/left or
    right_table/right. Cardinality defaults from the file then to many_to_many; only
    surrounding whitespace is stripped.

    Example:
        >>> bool(_iter_interlinks())
        True


    :return: List of candidate records with original zero-based indices.
    """
    data = _load_toml("interlink_table_requests.toml")
    interlinks = data.get("interlinks", [])
    assert isinstance(interlinks, list)

    out: list[_InterlinkPick] = []
    for idx, entry in enumerate(interlinks):
        if not isinstance(entry, dict):
            continue
        left = entry.get("left_table") or entry.get("left")
        right = entry.get("right_table") or entry.get("right")
        if not left or not right:
            continue

        link_type = str(entry.get("link_type") or data.get("default_link_type") or "many_to_many").strip()
        requested_cols = _normalize_requested_columns(entry)
        allowed_types = entry.get("types") or entry.get("allowed_types")

        out.append(
            _InterlinkPick(
                idx=idx,
                left=str(left),
                right=str(right),
                link_type=link_type,
                requested_cols=requested_cols,
                allowed_types=allowed_types,
                raw=entry,
            )
        )
    return out


def _iter_intralinks() -> list[_IntralinkPick]:
    """
    Read usable intralink candidates, including string table-name shorthand.

    Uses table/table_name/name aliases, priority as the normalizer default and ordinary
    bool conversion for symmetric. Skips malformed or unnamed entries.

    Example:
        >>> bool(_iter_intralinks())
        True


    :return: List of candidate records in source order.
    """
    data = _load_toml("intralink_table_requests.toml")
    intralinks = data.get("intralinks", [])
    assert isinstance(intralinks, list)

    out: list[_IntralinkPick] = []
    for idx, entry in enumerate(intralinks):
        if isinstance(entry, str):
            entry = {"table": entry}
        if not isinstance(entry, dict):
            continue
        table = entry.get("table") or entry.get("table_name") or entry.get("name")
        if not table:
            continue

        requested_cols = _normalize_requested_columns(entry)
        allowed_types = entry.get("types") or entry.get("allowed_types")
        symmetric = bool(entry.get("symmetric", False))
        out.append(
            _IntralinkPick(
                idx=idx,
                table=str(table),
                requested_cols=requested_cols,
                allowed_types=allowed_types,
                symmetric=symmetric,
                raw=entry,
            )
        )
    return out


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------


def _pragma_cols(conn: sqlite3.Connection, table_name: str) -> set[str]:
    """
    Collect column names from PRAGMA table_info for a trusted table.

    The name is interpolated in backticks without escaping; an absent table produces an
    empty set.

    Example:
        >>> conn = sqlite3.connect(':memory:')
        >>> _ = conn.execute('CREATE TABLE demo (id INTEGER, value TEXT)')
        >>> _pragma_cols(conn, 'demo') == {'id', 'value'}
        True
        >>> conn.close()


    :param conn: Caller-owned SQLite connection; this helper does not close it.
    :param table_name: Trusted schema table name used in PRAGMA inspection.
    :return: Set of column-name strings.
    """
    return {row[1] for row in conn.execute(f"PRAGMA table_info(`{table_name}`);")}


def _existing_tables(conn: sqlite3.Connection) -> set[str]:
    """
    Read all SQLite table names, excluding views.

    Example:
        >>> conn = sqlite3.connect(':memory:')
        >>> _ = conn.execute('CREATE TABLE demo (id INTEGER)')
        >>> 'demo' in _existing_tables(conn)
        True
        >>> conn.close()


    :param conn: Caller-owned SQLite connection; this helper does not close it.
    :return: Set of table names from sqlite_master.
    """
    return {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table';")}


def _canonicalize_table_name(conn: sqlite3.Connection, candidate: str) -> str:
    """
    Strip the candidate and try exact, plural-mapped and singular-mapped table names.

    Preserves case and returns the stripped candidate if none exists.

    Example:
        >>> conn = sqlite3.connect(':memory:')
        >>> _ = conn.execute('CREATE TABLE works (work_id INTEGER)')
        >>> _canonicalize_table_name(conn, ' works ')
        'works'
        >>> conn.close()


    :param conn: Caller-owned SQLite connection; this helper does not close it.
    :param candidate: Candidate table name to normalize and match against the schema.
    :return: Matched table name or normalized fallback candidate.
    """

    candidate = str(candidate).strip()
    tables = _existing_tables(conn)
    if candidate in tables:
        return candidate

    # Try plural and singular variants.
    p = singular_plural_mapper(candidate)
    if p in tables:
        return p
    s = plural_singular_mapper(candidate)
    if s in tables:
        return s

    # As a last resort, keep the original candidate (tests will surface a clear failure).
    return candidate


def _pick_allowed_types(conn: sqlite3.Connection, link_table_name: str) -> list[str]:
    """
    Read at most twenty ordered type values from the preferred reference table.

    Prefers link_table__types even when empty; otherwise tries allowed_types__link_table
    with its conventionally derived column. Returns an empty list if neither exists.
    Names are trusted SQL identifiers.

    Example:
        >>> conn = sqlite3.connect(':memory:')
        >>> _ = conn.execute('CREATE TABLE links__types (type TEXT)')
        >>> _ = conn.executemany('INSERT INTO links__types VALUES (?)', [('b',), ('a',)])
        >>> _pick_allowed_types(conn, 'links')
        ['a', 'b']
        >>> conn.close()


    :param conn: Caller-owned SQLite connection; this helper does not close it.
    :param link_table_name: Link table whose modern or legacy type registry should be
        inspected.
    :return: List of stored type values ordered by the type column.
    """

    tables = _existing_tables(conn)
    out: list[str] = []

    types_table = f"{link_table_name}__types"
    if types_table in tables:
        out = [r[0] for r in conn.execute(f"SELECT type FROM `{types_table}` ORDER BY type LIMIT 20;").fetchall()]
        return out

    legacy = f"allowed_types__{link_table_name}"
    if legacy in tables:
        # legacy column is `{allowed_types__X}_type` where the base drops the trailing 's'
        base = legacy[:-1]
        col = f"{base}_type"
        out = [r[0] for r in conn.execute(f"SELECT `{col}` FROM `{legacy}` ORDER BY `{col}` LIMIT 20;").fetchall()]
        return out

    return []


def _insert_interlink_row(
    conn: sqlite3.Connection,
    *,
    link_table: str,
    col_base: str,
    left_table: str,
    right_table: str,
    left_id: int,
    right_id: int,
    type_val: Optional[str] = None,
    priority: Optional[int] = None,
) -> None:
    """
    Insert bound endpoint IDs and supported optional type/priority values without committing.

    Asserts endpoint columns exist. Optional values are omitted when None or when their
    column is absent; trusted table and column identifiers are interpolated.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_schema_conformance_02_cardinality_constraints.py


    :param conn: Caller-owned SQLite connection; this helper does not close it.
    :param link_table: Trusted destination interlink table name.
    :param col_base: Conventional column prefix for that table.
    :param left_table: Main-table name used to derive the left endpoint column.
    :param right_table: Main-table name used to derive the right endpoint column.
    :param left_id: Bound left endpoint ID.
    :param right_id: Bound right endpoint ID.
    :param type_val: Optional type value included only when the type column exists.
    :param priority: Optional priority included only when the priority column exists.
    :return: None; adds a pending row or propagates the insertion error.
    """
    cols = _pragma_cols(conn, link_table)

    left_row = plural_singular_mapper(left_table)
    right_row = plural_singular_mapper(right_table)
    left_col = f"{col_base}_{left_row}_id"
    right_col = f"{col_base}_{right_row}_id"

    assert left_col in cols and right_col in cols, (
        f"Expected link-table id columns missing for {link_table!r}:\n"
        f"  expected: {left_col!r}, {right_col!r}\n"
        f"  present: {sorted(cols)!r}"
    )

    insert_cols: list[str] = [left_col, right_col]
    params: list[Any] = [left_id, right_id]

    type_col = f"{col_base}_type"
    if type_val is not None and type_col in cols:
        insert_cols.append(type_col)
        params.append(type_val)

    prio_col = f"{col_base}_priority"
    if priority is not None and prio_col in cols:
        insert_cols.append(prio_col)
        params.append(priority)

    col_sql = ", ".join(f"`{c}`" for c in insert_cols)
    q_sql = ", ".join(["?"] * len(insert_cols))
    conn.execute(f"INSERT INTO `{link_table}` ({col_sql}) VALUES ({q_sql});", params)


def _intralink_table_and_base(conn: sqlite3.Connection, target_table: str) -> tuple[str, str]:
    """
    Resolve a target table and derive its conventional intralink name and prefix.

    Example:
        >>> conn = sqlite3.connect(':memory:')
        >>> _intralink_table_and_base(conn, 'works')
        ('work_work_intralinks', 'work_work_intralink')
        >>> conn.close()


    :param conn: Caller-owned SQLite connection; this helper does not close it.
    :param target_table: Main-table candidate passed through best-effort schema
        matching.
    :return: Intralink table name followed by its column prefix.
    """

    target_table_name = _canonicalize_table_name(conn, target_table)
    row = plural_singular_mapper(target_table_name)
    base = f"{row}_{row}_intralink"
    return f"{base}s", base


def _insert_intralink_row(
    conn: sqlite3.Connection,
    *,
    intralink_table: str,
    col_base: str,
    primary_id: int,
    secondary_id: int,
    type_val: str,
) -> None:
    """
    Assert required endpoint/type columns and insert a bound intralink row.

    Trusted identifiers are interpolated; insertion errors propagate and no commit is
    issued.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_schema_conformance_02_cardinality_constraints.py


    :param conn: Caller-owned SQLite connection; this helper does not close it.
    :param intralink_table: Trusted destination intralink table name.
    :param col_base: Conventional intralink column prefix.
    :param primary_id: Bound primary endpoint ID.
    :param secondary_id: Bound secondary endpoint ID.
    :param type_val: Bound relationship type value.
    :return: None; inserts a pending row on success.
    """
    cols = _pragma_cols(conn, intralink_table)

    p_col = f"{col_base}_primary_id"
    s_col = f"{col_base}_secondary_id"
    t_col = f"{col_base}_type"

    assert p_col in cols and s_col in cols and t_col in cols, (
        f"Expected intralink core columns missing for {intralink_table!r}:\n"
        f"  expected: {p_col!r}, {s_col!r}, {t_col!r}\n"
        f"  present: {sorted(cols)!r}"
    )

    conn.execute(
        f"INSERT INTO `{intralink_table}` (`{p_col}`, `{s_col}`, `{t_col}`) VALUES (?, ?, ?);",
        (primary_id, secondary_id, type_val),
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def frbr_conn() -> sqlite3.Connection:
    """
    Build and return a module-scoped in-memory FRBR schema.

    Enables foreign keys before generation. The fixture shares one mutable connection
    and supplies no explicit close, rollback or per-test reset.

    Example:
        Request frbr_conn as a pytest test argument; do not call the decorated fixture directly.


    :return: Open SQLite connection shared by tests in this module.
    """
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON;")
    frbr_gen.create_new_database(conn)
    return conn


# ---------------------------------------------------------------------------
# Interlink cardinality tests
# ---------------------------------------------------------------------------


def _find_first(
    items: Iterable[_InterlinkPick],
    *,
    want_type: str,
    pred,
) -> _InterlinkPick:
    """
    Return the first candidate matching a normalized cardinality and predicate.

    Lowercases and strips each candidate link_type, but uses want_type unchanged. Stops
    iteration on a match and raises AssertionError if no match exists.

    Example:
        >>> item = _InterlinkPick(0, 'a', 'b', ' MANY_TO_MANY ', set(), None, {})
        >>> _find_first([item], want_type='many_to_many', pred=lambda value: True) is item
        True


    :param items: Iterable of interlink candidates, consumed until a match.
    :param want_type: Already-normalized cardinality string to match.
    :param pred: Predicate called only for candidates with the requested cardinality.
    :return: First matching candidate object.
    """
    for it in items:
        if str(it.link_type).strip().lower() != want_type:
            continue
        if pred(it):
            return it
    raise AssertionError(f"No interlink entry found for link_type={want_type!r} matching predicate")


def _requested_has_type(req: Any) -> bool:
    """
    Recognize a type request only in all or a set containing type.

    Lists, frozensets and other representations are not accepted by the set branch.

    Example:
        >>> [_requested_has_type(x) for x in ('all', {'type'}, ['type'])]
        [True, True, False]


    :param req: Normalized requested-column value to inspect.
    :return: True for all or a set containing type; otherwise False.
    """
    if req == "all":
        return True
    if isinstance(req, set):
        return "type" in req
    return False


def test_frbr_schema_conformance_02_many_many_rejects_duplicate_pairs(frbr_conn: sqlite3.Connection) -> None:
    """
    Reject a duplicate endpoint pair on the first plain many-to-many candidate.

    Uses different priorities to isolate pair uniqueness; leaves the initial probe row
    in the shared connection.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_schema_conformance_02_cardinality_constraints.py::test_frbr_schema_conformance_02_many_many_rejects_duplicate_pairs


    :param frbr_conn: Module-scoped in-memory FRBR connection shared by the insertion
        probes.
    :return: None; failed expectations raise AssertionError.
    """

    interlinks = _iter_interlinks()
    plain = _find_first(
        interlinks,
        want_type="many_to_many",
        pred=lambda e: (not _requested_has_type(e.requested_cols)) and (not e.allowed_types),
    )

    left = _canonicalize_table_name(frbr_conn, plain.left)
    right = _canonicalize_table_name(frbr_conn, plain.right)
    link_table, col_base = ColumnNameMixin.get_interlink_table_name(left, right)

    # Insert probes with FK checks off: we only care about uniqueness constraints.
    frbr_conn.execute("PRAGMA foreign_keys = OFF;")

    _insert_interlink_row(
        frbr_conn,
        link_table=link_table,
        col_base=col_base,
        left_table=left,
        right_table=right,
        left_id=1,
        right_id=1,
        priority=0,
    )

    with pytest.raises(sqlite3.IntegrityError):
        _insert_interlink_row(
            frbr_conn,
            link_table=link_table,
            col_base=col_base,
            left_table=left,
            right_table=right,
            left_id=1,
            right_id=1,
            priority=1,
        )


def test_frbr_schema_conformance_02_many_many_non_exclusive_allows_multiple_types(frbr_conn: sqlite3.Connection) -> None:
    """
    Probe same-type rejection, different-type acceptance and role-scoped priority uniqueness.

    If only one allowed type is available, pytest.skip ends the case after duplicate
    rejection, so later priority checks also do not run. Priority checks otherwise run
    only when both relevant columns exist.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_schema_conformance_02_cardinality_constraints.py::test_frbr_schema_conformance_02_many_many_non_exclusive_allows_multiple_types


    :param frbr_conn: Module-scoped in-memory FRBR connection shared by the insertion
        probes.
    :return: None; failed expectations raise AssertionError.
    """

    interlinks = _iter_interlinks()
    role = _find_first(
        interlinks,
        want_type="many_to_many",
        pred=lambda e: _requested_has_type(e.requested_cols) or bool(e.allowed_types),
    )

    left = _canonicalize_table_name(frbr_conn, role.left)
    right = _canonicalize_table_name(frbr_conn, role.right)
    link_table, col_base = ColumnNameMixin.get_interlink_table_name(left, right)

    frbr_conn.execute("PRAGMA foreign_keys = OFF;")

    # Obtain a valid type if a types reference table exists (avoids trigger failures).
    allowed = _pick_allowed_types(frbr_conn, link_table)
    t1 = allowed[0] if allowed else "type_a"
    t2 = allowed[1] if len(allowed) > 1 else ("type_b" if not allowed else None)

    _insert_interlink_row(
        frbr_conn,
        link_table=link_table,
        col_base=col_base,
        left_table=left,
        right_table=right,
        left_id=10,
        right_id=20,
        type_val=t1,
        priority=0,
    )

    # Same pair + same type -> must fail.
    with pytest.raises(sqlite3.IntegrityError):
        _insert_interlink_row(
            frbr_conn,
            link_table=link_table,
            col_base=col_base,
            left_table=left,
            right_table=right,
            left_id=10,
            right_id=20,
            type_val=t1,
            priority=1,
        )

    # Same pair + different type -> allowed (when we can produce a distinct allowed type).
    if t2 is not None and t2 != t1:
        _insert_interlink_row(
            frbr_conn,
            link_table=link_table,
            col_base=col_base,
            left_table=left,
            right_table=right,
            left_id=10,
            right_id=20,
            type_val=t2,
            priority=2,
        )
    else:
        pytest.skip(
            f"Could not obtain two distinct allowed types for {link_table!r}; skipping multi-type acceptance check."
        )

    # If priority exists, (primary_id,type,priority) must be unique.
    cols = _pragma_cols(frbr_conn, link_table)
    prio_col = f"{col_base}_priority"
    type_col = f"{col_base}_type"
    if prio_col in cols and type_col in cols:
        # Re-use the same primary (left_id) but different right_id: ordering constraint should still fire.
        _insert_interlink_row(
            frbr_conn,
            link_table=link_table,
            col_base=col_base,
            left_table=left,
            right_table=right,
            left_id=10,
            right_id=21,
            type_val=t1,
            priority=7,
        )
        with pytest.raises(sqlite3.IntegrityError):
            _insert_interlink_row(
                frbr_conn,
                link_table=link_table,
                col_base=col_base,
                left_table=left,
                right_table=right,
                left_id=10,
                right_id=22,
                type_val=t1,
                priority=7,
            )


def test_frbr_schema_conformance_02_one_to_many_rejects_secondary_reuse(frbr_conn: sqlite3.Connection) -> None:
    """
    Reject reuse of a secondary ID across primaries in a representative one-to-many table.

    Also checks primary/priority uniqueness when the priority column exists.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_schema_conformance_02_cardinality_constraints.py::test_frbr_schema_conformance_02_one_to_many_rejects_secondary_reuse


    :param frbr_conn: Module-scoped in-memory FRBR connection shared by the insertion
        probes.
    :return: None; failed expectations raise AssertionError.
    """

    interlinks = _iter_interlinks()
    pick = _find_first(interlinks, want_type="one_to_many", pred=lambda e: True)

    left = _canonicalize_table_name(frbr_conn, pick.left)
    right = _canonicalize_table_name(frbr_conn, pick.right)
    link_table, col_base = ColumnNameMixin.get_interlink_table_name(left, right)

    frbr_conn.execute("PRAGMA foreign_keys = OFF;")

    # Right side (secondary) reused across two different left ids should violate UNIQUE.
    _insert_interlink_row(
        frbr_conn,
        link_table=link_table,
        col_base=col_base,
        left_table=left,
        right_table=right,
        left_id=1,
        right_id=100,
        priority=0,
    )

    with pytest.raises(sqlite3.IntegrityError):
        _insert_interlink_row(
            frbr_conn,
            link_table=link_table,
            col_base=col_base,
            left_table=left,
            right_table=right,
            left_id=2,
            right_id=100,
            priority=1,
        )

    # If priority exists, (primary_id,priority) should be unique.
    cols = _pragma_cols(frbr_conn, link_table)
    prio_col = f"{col_base}_priority"
    if prio_col in cols:
        _insert_interlink_row(
            frbr_conn,
            link_table=link_table,
            col_base=col_base,
            left_table=left,
            right_table=right,
            left_id=1,
            right_id=101,
            priority=5,
        )
        with pytest.raises(sqlite3.IntegrityError):
            _insert_interlink_row(
                frbr_conn,
                link_table=link_table,
                col_base=col_base,
                left_table=left,
                right_table=right,
                left_id=1,
                right_id=102,
                priority=5,
            )


def test_frbr_schema_conformance_02_many_to_one_rejects_primary_reuse(frbr_conn: sqlite3.Connection) -> None:
    """
    Reject reuse of a primary ID across secondaries in a representative many-to-one table.

    Also checks secondary/priority uniqueness when the priority column exists.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_schema_conformance_02_cardinality_constraints.py::test_frbr_schema_conformance_02_many_to_one_rejects_primary_reuse


    :param frbr_conn: Module-scoped in-memory FRBR connection shared by the insertion
        probes.
    :return: None; failed expectations raise AssertionError.
    """

    interlinks = _iter_interlinks()
    pick = _find_first(interlinks, want_type="many_to_one", pred=lambda e: True)

    left = _canonicalize_table_name(frbr_conn, pick.left)
    right = _canonicalize_table_name(frbr_conn, pick.right)
    link_table, col_base = ColumnNameMixin.get_interlink_table_name(left, right)

    frbr_conn.execute("PRAGMA foreign_keys = OFF;")

    _insert_interlink_row(
        frbr_conn,
        link_table=link_table,
        col_base=col_base,
        left_table=left,
        right_table=right,
        left_id=500,
        right_id=1,
        priority=0,
    )

    # Same left id, different right id -> must violate UNIQUE.
    with pytest.raises(sqlite3.IntegrityError):
        _insert_interlink_row(
            frbr_conn,
            link_table=link_table,
            col_base=col_base,
            left_table=left,
            right_table=right,
            left_id=500,
            right_id=2,
            priority=1,
        )

    # If priority exists, (secondary_id,priority) should be unique.
    cols = _pragma_cols(frbr_conn, link_table)
    prio_col = f"{col_base}_priority"
    if prio_col in cols:
        _insert_interlink_row(
            frbr_conn,
            link_table=link_table,
            col_base=col_base,
            left_table=left,
            right_table=right,
            left_id=501,
            right_id=1,
            priority=9,
        )
        with pytest.raises(sqlite3.IntegrityError):
            _insert_interlink_row(
                frbr_conn,
                link_table=link_table,
                col_base=col_base,
                left_table=left,
                right_table=right,
                left_id=502,
                right_id=1,
                priority=9,
            )


# ---------------------------------------------------------------------------
# Intralink cardinality tests
# ---------------------------------------------------------------------------


def test_frbr_schema_conformance_02_intralinks_pair_unique_and_no_self_link(frbr_conn: sqlite3.Connection) -> None:
    """
    Reject a duplicate same-type intralink and a self-link on the first intralink spec.

    Uses an ascending pair and a registry type when available to avoid unrelated
    ordering/type errors.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_schema_conformance_02_cardinality_constraints.py::test_frbr_schema_conformance_02_intralinks_pair_unique_and_no_self_link


    :param frbr_conn: Module-scoped in-memory FRBR connection shared by the insertion
        probes.
    :return: None; failed expectations raise AssertionError.
    """

    intralinks = _iter_intralinks()
    assert intralinks, "Expected intralink specs in intralink_table_requests.toml"

    pick = intralinks[0]
    target = _canonicalize_table_name(frbr_conn, pick.table)
    intralink_table, col_base = _intralink_table_and_base(frbr_conn, target)

    frbr_conn.execute("PRAGMA foreign_keys = OFF;")

    # Choose a valid type if a types table exists (avoids trigger failures).
    allowed = _pick_allowed_types(frbr_conn, intralink_table)
    t = allowed[0] if allowed else "same_as"

    # Insert an ordered pair to avoid symmetric-ordering triggers.
    _insert_intralink_row(
        frbr_conn,
        intralink_table=intralink_table,
        col_base=col_base,
        primary_id=1,
        secondary_id=2,
        type_val=t,
    )

    # Duplicate same edge + same type must be rejected.
    with pytest.raises(sqlite3.IntegrityError):
        _insert_intralink_row(
            frbr_conn,
            intralink_table=intralink_table,
            col_base=col_base,
            primary_id=1,
            secondary_id=2,
            type_val=t,
        )

    # Self-link must be rejected (CHECK constraint).
    with pytest.raises(sqlite3.IntegrityError):
        _insert_intralink_row(
            frbr_conn,
            intralink_table=intralink_table,
            col_base=col_base,
            primary_id=3,
            secondary_id=3,
            type_val=t,
        )
