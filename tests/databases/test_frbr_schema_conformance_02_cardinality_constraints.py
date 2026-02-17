"""FRBR schema conformance suite (02): link-table cardinality constraints.

This suite performs *behavioral* verification of generated link tables by inserting rows and
asserting that SQLite constraints reject (or allow) specific patterns.

Scope (part 02):
  1) Interlinks: verify the core cardinality contracts for the three TOML link types in use:
       - many_to_many           -> internal many_many OR many_many_non_exclusive
       - one_to_many            -> internal one_many
       - many_to_one            -> internal many_one
     We do this by selecting representative pairs from interlink_table_requests.toml.

  2) Intralinks: verify pair-uniqueness and the "no self link" CHECK constraint.

Design notes:
  - We intentionally run the insertion probes with PRAGMA foreign_keys=OFF so we don't need
    to create referenced rows in the main tables. This suite is about *link-table* constraints,
    not referential integrity.
  - Allowed-type guards ("__types" + triggers) may still be active even when foreign keys are
    disabled. Where present, we read a valid type value from the relevant `{table}__types`
    table so our inserts focus on cardinality rather than type validation.
  - Where a link table has a role-style `type` column (many_many_non_exclusive), we verify:
       * same (A,B) with different type -> allowed
       * same (A,B) with same type      -> rejected (UNIQUE)
       * same (primary,type,priority)   -> rejected when priority exists
    If we cannot obtain two distinct allowed type values, we skip only the "different type"
    assertion (the duplicate-same-type checks still run).
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
    return pathlib.Path(frbr_gen.__file__).resolve().parent


def _load_toml(name: str) -> dict[str, Any]:
    path = _frbr_pkg_root() / name
    return tomllib.loads(path.read_text(encoding="utf-8", errors="replace"))


def _normalize_requested_columns(entry: dict[str, Any]) -> Any:
    """Return requested_cols as either 'all' or set[str].

    This intentionally mirrors the permissive TOML parsing in the generator.
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
    idx: int
    left: str
    right: str
    link_type: str
    requested_cols: Any
    allowed_types: Optional[Any]
    raw: dict[str, Any]


@dataclass(frozen=True)
class _IntralinkPick:
    idx: int
    table: str
    requested_cols: Any
    allowed_types: Optional[Any]
    symmetric: bool
    raw: dict[str, Any]


def _iter_interlinks() -> list[_InterlinkPick]:
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
    return {row[1] for row in conn.execute(f"PRAGMA table_info(`{table_name}`);")}


def _existing_tables(conn: sqlite3.Connection) -> set[str]:
    return {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table';")}


def _canonicalize_table_name(conn: sqlite3.Connection, candidate: str) -> str:
    """Best-effort match for a main-table name.

    Mirrors the generator's fuzzy singular/plural mapping.
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
    """Return up to a handful of allowed type values for a link table.

    Preference order:
      1) FRBR reference table `{link_table}__types`
      2) legacy allowed-types table `allowed_types__{link_table}`
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
    """Return (intralink_table_name, intralink_col_base) for a target table."""

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
    """Build a fresh FRBR schema once for this module."""
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
    for it in items:
        if str(it.link_type).strip().lower() != want_type:
            continue
        if pred(it):
            return it
    raise AssertionError(f"No interlink entry found for link_type={want_type!r} matching predicate")


def _requested_has_type(req: Any) -> bool:
    if req == "all":
        return True
    if isinstance(req, set):
        return "type" in req
    return False


def test_frbr_schema_conformance_02_many_many_rejects_duplicate_pairs(frbr_conn: sqlite3.Connection) -> None:
    """Pick a plain many-to-many interlink (no type column requested) and assert pair uniqueness."""

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
    """Pick a role-style many-to-many and assert (A,B,type) uniqueness behaviour."""

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
    """Pick a one-to-many and assert the secondary side cannot be linked to multiple primaries."""

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
    """Pick a many-to-one and assert the primary side cannot be linked to multiple secondaries."""

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
    """Pick a representative intralink table and verify uniqueness + no-self-link constraints."""

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
