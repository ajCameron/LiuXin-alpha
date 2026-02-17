"""FRBR schema conformance suite (03): strict type enumerations.

This suite validates that TOML-declared `types` / `allowed_types` enumerations are:

  1) Materialised as `{link_table}__types` reference tables.
  2) Seeded with *exactly* the expanded values from TOML (strict equality).
  3) Enforced by the generated `__type_guard_{insert,update}` triggers.

Design notes:
  - The FRBR generator supports two special placeholders inside `types`:
       * insert_marc_roles
       * insert_known_hash_types
    We mirror the generator's expansion logic so the tests remain deterministic.

  - Where we probe trigger behaviour, we run with `PRAGMA foreign_keys=OFF`.
    That keeps these tests focused on type enforcement rather than referential integrity.

If strict equality ever fails, it indicates drift between:
  - the TOML spec (source of truth), and
  - the generated schema's seeded reference tables.
"""

from __future__ import annotations

import pathlib
import sqlite3
from dataclasses import dataclass
from typing import Any, Optional

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
# TOML helpers (mirrors generator logic)
# ---------------------------------------------------------------------------


def _frbr_pkg_root() -> pathlib.Path:
    return pathlib.Path(frbr_gen.__file__).resolve().parent


def _load_toml(name: str) -> dict[str, Any]:
    path = _frbr_pkg_root() / name
    return tomllib.loads(path.read_text(encoding="utf-8", errors="replace"))


def _expand_types(items: list[Any], *, idx: int, kind: str) -> list[str]:
    """Expand TOML `types` items exactly as the FRBR generator does."""

    raw_items = [str(x).strip() for x in items if str(x).strip()]

    expanded: list[str] = []
    for item in raw_items:
        key = item.strip()
        if key.lower() == "insert_marc_roles":
            from LiuXin_alpha.constants.marc_relator_dicts import MARC_ROLE_DESC

            expanded.extend(sorted(MARC_ROLE_DESC.keys()))
            continue
        if key.lower() == "insert_known_hash_types":
            import hashlib

            expanded.extend(sorted(hashlib.algorithms_guaranteed))
            continue
        if key.lower().startswith("insert_"):
            raise TypeError(f"Unknown types placeholder {key!r} in {kind} {idx}")
        expanded.append(key)

    seen: set[str] = set()
    out: list[str] = []
    for v in expanded:
        if v in seen:
            continue
        seen.add(v)
        out.append(v)

    if not out:
        raise TypeError(f"types list is empty after expansion in {kind} {idx}")
    return out


def _canonicalize_table_name(conn: sqlite3.Connection, candidate: str) -> str:
    """Best-effort mirror of generator plural/singular matching."""

    cand = str(candidate).strip().lower()
    existing = {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
    }
    if cand in existing:
        return cand
    alt = singular_plural_mapper(cand)
    if alt in existing:
        return alt
    return cand


@dataclass(frozen=True)
class _InterlinkTypesSpec:
    idx: int
    left: str
    right: str
    types: list[str]


@dataclass(frozen=True)
class _IntralinkTypesSpec:
    idx: int
    table: str
    types: list[str]
    symmetric: bool
    symmetric_types: Optional[list[str]]


def _iter_interlink_type_specs() -> list[_InterlinkTypesSpec]:
    data = _load_toml("interlink_table_requests.toml")
    interlinks = data.get("interlinks", [])
    assert isinstance(interlinks, list)

    out: list[_InterlinkTypesSpec] = []
    for idx, entry in enumerate(interlinks):
        if not isinstance(entry, dict):
            continue

        left = entry.get("left_table") or entry.get("left") or entry.get("table1") or entry.get("a")
        right = entry.get("right_table") or entry.get("right") or entry.get("table2") or entry.get("b")
        if not left or not right:
            continue

        allowed_types = entry.get("allowed_types") or entry.get("types")
        if allowed_types is None:
            continue
        if not isinstance(allowed_types, list):
            raise TypeError(f"allowed_types must be a list in interlinks[{idx}]")

        out.append(
            _InterlinkTypesSpec(
                idx=idx,
                left=str(left),
                right=str(right),
                types=_expand_types(allowed_types, idx=idx, kind="interlinks"),
            )
        )

    return out


def _iter_intralink_type_specs() -> list[_IntralinkTypesSpec]:
    data = _load_toml("intralink_table_requests.toml")
    intralinks = data.get("intralinks", [])
    assert isinstance(intralinks, list)

    out: list[_IntralinkTypesSpec] = []
    for idx, entry in enumerate(intralinks):
        if isinstance(entry, str):
            entry = {"table": entry}
        if not isinstance(entry, dict):
            continue

        table = entry.get("table") or entry.get("table_name") or entry.get("name")
        if not table:
            continue

        allowed_types = entry.get("allowed_types") or entry.get("types")
        if allowed_types is None:
            continue
        if not isinstance(allowed_types, list):
            raise TypeError(f"types must be a list in intralinks[{idx}]")

        symmetric = bool(entry.get("symmetric", False))
        sym_types = entry.get("symmetric_types") or entry.get("symmetric_type")
        sym_types_list: Optional[list[str]] = None
        if isinstance(sym_types, list):
            sym_types_list = [str(x).strip() for x in sym_types if str(x).strip()]
            if not sym_types_list:
                sym_types_list = None

        out.append(
            _IntralinkTypesSpec(
                idx=idx,
                table=str(table),
                types=_expand_types(allowed_types, idx=idx, kind="intralinks"),
                symmetric=symmetric,
                symmetric_types=sym_types_list,
            )
        )

    return out


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def frbr_schema_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON;")
    frbr_gen.create_new_database(conn)
    return conn


# ---------------------------------------------------------------------------
# 03a) Strict equality: the set of __types tables matches TOML exactly
# ---------------------------------------------------------------------------


def _expected_types_tables(conn: sqlite3.Connection) -> dict[str, set[str]]:
    """Return mapping of types_table_name -> expected set of type values (strict)."""

    expected: dict[str, set[str]] = {}

    # Interlink types
    for spec in _iter_interlink_type_specs():
        left = _canonicalize_table_name(conn, spec.left)
        right = _canonicalize_table_name(conn, spec.right)

        if left == right:
            # Generator ignores self-link interlinks.
            continue

        interlink_table, _col_base = ColumnNameMixin.get_interlink_table_name(left, right)
        types_table = f"{interlink_table}__types"
        expected.setdefault(types_table, set()).update(spec.types)

    # Intralink types
    for spec in _iter_intralink_type_specs():
        main_table = _canonicalize_table_name(conn, spec.table)
        target_row = plural_singular_mapper(main_table)
        col_base = f"{target_row}_{target_row}_intralink"
        intralink_table = f"{col_base}s"
        types_table = f"{intralink_table}__types"
        expected.setdefault(types_table, set()).update(spec.types)

    return expected


def test_frbr_schema_conformance_types_tables_set_is_strict(frbr_schema_conn: sqlite3.Connection) -> None:
    expected = _expected_types_tables(frbr_schema_conn)

    actual_tables = {
        row[0]
        for row in frbr_schema_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%__types';"
        ).fetchall()
    }

    assert actual_tables == set(expected.keys()), (
        "Mismatch in __types table set.\n"
        f"  expected-only: {sorted(set(expected.keys()) - actual_tables)!r}\n"
        f"  actual-only: {sorted(actual_tables - set(expected.keys()))!r}"
    )


# ---------------------------------------------------------------------------
# 03b) Strict equality: each __types table contains exactly the expected values
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("types_table", ["__ALL__"])
def test_frbr_schema_conformance_types_tables_contents_strict(
    frbr_schema_conn: sqlite3.Connection, types_table: str
) -> None:
    # Param trick keeps pytest output tidy while still running a single cohesive test.
    assert types_table == "__ALL__"

    expected = _expected_types_tables(frbr_schema_conn)

    failures: list[str] = []
    for table_name, exp_set in sorted(expected.items()):
        rows = frbr_schema_conn.execute(f"SELECT type FROM `{table_name}` ORDER BY type;").fetchall()
        got_set = {r[0] for r in rows}
        if got_set != exp_set:
            failures.append(
                "\n".join(
                    [
                        f"Types table mismatch for {table_name!r}:",
                        f"  missing: {sorted(exp_set - got_set)!r}",
                        f"  extra:   {sorted(got_set - exp_set)!r}",
                        f"  expected_count: {len(exp_set)}  actual_count: {len(got_set)}",
                    ]
                )
            )

    assert not failures, "\n\n".join(failures)


# ---------------------------------------------------------------------------
# 03c) Guard triggers exist and enforce the strict enumerations
# ---------------------------------------------------------------------------


def _trigger_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='trigger' AND name=?;",
        (name,),
    ).fetchone()
    return row is not None


def _pick_one(values: set[str]) -> str:
    v = sorted(values)[0]
    assert isinstance(v, str) and v
    return v


def test_frbr_schema_conformance_type_guards_reject_unknown_values(frbr_schema_conn: sqlite3.Connection) -> None:
    expected = _expected_types_tables(frbr_schema_conn)

    # Focus on type guard triggers rather than referential integrity.
    frbr_schema_conn.execute("PRAGMA foreign_keys = OFF;")

    failures: list[str] = []

    for types_table, exp_types in sorted(expected.items()):
        link_table = types_table[: -len("__types")]

        trig_ins = f"{link_table}__type_guard_insert"
        trig_upd = f"{link_table}__type_guard_update"
        if not _trigger_exists(frbr_schema_conn, trig_ins) or not _trigger_exists(frbr_schema_conn, trig_upd):
            failures.append(
                f"Missing type-guard triggers for {link_table!r}: insert={_trigger_exists(frbr_schema_conn, trig_ins)!r} update={_trigger_exists(frbr_schema_conn, trig_upd)!r}"
            )
            continue

        cols = [
            r[1]
            for r in frbr_schema_conn.execute(f"PRAGMA table_info(`{link_table}`);").fetchall()
        ]
        type_cols = [c for c in cols if c.endswith("_type")]
        if len(type_cols) != 1:
            failures.append(
                f"Expected exactly one *_type column in {link_table!r}, got {type_cols!r}"
            )
            continue
        type_col = type_cols[0]

        info = frbr_schema_conn.execute(f"PRAGMA table_info(`{link_table}`);").fetchall()

        insert_cols: list[str] = []
        insert_vals: list[Any] = []

        # Stable per-table numeric seed (doesn't need to be reproducible across processes).
        seed = abs(hash(link_table)) % 1_000_000
        for cid, name, coltype, notnull, dflt, pk in info:
            if pk:
                continue
            if notnull and dflt is None:
                insert_cols.append(name)
                if name == type_col:
                    insert_vals.append("___NOT_A_VALID_TYPE___")
                else:
                    insert_vals.append(seed + int(cid) + 1)

        if type_col not in insert_cols:
            insert_cols.append(type_col)
            insert_vals.append("___NOT_A_VALID_TYPE___")

        cols_sql = ", ".join([f"`{c}`" for c in insert_cols])
        qmarks = ", ".join(["?"] * len(insert_cols))

        with pytest.raises(sqlite3.IntegrityError):
            frbr_schema_conn.execute(
                f"INSERT INTO `{link_table}` ({cols_sql}) VALUES ({qmarks});",
                insert_vals,
            )

        valid = _pick_one(exp_types)
        ok_vals = list(insert_vals)
        ok_vals[insert_cols.index(type_col)] = valid
        frbr_schema_conn.execute(
            f"INSERT INTO `{link_table}` ({cols_sql}) VALUES ({qmarks});",
            ok_vals,
        )

    assert not failures, "\n".join(failures)
