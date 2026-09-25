"""
Compare generated type registries with expanded TOML values and probe insert guards.

Requires expected registry sets and checks both insert/update trigger names.
Behavioral probes execute inserts only; update rejection is not directly tested.
Uses one shared connection without per-test rollback.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/test_frbr_schema_conformance_03_type_reference_tables.py
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


def _expand_types(items: list[Any], *, idx: int, kind: str) -> list[str]:
    """
    Expand MARC-role and guaranteed-hash placeholders, then deduplicate in first-seen order.

    Stringifies and strips nonblank items, recognizes placeholders case-insensitively,
    and retains literal value case. Unknown insert_ placeholders or an empty expanded
    result raise TypeError with source context.

    Example:
        >>> _expand_types([' a ', 'a', 'B'], idx=0, kind='interlinks')
        ['a', 'B']


    :param items: Raw TOML type items to stringify and expand.
    :param idx: Source entry index included in errors.
    :param kind: Spec family label included in errors.
    :return: Ordered list of unique expanded type strings.
    """

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
    """
    Lowercase and strip the candidate, then try exact and singular/plural mapped names.

    Returns the normalized candidate if neither exists; does not create or validate a
    table.

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
    """
    Store an interlink source index, endpoint names and expanded type list.

    Frozen attributes retain a mutable list of type strings.

    Example:
        >>> _InterlinkTypesSpec(0, 'works', 'agents', ['aut']).types
        ['aut']
    """
    idx: int
    left: str
    right: str
    types: list[str]


@dataclass(frozen=True)
class _IntralinkTypesSpec:
    """
    Store an intralink source index, table, expanded types and symmetry settings.

    The optional symmetric-type list and expanded-type list remain mutable.

    Example:
        >>> _IntralinkTypesSpec(0, 'works', ['same_as'], True, None).symmetric
        True
    """
    idx: int
    table: str
    types: list[str]
    symmetric: bool
    symmetric_types: Optional[list[str]]


def _iter_interlink_type_specs() -> list[_InterlinkTypesSpec]:
    """
    Read interlink entries that declare allowed types and expand their values.

    Accepts long, short, table1/table2 and a/b endpoint aliases. Skips malformed,
    unnamed or untyped entries; a present non-list type declaration raises TypeError.
    Retains original indices.

    Example:
        >>> bool(_iter_interlink_type_specs())
        True


    :return: List of expanded interlink type specifications.
    """
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
    """
    Read intralink type declarations, string shorthand and optional symmetric-type lists.

    Skips malformed, unnamed or untyped entries; rejects non-list type declarations.
    Ordinary bool conversion sets symmetric. Symmetric-type strings are stripped and
    blank entries removed without placeholder expansion.

    Example:
        >>> bool(_iter_intralink_type_specs())
        True


    :return: List of expanded intralink type specifications.
    """
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
    """
    Build and return a module-scoped in-memory FRBR schema.

    Enables foreign keys before generation. The fixture shares one mutable connection
    and supplies no explicit close, rollback or per-test reset.

    Example:
        Request frbr_schema_conn as a pytest test argument; do not call the decorated fixture directly.


    :return: Open SQLite connection shared by tests in this module.
    """
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON;")
    frbr_gen.create_new_database(conn)
    return conn


# ---------------------------------------------------------------------------
# 03a) Strict equality: the set of __types tables matches TOML exactly
# ---------------------------------------------------------------------------


def _expected_types_tables(conn: sqlite3.Connection) -> dict[str, set[str]]:
    """
    Build expected registry-name sets from resolved interlink and intralink specifications.

    Skips interlinks resolving to the same endpoint table and unions values when
    multiple entries map to one registry name.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_schema_conformance_03_type_reference_tables.py


    :param conn: Caller-owned SQLite connection; this helper does not close it.
    :return: Mapping from expected __types table names to sets of expanded type values.
    """

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
    """
    Compare registry table names selected by SQLite LIKE with the expected mapping keys.

    The query uses an unescaped %__types pattern, so underscores retain SQL wildcard
    meaning.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_schema_conformance_03_type_reference_tables.py::test_frbr_schema_conformance_types_tables_set_is_strict


    :param frbr_schema_conn: Module-scoped in-memory FRBR connection used for schema
        inspection.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Compare every expected registry with its exact set of expanded TOML values.

    Accumulates missing/extra value diagnostics. The single __ALL__ parameter groups all
    registries into one pytest case; duplicates and row order are discarded by set
    comparison.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_schema_conformance_03_type_reference_tables.py::test_frbr_schema_conformance_types_tables_contents_strict


    :param frbr_schema_conn: Module-scoped in-memory FRBR connection used for schema
        inspection.
    :param types_table: The fixed __ALL__ sentinel used by the parameter table.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Check for an exact trigger name using a bound sqlite_master lookup.

    Example:
        >>> conn = sqlite3.connect(':memory:')
        >>> _trigger_exists(conn, 'missing')
        False
        >>> conn.close()


    :param conn: Caller-owned SQLite connection; this helper does not close it.
    :param name: Trigger name bound to the lookup.
    :return: True when any matching trigger row exists.
    """
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='trigger' AND name=?;",
        (name,),
    ).fetchone()
    return row is not None


def _pick_one(values: set[str]) -> str:
    """
    Select the lexically first value and assert it is a nonempty string.

    An empty set raises IndexError before the assertion; sorting incompatible values can
    raise TypeError.

    Example:
        >>> _pick_one({'b', 'a'})
        'a'


    :param values: Nonempty comparable set expected to contain valid type strings.
    :return: First sorted nonempty string.
    """
    v = sorted(values)[0]
    assert isinstance(v, str) and v
    return v


def test_frbr_schema_conformance_type_guards_reject_unknown_values(frbr_schema_conn: sqlite3.Connection) -> None:
    """
    Require insert/update guard names and reject unknown types in every expected link table.

    Inspects exactly one type column, synthesizes required fields, and confirms a valid
    type inserts successfully. Requests foreign keys off and leaves accepted rows
    pending; update guards are checked for existence only.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_schema_conformance_03_type_reference_tables.py::test_frbr_schema_conformance_type_guards_reject_unknown_values


    :param frbr_schema_conn: Module-scoped in-memory FRBR connection used for schema
        inspection.
    :return: None; failed expectations raise AssertionError.
    """
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
