"""
Compare requested link metadata columns with the generated SQLite schema.

Iterates usable shipped TOML entries, resolves table names and checks required
column subsets. Nullable is configuration, not a physical column; allowed types
imply a type column. Cardinality and trigger behavior are handled by later suites.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/test_frbr_schema_conformance_01_requested_columns.py
"""

from __future__ import annotations

import pathlib
import sqlite3
from dataclasses import dataclass
from typing import Any, Iterable, Optional, Union

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
# Helpers
# ---------------------------------------------------------------------------


STANDARD_LINK_COLS: frozenset[str] = frozenset(
    {
        "priority",
        "primary",
        "type",
        "origin",
        "source",
        "policy",
        "data",
        "index",
    }
)


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


def _normalize_requested_columns(
    *,
    entry: dict[str, Any],
    default: set[str],
    allowed_types_present: bool,
) -> tuple[Union[str, set[str]], bool]:
    """
    Normalize requested-column aliases into all or a column set plus a nullable flag.

    Uses requested_columns first, then requested_cols or columns, with a copied default
    when absent. Lists are stringified, stripped and lowercased; all overrides
    individual names. Nullable tokens and any non-None nullable setting mark
    configuration presence without Boolean validation. Adds type when an allowed-types
    list is present. Other input shapes or non-all strings raise TypeError.

    Example:
        >>> cols, nullable = _normalize_requested_columns(entry={'requested_columns': [' Origin ', 'nullable']}, default={'priority'}, allowed_types_present=True)
        >>> cols == {'origin', 'type'}, nullable
        (True, True)


    :param entry: TOML entry with requested-column aliases and optional nullable
        setting.
    :param default: Column set copied when no request value is found.
    :param allowed_types_present: Whether to add type to an explicit/default set.
    :return: Pair of normalized requested columns and whether nullable configuration was
        present.
    """

    requested = entry.get("requested_columns")
    if requested is None:
        requested = entry.get("requested_cols") or entry.get("columns")

    nullable_sentinel = False

    if requested is None:
        cols: Union[str, set[str]] = set(default)
    elif isinstance(requested, str):
        if requested.strip().lower() != "all":
            raise TypeError(f"requested_columns must be 'all' or a list; got: {requested!r}")
        cols = "all"
    elif isinstance(requested, list):
        lowered = [str(x).strip().lower() for x in requested]
        if "nullable" in lowered:
            nullable_sentinel = True
            lowered = [x for x in lowered if x != "nullable"]
        if "all" in lowered:
            cols = "all"
        else:
            cols = set(lowered)
    else:
        raise TypeError(f"requested_columns must be 'all' or a list; got: {type(requested)}")

    # TOML may also provide a dedicated `nullable=` key; it's a config knob, not a column.
    if entry.get("nullable") is not None:
        # treat as nullable-config present for reporting; still no physical column.
        nullable_sentinel = True

    # If a `types` enum is present, the generator ensures a type column exists.
    if allowed_types_present and cols != "all":
        assert isinstance(cols, set)
        cols.add("type")

    # Never treat nullable as a physical column.
    if cols != "all" and isinstance(cols, set) and "nullable" in cols:
        cols.remove("nullable")

    return cols, nullable_sentinel


@dataclass(frozen=True)
class _InterlinkSpec:
    """
    Carry a normalized interlink request and its original TOML mapping.

    Fields hold the zero-based source index, endpoint names, all or a requested-column
    set, nullable presence and raw entry. Frozen attributes do not make nested sets or
    mappings immutable.

    Example:
        >>> spec = _InterlinkSpec(0, 'works', 'expressions', {'priority'}, False, {})
        >>> spec.left, spec.nullable_config
        ('works', False)
    """
    idx: int
    left: str
    right: str
    requested_cols: Union[str, set[str]]
    nullable_config: bool
    raw: dict[str, Any]


@dataclass(frozen=True)
class _IntralinkSpec:
    """
    Carry a normalized intralink request and its original TOML mapping.

    Fields hold the source index, target table, requested columns, nullable presence and
    raw entry. Nested values remain mutable despite frozen attributes.

    Example:
        >>> _IntralinkSpec(0, 'works', {'type'}, False, {}).table
        'works'
    """
    idx: int
    table: str
    requested_cols: Union[str, set[str]]
    nullable_config: bool
    raw: dict[str, Any]


def _iter_interlink_specs() -> list[_InterlinkSpec]:
    """
    Parse usable interlink entries, preserving their source indices and raw mappings.

    Requires a list root; skips non-dicts and missing endpoints. Accepts
    left_table/left/a and right_table/right/b aliases, defaults to priority, and infers
    type from a nonblank allowed-types list.

    Example:
        >>> bool(_iter_interlink_specs())
        True


    :return: Ordered list of normalized interlink specifications.
    """
    data = _load_toml("interlink_table_requests.toml")
    interlinks = data.get("interlinks", [])
    assert isinstance(interlinks, list)

    out: list[_InterlinkSpec] = []
    for idx, entry in enumerate(interlinks):
        if not isinstance(entry, dict):
            continue
        left = entry.get("left_table") or entry.get("left") or entry.get("a")
        right = entry.get("right_table") or entry.get("right") or entry.get("b")
        if not left or not right:
            continue

        allowed_types = entry.get("types") or entry.get("allowed_types")
        allowed_types_present = isinstance(allowed_types, list) and any(str(x).strip() for x in allowed_types)

        requested_cols, nullable_cfg = _normalize_requested_columns(
            entry=entry,
            default={"priority"},
            allowed_types_present=allowed_types_present,
        )

        out.append(
            _InterlinkSpec(
                idx=idx,
                left=str(left),
                right=str(right),
                requested_cols=requested_cols,
                nullable_config=nullable_cfg,
                raw=entry,
            )
        )

    return out


def _iter_intralink_specs() -> list[_IntralinkSpec]:
    """
    Parse usable intralink entries, accepting a table-name string shorthand.

    Requires a list root, skips malformed or unnamed entries and accepts
    table/table_name/name aliases. Defaults to type; nonblank allowed-types lists also
    require type.

    Example:
        >>> bool(_iter_intralink_specs())
        True


    :return: Ordered list of normalized intralink specifications.
    """
    data = _load_toml("intralink_table_requests.toml")
    intralinks = data.get("intralinks", [])
    assert isinstance(intralinks, list)

    out: list[_IntralinkSpec] = []
    for idx, entry in enumerate(intralinks):
        if isinstance(entry, str):
            entry = {"table": entry}
        if not isinstance(entry, dict):
            continue
        name = entry.get("table") or entry.get("table_name") or entry.get("name")
        if not name:
            continue

        allowed_types = entry.get("types") or entry.get("allowed_types")
        allowed_types_present = isinstance(allowed_types, list) and any(str(x).strip() for x in allowed_types)

        requested_cols, nullable_cfg = _normalize_requested_columns(
            entry=entry,
            default={"type"},
            allowed_types_present=allowed_types_present,
        )

        out.append(
            _IntralinkSpec(
                idx=idx,
                table=str(name),
                requested_cols=requested_cols,
                nullable_config=nullable_cfg,
                raw=entry,
            )
        )

    return out


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


def _canonicalize_table_name(conn: sqlite3.Connection, candidate: str) -> str:
    """
    Strip the candidate, try an exact name, then one singular/plural mapping.

    Preserves case and returns the stripped candidate if neither spelling exists; does
    not create or validate a table.

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

    cand = str(candidate).strip()
    existing = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table';"
        ).fetchall()
    }
    if cand in existing:
        return cand
    alt = singular_plural_mapper(cand)
    if alt in existing:
        return alt
    return cand


def _expected_link_table_cols(
    *,
    left: str,
    right: str,
    requested_cols: Union[str, set[str]],
) -> tuple[str, str, set[str]]:
    """
    Derive a conventional interlink name, prefix and required column subset.

    Includes ID, both endpoints, source, datestamp and scratch. Expands all through
    STANDARD_LINK_COLS; other requested names are prefixed without validation.

    Example:
        >>> table, base, cols = _expected_link_table_cols(left='works', right='expressions', requested_cols={'priority'})
        >>> base + '_priority' in cols and base + '_source' in cols
        True


    :param left: Resolved left table name.
    :param right: Resolved right table name.
    :param requested_cols: Normalized metadata-column set or the string all.
    :return: Table name, column prefix and expected column-name set.
    """

    table_name, col_base = ColumnNameMixin.get_interlink_table_name(left, right)
    a, b = sorted([left, right])
    a_s = plural_singular_mapper(a)
    b_s = plural_singular_mapper(b)

    expected: set[str] = {
        f"{col_base}_id",
        f"{col_base}_{a_s}_id",
        f"{col_base}_{b_s}_id",
        f"{col_base}_source",
        f"{col_base}_datestamp",
        f"{col_base}_scratch",
    }

    if requested_cols == "all":
        req_set: Iterable[str] = STANDARD_LINK_COLS
    else:
        req_set = requested_cols

    for c in req_set:
        expected.add(f"{col_base}_{c}")

    return table_name, col_base, expected


def _expected_intralink_table_cols(
    *,
    table: str,
    requested_cols: Union[str, set[str]],
) -> tuple[str, str, set[str]]:
    """
    Derive an intralink name, prefix and required structural/metadata column subset.

    Singularizes the table name, includes primary/secondary endpoint IDs and expands all
    through STANDARD_LINK_COLS.

    Example:
        >>> table, base, cols = _expected_intralink_table_cols(table='works', requested_cols={'type'})
        >>> table, base + '_type' in cols
        ('work_work_intralinks', True)


    :param table: Trusted test table name, interpolated into SQL where needed.
    :param requested_cols: Normalized metadata-column set or the string all.
    :return: Table name, column prefix and expected column-name set.
    """
    target_row = plural_singular_mapper(table)
    col_base = f"{target_row}_{target_row}_intralink"
    table_name = f"{col_base}s"

    expected: set[str] = {
        f"{col_base}_id",
        f"{col_base}_primary_id",
        f"{col_base}_secondary_id",
        f"{col_base}_source",
        f"{col_base}_datestamp",
        f"{col_base}_scratch",
    }

    if requested_cols == "all":
        req_set: Iterable[str] = STANDARD_LINK_COLS
    else:
        req_set = requested_cols

    for c in req_set:
        expected.add(f"{col_base}_{c}")

    return table_name, col_base, expected


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
# 01a) Interlinks: requested columns exist
# ---------------------------------------------------------------------------


def test_frbr_schema_conformance_interlinks_requested_columns_exist(frbr_schema_conn: sqlite3.Connection) -> None:
    """
    Require structural and requested columns for every usable non-self interlink spec.

    Accumulates missing-table/column diagnostics and rejects a materialized nullable
    sentinel when nullable configuration is present.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_schema_conformance_01_requested_columns.py::test_frbr_schema_conformance_interlinks_requested_columns_exist


    :param frbr_schema_conn: Module-scoped in-memory FRBR connection used for schema
        inspection.
    :return: None; failed expectations raise AssertionError.
    """
    specs = _iter_interlink_specs()
    assert specs, "Expected interlink specs in interlink_table_requests.toml"

    existing_tables = {
        row[0]
        for row in frbr_schema_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table';"
        ).fetchall()
    }

    failures: list[str] = []

    for spec in specs:
        left = _canonicalize_table_name(frbr_schema_conn, spec.left)
        right = _canonicalize_table_name(frbr_schema_conn, spec.right)

        if left == right:
            # Generator ignores self-link interlinks.
            continue

        table_name, col_base, expected = _expected_link_table_cols(
            left=left,
            right=right,
            requested_cols=spec.requested_cols,
        )

        if table_name not in existing_tables:
            failures.append(
                "\n".join(
                    [
                        f"interlinks[{spec.idx}] expected table missing: {table_name!r}",
                        f"  pair: {left!r} ↔ {right!r}",
                        f"  requested_cols: {spec.requested_cols!r}",
                        f"  raw: {spec.raw!r}",
                    ]
                )
            )
            continue

        cols = _pragma_cols(frbr_schema_conn, table_name)

        missing = sorted(expected - cols)
        if missing:
            failures.append(
                "\n".join(
                    [
                        f"interlinks[{spec.idx}] table {table_name!r} missing requested/required columns:",
                        f"  missing: {missing!r}",
                        f"  present sample: {sorted(list(cols))[:30]!r}",
                        f"  col_base: {col_base!r}",
                        f"  pair: {left!r} ↔ {right!r}",
                        f"  requested_cols: {spec.requested_cols!r}",
                        f"  nullable_config: {spec.nullable_config!r}",
                    ]
                )
            )

        # Ensure the nullable sentinel never materialises as a column.
        if spec.nullable_config:
            bad = f"{col_base}_nullable"
            if bad in cols:
                failures.append(
                    "\n".join(
                        [
                            f"interlinks[{spec.idx}] table {table_name!r} incorrectly materialised nullable sentinel:",
                            f"  unexpected column: {bad!r}",
                            f"  pair: {left!r} ↔ {right!r}",
                            f"  raw requested_columns: {spec.raw.get('requested_columns')!r}",
                            f"  raw nullable: {spec.raw.get('nullable')!r}",
                        ]
                    )
                )

    assert not failures, "\n\n".join(failures)


# ---------------------------------------------------------------------------
# 01b) Intralinks: requested columns exist
# ---------------------------------------------------------------------------


def test_frbr_schema_conformance_intralinks_requested_columns_exist(frbr_schema_conn: sqlite3.Connection) -> None:
    """
    Require structural and requested columns for every usable intralink spec.

    Accumulates diagnostics and rejects a physical nullable column when nullable
    configuration is present.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_schema_conformance_01_requested_columns.py::test_frbr_schema_conformance_intralinks_requested_columns_exist


    :param frbr_schema_conn: Module-scoped in-memory FRBR connection used for schema
        inspection.
    :return: None; failed expectations raise AssertionError.
    """
    specs = _iter_intralink_specs()
    assert specs, "Expected intralink specs in intralink_table_requests.toml"

    existing_tables = {
        row[0]
        for row in frbr_schema_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table';"
        ).fetchall()
    }

    failures: list[str] = []

    for spec in specs:
        table = _canonicalize_table_name(frbr_schema_conn, spec.table)
        table_name, col_base, expected = _expected_intralink_table_cols(
            table=table,
            requested_cols=spec.requested_cols,
        )

        if table_name not in existing_tables:
            failures.append(
                "\n".join(
                    [
                        f"intralinks[{spec.idx}] expected table missing: {table_name!r}",
                        f"  table: {table!r}",
                        f"  requested_cols: {spec.requested_cols!r}",
                        f"  raw: {spec.raw!r}",
                    ]
                )
            )
            continue

        cols = _pragma_cols(frbr_schema_conn, table_name)

        missing = sorted(expected - cols)
        if missing:
            failures.append(
                "\n".join(
                    [
                        f"intralinks[{spec.idx}] table {table_name!r} missing requested/required columns:",
                        f"  missing: {missing!r}",
                        f"  present sample: {sorted(list(cols))[:30]!r}",
                        f"  col_base: {col_base!r}",
                        f"  table: {table!r}",
                        f"  requested_cols: {spec.requested_cols!r}",
                        f"  nullable_config: {spec.nullable_config!r}",
                    ]
                )
            )

        if spec.nullable_config:
            bad = f"{col_base}_nullable"
            if bad in cols:
                failures.append(
                    "\n".join(
                        [
                            f"intralinks[{spec.idx}] table {table_name!r} incorrectly materialised nullable sentinel:",
                            f"  unexpected column: {bad!r}",
                            f"  table: {table!r}",
                            f"  raw requested_cols: {spec.raw.get('requested_cols') or spec.raw.get('requested_columns')!r}",
                            f"  raw nullable: {spec.raw.get('nullable')!r}",
                        ]
                    )
                )

    assert not failures, "\n\n".join(failures)


# ---------------------------------------------------------------------------
# 01c) Sanity: at least one spec uses `all`, and `all` implies the standard surface
# ---------------------------------------------------------------------------


def test_frbr_schema_conformance_all_implies_standard_surface(frbr_schema_conn: sqlite3.Connection) -> None:
    """
    Check all standard metadata columns on every request normalized to all.

    Skips when neither link-spec family currently uses all; otherwise aggregates
    missing-column diagnostics.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_schema_conformance_01_requested_columns.py::test_frbr_schema_conformance_all_implies_standard_surface


    :param frbr_schema_conn: Module-scoped in-memory FRBR connection used for schema
        inspection.
    :return: None; failed expectations raise AssertionError.
    """

    inter_all = [s for s in _iter_interlink_specs() if s.requested_cols == "all"]
    intra_all = [s for s in _iter_intralink_specs() if s.requested_cols == "all"]

    # `all` is optional; if the spec doesn't use it yet, this test is a no-op.
    if not (inter_all or intra_all):
        pytest.skip("No TOML entries currently use requested_cols='all'")

    failures: list[str] = []

    for spec in inter_all:
        left = _canonicalize_table_name(frbr_schema_conn, spec.left)
        right = _canonicalize_table_name(frbr_schema_conn, spec.right)
        table_name, col_base, _expected = _expected_link_table_cols(
            left=left,
            right=right,
            requested_cols="all",
        )
        cols = _pragma_cols(frbr_schema_conn, table_name)
        for c in STANDARD_LINK_COLS:
            want = f"{col_base}_{c}"
            if want not in cols:
                failures.append(
                    f"interlinks[{spec.idx}] expected 'all' to include {want!r} on table {table_name!r}"
                )

    for spec in intra_all:
        table = _canonicalize_table_name(frbr_schema_conn, spec.table)
        table_name, col_base, _expected = _expected_intralink_table_cols(
            table=table,
            requested_cols="all",
        )
        cols = _pragma_cols(frbr_schema_conn, table_name)
        for c in STANDARD_LINK_COLS:
            want = f"{col_base}_{c}"
            if want not in cols:
                failures.append(
                    f"intralinks[{spec.idx}] expected 'all' to include {want!r} on table {table_name!r}"
                )

    assert not failures, "\n".join(failures)
