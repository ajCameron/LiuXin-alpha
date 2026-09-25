"""
Check storage operational roles and backup workflow columns from selected SQL resources.

Tests build isolated file databases, close connections in finally and inspect schema
or bound inserts without accessing physical storage assets.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/test_frbr_storage_backup_workflow_schema.py
"""
from __future__ import annotations

import pathlib
import sqlite3

from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import database_generator as frbr_gen


def _storage_sql_root() -> pathlib.Path:
    """
    Locate FRBR SQL resources from this test file's checkout layout.

    Builds the source-tree path without checking that the directory exists.

    Example:
        >>> _storage_sql_root().is_dir()
        True


    :return: Path to the checkout's FRBR generator resource directory.
    """
    return (
        pathlib.Path(__file__).resolve().parents[2]
        / "src"
        / "LiuXin_alpha"
        / "databases"
        / "database_driver_plugins"
        / "SQL"
        / "database_generator_frbr"
    )


def _read_sql_script(path: pathlib.Path) -> str:
    """
    Read a UTF-8 SQL file and remove lines beginning exactly with -- BREAK.

    Invalid bytes are replaced, other comments remain and one trailing newline is added.
    Indented markers are not removed; filesystem errors propagate.

    Example:
        >>> from tempfile import TemporaryDirectory
        >>> with TemporaryDirectory() as tmp:
        ...     source = pathlib.Path(tmp) / 'sample.sql'
        ...     _ = source.write_text('-- BREAK' + chr(10) + 'SELECT 1;', encoding='utf-8')
        ...     print(_read_sql_script(source).strip())
        SELECT 1;


    :param path: SQL file to read.
    :return: SQL script text with unindented BREAK-marker lines removed.
    """
    text = path.read_text(encoding="utf-8", errors="replace")
    lines = [line for line in text.splitlines() if not line.startswith("-- BREAK")]
    return "\n".join(lines) + "\n"


def _create_storage_schema(tmp_path: pathlib.Path) -> sqlite3.Connection:
    """
    Open a temporary file database and execute the selected storage SQL resources.

    Enables foreign keys and loads policy, store, backup-workflow, folder, asset,
    presence-link and asset-trigger scripts. Uses executescript boundaries; no cleanup
    is provided if setup fails before returning.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_storage_backup_workflow_schema.py


    :param tmp_path: Pytest-provided temporary directory for isolated database or TOML
        files.
    :return: Open SQLite connection; the caller must close it.
    """
    db_path = tmp_path / "storage_backup_workflow_schema.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON;")

    root = _storage_sql_root()
    scripts = [
        root / "table_sql" / "storage_tables" / "1a-storage-policies.sql",
        root / "table_sql" / "storage_tables" / "1-storages.sql",
        root / "table_sql" / "storage_tables" / "1b-backup-workflows.sql",
        root / "table_sql" / "storage_tables" / "2-folders.sql",
        root / "table_sql" / "storage_tables" / "3-digital-assets.sql",
        root / "table_sql" / "storage_tables" / "1c-backup-presence-links.sql",
        root / "trigger_sql" / "storage" / "3-digital-assets_triggers.sql",
    ]
    for script_path in scripts:
        conn.executescript(_read_sql_script(script_path))
    return conn


def _pragma_cols(conn: sqlite3.Connection, table_name: str) -> set[str]:
    """
    Read the column-name set for a trusted table via PRAGMA table_info.

    Interpolates the identifier in backticks without escaping; an absent table yields an
    empty set.

    Example:
        >>> conn = sqlite3.connect(':memory:')
        >>> _pragma_cols(conn, 'missing')
        set()
        >>> conn.close()


    :param conn: Caller-owned SQLite connection; this helper does not close it.
    :param table_name: Trusted schema table name used in PRAGMA inspection.
    :return: Set of stored column names.
    """
    return {row[1] for row in conn.execute(f"PRAGMA table_info(`{table_name}`);")}


def test_storage_schema_contains_store_operational_role_and_backup_workflow_tables(tmp_path: pathlib.Path) -> None:
    """
    Require the listed store role, workflow, source, state, output and presence-link columns.

    Checks representative column presence rather than every workflow constraint.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_storage_backup_workflow_schema.py::test_storage_schema_contains_store_operational_role_and_backup_workflow_tables


    :param tmp_path: Pytest-provided temporary directory for isolated database or TOML
        files.
    :return: None; failed expectations raise AssertionError.
    """
    conn = _create_storage_schema(tmp_path)
    try:
        store_cols = _pragma_cols(conn, "stores")
        assert "store_operational_role" in store_cols

        workflow_cols = _pragma_cols(conn, "backup_workflows")
        assert "backup_workflow_destination_store_id" in workflow_cols
        assert "backup_workflow_status" in workflow_cols

        source_cols = _pragma_cols(conn, "backup_workflow_sources")
        assert "backup_workflow_source_archive_path" in source_cols

        state_cols = _pragma_cols(conn, "backup_workflow_state")
        assert "backup_workflow_state_source_results_json" in state_cols

        output_cols = _pragma_cols(conn, "backup_workflow_outputs")
        assert "backup_workflow_output_asset_replica_id" in output_cols

        presence_cols = _pragma_cols(conn, "backup_presence_links")
        assert "backup_presence_link_source" in presence_cols
    finally:
        conn.close()


def test_store_operational_role_check_allows_known_roles_only(tmp_path: pathlib.Path) -> None:
    """
    Accept cache and reject banana as store operational roles.

    Requires the invalid insert to raise IntegrityError mentioning
    store_operational_role; other valid roles are not exercised.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_storage_backup_workflow_schema.py::test_store_operational_role_check_allows_known_roles_only


    :param tmp_path: Pytest-provided temporary directory for isolated database or TOML
        files.
    :return: None; failed expectations raise AssertionError.
    """
    conn = _create_storage_schema(tmp_path)
    try:
        conn.execute(
            "INSERT INTO stores (store_name, store_kind, store_root_uri, store_operational_role) VALUES (?, ?, ?, ?)",
            ("cache-store", "on_disk_flat", "file:///tmp/cache", "cache"),
        )
        stored_role = conn.execute("SELECT store_operational_role FROM stores LIMIT 1;").fetchone()[0]
        assert stored_role == "cache"

        try:
            conn.execute(
                "INSERT INTO stores (store_name, store_kind, store_root_uri, store_operational_role) VALUES (?, ?, ?, ?)",
                ("bad-store", "on_disk_flat", "file:///tmp/bad", "banana"),
            )
        except sqlite3.IntegrityError as exc:
            assert "store_operational_role" in str(exc)
        else:  # pragma: no cover
            raise AssertionError("Expected invalid store_operational_role insert to fail.")
    finally:
        conn.close()
