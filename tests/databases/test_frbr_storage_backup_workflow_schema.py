from __future__ import annotations

import pathlib
import sqlite3

from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import database_generator as frbr_gen


def _storage_sql_root() -> pathlib.Path:
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
    text = path.read_text(encoding="utf-8", errors="replace")
    lines = [line for line in text.splitlines() if not line.startswith("-- BREAK")]
    return "\n".join(lines) + "\n"


def _create_storage_schema(tmp_path: pathlib.Path) -> sqlite3.Connection:
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
        root / "trigger_sql" / "storage" / "3-digital-assets_triggers.sql",
    ]
    for script_path in scripts:
        conn.executescript(_read_sql_script(script_path))
    return conn


def _pragma_cols(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info(`{table_name}`);")}


def test_storage_schema_contains_store_operational_role_and_backup_workflow_tables(tmp_path: pathlib.Path) -> None:
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
    finally:
        conn.close()


def test_store_operational_role_check_allows_known_roles_only(tmp_path: pathlib.Path) -> None:
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
