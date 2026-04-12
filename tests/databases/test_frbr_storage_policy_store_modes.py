from __future__ import annotations

import pathlib
import sqlite3


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
    db_path = tmp_path / "storage_policy_modes.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON;")

    root = _storage_sql_root()
    scripts = [
        root / "table_sql" / "storage_tables" / "1a-storage-policies.sql",
        root / "table_sql" / "storage_tables" / "1-storages.sql",
        root / "table_sql" / "storage_tables" / "2-folders.sql",
        root / "table_sql" / "storage_tables" / "3-digital-assets.sql",
        root / "trigger_sql" / "storage" / "3-digital-assets_triggers.sql",
    ]
    for script_path in scripts:
        conn.executescript(_read_sql_script(script_path))
    return conn


def _pragma_cols(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info(`{table_name}`);")}



def test_storage_schema_contains_store_folder_policy_and_replica_mode_fields(tmp_path: pathlib.Path) -> None:
    conn = _create_storage_schema(tmp_path)
    try:
        store_cols = _pragma_cols(conn, "stores")
        assert "store_default_replication_policy_id" in store_cols
        assert "store_default_backup_policy_id" in store_cols
        assert "store_supports_active_replica_mode" in store_cols
        assert "store_supports_backup_replica_mode" in store_cols
        assert "store_supports_archive_replica_mode" in store_cols

        folder_cols = _pragma_cols(conn, "folders")
        assert "folder_default_replication_policy_id" in folder_cols
        assert "folder_default_backup_policy_id" in folder_cols

        replica_cols = _pragma_cols(conn, "asset_replicas")
        assert "asset_replica_mode" in replica_cols

        composition_indexes = {
            row[1] for row in conn.execute("PRAGMA index_list(`digital_asset_compositions`);").fetchall()
        }
        assert "idx_digital_asset_compositions_member_asset_id" in composition_indexes
    finally:
        conn.close()



def test_asset_replica_mode_must_be_supported_by_store(tmp_path: pathlib.Path) -> None:
    conn = _create_storage_schema(tmp_path)
    try:
        conn.execute(
            """
            INSERT INTO stores (
                store_name,
                store_kind,
                store_access_protocol,
                store_supports_active_replica_mode,
                store_supports_backup_replica_mode,
                store_supports_archive_replica_mode
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("tape-archive", "tape", "tape", 0, 0, 1),
        )
        store_id = int(conn.execute("SELECT store_id FROM stores LIMIT 1;").fetchone()[0])

        conn.execute(
            "INSERT INTO digital_assets (digital_asset_name, digital_asset_kind) VALUES (?, ?)",
            ("chapter01.mp3", "atomic"),
        )
        digital_asset_id = int(conn.execute("SELECT digital_asset_id FROM digital_assets LIMIT 1;").fetchone()[0])

        try:
            conn.execute(
                """
                INSERT INTO asset_replicas (
                    asset_replica_digital_asset_id,
                    asset_replica_store_id,
                    asset_replica_storage_key,
                    asset_replica_mode
                ) VALUES (?, ?, ?, ?)
                """,
                (digital_asset_id, store_id, "audio/chapter01.mp3", "active"),
            )
        except sqlite3.IntegrityError as exc:
            assert "not supported by the target store" in str(exc)
        else:  # pragma: no cover
            raise AssertionError("Expected active replica insert to fail for archive-only store.")

        conn.execute(
            """
            INSERT INTO asset_replicas (
                asset_replica_digital_asset_id,
                asset_replica_store_id,
                asset_replica_storage_key,
                asset_replica_mode
            ) VALUES (?, ?, ?, ?)
            """,
            (digital_asset_id, store_id, "audio/chapter01.mp3", "archive"),
        )

        stored_mode = conn.execute(
            "SELECT asset_replica_mode FROM asset_replicas WHERE asset_replica_store_id = ? LIMIT 1;",
            (store_id,),
        ).fetchone()[0]
        assert stored_mode == "archive"
    finally:
        conn.close()
