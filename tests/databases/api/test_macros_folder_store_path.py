from __future__ import annotations

import sqlite3
from types import SimpleNamespace

from LiuXin_alpha.databases.database_driver_plugins.SQL.macros import SQLiteDatabaseMacros


def test_replace_in_folder_store_path_updates_all_matching_rows() -> None:
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute(
            """
            CREATE TABLE folder_stores (
                folder_store_id INTEGER PRIMARY KEY,
                folder_store_path TEXT,
                folder_store_marker_path TEXT
            )
            """
        )
        conn.executemany(
            "INSERT INTO folder_stores (folder_store_id, folder_store_path, folder_store_marker_path) VALUES (?, ?, ?)",
            [
                (1, "/old/root/alpha", "/old/root/.marker"),
                (2, "/elsewhere/beta", "/elsewhere/.marker"),
                (3, "/old/root/gamma", "/old/root/.marker-2"),
            ],
        )

        fake_db = SimpleNamespace(
            driver_wrapper=SimpleNamespace(execute=conn.execute, executemany=conn.executemany),
            get=conn.execute,
        )
        macros = SQLiteDatabaseMacros(fake_db)

        macros.replace_in_folder_store_path("/old/root", "/new/root")

        rows = conn.execute(
            "SELECT folder_store_id, folder_store_path FROM folder_stores ORDER BY folder_store_id"
        ).fetchall()
        assert rows == [
            (1, "/new/root/alpha"),
            (2, "/elsewhere/beta"),
            (3, "/new/root/gamma"),
        ]
    finally:
        conn.close()
