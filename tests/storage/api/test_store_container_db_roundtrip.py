from __future__ import annotations

import dataclasses

from pathlib import Path

from tests.storage._mini_db import build_mini_db

from LiuXin_alpha.databases import Row
from LiuXin_alpha.storage.store_manager import StorageManager


def test_store_container_roundtrips_store_operational_role_via_db(tmp_path: Path) -> None:
    db_path = tmp_path / "store_container_roundtrip.sqlite"
    store_root = tmp_path / "flat-cache"
    store_root.mkdir(parents=True, exist_ok=True)

    db = build_mini_db(db_path)
    try:
        store_row = Row.from_idless_row_dict(
            db,
            row_dict={
                "store_name": "flat-cache",
                "store_kind": "on_disk_flat",
                "store_access_protocol": "file",
                "store_root_uri": str(store_root),
                "store_operational_role": "cache",
                "store_is_read_only": 0,
                "store_supports_folders": 0,
            },
            table="stores",
        )
        store_id = int(store_row["store_id"])

        manager = StorageManager(db=db, startup_on_add=False)
        spec = manager.get_store_spec_from_db(store_id)
        assert spec.store_operational_role == "cache"

        container = manager.build_store_container(spec)
        container._spec = dataclasses.replace(container.spec, store_operational_role="backup")
        saved = container.save_spec_to_db()
        reloaded = container.reload_spec_from_db()

        assert saved.store_operational_role == "backup"
        assert reloaded.store_operational_role == "backup"
        db_row = db.get_row_from_id("stores", store_id)
        assert db_row["store_operational_role"] == "backup"
    finally:
        db.conn.close()
