from __future__ import annotations

from pathlib import Path

from tests.storage._mini_db import build_mini_db

from LiuXin_alpha.storage.backup import StoreBackupPlanner


def _create_legacy_files_table(db) -> None:
    db.conn.execute(
        """
        CREATE TABLE IF NOT EXISTS `files` (
            `file_id` INTEGER PRIMARY KEY,
            `file_store_id` INTEGER NOT NULL,
            `file_storage_key` TEXT NOT NULL,
            `file_name` TEXT NULL,
            `file_extension` TEXT NULL,
            `file_size_bytes` INTEGER NULL,
            `file_hash_sha256` TEXT NULL
        )
        """
    )
    db.conn.commit()


def test_store_backup_planner_groups_indexed_store_into_pack_specs(tmp_path: Path) -> None:
    db = build_mini_db(tmp_path / "planner.sqlite")
    try:
        _create_legacy_files_table(db)
        store = db.driver_wrapper.add_row(
            {
                "store_name": "ebooks",
                "store_kind": "OnDiskUnmanagedStorageBackend",
                "store_root_uri": str(tmp_path / "ebooks"),
                "store_access_protocol": "file",
                "store_operational_role": "live",
            }
        )
        db.conn.executemany(
            "INSERT INTO `files` (`file_store_id`, `file_storage_key`, `file_name`, `file_extension`, `file_size_bytes`, `file_hash_sha256`) VALUES (?, ?, ?, ?, ?, ?)",
            [
                (store, "a/book1.epub", "book1.epub", "epub", 10, "a" * 64),
                (store, "a/book2.epub", "book2.epub", "epub", 11, "b" * 64),
                (store, "notes/readme.txt", "readme.txt", "txt", 5, "c" * 64),
                (store, "b/book3.epub", "book3.epub", "epub", 12, "d" * 64),
            ],
        )
        db.conn.commit()

        planner = StoreBackupPlanner(db)
        packs = planner.plan_squashfs_packs_for_store(
            source_store_id=int(store),
            output_dir=str(tmp_path / "packs"),
            target_pack_size_bytes=25,
            workflow_name_prefix="ebooks-nightly",
            allowed_extensions=["epub"],
        )

        assert len(packs) == 2
        assert packs[0].source_count == 2
        assert packs[0].estimated_size_bytes == 21
        assert packs[0].workflow_spec.output_url.endswith("ebooks-nightly-pack-0001.sqsh")
        assert [src.archive_path for src in packs[0].workflow_spec.sources] == ["a/book1.epub", "a/book2.epub"]
        assert packs[0].workflow_spec.sources[0].source_file_id is not None
        assert packs[0].workflow_spec.sources[0].source_store_id == int(store)
        assert packs[1].source_count == 1
        assert packs[1].workflow_spec.sources[0].archive_path == "b/book3.epub"
    finally:
        db.conn.close()
