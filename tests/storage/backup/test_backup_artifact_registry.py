from __future__ import annotations

import sqlite3
from pathlib import Path

from tests.storage._mini_db import build_mini_db

from LiuXin_alpha.storage.api.backup_api import BackupSourceKind, BackupSourceSpec, BackupWorkflowKind, BackupWorkflowSpec
from LiuXin_alpha.storage.backup import BackupArtifactRegistry, BackupWorkflowRepository


def test_backup_artifact_registry_registers_pack_store_and_protected_presence_links(tmp_path: Path) -> None:
    db = build_mini_db(tmp_path / "artifact_registry.sqlite")
    try:
        repo = BackupWorkflowRepository(db)
        source_store_id = db.driver_wrapper.add_row(
            {
                "store_name": "ebooks-live",
                "store_kind": "OnDiskUnmanagedStorageBackend",
                "store_root_uri": str(tmp_path / "src"),
                "store_access_protocol": "file",
                "store_operational_role": "live",
            }
        )
        spec = BackupWorkflowSpec(
            workflow_name="ebooks-pack-0001",
            workflow_kind=BackupWorkflowKind.SQUASHFS_PACK,
            output_url=str(tmp_path / "packs" / "ebooks-pack-0001.sqsh"),
            sources=(
                BackupSourceSpec(
                    source_kind=BackupSourceKind.LOCAL_PATH,
                    source_identifier=str(tmp_path / "src" / "a.epub"),
                    archive_path="a.epub",
                    expected_size=10,
                    expected_hash="a" * 64,
                    source_file_id=101,
                    source_store_id=int(source_store_id),
                ),
                BackupSourceSpec(
                    source_kind=BackupSourceKind.LOCAL_PATH,
                    source_identifier=str(tmp_path / "src" / "b.epub"),
                    archive_path="b.epub",
                    expected_size=20,
                    expected_hash="b" * 64,
                    source_file_id=102,
                    source_store_id=int(source_store_id),
                ),
            ),
        )
        workflow_row = repo.save_workflow_spec(spec)
        workflow_id = int(workflow_row["backup_workflow_id"])

        registry = BackupArtifactRegistry(db)
        registered = registry.register_workflow_output_as_store(workflow_id)

        assert registered.backup_store_id > 0
        assert registered.presence_links_created == 2

        store_row = db.get_row_from_id("stores", int(registered.backup_store_id))
        assert store_row is not None
        assert store_row["store_access_protocol"] == "squashfs"
        assert store_row["store_operational_role"] == "archive"
        assert int(store_row["store_supports_immutable_objects"]) == 1

        output_rows = db.search("backup_workflow_outputs", "backup_workflow_output_workflow_id", workflow_id)
        assert len(output_rows) == 1
        assert int(output_rows[0]["backup_workflow_output_store_id"]) == int(registered.backup_store_id)

        link_rows = db.search("backup_presence_links", "backup_presence_link_backup_store_id", int(registered.backup_store_id))
        assert len(link_rows) == 2
        first_link_id = int(link_rows[0]["backup_presence_link_id"])
        assert int(link_rows[0]["backup_presence_link_is_protected"]) == 1
        assert int(link_rows[0]["backup_presence_link_is_immutable"]) == 1

        with db.conn:
            try:
                db.conn.execute(
                    "UPDATE `backup_presence_links` SET `backup_presence_link_archive_path` = ? WHERE `backup_presence_link_id` = ?",
                    ("renamed.epub", first_link_id),
                )
            except sqlite3.IntegrityError as exc:
                assert "immutable" in str(exc).lower()
            else:
                raise AssertionError("Expected immutable backup presence link update to fail")

        with db.conn:
            try:
                db.conn.execute(
                    "DELETE FROM `backup_presence_links` WHERE `backup_presence_link_id` = ?",
                    (first_link_id,),
                )
            except sqlite3.IntegrityError as exc:
                assert "protected" in str(exc).lower()
            else:
                raise AssertionError("Expected protected backup presence link delete to fail")
    finally:
        db.conn.close()
