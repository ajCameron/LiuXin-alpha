"""Registration helpers for completed backup artifacts.

Backup workflows create artifacts; this helper gives those outputs durable DB
presence by:
- creating/reusing a store row for an archive-like artifact that can itself be
  mounted/read as a store
- recording protected backup-presence links from source files/replicas to that
  store

This keeps the medium/store concept separate from the workflow execution while
still making completed packs queryable.
"""

from __future__ import annotations

import dataclasses
import pathlib
from typing import TYPE_CHECKING

from LiuXin_alpha.databases import Row
from LiuXin_alpha.storage.api.backup_api import BackupSourceSpec
from LiuXin_alpha.storage.backup.backup_workflow_repository import BackupWorkflowRepository

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database import DatabaseAPI


@dataclasses.dataclass(slots=True, frozen=True)
class RegisteredBackupArtifact:
    workflow_id: int | None
    backup_store_id: int
    backup_store_name: str
    artifact_url: str
    presence_links_created: int = 0


class BackupArtifactRegistry:
    """Create DB presence for completed backup artifacts."""

    def __init__(self, db: "DatabaseAPI") -> None:
        self.db = db
        self.repo = BackupWorkflowRepository(db)

    def register_squashfs_artifact_store(
        self,
        artifact_url: str,
        *,
        store_name: str | None = None,
        workflow_id: int | None = None,
        operational_role: str = "archive",
    ) -> int:
        artifact_path = pathlib.Path(artifact_url).expanduser().resolve()
        existing = self._find_store_by_root_uri(str(artifact_path))
        if existing is not None:
            return int(existing["store_id"])
        allowed = set(self.db.get_column_headings("stores"))
        payload = {
            "store_name": store_name or artifact_path.stem or artifact_path.name,
            "store_kind": "SquashfsReadOnlyStorageBackend",
            "store_access_protocol": "squashfs",
            "store_root_uri": str(artifact_path),
            "store_operational_role": operational_role,
            "store_is_read_only": 1,
            "store_supports_folders": 1,
            "store_supports_hierarchical_list": 1,
            "store_supports_random_read": 1,
            "store_supports_random_write": 0,
            "store_supports_delete": 0,
            "store_supports_immutable_objects": 1,
            "store_online_status": "online",
            "store_supports_active_replica_mode": 0,
            "store_supports_backup_replica_mode": 1,
            "store_supports_archive_replica_mode": 1,
        }
        if workflow_id is not None and "store_scratch" in allowed:
            payload["store_scratch"] = '{"created_by_backup_workflow_id": %d}' % int(workflow_id)
        row_dict = {k: v for k, v in payload.items() if k in allowed}
        row = Row.from_idless_row_dict(self.db, row_dict=row_dict, table="stores")
        return int(row["store_id"])

    def register_workflow_output_as_store(
        self,
        workflow_id: int,
        *,
        artifact_url: str | None = None,
        store_name: str | None = None,
        link_sources: bool = True,
    ) -> RegisteredBackupArtifact:
        spec = self.repo.load_workflow_spec(int(workflow_id))
        resolved_artifact = str(artifact_url or spec.output_url)
        backup_store_id = self.register_squashfs_artifact_store(
            resolved_artifact,
            store_name=store_name or pathlib.Path(resolved_artifact).stem,
            workflow_id=int(workflow_id),
        )
        self.repo.record_output(
            int(workflow_id),
            output_url=resolved_artifact,
            output_store_id=int(backup_store_id),
            verified_ok=True,
        )
        presence_links_created = 0
        if link_sources:
            for source in spec.sources:
                self.repo.record_backup_presence_link(
                    backup_store_id=int(backup_store_id),
                    source=source,
                    archive_path=str(source.archive_path or pathlib.Path(source.source_identifier).name),
                    workflow_id=int(workflow_id),
                    output_url=resolved_artifact,
                )
                presence_links_created += 1
        return RegisteredBackupArtifact(
            workflow_id=int(workflow_id),
            backup_store_id=int(backup_store_id),
            backup_store_name=store_name or pathlib.Path(resolved_artifact).stem,
            artifact_url=resolved_artifact,
            presence_links_created=presence_links_created,
        )

    def _find_store_by_root_uri(self, root_uri: str):
        for row in self.db.search("stores", "store_root_uri", str(root_uri)):
            return row
        return None


__all__ = ["BackupArtifactRegistry", "RegisteredBackupArtifact"]
