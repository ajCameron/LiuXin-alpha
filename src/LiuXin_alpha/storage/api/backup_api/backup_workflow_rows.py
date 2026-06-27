"""Fixed-table row helpers for backup workflow persistence."""

from __future__ import annotations

from typing import Optional

from LiuXin_alpha.databases.row import FixedTableStorageRow


class BackupWorkflowRow(FixedTableStorageRow):
    TABLE_NAME = "backup_workflows"
    ID_COLUMN = "backup_workflow_id"

    @property
    def backup_workflow_id(self) -> Optional[int]:
        return self[self.ID_COLUMN]

    @backup_workflow_id.setter
    def backup_workflow_id(self, value: Optional[int]) -> None:
        self.primary_id = value


class BackupWorkflowSourceRow(FixedTableStorageRow):
    TABLE_NAME = "backup_workflow_sources"
    ID_COLUMN = "backup_workflow_source_id"

    @property
    def backup_workflow_source_id(self) -> Optional[int]:
        return self[self.ID_COLUMN]

    @backup_workflow_source_id.setter
    def backup_workflow_source_id(self, value: Optional[int]) -> None:
        self.primary_id = value


class BackupWorkflowStateRow(FixedTableStorageRow):
    TABLE_NAME = "backup_workflow_state"
    ID_COLUMN = "backup_workflow_state_id"

    @property
    def backup_workflow_state_id(self) -> Optional[int]:
        return self[self.ID_COLUMN]

    @backup_workflow_state_id.setter
    def backup_workflow_state_id(self, value: Optional[int]) -> None:
        self.primary_id = value


class BackupWorkflowOutputRow(FixedTableStorageRow):
    TABLE_NAME = "backup_workflow_outputs"
    ID_COLUMN = "backup_workflow_output_id"

    @property
    def backup_workflow_output_id(self) -> Optional[int]:
        return self[self.ID_COLUMN]

    @backup_workflow_output_id.setter
    def backup_workflow_output_id(self, value: Optional[int]) -> None:
        self.primary_id = value


class BackupPresenceLinkRow(FixedTableStorageRow):
    TABLE_NAME = "backup_presence_links"
    ID_COLUMN = "backup_presence_link_id"

    @property
    def backup_presence_link_id(self) -> Optional[int]:
        return self[self.ID_COLUMN]

    @backup_presence_link_id.setter
    def backup_presence_link_id(self, value: Optional[int]) -> None:
        self.primary_id = value


__all__ = [
    "BackupWorkflowRow",
    "BackupWorkflowSourceRow",
    "BackupWorkflowStateRow",
    "BackupWorkflowOutputRow",
    "BackupPresenceLinkRow",
]
