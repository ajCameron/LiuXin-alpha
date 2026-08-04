"""Fixed-table row helpers for backup workflow persistence.

Examples:
    Load a persisted workflow row with the inherited row factory::

        row = BackupWorkflowRow.from_row_id(db, 3, read_only=True)
"""

from __future__ import annotations

from typing import Optional

from LiuXin_alpha.databases.row import FixedTableStorageRow


class BackupWorkflowRow(FixedTableStorageRow):
    """Typed row for the ``backup_workflows`` table.

    Examples:
        Load workflow ``3``::

            row = BackupWorkflowRow.from_row_id(db, 3)
    """
    TABLE_NAME = "backup_workflows"
    ID_COLUMN = "backup_workflow_id"

    @property
    def backup_workflow_id(self) -> Optional[int]:
        """Return the workflow row's primary id.

        Examples:
            Read the id after loading or inserting a row::

                workflow_id = row.backup_workflow_id
        """
        return self[self.ID_COLUMN]

    @backup_workflow_id.setter
    def backup_workflow_id(self, value: Optional[int]) -> None:
        """Set the workflow row's primary id.

        Examples:
            Bind an id assigned by a repository::

                row.backup_workflow_id = 3
        """
        self.primary_id = value


class BackupWorkflowSourceRow(FixedTableStorageRow):
    """Typed row for the ``backup_workflow_sources`` table.

    Examples:
        Load source row ``11``::

            row = BackupWorkflowSourceRow.from_row_id(db, 11)
    """
    TABLE_NAME = "backup_workflow_sources"
    ID_COLUMN = "backup_workflow_source_id"

    @property
    def backup_workflow_source_id(self) -> Optional[int]:
        """Return the source row's primary id.

        Examples:
            Read the source id::

                source_id = row.backup_workflow_source_id
        """
        return self[self.ID_COLUMN]

    @backup_workflow_source_id.setter
    def backup_workflow_source_id(self, value: Optional[int]) -> None:
        """Set the source row's primary id.

        Examples:
            Bind the repository-assigned id::

                row.backup_workflow_source_id = 11
        """
        self.primary_id = value


class BackupWorkflowStateRow(FixedTableStorageRow):
    """Typed row for the ``backup_workflow_state`` table.

    Examples:
        Load checkpoint row ``8``::

            row = BackupWorkflowStateRow.from_row_id(db, 8)
    """
    TABLE_NAME = "backup_workflow_state"
    ID_COLUMN = "backup_workflow_state_id"

    @property
    def backup_workflow_state_id(self) -> Optional[int]:
        """Return the checkpoint row's primary id.

        Examples:
            Read the checkpoint id::

                state_id = row.backup_workflow_state_id
        """
        return self[self.ID_COLUMN]

    @backup_workflow_state_id.setter
    def backup_workflow_state_id(self, value: Optional[int]) -> None:
        """Set the checkpoint row's primary id.

        Examples:
            Bind the repository-assigned id::

                row.backup_workflow_state_id = 8
        """
        self.primary_id = value


class BackupWorkflowOutputRow(FixedTableStorageRow):
    """Typed row for the ``backup_workflow_outputs`` table.

    Examples:
        Load output row ``5``::

            row = BackupWorkflowOutputRow.from_row_id(db, 5)
    """
    TABLE_NAME = "backup_workflow_outputs"
    ID_COLUMN = "backup_workflow_output_id"

    @property
    def backup_workflow_output_id(self) -> Optional[int]:
        """Return the output row's primary id.

        Examples:
            Read the output id::

                output_id = row.backup_workflow_output_id
        """
        return self[self.ID_COLUMN]

    @backup_workflow_output_id.setter
    def backup_workflow_output_id(self, value: Optional[int]) -> None:
        """Set the output row's primary id.

        Examples:
            Bind the repository-assigned id::

                row.backup_workflow_output_id = 5
        """
        self.primary_id = value


class BackupPresenceLinkRow(FixedTableStorageRow):
    """Typed row linking a backup artifact to a protected source.

    Examples:
        Load presence link ``19``::

            row = BackupPresenceLinkRow.from_row_id(db, 19)
    """
    TABLE_NAME = "backup_presence_links"
    ID_COLUMN = "backup_presence_link_id"

    @property
    def backup_presence_link_id(self) -> Optional[int]:
        """Return the presence link's primary id.

        Examples:
            Read the link id::

                link_id = row.backup_presence_link_id
        """
        return self[self.ID_COLUMN]

    @backup_presence_link_id.setter
    def backup_presence_link_id(self, value: Optional[int]) -> None:
        """Set the presence link's primary id.

        Examples:
            Bind the repository-assigned id::

                row.backup_presence_link_id = 19
        """
        self.primary_id = value


__all__ = [
    "BackupWorkflowRow",
    "BackupWorkflowSourceRow",
    "BackupWorkflowStateRow",
    "BackupWorkflowOutputRow",
    "BackupPresenceLinkRow",
]
