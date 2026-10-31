"""Batch reads of physical backup_workflow_sources rows; policy stays with callers."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

from LiuXin_alpha.errors import InputIntegrityError

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


class BackupWorkflowSourcesSQLMixin:
    """Own queries returning backup_workflow_sources rows on the current connection.

    Example:
        Access these operations through ``db.metadata_sql``.
    """

    db: DatabaseAPI

    def replace_backup_workflow_sources(
        self, workflow_id: int, rows: Iterable[Mapping[str, Any]]
    ) -> None:
        """Replace one workflow's physical source rows as an atomic batch.

        The caller supplies encoded source fields and ordinals. Reject rows
        assigned to another workflow; omitted owner fields use workflow_id.
        No workflow status, checkpoint or source bytes are changed here.
        """
        if isinstance(workflow_id, bool) or not isinstance(workflow_id, int):
            raise InputIntegrityError("workflow_id must be an integer")
        column = "backup_workflow_source_workflow_id"
        payloads = []
        for row in rows:
            if column in row and row[column] != workflow_id:
                raise InputIntegrityError("Source row belongs to another workflow")
            payloads.append({**row, column: workflow_id})
        with self.db.macros.transaction():
            self.db.macros.delete_rows_where(
                "backup_workflow_sources", where={column: workflow_id}
            )
            self.db.macros.insert_rows("backup_workflow_sources", payloads)

    def get_backup_workflow_sources_for_workflows(
        self,
        workflow_ids: Iterable[int],
    ) -> dict[int, tuple[Mapping[str, Any], ...]]:
        """Batch rows by backup_workflow_source_workflow_id, retaining empty owner groups.

        Order each group by physical row ID. Do not require owners to exist or
        interpret stored metadata. Reads share the caller's current transaction.
        """
        return self.db.macros.get_rows_for_ids(
            "backup_workflow_sources",
            "backup_workflow_source_workflow_id",
            workflow_ids,
        )
