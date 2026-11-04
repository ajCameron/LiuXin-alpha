"""Batch reads of physical backup_workflow_state rows; policy stays with callers."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


class BackupWorkflowStateSQLMixin:
    """Own queries returning backup_workflow_state rows on the current connection.

    Example:
        Access these operations through ``db.metadata_sql``.
    """

    db: DatabaseAPI

    def get_backup_workflow_state_for_workflows(
        self,
        workflow_ids: Iterable[int],
    ) -> dict[int, tuple[Mapping[str, Any], ...]]:
        """Batch rows by backup_workflow_state_workflow_id, retaining empty owner groups.

        Order each group by physical row ID. Do not require owners to exist or
        interpret stored metadata. Reads share the caller's current transaction.
        """
        return self.db.macros.get_rows_for_ids(
            "backup_workflow_state",
            "backup_workflow_state_workflow_id",
            workflow_ids,
        )
