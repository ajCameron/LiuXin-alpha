"""Batch reads of physical backup_workflows rows; policy stays with callers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


class BackupWorkflowsSQLMixin:
    """Own queries returning backup_workflows rows on the current connection.

    Example:
        Access these operations through ``db.metadata_sql``.
    """

    db: DatabaseAPI

    def get_backup_workflows_by_status(
        self,
        *,
        status: str | None = None,
    ) -> tuple[Mapping[str, Any], ...]:
        """Read workflow rows in ID order with an optional exact stored status.

        None selects all workflows. Lifecycle validation and declaration decoding
        belong to the backup owner; this query neither normalizes nor transitions.
        """
        return self.db.macros.get_rows(
            "backup_workflows",
            where=None if status is None else {"backup_workflow_status": status},
            order_by=("backup_workflow_id",),
        )
