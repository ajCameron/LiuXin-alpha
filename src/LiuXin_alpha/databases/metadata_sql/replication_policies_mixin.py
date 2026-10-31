"""Batch reads of physical replication_policies rows; policy stays with callers."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


class ReplicationPoliciesSQLMixin:
    """Own queries returning replication_policies rows on the current connection.

    Example:
        Access these operations through ``db.metadata_sql``.
    """

    db: DatabaseAPI

    def get_replication_policies_by_ids(
        self,
        policy_ids: Iterable[int],
    ) -> dict[int, Mapping[str, Any]]:
        """Batch explicit policy references without resolving inheritance or defaults.

        Collapse repeated requests, omit missing rows and preserve request order.
        Bound ID lookups in shared portable SQL; no policy evaluation occurs here.
        """
        return self.db.macros.get_rows_by_ids("replication_policies", policy_ids)
