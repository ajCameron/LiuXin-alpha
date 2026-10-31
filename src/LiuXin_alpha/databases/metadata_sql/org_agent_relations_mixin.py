"""Batch reads of physical org_agent_relations rows; policy stays with callers."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


class OrgAgentRelationsSQLMixin:
    """Own queries returning org_agent_relations rows on the current connection.

    Example:
        Access these operations through ``db.metadata_sql``.
    """

    db: DatabaseAPI

    def get_org_agent_relations_for_parents(
        self,
        agent_ids: Iterable[int],
    ) -> dict[int, tuple[Mapping[str, Any], ...]]:
        """Batch rows by org_agent_relation_parent_agent_id, retaining empty owner groups.

        Order each group by physical row ID. Do not require owners to exist or
        interpret stored metadata. Reads share the caller's current transaction.
        """
        return self.db.macros.get_rows_for_ids(
            "org_agent_relations",
            "org_agent_relation_parent_agent_id",
            agent_ids,
        )

    def get_org_agent_relations_for_children(
        self,
        agent_ids: Iterable[int],
    ) -> dict[int, tuple[Mapping[str, Any], ...]]:
        """Batch rows by org_agent_relation_child_agent_id, retaining empty owner groups.

        Order each group by physical row ID. Do not require owners to exist or
        interpret stored metadata. Reads share the caller's current transaction.
        """
        return self.db.macros.get_rows_for_ids(
            "org_agent_relations",
            "org_agent_relation_child_agent_id",
            agent_ids,
        )
