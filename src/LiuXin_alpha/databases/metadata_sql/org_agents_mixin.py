"""Read physical org_agents rows through shared portable SQL machinery."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


class OrgAgentsSQLMixin:
    """Own table-specific reads while callers retain interpretation and validation.

    Example:
        Use these methods through ``db.metadata_sql`` on an open database.
    """

    db: DatabaseAPI

    def get_org_agents_for_agents(
        self,
        agent_ids: Iterable[int],
    ) -> dict[int, tuple[Mapping[str, Any], ...]]:
        """Batch org_agents rows by their stored org_agent_agent_id references.

        Keep an ID-ordered tuple for every requested owner, including empty
        groups. No owner selection, validation or value normalization is applied.
        Optional filters use exact stored values; None leaves them unfiltered.
        Read the current connection without committing or creating a snapshot.
        """
        return self.db.macros.get_rows_for_ids(
            "org_agents",
            "org_agent_agent_id",
            agent_ids,
        )
