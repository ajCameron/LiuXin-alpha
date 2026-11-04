"""Read physical entity_identifiers rows through shared portable SQL machinery."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


class EntityIdentifiersSQLMixin:
    """Own table-specific reads while callers retain interpretation and validation.

    Example:
        Use these methods through ``db.metadata_sql`` on an open database.
    """

    db: DatabaseAPI

    def get_entity_identifiers_for_owners(
        self,
        owner_ids: Iterable[int],
        *,
        entity_type: str,
    ) -> dict[int, tuple[Mapping[str, Any], ...]]:
        """Batch entity_identifiers rows by their stored entity_identifier_entity_id references.

        Keep an ID-ordered tuple for every requested owner, including empty
        groups. No owner selection, validation or value normalization is applied.
        Optional filters use exact stored values; None leaves them unfiltered.
        Read the current connection without committing or creating a snapshot.
        """
        where = {}
        where["entity_identifier_entity_type"] = entity_type
        return self.db.macros.get_rows_for_ids(
            "entity_identifiers",
            "entity_identifier_entity_id",
            owner_ids,
            where=where,
        )
