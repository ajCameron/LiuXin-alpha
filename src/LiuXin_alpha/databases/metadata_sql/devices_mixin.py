"""Read physical devices rows through shared portable SQL machinery."""

from __future__ import annotations

from typing import TYPE_CHECKING

from LiuXin_alpha.databases.db_types.macro_types import LINK_TYPE_UNSET, LinkedTableRow

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


class DevicesSQLMixin:
    """Own table-specific reads while callers retain interpretation and validation.

    Example:
        Use these methods through ``db.metadata_sql`` on an open database.
    """

    db: DatabaseAPI

    def get_devices_linked_from(
        self,
        source_table: str,
        source_id: int,
        *,
        link_type: object = LINK_TYPE_UNSET,
    ) -> tuple[LinkedTableRow, ...]:
        """Batch physical devices destinations for one declared relationship.

        Resolve the explicit source table through schema introspection. Preserve
        link order, duplicate destinations and all link metadata; missing rows
        remain None. Callers validate source existence and choose relationships.
        Supplied type filters require a typed relation. No new transaction opens.
        """
        spec = self.db.driver_wrapper.get_link_spec(source_table, "devices")
        return self.db.macros.get_linked_rows(spec, source_id, link_type=link_type)
