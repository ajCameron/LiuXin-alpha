"""Look up, ensure and delete physical Series rows using declared identities."""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

from LiuXin_alpha.databases.db_types.macro_types import LINK_TYPE_UNSET, LinkedTableRow

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


class SeriesSQLMixin:
    """Operate on stored Series without choosing memberships or positions."""

    db: DatabaseAPI

    def get_series_id_from_value(self, series: str) -> int | None:
        """Return the canonical series row ID, or None when no identity matches.

        This reads the physical series table. It does not choose memberships,
        a primary Series, a sequence position or a WEMI owner.
        """
        identity = self.db.get_canonical_identity("series", "series", series)
        return None if identity is None else identity.row_id

    def get_series_ids_from_values(
        self, values: Iterable[str]
    ) -> dict[str, int | None]:
        """Map original Series spellings to stored IDs or None using batch lookup."""
        return self.db.macros.find_table_values("series", "series", values)

    def ensure_series(self, values: Iterable[str]) -> dict[str, int]:
        """Atomically ensure Series rows without choosing memberships or positions."""
        return self.db.macros.ensure_table_values("series", "series", values)

    def delete_series_by_id(self, series_ids: Iterable[int]) -> None:
        """Atomically delete Series rows by ID, subject to hierarchy/link constraints."""
        self.db.macros.delete_rows_by_id("series", series_ids)

    def get_series_linked_from(
        self,
        source_table: str,
        source_id: int,
        *,
        link_type: object = LINK_TYPE_UNSET,
    ) -> tuple[LinkedTableRow, ...]:
        """Batch physical series destinations for one declared relationship.

        Resolve the explicit source table through schema introspection. Preserve
        link order, duplicate destinations and all link metadata; missing rows
        remain None. Callers validate source existence and choose relationships.
        Supplied type filters require a typed relation. No new transaction opens.
        """
        spec = self.db.driver_wrapper.get_link_spec(source_table, "series")
        return self.db.macros.get_linked_rows(spec, source_id, link_type=link_type)
