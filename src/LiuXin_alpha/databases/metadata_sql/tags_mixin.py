"""Operate on physical tag vocabulary rows using shared canonical identities.

These helpers neither choose WEMI owners nor attach tags. Catalog owns those
semantic operations. Normalization delegates to the existing database identity
contract, which consumes metadata value rules; no separate rules live here.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

from LiuXin_alpha.databases.db_types.macro_types import LINK_TYPE_UNSET, LinkedTableRow

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


class TagsSQLMixin:
    """Look up, ensure and delete canonical rows in the physical tags table."""

    db: DatabaseAPI

    def delete_tag_by_value(self, tag: str) -> None:
        """Resolve and delete one tag row in a shared portable transaction.

        An absent identity is a no-op. Schema cascades apply to links; this is
        row deletion, not removal of a particular Work's tag membership.
        """
        with self.db.macros.transaction():
            identity = self.db.get_canonical_identity("tags", "tag", tag)
            if identity is not None:
                self.db.macros.delete_row("tags", identity.row_id)

    def get_tag_id_from_value(self, tag: str) -> int | None:
        """Return the canonical tags row ID, or None when no identity matches."""
        identity = self.db.get_canonical_identity("tags", "tag", tag)
        return None if identity is None else identity.row_id

    def add_tag(self, tag_value: str) -> int:
        """Ensure a canonical tags row and return its ID, without linking owners."""
        return self.db.macros.ensure_table_value("tags", "tag", tag_value)

    def get_tag_ids_from_values(self, values: Iterable[str]) -> dict[str, int | None]:
        """Map original Tag spellings to stored IDs or None using batch lookup."""
        return self.db.macros.find_table_values("tags", "tag", values)

    def ensure_tags(self, values: Iterable[str]) -> dict[str, int]:
        """Atomically ensure Tag rows, preserving existing/first input spellings."""
        return self.db.macros.ensure_table_values("tags", "tag", values)

    def delete_tags_by_id(self, tag_ids: Iterable[int]) -> None:
        """Atomically delete Tag rows by ID; schema cascades remove their links."""
        self.db.macros.delete_rows_by_id("tags", tag_ids)

    def get_tags_linked_from(
        self,
        source_table: str,
        source_id: int,
        *,
        link_type: object = LINK_TYPE_UNSET,
    ) -> tuple[LinkedTableRow, ...]:
        """Batch physical tags destinations for one declared relationship.

        Resolve the explicit source table through schema introspection. Preserve
        link order, duplicate destinations and all link metadata; missing rows
        remain None. Callers validate source existence and choose relationships.
        Supplied type filters require a typed relation. No new transaction opens.
        """
        spec = self.db.driver_wrapper.get_link_spec(source_table, "tags")
        return self.db.macros.get_linked_rows(spec, source_id, link_type=link_type)
