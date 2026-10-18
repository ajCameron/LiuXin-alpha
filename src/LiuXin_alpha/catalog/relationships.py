"""Own endpoint validation, cardinality and persistence for Catalog relationships."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from LiuXin_alpha.catalog.api.common import CatalogMutationError, CatalogNotFoundError
from LiuXin_alpha.databases.macro_types import LinkRow, LinkValue
from LiuXin_alpha.databases.schema_specs import LinkCardinality, StorageLinkSpec


class CatalogRelationships:
    """Share relationship mutations between repositories and Row-oriented adapters."""

    def __init__(self, db: Any) -> None:
        self.db = db

    def _spec(
        self,
        primary_table: str,
        primary_id: int,
        secondary_table: str,
        secondary_id: int,
    ) -> StorageLinkSpec:
        for table, row_id in (
            (primary_table, primary_id),
            (secondary_table, secondary_id),
        ):
            if not isinstance(row_id, int) or isinstance(row_id, bool) or row_id < 0:
                raise CatalogMutationError(
                    "relationship endpoint IDs must be nonnegative integers"
                )
            if self.db.macros.get_row(table, row_id) is None:
                raise CatalogNotFoundError(f"{table} row not found: {row_id}")
        spec = (
            self.db.driver_wrapper.get_intralink_spec(primary_table)
            if primary_table == secondary_table
            else self.db.driver_wrapper.get_link_spec(primary_table, secondary_table)
        )
        if not isinstance(spec, StorageLinkSpec):
            raise CatalogMutationError(
                f"no catalog link exists from {primary_table!r} to {secondary_table!r}"
            )
        return spec

    def link(
        self,
        primary_table: str,
        primary_id: int,
        secondary_table: str,
        secondary_id: int,
        *,
        link_type: str | None = None,
        priority: Any = None,
        extra: Mapping[str, Any] | None = None,
        legacy_priority: bool = False,
    ) -> LinkRow:
        """Validate and upsert one relationship within its read/write transaction."""
        macros = self.db.macros
        with macros.transaction():
            spec = self._spec(primary_table, primary_id, secondary_table, secondary_id)
            rows = macros.get_link_rows(spec, primary_id)
            if spec.cardinality in {
                LinkCardinality.ONE_TO_ONE,
                LinkCardinality.MANY_TO_ONE,
            }:
                if any(row.secondary_id != secondary_id for row in rows):
                    raise CatalogMutationError(
                        "relationship permits only one destination for this source"
                    )
            if spec.ordered:
                priority = self._priority(
                    spec, rows, secondary_id, link_type, priority, legacy_priority
                )
            elif legacy_priority:
                priority = None
            payload = dict(extra or {})
            if legacy_priority:
                payload = {
                    self.db.driver_wrapper.get_link_column(
                        primary_table, secondary_table, key
                    ): value
                    for key, value in payload.items()
                }
            return macros.upsert_link(
                spec,
                primary_id,
                LinkValue(
                    secondary_id,
                    link_type=link_type,
                    priority=priority,
                    extra=payload,
                ),
            )

    def _priority(
        self,
        spec: StorageLinkSpec,
        rows: tuple[LinkRow, ...],
        secondary_id: int,
        link_type: Any,
        priority: Any,
        legacy: bool,
    ) -> Any:
        if legacy:
            if priority in ("highest", "lowest"):
                minimum, maximum = self.db.macros.get_column_extrema(
                    spec.link_table, spec.priority_link_col
                )
                boundary = maximum if priority == "highest" else minimum
                return (
                    1
                    if boundary is None
                    else int(boundary) + (1 if priority == "highest" else -1)
                )
            if priority is None:
                return 0
            if priority == "not_set":
                priority = None
        if priority is not None:
            return priority
        existing = next(
            (
                row
                for row in rows
                if row.secondary_id == secondary_id
                and (not spec.type_part_of_identity or row.link_type == link_type)
            ),
            None,
        )
        if existing is not None:
            return existing.priority
        assigned = [
            row.priority
            for row in rows
            if isinstance(row.priority, (int, float))
            and not isinstance(row.priority, bool)
        ]
        return int(max(assigned, default=0)) + 1

    def unlink(
        self,
        primary_table: str,
        primary_id: int,
        secondary_table: str,
        secondary_id: int,
    ) -> None:
        """Remove all types for this endpoint pair while preserving every other link."""
        macros = self.db.macros
        with macros.transaction():
            spec = self._spec(primary_table, primary_id, secondary_table, secondary_id)
            remaining = tuple(
                LinkValue(
                    row.secondary_id,
                    link_type=row.link_type,
                    priority=row.priority,
                    extra={
                        key: value
                        for key, value in row.extra.items()
                        if key != self.db.driver_wrapper.get_id_column(spec.link_table)
                    },
                )
                for row in macros.get_link_rows(spec, primary_id)
                if row.secondary_id != secondary_id
            )
            macros.replace_links(spec, primary_id, remaining)
