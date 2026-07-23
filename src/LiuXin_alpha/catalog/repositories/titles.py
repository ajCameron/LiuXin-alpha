"""Logical title repository over the WEMI tables that own title values."""

from __future__ import annotations

from typing import Any, ClassVar, Sequence, cast

from ..api.common import CatalogMutationError, EntityId, RowInput, RowMapping, WemiLevel
from .base import BaseRepository, WEMI_TABLES


class TitleRepository(BaseRepository):
    """Expose embedded WEMI title columns as logical title records.

    The schema's ``titles`` relation is a read-only compatibility view. This
    repository therefore writes the owning WEMI base row and never writes the
    view.
    """

    table_name = "titles"
    id_column = "title_id"

    _TITLE_COLUMNS: ClassVar[dict[WemiLevel, tuple[str, ...]]] = {
        "work": ("work_title", "work_canonical_title", "work_sort_title"),
        "expression": ("expression_title_override", "expression_subtitle"),
        "manifestation": ("manifestation_subtitle",),
        "item": (),
    }

    @staticmethod
    def _logical_row(level: WemiLevel, row: RowMapping) -> dict[str, Any]:
        entity_id = row[f"{level}_id"]
        columns = TitleRepository._TITLE_COLUMNS[level]
        values = tuple(row.get(column) for column in columns)
        preferred = next((value for value in values if value not in (None, "")), None)
        return {
            "title_id": entity_id,
            "title_entity_type": level,
            "title_entity_id": entity_id,
            "title": preferred,
            "title_values": dict(zip(columns, values)),
        }

    def get(self, entity_id: EntityId) -> RowMapping | None:
        """Return the logical Work title identified by ``entity_id``."""

        row = self._macros.get_row("works", entity_id, id_column="work_id")
        return None if not row else self._logical_row("work", self._as_mapping(row))

    def list(self, *, limit: int = 100, offset: int = 0) -> Sequence[RowMapping]:
        """Return a page of logical Work title records."""

        if limit < 0 or offset < 0:
            raise ValueError("limit and offset cannot be negative")
        works = self._macros.get_rows("works", order_by=("work_id",))
        rows = tuple(self._logical_row("work", self._as_mapping(row)) for row in works)
        return rows[offset : offset + limit]

    def create(self, data: RowInput) -> EntityId:
        """Create a Work containing the supplied logical title."""

        title = data.get("title", data.get("work_title"))
        if not isinstance(title, str) or not title.strip():
            raise CatalogMutationError("a non-empty title is required")
        return cast(
            EntityId,
            self.repositories.works.create({"work_title": title}),
        )

    def update(self, entity_id: EntityId, data: RowInput) -> None:
        """Update the logical title on a Work."""

        mapped = {
            "work_title" if key == "title" else key: value
            for key, value in data.items()
        }
        self.repositories.works.update(entity_id, mapped)

    def delete(self, entity_id: EntityId) -> None:
        """Clear all title-bearing columns on a Work without deleting it."""

        self.repositories.works.update(
            entity_id,
            {column: None for column in self._TITLE_COLUMNS["work"]},
        )

    def add_for_wemi(self, *, level: WemiLevel, entity_id: EntityId, data: RowInput) -> EntityId:
        """Write logical title values onto an existing WEMI row.

        :return: The WEMI entity ID, which identifies the logical title record.
        """

        if level not in WEMI_TABLES:
            raise ValueError(f"unknown WEMI level: {level!r}")
        columns = self._TITLE_COLUMNS[level]
        if not columns:
            raise CatalogMutationError("Items do not own title columns")
        title = data.get("title")
        changes = {
            key: value
            for key, value in data.items()
            if key in columns
        }
        if title is not None:
            changes[columns[0]] = title
        if not changes:
            raise CatalogMutationError(
                f"no writable {level} title values were supplied"
            )
        repository = getattr(self.repositories, f"{level}s")
        repository.update(entity_id, changes)
        return entity_id

    def list_for_wemi(self, *, level: WemiLevel, entity_id: EntityId) -> Sequence[RowMapping]:
        """Return a logical title record when the WEMI row has a title."""

        if level not in WEMI_TABLES:
            raise ValueError(f"unknown WEMI level: {level!r}")
        row = self._require_table_row(WEMI_TABLES[level], entity_id)
        logical = self._logical_row(level, row)
        return () if logical["title"] in (None, "") else (logical,)

    def preferred_for_wemi(self, *, level: WemiLevel, entity_id: EntityId) -> RowMapping | None:
        titles = self.list_for_wemi(level=level, entity_id=entity_id)
        return titles[0] if titles else None


__all__ = ["TitleRepository"]
