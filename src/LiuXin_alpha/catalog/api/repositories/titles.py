"""Title repository API."""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from ..common import EntityId, RowInput, RowMapping, WemiLevel
from .base import BaseRepositoryAPI


@runtime_checkable
class TitleRepositoryAPI(BaseRepositoryAPI, Protocol):
    """Logical title access over title-bearing WEMI columns.

    The schema's ``titles`` relation is a compatibility view, not a writable
    entity table. This repository presents logical title records while writing
    the owning Work, Expression, or Manifestation row:

    - Work: ``work_title``, then canonical/sort variants;
    - Expression: title override and subtitle;
    - Manifestation: subtitle;
    - Item: no owned title columns, so writes are rejected.

    ``title_id`` in returned logical records is the owning WEMI entity ID.

    Example::

        catalog.titles.add_for_wemi(
            level="expression",
            entity_id=expression_id,
            data={"title": "Frankenstein (revised text)"},
        )
        title = catalog.titles.preferred_for_wemi(
            level="expression",
            entity_id=expression_id,
        )
    """

    def add_for_wemi(self, *, level: WemiLevel, entity_id: EntityId, data: RowInput) -> EntityId:
        """Write logical title values onto an existing WEMI entity.

        ``data["title"]`` targets the level's preferred title column. Storage
        column names for that level may also be supplied.

        :param level: ``"work"``, ``"expression"``, or ``"manifestation"``.
        :param entity_id: Existing entity ID at ``level``.
        :param data: Title value and/or writable level-specific title columns.
        :return: ``entity_id``, which also identifies the logical title record.
        :raises CatalogMutationError: For Items or when no writable title value
            is supplied.
        """

    def list_for_wemi(self, *, level: WemiLevel, entity_id: EntityId) -> Sequence[RowMapping]:
        """
        Return titles linked to a WEMI entity.

        :param level: WEMI level containing ``entity_id``.
        :param entity_id: Existing WEMI entity ID.
        :return: A zero- or one-element sequence containing ``title``,
            ``title_values``, owner type, and owner ID.
        """

    def preferred_for_wemi(self, *, level: WemiLevel, entity_id: EntityId) -> RowMapping | None:
        """
        Return the preferred title for a WEMI entity, if present.

        Preference follows the owning row's declared title-column order.

        :param level: WEMI level containing ``entity_id``.
        :param entity_id: Existing WEMI entity ID.
        :return: Logical title mapping, or ``None`` when all title-bearing
            columns are blank.
        """

    def clear_for_wemi(self, *, level: WemiLevel, entity_id: EntityId) -> None:
        """Clear every title-bearing column owned by one WEMI entity."""

    def replace_for_wemi(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
        data: RowInput | str | None,
    ) -> EntityId | None:
        """Replace all logical title values, or clear them with ``None``."""
