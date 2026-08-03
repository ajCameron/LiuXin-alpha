"""Metadata writer API."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol, runtime_checkable

from ..common import CreatedWemiStack, EntityId, RowInput, WemiLevel


@runtime_checkable
class MetadataWriterAPI(Protocol):
    """Coordinated writes that touch multiple catalog tables/links.

    Example::

        catalog.mutations.writer.attach_metadata(
            level="work",
            entity_id=work_id,
            data={
                "title": "Frankenstein",
                "agents": [{"name": "Mary Shelley", "role": "author"}],
                "identifiers": [
                    {"scheme": "wikipedia", "value": "Frankenstein"}
                ],
            },
        )
    """

    def create_wemi_stack(
        self,
        *,
        work: RowInput,
        expression: RowInput,
        manifestation: RowInput,
        items: Sequence[RowInput] = (),
        origin: str | None = None,
        work_id: EntityId | None = None,
    ) -> CreatedWemiStack:
        """Atomically create and link one Work-to-Items WEMI path.

        :param work: New Work values, or replacement values for ``work_id``.
        :param expression: New preferred Expression values.
        :param manifestation: New preferred Manifestation values.
        :param items: Zero or more Items owned by the new Manifestation.
        :param origin: Optional provenance recorded on graph links.
        :param work_id: Optional existing or explicitly requested Work ID.
        :return: IDs of every created WEMI entity.
        """

    def attach_metadata(self, *, level: WemiLevel, entity_id: EntityId, data: RowInput) -> None:
        """Attach normalized metadata to an existing WEMI entity atomically.

        :param level: WEMI level containing ``entity_id``.
        :param entity_id: Existing entity receiving metadata.
        :param data: Supported scalar fields and semantic collections such as
            Agents, identifiers, titles, and notes.
        :return: ``None``.
        :raises CatalogMutationError: If policy or validation rejects any part;
            the coordinated write is rolled back.
        """

    def replace_metadata(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
        data: RowInput,
    ) -> None:
        """Atomically replace selected semantic metadata groups.

        Omitted groups remain unchanged. Present empty groups clear their
        relationships; present values replace the complete selected group.

        :param level: WEMI level containing ``entity_id``.
        :param entity_id: Existing entity whose metadata should change.
        :param data: Direct fields and replacement groups.
        :return: ``None``.
        """

    def link_wemi(
        self,
        *,
        parent_level: WemiLevel,
        parent_id: EntityId,
        child_level: WemiLevel,
        child_id: EntityId,
        primary: bool | None = None,
        priority: int | None = None,
        origin: str | None = None,
    ) -> Mapping[str, object]:
        """Link two existing adjacent WEMI entities atomically.

        :return: A transport-safe authoritative relationship receipt.
        """

    def unlink_wemi(
        self,
        *,
        parent_level: WemiLevel,
        parent_id: EntityId,
        child_level: WemiLevel,
        child_id: EntityId,
    ) -> bool:
        """Remove one adjacent WEMI relationship if it exists.

        :return: ``True`` when a relationship was removed.
        """

    def merge_entities(self, *, level: WemiLevel, source_id: EntityId, target_id: EntityId) -> None:
        """Merge a source WEMI entity into a target transactionally.

        Incoming/outgoing metadata relationships are preserved where schema and
        policy allow, then the source entity is deleted.

        :param level: Shared WEMI level of source and target.
        :param source_id: Duplicate entity to absorb and delete.
        :param target_id: Canonical entity to retain.
        :return: ``None``.
        :raises CatalogMutationError: If IDs are equal, missing, incompatible,
            or a relationship cannot be transferred safely.
        """
