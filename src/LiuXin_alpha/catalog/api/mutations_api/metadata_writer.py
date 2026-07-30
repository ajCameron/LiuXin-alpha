"""Metadata writer API."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from ..common import EntityId, RowInput, WemiLevel


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
