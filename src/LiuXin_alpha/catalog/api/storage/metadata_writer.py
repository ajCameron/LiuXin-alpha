"""Metadata writer API."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from ..common import EntityId, RowInput, WemiLevel


@runtime_checkable
class MetadataWriterAPI(Protocol):
    """Coordinated writes that touch multiple catalog tables/links."""

    def attach_metadata(self, *, level: WemiLevel, entity_id: EntityId, data: RowInput) -> None:
        """Attach normalised metadata to a WEMI entity."""

    def merge_entities(self, *, level: WemiLevel, source_id: EntityId, target_id: EntityId) -> None:
        """Merge one WEMI entity into another, preserving links where policy allows."""
