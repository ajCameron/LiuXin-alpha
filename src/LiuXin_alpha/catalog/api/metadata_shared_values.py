"""Passive shared metadata identities and pages, independent of editing protocols."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

type VocabularyKind = Literal[
    "tags", "labels", "genres", "subjects", "series", "languages", "ratings"
]
type SharedMetadataKind = Literal[
    "agents",
    "tags",
    "labels",
    "genres",
    "subjects",
    "series",
    "languages",
    "ratings",
    "identifiers",
    "item_identifiers",
    "notes",
    "comments",
    "synopses",
    "annotations",
]


@dataclass(frozen=True, slots=True)
class SharedMetadataRecord:
    """Detached metadata record identity and public aliases, without live SQL Rows."""

    kind: SharedMetadataKind
    entity_id: int
    fields: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class SharedMetadataPage:
    """A complete shared-record page with execution provenance and exact total."""

    records: tuple[SharedMetadataRecord, ...]
    total_count: int
    offset: int
    limit: int
    source: Literal["cache", "database"]
    consistency: Literal["snapshot", "live"]
    generation: int | None

    @property
    def has_more(self) -> bool:
        """Whether the selected population extends beyond this result window."""
        return self.offset + len(self.records) < self.total_count
