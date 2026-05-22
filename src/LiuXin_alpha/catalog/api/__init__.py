"""Public API contracts for the catalog layer."""

from .catalog import CatalogAPI
from .common import (
    CatalogError,
    CatalogMutationError,
    CatalogNotFoundError,
    EntityId,
    IdentifierCandidate,
    MatchResult,
    MetadataCandidate,
    RowInput,
    RowMapping,
    WemiBundle,
    WemiLevel,
)

__all__ = [
    "CatalogAPI",
    "CatalogError",
    "CatalogMutationError",
    "CatalogNotFoundError",
    "EntityId",
    "IdentifierCandidate",
    "MatchResult",
    "MetadataCandidate",
    "RowInput",
    "RowMapping",
    "WemiBundle",
    "WemiLevel",
]
