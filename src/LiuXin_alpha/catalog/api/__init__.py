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
from .mutations import CatalogMutationsAPI

__all__ = [
    "CatalogAPI",
    "CatalogError",
    "CatalogMutationError",
    "CatalogMutationsAPI",
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
