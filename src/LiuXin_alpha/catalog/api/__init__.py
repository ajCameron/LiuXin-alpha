"""
Export Catalog protocols, candidate/result values and errors.

The concrete facade is imported from LiuXin_alpha.catalog. This module collects
the structural CatalogAPI, matching/WEMI values and errors,
mutation contracts, and legacy metadata-tool
protocols. Concrete entity repository contracts have their own api.repositories
namespace. Calibre field-descriptor contracts live in
utils.calibre_compat.metadata.field_metadata_api; native descriptor snapshots live
in caches.field_metadata. Importing these values creates no database or Catalog.

The separate api.metadata_access module declares MetadataAPIInterface, implemented
by caches.metadata.MetadataAPI and its CatalogCache composition. Its placement here supplies the semantic contract;
the existing Catalog facade does not implement its reconciliation obligations.

Example:
    >>> candidate = MetadataCandidate({"title": "Frankenstein"}, source="manual")
    >>> candidate.data["title"]
    'Frankenstein'
    >>> from LiuXin_alpha.catalog.catalog import Catalog
    >>> catalog: CatalogAPI = Catalog(db)  # doctest: +SKIP
    >>> work_id = catalog.works.match_or_create(candidate)  # doctest: +SKIP

Type aliases and protocol annotations do not establish runtime data validity.
Consult each value's constructor contract and the receiving repository or writer
for validation, copying, persistence, and identity-decision behavior.
"""

from .catalog import CatalogAPI
from .search import CatalogSearchAPI
from .calibre import CalibreCompatibilityAPI
from .common import (
    CatalogAmbiguousMatchError,
    CatalogError,
    CatalogMatchConflictError,
    CatalogMatchError,
    CatalogMutationError,
    CatalogNotFoundError,
    CreatedWemiStack,
    EntityId,
    IdentifierCandidate,
    MatchDecision,
    MatchEvidence,
    MatchEvidenceKind,
    MatchResult,
    MetadataCandidate,
    RowInput,
    RowMapping,
    WemiAdjacency,
    WemiBundle,
    WemiDirection,
    WemiGraph,
    WemiLevel,
)
from .metadata_tools_api import (
    AddAPI,
    ApplyAPI,
    BackendGetterAPI,
    CatalogMetadataToolsAPI,
    EnsureAPI,
    FingerprintToolsAPI,
    IntralinkerAPI,
)
from .mutations_api import (
    CatalogMutationsAPI,
    MetadataWriteReportAPI,
    MetadataWriteReportMapping,
)

__all__ = [
    "AddAPI",
    "ApplyAPI",
    "BackendGetterAPI",
    "CatalogAPI",
    "CatalogSearchAPI",
    "CalibreCompatibilityAPI",
    "CatalogAmbiguousMatchError",
    "CatalogError",
    "CatalogMatchConflictError",
    "CatalogMatchError",
    "CatalogMetadataToolsAPI",
    "CatalogMutationError",
    "CatalogMutationsAPI",
    "CatalogNotFoundError",
    "CreatedWemiStack",
    "EnsureAPI",
    "EntityId",
    "FingerprintToolsAPI",
    "IdentifierCandidate",
    "IntralinkerAPI",
    "MatchDecision",
    "MatchEvidence",
    "MatchEvidenceKind",
    "MatchResult",
    "MetadataCandidate",
    "MetadataWriteReportAPI",
    "MetadataWriteReportMapping",
    "RowInput",
    "RowMapping",
    "WemiAdjacency",
    "WemiBundle",
    "WemiDirection",
    "WemiGraph",
    "WemiLevel",
]
