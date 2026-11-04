"""
Export Catalog protocols, candidate/result values, errors, and field metadata types.

The concrete facade is imported from LiuXin_alpha.catalog. This module collects
the structural CatalogAPI, matching/WEMI values and errors, field-metadata
contracts and serialized shapes, mutation contracts, and legacy metadata-tool
protocols. Concrete entity repository contracts have their own api.repositories
namespace. Importing these values does not create a database or Catalog instance.

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
from .field_metadata_api import (
    CalibreFieldMetadataAPI,
    FieldMetadataAPI,
    FieldMetadataDataType,
    FieldMetadataDeserializerAPI,
    FieldMetadataDisplay,
    FieldMetadataEntry,
    FieldMetadataGetterAPI,
    FieldMetadataKind,
    FieldMetadataMultiplicity,
    FieldMetadataRecord,
    FieldMetadataSearchTarget,
    FieldRecordIndexMap,
    GroupedSearchTerms,
    KnownFieldMetadataDataType,
    SerializedFieldMetadataState,
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
    "CalibreFieldMetadataAPI",
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
    "FieldMetadataAPI",
    "FieldMetadataDataType",
    "FieldMetadataDeserializerAPI",
    "FieldMetadataDisplay",
    "FieldMetadataEntry",
    "FieldMetadataGetterAPI",
    "FieldMetadataKind",
    "FieldMetadataMultiplicity",
    "FieldMetadataRecord",
    "FieldMetadataSearchTarget",
    "FieldRecordIndexMap",
    "FingerprintToolsAPI",
    "GroupedSearchTerms",
    "IdentifierCandidate",
    "IntralinkerAPI",
    "KnownFieldMetadataDataType",
    "MatchDecision",
    "MatchEvidence",
    "MatchEvidenceKind",
    "MatchResult",
    "MetadataCandidate",
    "MetadataWriteReportAPI",
    "MetadataWriteReportMapping",
    "RowInput",
    "RowMapping",
    "SerializedFieldMetadataState",
    "WemiAdjacency",
    "WemiBundle",
    "WemiDirection",
    "WemiGraph",
    "WemiLevel",
]
