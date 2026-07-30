"""Public contracts and value objects for LiuXin's Catalog layer.

Import the concrete facade from :mod:`LiuXin_alpha.catalog`; import candidates,
results, errors, and protocols from here::

    from LiuXin_alpha.catalog import Catalog
    from LiuXin_alpha.catalog.api import (
        CatalogAPI,
        IdentifierCandidate,
        MetadataCandidate,
    )

    catalog: CatalogAPI = Catalog(db)
    work_id = catalog.works.match_or_create(
        MetadataCandidate({"title": "Frankenstein"})
    )

The semantic entry points are repositories, matching, retrieval, and mutations.
Field-metadata and ``add``/``ensure``/``apply`` protocols describe maintained
compatibility surfaces used by older Row-oriented workflows.
"""

from .catalog import CatalogAPI
from .common import (
    CatalogError,
    CatalogAmbiguousMatchError,
    CatalogMatchConflictError,
    CatalogMatchError,
    CatalogMutationError,
    CatalogNotFoundError,
    EntityId,
    IdentifierCandidate,
    MatchDecision,
    MatchEvidence,
    MatchEvidenceKind,
    MatchResult,
    MetadataCandidate,
    RowInput,
    RowMapping,
    WemiBundle,
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
from .mutations_api import CatalogMutationsAPI

__all__ = [
    "AddAPI",
    "ApplyAPI",
    "BackendGetterAPI",
    "CalibreFieldMetadataAPI",
    "CatalogAPI",
    "CatalogAmbiguousMatchError",
    "CatalogError",
    "CatalogMatchConflictError",
    "CatalogMatchError",
    "CatalogMetadataToolsAPI",
    "CatalogMutationError",
    "CatalogMutationsAPI",
    "CatalogNotFoundError",
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
    "RowInput",
    "RowMapping",
    "SerializedFieldMetadataState",
    "WemiBundle",
    "WemiLevel",
]
