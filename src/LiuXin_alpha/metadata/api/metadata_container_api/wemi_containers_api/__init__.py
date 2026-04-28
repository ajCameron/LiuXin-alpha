"""Canonical public WEMI metadata-container API surface.

This package exports abstract contracts and API value objects only. Concrete
container implementations live under ``LiuXin_alpha.metadata.containers``.
"""

from __future__ import annotations

from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.agent_containers.agent_identity_api import (
    AgentIdentityAPI,
)
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.agent_containers.agent_profile_api import (
    AgentProfileAPI,
)
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.expression_containers.expression_identity_api import (
    ExpressionIdentityAPI,
    ExpressionIdentityPropertiesAPI,
)
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.expression_containers.expression_metadata_api import (
    ExpressionMetadataAPI,
    ExpressionRelationEdge,
    ExpressionRelationLink,
    ExpressionRelationTarget,
)
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.item_containers.item_identity_api import (
    ItemIdentityAPI,
    ItemIdentityPropertiesAPI,
)
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.item_containers.item_metadata_api import (
    ItemMetadataAPI,
    ItemRelationEdge,
    ItemRelationLink,
    ItemRelationTarget,
)
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.relation_edge_api import (
    ManyManyRelationEdgeAPI,
    ManyOneRelationEdgeAPI,
    OneManyRelationEdgeAPI,
    OneOneRelationEdgeAPI,
    RelationCardinality,
    RelationCardinalityValue,
    RelationEdge,
    RelationEdgeAPI,
    RelationEdgeID,
    RelationEdgeSource,
    normalize_relation_cardinality,
    validate_relation_edge_cardinality,
)
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.relation_target_api import (
    MetadataRecord,
    MetadataScalar,
    MetadataValue,
    MutableMetadataRecord,
    RelationEdgeType,
    RelationTarget,
    SupportsMetadataMapping,
    SupportsRowMapping,
)
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.manifestation_containers.manifestation_identity_api import (
    ManifestationIdentityAPI,
    ManifestationIdentityPropertiesAPI,
)
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.manifestation_containers.manifestation_metadata_api import (
    ManifestationMetadataAPI,
    ManifestationRelationEdge,
    ManifestationRelationLink,
    ManifestationRelationTarget,
)
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.work_containers.work_identity_api import (
    WorkIdentityAPI,
    WorkIdentityPropertiesAPI,
)
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.work_containers.work_metadata_api import (
    WorkMetadataAPI,
    WorkRelationEdge,
    WorkRelationLink,
    WorkRelationTarget,
)

__all__ = [
    "AgentIdentityAPI",
    "AgentProfileAPI",
    "ExpressionIdentityAPI",
    "ExpressionIdentityPropertiesAPI",
    "ExpressionMetadataAPI",
    "ExpressionRelationEdge",
    "ExpressionRelationLink",
    "ExpressionRelationTarget",
    "ItemIdentityAPI",
    "ItemIdentityPropertiesAPI",
    "ItemMetadataAPI",
    "ItemRelationEdge",
    "ItemRelationLink",
    "ItemRelationTarget",
    "MetadataRecord",
    "MetadataScalar",
    "MetadataValue",
    "ManyManyRelationEdgeAPI",
    "ManyOneRelationEdgeAPI",
    "MutableMetadataRecord",
    "OneManyRelationEdgeAPI",
    "OneOneRelationEdgeAPI",
    "RelationCardinality",
    "RelationCardinalityValue",
    "RelationEdge",
    "RelationEdgeAPI",
    "RelationEdgeID",
    "RelationEdgeSource",
    "RelationEdgeType",
    "RelationTarget",
    "SupportsMetadataMapping",
    "SupportsRowMapping",
    "ManifestationIdentityAPI",
    "ManifestationIdentityPropertiesAPI",
    "ManifestationMetadataAPI",
    "ManifestationRelationEdge",
    "ManifestationRelationLink",
    "ManifestationRelationTarget",
    "WorkIdentityAPI",
    "WorkIdentityPropertiesAPI",
    "WorkMetadataAPI",
    "WorkRelationEdge",
    "WorkRelationLink",
    "WorkRelationTarget",
    "normalize_relation_cardinality",
    "validate_relation_edge_cardinality",
]
