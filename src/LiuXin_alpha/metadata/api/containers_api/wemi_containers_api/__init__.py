"""Canonical public WEMI metadata-container API surface.

This package exports abstract contracts and API value objects only. Concrete
container implementations live under ``LiuXin_alpha.metadata.containers``.
"""

from __future__ import annotations

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.identity_api import (
    WemiIdentityAPI,
)
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.agent_containers.agent_identity_api import (
    AgentIdentityAPI,
)
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.agent_containers.agent_profile_api import (
    AgentProfileAPI,
    HumanAgentProfileAPI,
    OrganisationAgentProfileAPI,
)
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.expression_containers.expression_identity_api import (
    ExpressionFlags,
    ExpressionIdentityAPI,
    ExpressionIdentityPropertiesAPI,
)
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.expression_containers.expression_metadata_api import (
    ExpressionMetadataAPI,
    ExpressionRelationKey,
    ExpressionRelationLink,
    ExpressionRelationTarget,
)
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.item_containers.item_identity_api import (
    ItemIdentityAPI,
    ItemIdentityPropertiesAPI,
)
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.item_containers.item_metadata_api import (
    ItemMetadataAPI,
    ItemRelationKey,
    ItemRelationLink,
    ItemRelationTarget,
)
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.projection_view_api import (
    MetadataTextViewAPI,
    MetadataValuesViewAPI,
    ProjectionIdentifierMap,
    UnloadedMetadataProjectionError,
)
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.relation_link_api import (
    ManyManyRelationLinkAPI,
    ManyOneRelationLinkAPI,
    OneManyRelationLinkAPI,
    OneOneRelationLinkAPI,
    RelationCardinality,
    RelationCardinalityValue,
    RelationLink,
    RelationLinkAPI,
    RelationLinkID,
    RelationLinkSource,
    normalize_relation_cardinality,
    select_primary_relation_link,
    validate_relation_link_cardinality,
)
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.relation_target_api import (
    MetadataRecord,
    MetadataScalar,
    MetadataValue,
    MutableMetadataRecord,
    relation_target_id,
    RelationLinkType,
    RelationTarget,
    SupportsMetadataMapping,
    SupportsRowMapping,
)
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.manifestation_containers.manifestation_identity_api import (
    ManifestationIdentityAPI,
    ManifestationIdentityPropertiesAPI,
)
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.manifestation_containers.manifestation_metadata_api import (
    ManifestationMetadataAPI,
    ManifestationRelationKey,
    ManifestationRelationLink,
    ManifestationRelationTarget,
)
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.work_containers.work_identity_api import (
    WorkIdentityAPI,
    WorkIdentityPropertiesAPI,
)
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.work_containers.work_metadata_api import (
    WorkMetadataAPI,
    WorkRelationKey,
    WorkRelationLink,
    WorkRelationTarget,
)

__all__ = [
    "AgentIdentityAPI",
    "AgentProfileAPI",
    "HumanAgentProfileAPI",
    "OrganisationAgentProfileAPI",
    "WemiIdentityAPI",
    "ExpressionFlags",
    "ExpressionIdentityAPI",
    "ExpressionIdentityPropertiesAPI",
    "ExpressionMetadataAPI",
    "ExpressionRelationKey",
    "ExpressionRelationLink",
    "ExpressionRelationTarget",
    "ItemIdentityAPI",
    "ItemIdentityPropertiesAPI",
    "ItemMetadataAPI",
    "ItemRelationKey",
    "ItemRelationLink",
    "ItemRelationTarget",
    "MetadataRecord",
    "MetadataScalar",
    "MetadataValue",
    "ManyManyRelationLinkAPI",
    "ManyOneRelationLinkAPI",
    "MutableMetadataRecord",
    "OneManyRelationLinkAPI",
    "OneOneRelationLinkAPI",
    "RelationCardinality",
    "RelationCardinalityValue",
    "RelationLink",
    "RelationLinkAPI",
    "RelationLinkID",
    "RelationLinkSource",
    "RelationLinkType",
    "RelationTarget",
    "relation_target_id",
    "SupportsMetadataMapping",
    "SupportsRowMapping",
    "ManifestationIdentityAPI",
    "ManifestationIdentityPropertiesAPI",
    "ManifestationMetadataAPI",
    "ManifestationRelationKey",
    "ManifestationRelationLink",
    "ManifestationRelationTarget",
    "MetadataTextViewAPI",
    "MetadataValuesViewAPI",
    "ProjectionIdentifierMap",
    "UnloadedMetadataProjectionError",
    "WorkIdentityAPI",
    "WorkIdentityPropertiesAPI",
    "WorkMetadataAPI",
    "WorkRelationKey",
    "WorkRelationLink",
    "WorkRelationTarget",
    "normalize_relation_cardinality",
    "select_primary_relation_link",
    "validate_relation_link_cardinality",
]
