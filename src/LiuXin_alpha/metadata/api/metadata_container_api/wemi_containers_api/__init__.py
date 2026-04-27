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
    ExpressionRelationLink,
    ExpressionStorageHints,
)
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.item_containers.item_identity_api import (
    ItemIdentityAPI,
    ItemIdentityPropertiesAPI,
)
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.item_containers.item_metadata_api import (
    ItemMetadataAPI,
    ItemRelationLink,
    ItemStorageHints,
)
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.manifestation_containers.manifestation_identity_api import (
    ManifestationIdentityAPI,
    ManifestationIdentityPropertiesAPI,
)
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.manifestation_containers.manifestation_metadata_api import (
    ManifestationMetadataAPI,
    ManifestationRelationLink,
    ManifestationStorageHints,
)
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.work_containers.work_identity_api import (
    WorkIdentityAPI,
    WorkIdentityPropertiesAPI,
)
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.work_containers.work_metadata_api import (
    WorkMetadataAPI,
    WorkRelationLink,
    WorkStorageHints,
)

__all__ = [
    "AgentIdentityAPI",
    "AgentProfileAPI",
    "ExpressionIdentityAPI",
    "ExpressionIdentityPropertiesAPI",
    "ExpressionMetadataAPI",
    "ExpressionRelationLink",
    "ExpressionStorageHints",
    "ItemIdentityAPI",
    "ItemIdentityPropertiesAPI",
    "ItemMetadataAPI",
    "ItemRelationLink",
    "ItemStorageHints",
    "ManifestationIdentityAPI",
    "ManifestationIdentityPropertiesAPI",
    "ManifestationMetadataAPI",
    "ManifestationRelationLink",
    "ManifestationStorageHints",
    "WorkIdentityAPI",
    "WorkIdentityPropertiesAPI",
    "WorkMetadataAPI",
    "WorkRelationLink",
    "WorkStorageHints",
]
