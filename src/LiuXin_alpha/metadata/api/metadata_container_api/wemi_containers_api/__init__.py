from __future__ import annotations

from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.work_container_api import (
    WorkContainerPropertiesApi,
)
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.wemi_container_api import (
    WorkContainerAPI,
)
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.work_metadata_container_api import (
    WorkMetadataContainerAPI,
    WorkRelationLink,
    WorkStorageHints,
)
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.item_container_api import (
    ItemContainerAPI,
    ItemContainerPropertiesApi,
)
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.item_metadata_container_api import (
    ItemMetadataContainerAPI,
    ItemRelationLink,
    ItemStorageHints,
)

# Historical name used by tests and older imports.
WorkMetadataContainerAPIFromWemiApi = WorkMetadataContainerAPI
ItemMetadataContainerAPIFromWemiApi = ItemMetadataContainerAPI

__all__ = [
    "IdentifierBase",
    "IdentifierStatus",
    "WorkIdentifier",
    "ExpressionIdentifier",
    "ManifestationIdentifier",
    "ItemIdentifier",
    "WorkIdentifiersContainer",
    "ExpressionIdentifiersContainer",
    "ManifestationIdentifiersContainer",
    "ItemIdentifiersContainer",
    "WorkContainerAPI",
    "WorkContainerPropertiesApi",
    "WorkMetadataContainerAPI",
    "WorkMetadataContainerAPIFromWemiApi",
    "WorkRelationLink",
    "WorkStorageHints",
    "ItemContainerAPI",
    "ItemContainerPropertiesApi",
    "ItemMetadataContainerAPI",
    "ItemMetadataContainerAPIFromWemiApi",
    "ItemRelationLink",
    "ItemStorageHints",
]

from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.identifier_containers import (
    IdentifierBase,
    IdentifierStatus,
    WorkIdentifier,
    ExpressionIdentifier,
    ManifestationIdentifier,
    ItemIdentifier,
    WorkIdentifiersContainer,
    ExpressionIdentifiersContainer,
    ManifestationIdentifiersContainer,
    ItemIdentifiersContainer,
)
