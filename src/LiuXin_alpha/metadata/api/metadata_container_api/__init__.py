
"""
Specialized containers for all metadata options.

There is a container for every table on the system.

Usually these containers have two components
 - the item container - which represents the object itself
   - This is typically called something like "AgentIdentityAPI"
 - the item metadata container - represents the items links to other objects.

"""

from __future__ import annotations

from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api import (
    ItemMetadataContainerAPI,
    ItemMetadataContainerAPIFromWemiApi,
    ItemRelationLink,
    ItemStorageHints,
    WorkContainerAPI,
    WorkMetadataContainerAPI,
    WorkMetadataContainerAPIFromWemiApi,
    WorkRelationLink,
    WorkStorageHints,
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
    TitleKind,
    TitleBase,
    WorkTitle,
    ExpressionTitle,
    ManifestationTitle,
    ItemTitle,
    ItemWemiTitleSlice,
    WorkTitlesContainer,
    ExpressionTitlesContainer,
    ManifestationTitlesContainer,
    ItemTitlesContainer,
    NoteKind,
    NoteFormat,
    NoteVisibility,
    NoteBase,
    WorkNote,
    ExpressionNote,
    ManifestationNote,
    ItemNote,
    WorkNotesContainer,
    ExpressionNotesContainer,
    ManifestationNotesContainer,
    ItemNotesContainer,
)
from LiuXin_alpha.metadata.api.metadata_container_api import WorkContainerPropertiesApi

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
    "TitleKind",
    "TitleBase",
    "WorkTitle",
    "ExpressionTitle",
    "ManifestationTitle",
    "ItemTitle",
    "ItemWemiTitleSlice",
    "WorkTitlesContainer",
    "ExpressionTitlesContainer",
    "ManifestationTitlesContainer",
    "ItemTitlesContainer",
    "NoteKind",
    "NoteFormat",
    "NoteVisibility",
    "NoteBase",
    "WorkNote",
    "ExpressionNote",
    "ManifestationNote",
    "ItemNote",
    "WorkNotesContainer",
    "ExpressionNotesContainer",
    "ManifestationNotesContainer",
    "ItemNotesContainer",
    "WorkContainerAPI",
    "WorkContainerPropertiesApi",
    "WorkMetadataContainerAPI",
    "WorkMetadataContainerAPIFromWemiApi",
    "WorkRelationLink",
    "WorkStorageHints",
    "ItemMetadataContainerAPIFromWemiApi",
]


from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api import (
    LabelKind,
    LabelBase,
    WorkLabel,
    ExpressionLabel,
    ManifestationLabel,
    ItemLabel,
    WorkLabelsContainer,
    ExpressionLabelsContainer,
    ManifestationLabelsContainer,
    ItemLabelsContainer,
)

__all__.extend([
    "LabelKind",
    "LabelBase",
    "WorkLabel",
    "ExpressionLabel",
    "ManifestationLabel",
    "ItemLabel",
    "WorkLabelsContainer",
    "ExpressionLabelsContainer",
    "ManifestationLabelsContainer",
    "ItemLabelsContainer",
])
