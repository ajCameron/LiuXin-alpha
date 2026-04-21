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

from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.titles_containers import (
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
)
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.notes_containers import (
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


from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.labels_containers import (
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
