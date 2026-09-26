"""
Export the public contracts for metadata rows and same-table relations.

The surface covers vocabulary, note, annotation, agent and identifier rows outside
the core WEMI identity stack. Concrete row classes are exported from the metadata
containers package.

Example:
    Exercise this contract with pytest::

        python -m pytest -q tests/metadata/api/test_non_wemi_container_api.py
"""

from __future__ import annotations

from LiuXin_alpha.metadata.api.containers_api.main_table_containers_api.row_api import (
    AnnotationRowAPI,
    CommentRowAPI,
    EntityIdentifierRowAPI,
    GenreRowAPI,
    HumanAgentRowAPI,
    LabelRowAPI,
    LanguageRowAPI,
    MetadataRowMapping,
    MetadataRowValue,
    MetadataTableRowAPI,
    NoteRowAPI,
    ObservedItemIdentifierRowAPI,
    OrgAgentRelationRowAPI,
    OrgAgentRowAPI,
    RatingRowAPI,
    SeriesRowAPI,
    SubjectRowAPI,
    SynopsisRowAPI,
    TagRowAPI,
)
from LiuXin_alpha.metadata.api.containers_api.main_table_containers_api.self_relation_api import (
    GenreTreeRelationAPI,
    GenreTreeRelationsContainerAPI,
    InlineSelfRelationAPI,
    SelfRelationsContainerAPI,
    SeriesTreeRelationAPI,
    SeriesTreeRelationsContainerAPI,
    SubjectTreeRelationAPI,
    SubjectTreeRelationsContainerAPI,
)

__all__ = [
    "AnnotationRowAPI",
    "CommentRowAPI",
    "EntityIdentifierRowAPI",
    "GenreRowAPI",
    "GenreTreeRelationAPI",
    "GenreTreeRelationsContainerAPI",
    "HumanAgentRowAPI",
    "InlineSelfRelationAPI",
    "LabelRowAPI",
    "LanguageRowAPI",
    "MetadataRowMapping",
    "MetadataRowValue",
    "MetadataTableRowAPI",
    "NoteRowAPI",
    "ObservedItemIdentifierRowAPI",
    "OrgAgentRelationRowAPI",
    "OrgAgentRowAPI",
    "RatingRowAPI",
    "SelfRelationsContainerAPI",
    "SeriesRowAPI",
    "SeriesTreeRelationAPI",
    "SeriesTreeRelationsContainerAPI",
    "SubjectRowAPI",
    "SubjectTreeRelationAPI",
    "SubjectTreeRelationsContainerAPI",
    "SynopsisRowAPI",
    "TagRowAPI",
]
