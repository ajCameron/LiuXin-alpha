"""Compatibility exports for concrete non-WEMI main-table row containers."""

from __future__ import annotations

from .annotation_row import AnnotationRow
from .comment_row import CommentRow
from .entity_identifier_row import EntityIdentifierRow
from .genre_row import GenreRow
from .human_agent_row import HumanAgentRow
from .label_row import LabelRow
from .language_row import LanguageRow
from .note_row import NoteRow
from .observed_item_identifier_row import ObservedItemIdentifierRow
from .org_agent_relation_row import OrgAgentRelationRow
from .org_agent_row import OrgAgentRow
from .rating_row import RatingRow
from .series_row import SeriesRow
from .subject_row import SubjectRow
from .synopsis_row import SynopsisRow


NON_WEMI_MAIN_TABLE_ROW_CONTAINERS = (
    LanguageRow,
    GenreRow,
    SubjectRow,
    SeriesRow,
    LabelRow,
    NoteRow,
    CommentRow,
    SynopsisRow,
    RatingRow,
    AnnotationRow,
    HumanAgentRow,
    OrgAgentRow,
    OrgAgentRelationRow,
    EntityIdentifierRow,
    ObservedItemIdentifierRow,
)


__all__ = [
    "AnnotationRow",
    "CommentRow",
    "EntityIdentifierRow",
    "GenreRow",
    "HumanAgentRow",
    "LabelRow",
    "LanguageRow",
    "NON_WEMI_MAIN_TABLE_ROW_CONTAINERS",
    "NoteRow",
    "ObservedItemIdentifierRow",
    "OrgAgentRelationRow",
    "OrgAgentRow",
    "RatingRow",
    "SeriesRow",
    "SubjectRow",
    "SynopsisRow",
]
