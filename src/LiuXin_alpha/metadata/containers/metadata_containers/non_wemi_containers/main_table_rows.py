"""Concrete containers for metadata-owned non-WEMI main tables.

These are row-shaped implementation containers. They deliberately mirror the
database columns for durable lookup/sidecar metadata tables, while staying out
of the pure abstract ``metadata.api`` surface.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from LiuXin_alpha.databases.db_types import IdentifierEntityType, IdentifierScheme

from ._row_base import MetadataTableRow


@dataclass(slots=True, kw_only=True)
class LanguageRow(MetadataTableRow):
    TABLE_NAME: ClassVar[str] = "languages"
    ID_COLUMN: ClassVar[str] = "language_id"

    language_id: int | None = None
    language: str | None = None
    language_code: str | None = None
    language_iso639_1: str | None = None
    language_iso639_2_b: str | None = None
    language_iso639_2_t: str | None = None
    language_bcp47_primary: str | None = None
    language_bcp47_variants: str | None = None
    language_created_timestamp_ep_k: int | None = None
    language_modified_timestamp_ep_k: int | None = None
    language_source_created_datestamp_ep_k: int | None = None
    language_source_modified_datestamp_ep_k: int | None = None
    language_scratch: str | None = None

    @property
    def display_name(self) -> str | None:
        return self.language or self.language_code or self.language_bcp47_primary


@dataclass(slots=True, kw_only=True)
class GenreRow(MetadataTableRow):
    TABLE_NAME: ClassVar[str] = "genres"
    ID_COLUMN: ClassVar[str] = "genre_id"

    genre_id: int | None = None
    genre: str | None = None
    genre_sort: str | None = None
    genre_phash: str | None = None
    genre_parent_id: int | None = None
    genre_position: int | None = None
    genre_tree_id: int | None = None
    genre_full: str | None = None
    genre_created_timestamp_ep_k: int | None = None
    genre_modified_timestamp_ep_k: int | None = None
    genre_source_created_datestamp_ep_k: int | None = None
    genre_source_modified_datestamp_ep_k: int | None = None
    genre_scratch: str | None = None


@dataclass(slots=True, kw_only=True)
class SubjectRow(MetadataTableRow):
    TABLE_NAME: ClassVar[str] = "subjects"
    ID_COLUMN: ClassVar[str] = "subject_id"

    subject_id: int | None = None
    subject: str | None = None
    subject_phash: str | None = None
    subject_sort: str | None = None
    subject_parent_id: int | None = None
    subject_parent_position: int | None = None
    subject_tree_id: str | None = None
    subject_full: str | None = None
    subject_created_timestamp_ep_k: int | None = None
    subject_modified_timestamp_ep_k: int | None = None
    subject_source_created_datestamp_ep_k: int | None = None
    subject_source_modified_datestamp_ep_k: int | None = None
    subject_scratch: str | None = None


@dataclass(slots=True, kw_only=True)
class SeriesRow(MetadataTableRow):
    TABLE_NAME: ClassVar[str] = "series"
    ID_COLUMN: ClassVar[str] = "series_id"

    series_id: int | None = None
    series: str | None = None
    series_name_norm: str | None = None
    series_sort: str | None = None
    series_phash: str | None = None
    series_over_author: int | None = None
    series_parent_id: int | None = None
    series_parent_position: int | None = None
    series_tree_id: str | None = None
    series_full: str | None = None
    series_created_timestamp_ep_k: int | None = None
    series_modified_timestamp_ep_k: int | None = None
    series_source_created_datestamp_ep_k: int | None = None
    series_source_modified_datestamp_ep_k: int | None = None
    series_scratch: str | None = None


@dataclass(slots=True, kw_only=True)
class LabelRow(MetadataTableRow):
    TABLE_NAME: ClassVar[str] = "labels"
    ID_COLUMN: ClassVar[str] = "label_id"

    label_id: int | None = None
    label_text: str | None = None
    label_text_norm: str | None = None
    label_description: str | None = None
    label_scratch: str | None = None
    label_created_timestamp_ep_k: int | None = None
    label_modified_timestamp_ep_k: int | None = None
    label_source_created_datestamp_ep_k: int | None = None
    label_source_modified_datestamp_ep_k: int | None = None


@dataclass(slots=True, kw_only=True)
class NoteRow(MetadataTableRow):
    TABLE_NAME: ClassVar[str] = "notes"
    ID_COLUMN: ClassVar[str] = "note_id"

    note_id: int | None = None
    note: str | None = None
    note_created_timestamp_ep_k: int | None = None
    note_modified_timestamp_ep_k: int | None = None
    note_source_created_datestamp_ep_k: int | None = None
    note_source_modified_datestamp_ep_k: int | None = None
    note_scratch: str | None = None


@dataclass(slots=True, kw_only=True)
class CommentRow(MetadataTableRow):
    TABLE_NAME: ClassVar[str] = "comments"
    ID_COLUMN: ClassVar[str] = "comment_id"

    comment_id: int | None = None
    comment: str | None = None
    comment_created_timestamp_ep_k: int | None = None
    comment_modified_timestamp_ep_k: int | None = None
    comment_source_created_datestamp_ep_k: int | None = None
    comment_source_modified_datestamp_ep_k: int | None = None
    comment_scratch: str | None = None


@dataclass(slots=True, kw_only=True)
class SynopsisRow(MetadataTableRow):
    TABLE_NAME: ClassVar[str] = "synopses"
    ID_COLUMN: ClassVar[str] = "synopsis_id"

    synopsis_id: int | None = None
    synopsis: str | None = None
    synopsis_created_timestamp_ep_k: int | None = None
    synopsis_modified_timestamp_ep_k: int | None = None
    synopsis_source_created_datestamp_ep_k: int | None = None
    synopsis_source_modified_datestamp_ep_k: int | None = None
    synopsis_scratch: str | None = None


@dataclass(slots=True, kw_only=True)
class RatingRow(MetadataTableRow):
    TABLE_NAME: ClassVar[str] = "ratings"
    ID_COLUMN: ClassVar[str] = "rating_id"

    rating_id: int | None = None
    rating: float | None = None
    rating_out_of: int | None = None
    rating_for_calibre_tag_viewer: int | None = None
    rating_source: str | None = None
    rating_created_timestamp_ep_k: int | None = None
    rating_modified_timestamp_ep_k: int | None = None
    rating_source_created_datestamp_ep_k: int | None = None
    rating_source_modified_datestamp_ep_k: int | None = None
    rating_scratch: str | None = None


@dataclass(slots=True, kw_only=True)
class AnnotationRow(MetadataTableRow):
    TABLE_NAME: ClassVar[str] = "annotations"
    ID_COLUMN: ClassVar[str] = "annotation_id"

    annotation_id: int | None = None
    annotation_user_id: int | None = None
    annotation_item_id: int | None = None
    annotation_kind: str | None = None
    annotation_anchor_type: str | None = None
    annotation_anchor_start: str | None = None
    annotation_anchor_end: str | None = None
    annotation_selected_text: str | None = None
    annotation_note_text: str | None = None
    annotation_source_created_datestamp_ep_k: int | None = None
    annotation_source_modified_datestamp_ep_k: int | None = None
    annotation_source_deleted_datestamp_ep_k: int | None = None
    annotation_source: str | None = None
    annotation_device_id: int | None = None
    annotation_extra_json: str | None = None
    annotation_created_timestamp_ep_k: int | None = None
    annotation_modified_timestamp_ep_k: int | None = None
    annotation_scratch: str | None = None


@dataclass(slots=True, kw_only=True)
class HumanAgentRow(MetadataTableRow):
    TABLE_NAME: ClassVar[str] = "human_agents"
    ID_COLUMN: ClassVar[str] = "human_agent_id"

    human_agent_id: int | None = None
    human_agent_agent_id: int | None = None
    human_agent_given_name: str | None = None
    human_agent_middle_name: str | None = None
    human_agent_family_name: str | None = None
    human_agent_prefix: str | None = None
    human_agent_suffix: str | None = None
    human_agent_preferred_name: str | None = None
    human_agent_birth_date: str | None = None
    human_agent_death_date: str | None = None
    human_agent_nationality: str | None = None
    human_agent_biography: str | None = None
    human_agent_created_timestamp_ep_k: int | None = None
    human_agent_modified_timestamp_ep_k: int | None = None
    human_agent_scratch: str | None = None


@dataclass(slots=True, kw_only=True)
class OrgAgentRow(MetadataTableRow):
    TABLE_NAME: ClassVar[str] = "org_agents"
    ID_COLUMN: ClassVar[str] = "org_agent_id"

    org_agent_id: int | None = None
    org_agent_agent_id: int | None = None
    org_agent_legal_name: str | None = None
    org_agent_trading_name: str | None = None
    org_agent_registration_id: str | None = None
    org_agent_jurisdiction: str | None = None
    org_agent_founded_date: str | None = None
    org_agent_dissolved_date: str | None = None
    org_agent_website: str | None = None
    org_agent_contact_email: str | None = None
    org_agent_description: str | None = None
    org_agent_created_timestamp_ep_k: int | None = None
    org_agent_modified_timestamp_ep_k: int | None = None
    org_agent_scratch: str | None = None


@dataclass(slots=True, kw_only=True)
class OrgAgentRelationRow(MetadataTableRow):
    TABLE_NAME: ClassVar[str] = "org_agent_relations"
    ID_COLUMN: ClassVar[str] = "org_agent_relation_id"

    org_agent_relation_id: int | None = None
    org_agent_relation_child_agent_id: int | None = None
    org_agent_relation_parent_agent_id: int | None = None
    org_agent_relation_type: str | None = None
    org_agent_relation_start_date: str | None = None
    org_agent_relation_end_date: str | None = None
    org_agent_relation_note: str | None = None
    org_agent_relation_created_timestamp_ep_k: int | None = None
    org_agent_relation_modified_timestamp_ep_k: int | None = None
    org_agent_relation_source_created_datestamp_ep_k: int | None = None
    org_agent_relation_source_modified_datestamp_ep_k: int | None = None
    org_agent_relation_scratch: str | None = None


@dataclass(slots=True, kw_only=True)
class EntityIdentifierRow(MetadataTableRow):
    TABLE_NAME: ClassVar[str] = "entity_identifiers"
    ID_COLUMN: ClassVar[str] = "entity_identifier_id"

    entity_identifier_id: int | None = None
    entity_identifier_entity_type: IdentifierEntityType | str | None = None
    entity_identifier_entity_id: int | None = None
    entity_identifier_scheme: IdentifierScheme | str | None = None
    entity_identifier_value: str | None = None
    entity_identifier_is_primary: int | None = None
    entity_identifier_provenance: str | None = None
    entity_identifier_created_timestamp_ep_k: int | None = None
    entity_identifier_modified_timestamp_ep_k: int | None = None
    entity_identifier_source_created_datestamp_ep_k: int | None = None
    entity_identifier_source_modified_datestamp_ep_k: int | None = None
    entity_identifier_scratch: str | None = None


@dataclass(slots=True, kw_only=True)
class ObservedItemIdentifierRow(MetadataTableRow):
    TABLE_NAME: ClassVar[str] = "item_identifiers"
    ID_COLUMN: ClassVar[str] = "item_identifier_id"

    item_identifier_id: int | None = None
    item_identifier_item_id: int | None = None
    item_identifier_scheme: IdentifierScheme | str | None = None
    item_identifier_value: str | None = None
    item_identifier_source: str | None = None
    item_identifier_created_timestamp_ep_k: int | None = None
    item_identifier_modified_timestamp_ep_k: int | None = None
    item_identifier_source_created_datestamp_ep_k: int | None = None
    item_identifier_source_modified_datestamp_ep_k: int | None = None
    item_identifier_scratch: str | None = None


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
