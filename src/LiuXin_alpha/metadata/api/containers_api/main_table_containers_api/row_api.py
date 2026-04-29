"""Pure API contracts for non-WEMI metadata main-table row containers.

Category: metadata main-table row API.
This module defines structural contracts for metadata-owned lookup and agent
rows that sit outside the core W/E/M/I entity stack.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import ClassVar, Protocol, Self, TypeAlias, runtime_checkable

from LiuXin_alpha.databases.db_types import IdentifierEntityType, IdentifierScheme


MetadataRowValue: TypeAlias = str | int | float | bool | None
MetadataRowMapping: TypeAlias = Mapping[str, MetadataRowValue]


@runtime_checkable
class MetadataTableRowAPI(Protocol):
    """
    Structural API for one metadata-owned non-WEMI table row.

    Implementations should expose a stable table name, primary-id column, and
    mapping round-trip for database-bound row payloads.
    """

    TABLE_NAME: ClassVar[str]
    ID_COLUMN: ClassVar[str]

    @classmethod
    def from_mapping(cls, row: MetadataRowMapping) -> Self:
        """
        Build a row container from a database-like mapping.

        :param row:
        :return:
        """

    @property
    def primary_id(self) -> int | None:
        """
        Primary database id for this row.

        :return:
        """

    def to_mapping(self) -> dict[str, MetadataRowValue]:
        """
        Serialize this row container to column-keyed mapping form.

        :return:
        """


@runtime_checkable
class LanguageRowAPI(MetadataTableRowAPI, Protocol):
    TABLE_NAME: ClassVar[str]
    ID_COLUMN: ClassVar[str]

    language_id: int | None
    language: str | None
    language_code: str | None
    language_iso639_1: str | None
    language_iso639_2_b: str | None
    language_iso639_2_t: str | None
    language_bcp47_primary: str | None
    language_bcp47_variants: str | None
    language_created_timestamp_ep_k: int | None
    language_modified_timestamp_ep_k: int | None
    language_source_created_datestamp_ep_k: int | None
    language_source_modified_datestamp_ep_k: int | None
    language_scratch: str | None

    @property
    def display_name(self) -> str | None:
        """
        Human-facing language display name.

        :return:
        """


@runtime_checkable
class GenreRowAPI(MetadataTableRowAPI, Protocol):
    TABLE_NAME: ClassVar[str]
    ID_COLUMN: ClassVar[str]

    genre_id: int | None
    genre: str | None
    genre_sort: str | None
    genre_phash: str | None
    genre_parent_id: int | None
    genre_position: int | None
    genre_tree_id: int | None
    genre_full: str | None
    genre_created_timestamp_ep_k: int | None
    genre_modified_timestamp_ep_k: int | None
    genre_source_created_datestamp_ep_k: int | None
    genre_source_modified_datestamp_ep_k: int | None
    genre_scratch: str | None


@runtime_checkable
class SubjectRowAPI(MetadataTableRowAPI, Protocol):
    TABLE_NAME: ClassVar[str]
    ID_COLUMN: ClassVar[str]

    subject_id: int | None
    subject: str | None
    subject_phash: str | None
    subject_sort: str | None
    subject_parent_id: int | None
    subject_parent_position: int | None
    subject_tree_id: str | None
    subject_full: str | None
    subject_created_timestamp_ep_k: int | None
    subject_modified_timestamp_ep_k: int | None
    subject_source_created_datestamp_ep_k: int | None
    subject_source_modified_datestamp_ep_k: int | None
    subject_scratch: str | None


@runtime_checkable
class SeriesRowAPI(MetadataTableRowAPI, Protocol):
    TABLE_NAME: ClassVar[str]
    ID_COLUMN: ClassVar[str]

    series_id: int | None
    series: str | None
    series_name_norm: str | None
    series_sort: str | None
    series_phash: str | None
    series_over_author: int | None
    series_parent_id: int | None
    series_parent_position: int | None
    series_tree_id: str | None
    series_full: str | None
    series_created_timestamp_ep_k: int | None
    series_modified_timestamp_ep_k: int | None
    series_source_created_datestamp_ep_k: int | None
    series_source_modified_datestamp_ep_k: int | None
    series_scratch: str | None


@runtime_checkable
class LabelRowAPI(MetadataTableRowAPI, Protocol):
    TABLE_NAME: ClassVar[str]
    ID_COLUMN: ClassVar[str]

    label_id: int | None
    label_text: str | None
    label_text_norm: str | None
    label_description: str | None
    label_scratch: str | None
    label_created_timestamp_ep_k: int | None
    label_modified_timestamp_ep_k: int | None
    label_source_created_datestamp_ep_k: int | None
    label_source_modified_datestamp_ep_k: int | None


@runtime_checkable
class NoteRowAPI(MetadataTableRowAPI, Protocol):
    TABLE_NAME: ClassVar[str]
    ID_COLUMN: ClassVar[str]

    note_id: int | None
    note: str | None
    note_created_timestamp_ep_k: int | None
    note_modified_timestamp_ep_k: int | None
    note_source_created_datestamp_ep_k: int | None
    note_source_modified_datestamp_ep_k: int | None
    note_scratch: str | None


@runtime_checkable
class CommentRowAPI(MetadataTableRowAPI, Protocol):
    TABLE_NAME: ClassVar[str]
    ID_COLUMN: ClassVar[str]

    comment_id: int | None
    comment: str | None
    comment_created_timestamp_ep_k: int | None
    comment_modified_timestamp_ep_k: int | None
    comment_source_created_datestamp_ep_k: int | None
    comment_source_modified_datestamp_ep_k: int | None
    comment_scratch: str | None


@runtime_checkable
class SynopsisRowAPI(MetadataTableRowAPI, Protocol):
    TABLE_NAME: ClassVar[str]
    ID_COLUMN: ClassVar[str]

    synopsis_id: int | None
    synopsis: str | None
    synopsis_created_timestamp_ep_k: int | None
    synopsis_modified_timestamp_ep_k: int | None
    synopsis_source_created_datestamp_ep_k: int | None
    synopsis_source_modified_datestamp_ep_k: int | None
    synopsis_scratch: str | None


@runtime_checkable
class RatingRowAPI(MetadataTableRowAPI, Protocol):
    TABLE_NAME: ClassVar[str]
    ID_COLUMN: ClassVar[str]

    rating_id: int | None
    rating: float | None
    rating_out_of: int | None
    rating_for_calibre_tag_viewer: int | None
    rating_source: str | None
    rating_created_timestamp_ep_k: int | None
    rating_modified_timestamp_ep_k: int | None
    rating_source_created_datestamp_ep_k: int | None
    rating_source_modified_datestamp_ep_k: int | None
    rating_scratch: str | None


@runtime_checkable
class AnnotationRowAPI(MetadataTableRowAPI, Protocol):
    TABLE_NAME: ClassVar[str]
    ID_COLUMN: ClassVar[str]

    annotation_id: int | None
    annotation_user_id: int | None
    annotation_item_id: int | None
    annotation_kind: str | None
    annotation_anchor_type: str | None
    annotation_anchor_start: str | None
    annotation_anchor_end: str | None
    annotation_selected_text: str | None
    annotation_note_text: str | None
    annotation_source_created_datestamp_ep_k: int | None
    annotation_source_modified_datestamp_ep_k: int | None
    annotation_source_deleted_datestamp_ep_k: int | None
    annotation_source: str | None
    annotation_device_id: int | None
    annotation_extra_json: str | None
    annotation_created_timestamp_ep_k: int | None
    annotation_modified_timestamp_ep_k: int | None
    annotation_scratch: str | None


@runtime_checkable
class HumanAgentRowAPI(MetadataTableRowAPI, Protocol):
    TABLE_NAME: ClassVar[str]
    ID_COLUMN: ClassVar[str]

    human_agent_id: int | None
    human_agent_agent_id: int | None
    human_agent_given_name: str | None
    human_agent_middle_name: str | None
    human_agent_family_name: str | None
    human_agent_prefix: str | None
    human_agent_suffix: str | None
    human_agent_preferred_name: str | None
    human_agent_birth_date: str | None
    human_agent_death_date: str | None
    human_agent_nationality: str | None
    human_agent_biography: str | None
    human_agent_created_timestamp_ep_k: int | None
    human_agent_modified_timestamp_ep_k: int | None
    human_agent_scratch: str | None


@runtime_checkable
class OrgAgentRowAPI(MetadataTableRowAPI, Protocol):
    TABLE_NAME: ClassVar[str]
    ID_COLUMN: ClassVar[str]

    org_agent_id: int | None
    org_agent_agent_id: int | None
    org_agent_legal_name: str | None
    org_agent_trading_name: str | None
    org_agent_registration_id: str | None
    org_agent_jurisdiction: str | None
    org_agent_founded_date: str | None
    org_agent_dissolved_date: str | None
    org_agent_website: str | None
    org_agent_contact_email: str | None
    org_agent_description: str | None
    org_agent_created_timestamp_ep_k: int | None
    org_agent_modified_timestamp_ep_k: int | None
    org_agent_scratch: str | None


@runtime_checkable
class OrgAgentRelationRowAPI(MetadataTableRowAPI, Protocol):
    TABLE_NAME: ClassVar[str]
    ID_COLUMN: ClassVar[str]

    org_agent_relation_id: int | None
    org_agent_relation_child_agent_id: int | None
    org_agent_relation_parent_agent_id: int | None
    org_agent_relation_type: str | None
    org_agent_relation_start_date: str | None
    org_agent_relation_end_date: str | None
    org_agent_relation_note: str | None
    org_agent_relation_created_timestamp_ep_k: int | None
    org_agent_relation_modified_timestamp_ep_k: int | None
    org_agent_relation_source_created_datestamp_ep_k: int | None
    org_agent_relation_source_modified_datestamp_ep_k: int | None
    org_agent_relation_scratch: str | None


@runtime_checkable
class EntityIdentifierRowAPI(MetadataTableRowAPI, Protocol):
    TABLE_NAME: ClassVar[str]
    ID_COLUMN: ClassVar[str]

    entity_identifier_id: int | None
    entity_identifier_entity_type: IdentifierEntityType | str | None
    entity_identifier_entity_id: int | None
    entity_identifier_scheme: IdentifierScheme | str | None
    entity_identifier_value: str | None
    entity_identifier_is_primary: int | None
    entity_identifier_provenance: str | None
    entity_identifier_created_timestamp_ep_k: int | None
    entity_identifier_modified_timestamp_ep_k: int | None
    entity_identifier_source_created_datestamp_ep_k: int | None
    entity_identifier_source_modified_datestamp_ep_k: int | None
    entity_identifier_scratch: str | None


@runtime_checkable
class ObservedItemIdentifierRowAPI(MetadataTableRowAPI, Protocol):
    TABLE_NAME: ClassVar[str]
    ID_COLUMN: ClassVar[str]

    item_identifier_id: int | None
    item_identifier_item_id: int | None
    item_identifier_scheme: IdentifierScheme | str | None
    item_identifier_value: str | None
    item_identifier_source: str | None
    item_identifier_created_timestamp_ep_k: int | None
    item_identifier_modified_timestamp_ep_k: int | None
    item_identifier_source_created_datestamp_ep_k: int | None
    item_identifier_source_modified_datestamp_ep_k: int | None
    item_identifier_scratch: str | None


__all__ = [
    "AnnotationRowAPI",
    "CommentRowAPI",
    "EntityIdentifierRowAPI",
    "GenreRowAPI",
    "HumanAgentRowAPI",
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
    "SeriesRowAPI",
    "SubjectRowAPI",
    "SynopsisRowAPI",
]
