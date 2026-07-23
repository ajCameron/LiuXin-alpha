"""Identity specifications for exact-default catalog entities."""

from __future__ import annotations

from .exact_matcher import ExactEntitySpec


TAG_SPEC = ExactEntitySpec(
    entity_name="Tag",
    table_name="tags",
    id_column="tag_id",
    primary_field="tag",
    identity_fields=("tag", "tag_phash"),
    scalar_fields=("tag",),
    casefold_fields=frozenset({"tag"}),
    input_aliases={
        "id": "tag_id",
        "name": "tag",
        "text": "tag",
        "value": "tag",
        "phash": "tag_phash",
        "description": "tag_description",
    },
    policy_field="tag",
)

LABEL_SPEC = ExactEntitySpec(
    entity_name="Label",
    table_name="labels",
    id_column="label_id",
    primary_field="label_text",
    identity_fields=("label_text", "label_text_norm"),
    scalar_fields=("label_text", "label_text_norm"),
    casefold_fields=frozenset({"label_text", "label_text_norm"}),
    input_aliases={
        "id": "label_id",
        "name": "label_text",
        "text": "label_text",
        "value": "label_text",
        "normalized": "label_text_norm",
        "normalised": "label_text_norm",
        "description": "label_description",
    },
    policy_field="label_text",
    normalized_storage_fields=(("label_text_norm", "label_text"),),
)

GENRE_SPEC = ExactEntitySpec(
    entity_name="Genre",
    table_name="genres",
    id_column="genre_id",
    primary_field="genre",
    identity_fields=("genre", "genre_sort", "genre_phash", "genre_full"),
    scalar_fields=("genre", "genre_sort", "genre_full"),
    casefold_fields=frozenset({"genre", "genre_sort", "genre_full"}),
    input_aliases={
        "id": "genre_id",
        "name": "genre",
        "value": "genre",
        "sort": "genre_sort",
        "phash": "genre_phash",
        "parent_id": "genre_parent_id",
        "position": "genre_position",
        "tree_id": "genre_tree_id",
        "full": "genre_full",
    },
    scope_fields=("genre_parent_id",),
    policy_field="genre",
)

SUBJECT_SPEC = ExactEntitySpec(
    entity_name="Subject",
    table_name="subjects",
    id_column="subject_id",
    primary_field="subject",
    identity_fields=("subject", "subject_sort", "subject_phash", "subject_full"),
    scalar_fields=("subject", "subject_sort", "subject_full"),
    casefold_fields=frozenset({"subject", "subject_sort", "subject_full"}),
    input_aliases={
        "id": "subject_id",
        "name": "subject",
        "value": "subject",
        "sort": "subject_sort",
        "phash": "subject_phash",
        "parent_id": "subject_parent_id",
        "parent_position": "subject_parent_position",
        "tree_id": "subject_tree_id",
        "full": "subject_full",
    },
    scope_fields=("subject_parent_id",),
    policy_field="subject",
)

SERIES_SPEC = ExactEntitySpec(
    entity_name="Series",
    table_name="series",
    id_column="series_id",
    primary_field="series",
    identity_fields=("series", "series_name_norm", "series_phash", "series_full"),
    scalar_fields=("series", "series_name_norm", "series_full"),
    casefold_fields=frozenset({"series", "series_name_norm", "series_full"}),
    input_aliases={
        "id": "series_id",
        "name": "series",
        "value": "series",
        "normalized": "series_name_norm",
        "normalised": "series_name_norm",
        "sort": "series_sort",
        "phash": "series_phash",
        "over_author": "series_over_author",
        "parent_id": "series_parent_id",
        "parent_position": "series_parent_position",
        "tree_id": "series_tree_id",
        "full": "series_full",
    },
    scope_fields=("series_parent_id",),
    policy_field="series",
    normalized_storage_fields=(("series_name_norm", "series"),),
)

LANGUAGE_SPEC = ExactEntitySpec(
    entity_name="Language",
    table_name="languages",
    id_column="language_id",
    primary_field="language_code",
    identity_fields=(
        "language",
        "language_code",
        "language_iso639_1",
        "language_iso639_2_b",
        "language_iso639_2_t",
        "language_bcp47_primary",
    ),
    scalar_fields=(
        "language",
        "language_code",
        "language_iso639_1",
        "language_iso639_2_b",
        "language_iso639_2_t",
        "language_bcp47_primary",
    ),
    casefold_fields=frozenset(
        {
            "language",
            "language_code",
            "language_iso639_1",
            "language_iso639_2_b",
            "language_iso639_2_t",
            "language_bcp47_primary",
        }
    ),
    input_aliases={
        "id": "language_id",
        "name": "language",
        "value": "language_code",
        "code": "language_code",
        "iso639_1": "language_iso639_1",
        "iso639_2_b": "language_iso639_2_b",
        "iso639_2_t": "language_iso639_2_t",
        "bcp47": "language_bcp47_primary",
        "bcp47_variants": "language_bcp47_variants",
    },
    policy_field="language",
    mutable=False,
)

RATING_SPEC = ExactEntitySpec(
    entity_name="Rating",
    table_name="ratings",
    id_column="rating_id",
    primary_field="rating",
    identity_fields=("rating", "rating_out_of", "rating_source"),
    scalar_fields=("rating",),
    casefold_fields=frozenset({"rating_source"}),
    input_aliases={
        "id": "rating_id",
        "value": "rating",
        "out_of": "rating_out_of",
        "calibre_value": "rating_for_calibre_tag_viewer",
        "source": "rating_source",
    },
)

COMMENT_SPEC = ExactEntitySpec(
    entity_name="Comment",
    table_name="comments",
    id_column="comment_id",
    primary_field="comment",
    identity_fields=("comment",),
    scalar_fields=("comment",),
    input_aliases={
        "id": "comment_id",
        "text": "comment",
        "value": "comment",
    },
    reusable=False,
)

SYNOPSIS_SPEC = ExactEntitySpec(
    entity_name="Synopsis",
    table_name="synopses",
    id_column="synopsis_id",
    primary_field="synopsis",
    identity_fields=("synopsis",),
    scalar_fields=("synopsis",),
    input_aliases={
        "id": "synopsis_id",
        "text": "synopsis",
        "value": "synopsis",
    },
)

NOTE_SPEC = ExactEntitySpec(
    entity_name="Note",
    table_name="notes",
    id_column="note_id",
    primary_field="note",
    identity_fields=("note",),
    scalar_fields=("note",),
    input_aliases={
        "id": "note_id",
        "text": "note",
        "value": "note",
    },
)

ANNOTATION_SPEC = ExactEntitySpec(
    entity_name="Annotation",
    table_name="annotations",
    id_column="annotation_id",
    primary_field="annotation_anchor_start",
    identity_fields=(
        "annotation_user_id",
        "annotation_kind",
        "annotation_anchor_type",
        "annotation_anchor_start",
        "annotation_anchor_end",
        "annotation_source",
    ),
    scalar_fields=("annotation_anchor_start",),
    casefold_fields=frozenset(
        {"annotation_kind", "annotation_anchor_type", "annotation_source"}
    ),
    input_aliases={
        "id": "annotation_id",
        "user_id": "annotation_user_id",
        "item_id": "annotation_item_id",
        "kind": "annotation_kind",
        "anchor_type": "annotation_anchor_type",
        "anchor_start": "annotation_anchor_start",
        "anchor_end": "annotation_anchor_end",
        "selected_text": "annotation_selected_text",
        "note_text": "annotation_note_text",
        "source": "annotation_source",
        "device_id": "annotation_device_id",
        "extra_json": "annotation_extra_json",
    },
    scope_fields=("annotation_item_id",),
    required_scope_fields=("annotation_item_id",),
    required_identity_fields=(
        "annotation_kind",
        "annotation_anchor_type",
        "annotation_anchor_start",
    ),
    reusable=False,
)

EXACT_ENTITY_SPECS = (
    TAG_SPEC,
    LABEL_SPEC,
    GENRE_SPEC,
    SUBJECT_SPEC,
    SERIES_SPEC,
    LANGUAGE_SPEC,
    RATING_SPEC,
    COMMENT_SPEC,
    SYNOPSIS_SPEC,
    NOTE_SPEC,
    ANNOTATION_SPEC,
)

EXACT_ENTITY_SPEC_BY_TABLE = {spec.table_name: spec for spec in EXACT_ENTITY_SPECS}


__all__ = [
    "ANNOTATION_SPEC",
    "COMMENT_SPEC",
    "EXACT_ENTITY_SPECS",
    "EXACT_ENTITY_SPEC_BY_TABLE",
    "GENRE_SPEC",
    "LABEL_SPEC",
    "LANGUAGE_SPEC",
    "NOTE_SPEC",
    "RATING_SPEC",
    "SERIES_SPEC",
    "SUBJECT_SPEC",
    "SYNOPSIS_SPEC",
    "TAG_SPEC",
]
