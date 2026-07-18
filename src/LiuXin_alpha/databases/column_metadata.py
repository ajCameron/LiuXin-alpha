"""Canonical defaults for database-side column comparison metadata."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from enum import Enum
import re


COLUMN_METADATA_TABLE = "column_metadata"
DEFAULT_COLUMN_CASE_SENSITIVE = True


class ColumnSemanticRole(str, Enum):
    MACHINE_VALUE = "machine_value"
    IDENTIFIER = "identifier"
    RELATIONSHIP_KEY = "relationship_key"
    CODE = "code"
    BOOLEAN = "boolean"
    NUMBER = "number"
    ORDERING = "ordering"
    DATE_TIME = "date_time"
    LOCATOR = "locator"
    STRUCTURED_DATA = "structured_data"
    HASH = "hash"
    NORMALIZED_KEY = "normalized_key"
    PROVENANCE = "provenance"
    SCRATCH = "scratch"
    DISPLAY_NAME = "display_name"
    TITLE = "title"
    LABEL = "label"
    SORT_KEY = "sort_key"
    TAXONOMY_TERM = "taxonomy_term"
    VERBATIM_TEXT = "verbatim_text"
    RESOURCE_NAME = "resource_name"


class ColumnNormalizationProfile(str, Enum):
    NONE = "none"
    UNICODE_NFC = "unicode_nfc"
    UNICODE_NFC_TRIM_CASEFOLD = "unicode_nfc_trim_casefold"
    TAG_SEARCH_TERM = "tag_search_term"
    TITLE_SEARCH_TERM = "title_search_term"


class ColumnEmptyValuePolicy(str, Enum):
    NULL_IS_MISSING = "null_is_missing"
    NULL_OR_BLANK_IS_MISSING = "null_or_blank_is_missing"
    PRESERVE = "preserve"


class ColumnMergePolicy(str, Enum):
    REPLACE = "replace"
    SET_UNION = "set_union"
    APPEND = "append"
    PRESERVE_EXISTING = "preserve_existing"


class ColumnValidationProfile(str, Enum):
    NONE = "none"
    IDENTIFIER = "identifier"
    CODE = "code"
    BOOLEAN = "boolean"
    NUMBER = "number"
    DATE_TIME = "date_time"
    LOCATOR = "locator"
    JSON = "json"
    HASH = "hash"
    NORMALIZED_KEY = "normalized_key"
    DISPLAY_TEXT = "display_text"
    TAXONOMY_TERM = "taxonomy_term"
    VERBATIM_TEXT = "verbatim_text"
    RESOURCE_NAME = "resource_name"


@dataclass(frozen=True, slots=True)
class ColumnMetadata:
    """Database-owned semantic and writer policy for one physical column."""

    table: str
    column: str
    case_sensitive: bool
    semantic_role: ColumnSemanticRole
    normalization_profile: ColumnNormalizationProfile
    comparison_column: str | None
    empty_value_policy: ColumnEmptyValuePolicy
    merge_policy: ColumnMergePolicy
    validation_profile: ColumnValidationProfile

# Case sensitivity describes equality and deduplication, not storage: writers
# always retain the original display text.
#
# Identity-like display values use case-insensitive comparison because spelling
# case is presentation, not identity. Verbatim prose and resource names use
# case-sensitive comparison because a case-only edit can carry meaning, and
# case-distinct resource names can coexist on case-sensitive filesystems.
CASE_INSENSITIVE_DISPLAY_COLUMNS = frozenset(
    {
        ("custom_columns", "custom_column_label"),
        ("custom_columns", "custom_column_name"),
        ("database_metadata", "database_metadata_db_name"),
        ("feeds", "feed_title"),
        ("works", "work_title"),
        ("works", "work_canonical_title"),
        ("works", "work_sort_title"),
        ("works", "work_creator_sort"),
        ("expressions", "expression_label"),
        ("expressions", "expression_title_override"),
        ("expressions", "expression_subtitle"),
        ("manifestations", "manifestation_subtitle"),
        ("manifestations", "manifestation_format_detail"),
        ("manifestations", "manifestation_edition_statement"),
        ("manifestations", "manifestation_region_code"),
        ("items", "item_location"),
        ("agents", "agent_canonical_name"),
        ("agents", "agent_sort_name"),
        ("agents", "agent_aliases"),
        ("human_agents", "human_agent_given_name"),
        ("human_agents", "human_agent_middle_name"),
        ("human_agents", "human_agent_family_name"),
        ("human_agents", "human_agent_prefix"),
        ("human_agents", "human_agent_suffix"),
        ("human_agents", "human_agent_preferred_name"),
        ("human_agents", "human_agent_nationality"),
        ("org_agents", "org_agent_legal_name"),
        ("org_agents", "org_agent_trading_name"),
        ("org_agents", "org_agent_jurisdiction"),
        ("languages", "language"),
        ("labels", "label_text"),
        ("genres", "genre"),
        ("genres", "genre_sort"),
        ("genres", "genre_full"),
        ("subjects", "subject"),
        ("subjects", "subject_sort"),
        ("subjects", "subject_full"),
        ("series", "series"),
        ("series", "series_sort"),
        ("series", "series_full"),
        ("tags", "tag"),
        ("stores", "store_name"),
        ("replication_policies", "replication_policy_name"),
        ("backup_policies", "backup_policy_name"),
        ("backup_workflows", "backup_workflow_name"),
        ("digital_assets", "digital_asset_tag"),
        ("files", "file_tag"),
        ("images", "image_tag"),
        ("digital_asset_workflow", "digital_asset_workflow_assigned_to"),
        ("digital_asset_workflow_events", "digital_asset_workflow_event_actor"),
        ("item_workflow", "item_workflow_assigned_to"),
        ("item_workflow_events", "item_workflow_event_actor"),
        ("workflow_states", "workflow_state_label"),
        ("workflow_steps", "workflow_step_label"),
        ("workflow_steps", "workflow_step_group"),
    }
)

CASE_SENSITIVE_DISPLAY_COLUMNS = frozenset(
    {
        ("new_books", "new_book_name"),
        ("compressed_files", "compressed_file_name"),
        ("metadata_dirtied_books", "metadata_drtied_reason"),
        ("org_agent_relations", "org_agent_relation_note"),
        ("works", "work_discovery_note"),
        ("expressions", "expression_origin_note"),
        ("manifestations", "manifestation_note"),
        ("items", "item_source_name"),
        ("agents", "agent_note"),
        ("human_agents", "human_agent_biography"),
        ("org_agents", "org_agent_description"),
        ("annotations", "annotation_selected_text"),
        ("annotations", "annotation_note_text"),
        ("comments", "comment"),
        ("labels", "label_description"),
        ("notes", "note"),
        ("synopses", "synopsis"),
        ("tags", "tag_description"),
        ("stores", "store_location_note"),
        ("backup_workflows", "backup_workflow_last_error"),
        ("backup_workflow_state", "backup_workflow_state_last_error"),
        ("folders", "folder_name"),
        ("digital_assets", "digital_asset_name"),
        ("digital_assets", "digital_asset_base_name"),
        ("digital_assets", "digital_asset_auto_name"),
        ("digital_assets", "digital_asset_original_name"),
        ("composite_digital_assets", "composite_digital_asset_name"),
        ("asset_replicas", "asset_replica_name"),
        ("asset_replicas", "asset_replica_base_name"),
        ("asset_replicas", "asset_replica_failure_reason"),
        ("files", "file_name"),
        ("files", "file_base_name"),
        ("files", "file_auto_name"),
        ("files", "file_original_name"),
        ("images", "image_name"),
        ("images", "image_base_name"),
        ("images", "image_auto_name"),
        ("images", "image_original_name"),
        ("file_derivations", "file_derivation_note"),
        ("digital_asset_derivations", "digital_asset_derivation_note"),
        ("digital_asset_workflow", "digital_asset_workflow_reason"),
        ("digital_asset_workflow_events", "digital_asset_workflow_event_note"),
        ("item_workflow", "item_workflow_reason"),
        ("item_workflow_events", "item_workflow_event_note"),
        ("transform_run_inputs", "transform_run_input_note"),
        ("transform_run_outputs", "transform_run_output_note"),
        ("workflow_states", "workflow_state_description"),
    }
)

if CASE_INSENSITIVE_DISPLAY_COLUMNS & CASE_SENSITIVE_DISPLAY_COLUMNS:
    raise RuntimeError("display columns cannot have conflicting case-sensitivity policies")

DISPLAY_COLUMNS = CASE_INSENSITIVE_DISPLAY_COLUMNS | CASE_SENSITIVE_DISPLAY_COLUMNS

TITLE_DISPLAY_COLUMNS = frozenset(
    {
        ("feeds", "feed_title"),
        ("works", "work_title"),
        ("works", "work_canonical_title"),
        ("expressions", "expression_title_override"),
        ("expressions", "expression_subtitle"),
        ("manifestations", "manifestation_subtitle"),
    }
)

SORT_KEY_DISPLAY_COLUMNS = frozenset(
    {
        ("works", "work_sort_title"),
        ("works", "work_creator_sort"),
        ("agents", "agent_sort_name"),
        ("genres", "genre_sort"),
        ("subjects", "subject_sort"),
        ("series", "series_sort"),
    }
)

TAXONOMY_DISPLAY_COLUMNS = frozenset(
    {
        ("labels", "label_text"),
        ("genres", "genre"),
        ("genres", "genre_full"),
        ("subjects", "subject"),
        ("subjects", "subject_full"),
        ("series", "series"),
        ("series", "series_full"),
        ("tags", "tag"),
        ("digital_assets", "digital_asset_tag"),
        ("files", "file_tag"),
        ("images", "image_tag"),
    }
)

LABEL_DISPLAY_COLUMNS = frozenset(
    {
        ("custom_columns", "custom_column_label"),
        ("expressions", "expression_label"),
        ("manifestations", "manifestation_format_detail"),
        ("manifestations", "manifestation_edition_statement"),
        ("manifestations", "manifestation_region_code"),
        ("items", "item_location"),
        ("workflow_states", "workflow_state_label"),
        ("workflow_steps", "workflow_step_label"),
        ("workflow_steps", "workflow_step_group"),
    }
)

RESOURCE_NAME_DISPLAY_COLUMNS = frozenset(
    {
        ("new_books", "new_book_name"),
        ("compressed_files", "compressed_file_name"),
        ("items", "item_source_name"),
        ("folders", "folder_name"),
        ("digital_assets", "digital_asset_name"),
        ("digital_assets", "digital_asset_base_name"),
        ("digital_assets", "digital_asset_auto_name"),
        ("digital_assets", "digital_asset_original_name"),
        ("composite_digital_assets", "composite_digital_asset_name"),
        ("asset_replicas", "asset_replica_name"),
        ("asset_replicas", "asset_replica_base_name"),
        ("files", "file_name"),
        ("files", "file_base_name"),
        ("files", "file_auto_name"),
        ("files", "file_original_name"),
        ("images", "image_name"),
        ("images", "image_base_name"),
        ("images", "image_auto_name"),
        ("images", "image_original_name"),
    }
)

VERBATIM_DISPLAY_COLUMNS = CASE_SENSITIVE_DISPLAY_COLUMNS - RESOURCE_NAME_DISPLAY_COLUMNS
DISPLAY_NAME_COLUMNS = (
    CASE_INSENSITIVE_DISPLAY_COLUMNS
    - TITLE_DISPLAY_COLUMNS
    - SORT_KEY_DISPLAY_COLUMNS
    - TAXONOMY_DISPLAY_COLUMNS
    - LABEL_DISPLAY_COLUMNS
)

_ROLE_GROUPS = (
    DISPLAY_NAME_COLUMNS,
    TITLE_DISPLAY_COLUMNS,
    LABEL_DISPLAY_COLUMNS,
    SORT_KEY_DISPLAY_COLUMNS,
    TAXONOMY_DISPLAY_COLUMNS,
    VERBATIM_DISPLAY_COLUMNS,
    RESOURCE_NAME_DISPLAY_COLUMNS,
)
if set().union(*_ROLE_GROUPS) != DISPLAY_COLUMNS:
    raise RuntimeError("every display column must have exactly one semantic role")
if sum(len(group) for group in _ROLE_GROUPS) != len(DISPLAY_COLUMNS):
    raise RuntimeError("display-column semantic-role groups cannot overlap")

COMPARISON_COLUMNS: dict[tuple[str, str], tuple[str, ColumnNormalizationProfile]] = {
    ("tags", "tag"): ("tag_phash", ColumnNormalizationProfile.TAG_SEARCH_TERM),
    ("labels", "label_text"): (
        "label_text_norm",
        ColumnNormalizationProfile.TAG_SEARCH_TERM,
    ),
    ("genres", "genre"): (
        "genre_phash",
        ColumnNormalizationProfile.TITLE_SEARCH_TERM,
    ),
    ("subjects", "subject"): (
        "subject_sort",
        ColumnNormalizationProfile.TITLE_SEARCH_TERM,
    ),
    ("series", "series"): (
        "series_name_norm",
        ColumnNormalizationProfile.TITLE_SEARCH_TERM,
    ),
}

SET_UNION_DISPLAY_COLUMNS = TAXONOMY_DISPLAY_COLUMNS | {
    ("agents", "agent_aliases"),
}
APPEND_DISPLAY_COLUMNS = frozenset(
    {
        ("comments", "comment"),
        ("notes", "note"),
        ("synopses", "synopsis"),
    }
)


def _semantic_role(key: tuple[str, str]) -> ColumnSemanticRole:
    if key in TITLE_DISPLAY_COLUMNS:
        return ColumnSemanticRole.TITLE
    if key in LABEL_DISPLAY_COLUMNS:
        return ColumnSemanticRole.LABEL
    if key in SORT_KEY_DISPLAY_COLUMNS:
        return ColumnSemanticRole.SORT_KEY
    if key in TAXONOMY_DISPLAY_COLUMNS:
        return ColumnSemanticRole.TAXONOMY_TERM
    if key in VERBATIM_DISPLAY_COLUMNS:
        return ColumnSemanticRole.VERBATIM_TEXT
    if key in RESOURCE_NAME_DISPLAY_COLUMNS:
        return ColumnSemanticRole.RESOURCE_NAME
    return ColumnSemanticRole.DISPLAY_NAME


def _normalization_profile(key: tuple[str, str]) -> ColumnNormalizationProfile:
    comparison = COMPARISON_COLUMNS.get(key)
    if comparison is not None:
        return comparison[1]
    if key in CASE_INSENSITIVE_DISPLAY_COLUMNS:
        return ColumnNormalizationProfile.UNICODE_NFC_TRIM_CASEFOLD
    if key in VERBATIM_DISPLAY_COLUMNS:
        return ColumnNormalizationProfile.UNICODE_NFC
    return ColumnNormalizationProfile.NONE


def _merge_policy(key: tuple[str, str]) -> ColumnMergePolicy:
    if key in SET_UNION_DISPLAY_COLUMNS:
        return ColumnMergePolicy.SET_UNION
    if key in APPEND_DISPLAY_COLUMNS:
        return ColumnMergePolicy.APPEND
    return ColumnMergePolicy.REPLACE


def _validation_profile(
    semantic_role: ColumnSemanticRole,
) -> ColumnValidationProfile:
    if semantic_role is ColumnSemanticRole.TAXONOMY_TERM:
        return ColumnValidationProfile.TAXONOMY_TERM
    if semantic_role is ColumnSemanticRole.VERBATIM_TEXT:
        return ColumnValidationProfile.VERBATIM_TEXT
    if semantic_role is ColumnSemanticRole.RESOURCE_NAME:
        return ColumnValidationProfile.RESOURCE_NAME
    return ColumnValidationProfile.DISPLAY_TEXT


def _display_column_metadata(key: tuple[str, str]) -> ColumnMetadata:
    semantic_role = _semantic_role(key)
    comparison = COMPARISON_COLUMNS.get(key)
    return ColumnMetadata(
        table=key[0],
        column=key[1],
        case_sensitive=key in CASE_SENSITIVE_DISPLAY_COLUMNS,
        semantic_role=semantic_role,
        normalization_profile=_normalization_profile(key),
        comparison_column=comparison[0] if comparison is not None else None,
        empty_value_policy=ColumnEmptyValuePolicy.NULL_OR_BLANK_IS_MISSING,
        merge_policy=_merge_policy(key),
        validation_profile=_validation_profile(semantic_role),
    )


# These explicit display overrides are combined with schema-derived machine
# defaults when new databases are seeded. They also remain the compatibility
# fallback for older databases whose catalog lacks a particular display row.
COLUMN_METADATA_DEFAULTS: dict[tuple[str, str], ColumnMetadata] = {
    key: _display_column_metadata(key) for key in DISPLAY_COLUMNS
}
COLUMN_CASE_SENSITIVITY_DEFAULTS: dict[tuple[str, str], bool] = {
    key: metadata.case_sensitive for key, metadata in COLUMN_METADATA_DEFAULTS.items()
}


_NUMERIC_DECLARATIONS = {
    "BIGINT",
    "DOUBLE",
    "FLOAT",
    "INT",
    "INTEGER",
    "NUMERIC",
    "REAL",
    "SMALLINT",
}
_BOOLEAN_NAME_TOKENS = {
    "active",
    "attempted",
    "cached",
    "corrupt",
    "critical",
    "editable",
    "enabled",
    "healthy",
    "immutable",
    "locked",
    "multiple",
    "nullable",
    "ordered",
    "processed",
    "protected",
    "symmetric",
    "typed",
    "verified",
}
_CODE_NAME_TOKENS = {
    "category",
    "class",
    "code",
    "condition",
    "datatype",
    "domain",
    "extension",
    "flags",
    "format",
    "group",
    "kind",
    "method",
    "medium",
    "mode",
    "policy",
    "profile",
    "protocol",
    "region",
    "role",
    "scheme",
    "scope",
    "status",
    "tool",
    "type",
    "version",
    "visibility",
}
_LOCATOR_SUFFIXES = (
    "_archive_path",
    "_cfi",
    "_href",
    "_original_path",
    "_output_url",
    "_path",
    "_relpath",
    "_root",
    "_root_uri",
    "_storage_key",
    "_uri",
    "_url",
    "_website",
    "_email",
    "_link",
)
_STRUCTURED_SUFFIXES = (
    "_capabilities",
    "_credentials",
    "_extra",
    "_json",
    "_options",
    "_settings",
)


def _declared_type_family(declared_type: str | None) -> str:
    text = str(declared_type or "").strip().upper()
    if not text:
        return ""
    first = re.split(r"[\s(]", text, maxsplit=1)[0]
    if first in _NUMERIC_DECLARATIONS:
        return "number"
    if first in {"BOOL", "BOOLEAN"}:
        return "boolean"
    if first in {"DATE", "DATETIME", "TIME", "TIMESTAMP"}:
        return "date_time"
    if first in {"BLOB", "BYTEA"}:
        return "blob"
    if first in {"CHAR", "CLOB", "TEXT", "VARCHAR"}:
        return "text"
    return first.lower()


def _machine_role(
    table: str,
    column: str,
    declared_type: str | None,
    *,
    is_primary_key: bool,
    is_foreign_key: bool,
) -> ColumnSemanticRole:
    lowered = column.casefold()
    tokens = set(lowered.split("_"))
    declared_family = _declared_type_family(declared_type)

    if lowered.endswith("_scratch") or lowered == "scratch":
        return ColumnSemanticRole.SCRATCH
    if is_primary_key or _looks_like_conventional_primary_key(table, column):
        return ColumnSemanticRole.IDENTIFIER
    if (
        lowered.endswith(("_identifier", "_unique_id", "_uuid"))
        or lowered in {"identifier", "uuid"}
        or (
            "_identifier_" in lowered
            and lowered.endswith("_value")
        )
        or (
            str(table).casefold() == "last_read_positions"
            and lowered.endswith(("_book", "_device", "_user"))
        )
    ):
        return ColumnSemanticRole.IDENTIFIER
    if is_foreign_key or lowered.endswith("_id") or lowered == "id":
        return ColumnSemanticRole.RELATIONSHIP_KEY
    if (
        lowered.endswith(("_timestamp", "_timestamp_ep_k", "_datestamp", "_datestamp_ep_k"))
        or lowered.endswith(("_date", "_year", "_epoch"))
        or declared_family == "date_time"
    ):
        return ColumnSemanticRole.DATE_TIME
    if (
        lowered.startswith(("is_", "has_"))
        or "_is_" in lowered
        or "_has_" in lowered
        or "_supports_" in lowered
        or "_use_" in lowered
        or lowered.endswith(
            (
                "_ok",
                "_case_sensitive",
                "_cleanup_staging_after_success",
                "_in_table",
                "_link_primary",
                "_mark_for_delete",
                "_periodic_verification",
                "_verify_after_build",
            )
        )
        or (
            declared_family == "number"
            and lowered.endswith("_normalized")
        )
        or tokens & _BOOLEAN_NAME_TOKENS
    ):
        return ColumnSemanticRole.BOOLEAN
    if re.search(
        r"_(?:index|ordinal|position|priority|rank|sequence)(?:_number)?$",
        lowered,
    ):
        return ColumnSemanticRole.ORDERING
    if (
        lowered == "hash"
        or lowered.endswith(("_hash", "_phash"))
        or re.search(
            r"_hash_(?:blake3|md5|sha1|sha256|sha512|\d+)$",
            lowered,
        )
    ):
        return ColumnSemanticRole.HASH
    if lowered.endswith(("_norm", "_normalized")):
        return ColumnSemanticRole.NORMALIZED_KEY
    if (
        lowered.endswith(_LOCATOR_SUFFIXES)
        or lowered.endswith(("_anchor_start", "_anchor_end"))
        or any(marker in tokens for marker in {"locator", "path", "uri", "url"})
    ):
        return ColumnSemanticRole.LOCATOR
    if lowered.endswith(_STRUCTURED_SUFFIXES):
        return ColumnSemanticRole.STRUCTURED_DATA
    if (
        tokens & _CODE_NAME_TOKENS
        or "_iso639_" in lowered
        or "_bcp47_" in lowered
    ):
        return ColumnSemanticRole.CODE
    if declared_family == "boolean":
        return ColumnSemanticRole.BOOLEAN
    if declared_family == "number":
        return ColumnSemanticRole.NUMBER
    if tokens & {"origin", "provenance", "source"}:
        return ColumnSemanticRole.PROVENANCE
    return ColumnSemanticRole.MACHINE_VALUE


def _looks_like_conventional_primary_key(table: str, column: str) -> bool:
    """Recognize LiuXin's ``singular_table_id`` primary-key convention."""

    lowered_table = str(table).casefold()
    lowered_column = str(column).casefold()
    if not lowered_column.endswith("_id"):
        return False

    possible_stems = {lowered_table}
    if lowered_table.endswith("ies"):
        possible_stems.add(lowered_table[:-3] + "y")
    if lowered_table.endswith(("ches", "shes", "sses", "xes", "zes")):
        possible_stems.add(lowered_table[:-2])
    if lowered_table.endswith("s"):
        possible_stems.add(lowered_table[:-1])
    return lowered_column[:-3] in possible_stems


def _machine_validation_profile(
    role: ColumnSemanticRole,
) -> ColumnValidationProfile:
    return {
        ColumnSemanticRole.IDENTIFIER: ColumnValidationProfile.IDENTIFIER,
        ColumnSemanticRole.RELATIONSHIP_KEY: ColumnValidationProfile.IDENTIFIER,
        ColumnSemanticRole.CODE: ColumnValidationProfile.CODE,
        ColumnSemanticRole.BOOLEAN: ColumnValidationProfile.BOOLEAN,
        ColumnSemanticRole.NUMBER: ColumnValidationProfile.NUMBER,
        ColumnSemanticRole.ORDERING: ColumnValidationProfile.NUMBER,
        ColumnSemanticRole.DATE_TIME: ColumnValidationProfile.DATE_TIME,
        ColumnSemanticRole.LOCATOR: ColumnValidationProfile.LOCATOR,
        ColumnSemanticRole.STRUCTURED_DATA: ColumnValidationProfile.JSON,
        ColumnSemanticRole.HASH: ColumnValidationProfile.HASH,
        ColumnSemanticRole.NORMALIZED_KEY: ColumnValidationProfile.NORMALIZED_KEY,
    }.get(role, ColumnValidationProfile.NONE)


def infer_column_metadata(
    table: str,
    column: str,
    declared_type: str | None = None,
    *,
    is_primary_key: bool = False,
    is_foreign_key: bool = False,
) -> ColumnMetadata:
    """Infer a complete default policy from schema facts and naming conventions."""

    key = (str(table), str(column))
    configured = COLUMN_METADATA_DEFAULTS.get(key)
    if configured is not None:
        return configured

    role = _machine_role(
        key[0],
        key[1],
        declared_type,
        is_primary_key=is_primary_key,
        is_foreign_key=is_foreign_key,
    )
    preserve_existing = (
        role is ColumnSemanticRole.IDENTIFIER
        or key[0].endswith("_events")
        or "created_timestamp" in key[1]
        or "source_created_datestamp" in key[1]
    )
    return ColumnMetadata(
        table=key[0],
        column=key[1],
        case_sensitive=True,
        semantic_role=role,
        normalization_profile=ColumnNormalizationProfile.NONE,
        comparison_column=None,
        empty_value_policy=(
            ColumnEmptyValuePolicy.PRESERVE
            if role is ColumnSemanticRole.SCRATCH
            else ColumnEmptyValuePolicy.NULL_IS_MISSING
        ),
        merge_policy=(
            ColumnMergePolicy.PRESERVE_EXISTING
            if preserve_existing
            else ColumnMergePolicy.REPLACE
        ),
        validation_profile=_machine_validation_profile(role),
    )


def default_column_metadata(table: str, column: str) -> ColumnMetadata:
    """Return the canonical metadata fallback for one physical column."""

    return infer_column_metadata(table, column)


def default_column_case_sensitive(table: str, column: str) -> bool:
    """Return the built-in fallback for a physical table/column pair."""

    return default_column_metadata(table, column).case_sensitive


def is_display_column(table: str, column: str) -> bool:
    """Return whether a column is in the explicit human-facing display registry."""

    return (str(table), str(column)) in DISPLAY_COLUMNS


def iter_column_case_sensitivity_defaults() -> Iterator[tuple[str, str, bool]]:
    """Yield deterministic rows suitable for seeding a database catalog."""

    for (table, column), case_sensitive in sorted(COLUMN_CASE_SENSITIVITY_DEFAULTS.items()):
        yield table, column, case_sensitive


def iter_column_metadata_defaults() -> Iterator[ColumnMetadata]:
    """Yield deterministic explicit display-policy overrides.

    Schema generators should call :func:`infer_column_metadata` for every
    physical column so machine-facing columns also receive stored records.
    """

    for key in sorted(COLUMN_METADATA_DEFAULTS):
        yield COLUMN_METADATA_DEFAULTS[key]


__all__ = [
    "CASE_INSENSITIVE_DISPLAY_COLUMNS",
    "CASE_SENSITIVE_DISPLAY_COLUMNS",
    "ColumnEmptyValuePolicy",
    "ColumnMergePolicy",
    "ColumnMetadata",
    "ColumnNormalizationProfile",
    "ColumnSemanticRole",
    "ColumnValidationProfile",
    "COLUMN_CASE_SENSITIVITY_DEFAULTS",
    "COLUMN_METADATA_DEFAULTS",
    "COLUMN_METADATA_TABLE",
    "COMPARISON_COLUMNS",
    "DEFAULT_COLUMN_CASE_SENSITIVE",
    "DISPLAY_COLUMNS",
    "default_column_metadata",
    "default_column_case_sensitive",
    "is_display_column",
    "infer_column_metadata",
    "iter_column_case_sensitivity_defaults",
    "iter_column_metadata_defaults",
]
