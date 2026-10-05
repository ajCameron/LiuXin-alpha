"""
Define column-policy defaults, schema-based inference and immutable presentation options.

Explicit display-column registries determine comparison, normalization, empty-value, merge and validation policies. Machine-column defaults are inferred from schema facts and naming rules. These records describe policy; this module does not normalize stored values or execute database writes. Import-time checks require disjoint case-sensitivity sets and complete, non-overlapping display-role groups.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
import json
import math
import re
from types import MappingProxyType
from typing import cast


COLUMN_METADATA_TABLE = "column_metadata"
DEFAULT_COLUMN_CASE_SENSITIVE = True


type ColumnOptionValue = (
    str
    | int
    | float
    | bool
    | None
    | tuple[ColumnOptionValue, ...]
    | Mapping[str, ColumnOptionValue]
)
type ColumnOptions = Mapping[str, ColumnOptionValue]


def _freeze_column_option_value(
    value: object,
    *,
    path: str,
) -> ColumnOptionValue:
    """
    Validate one option subtree and return an immutable copy of its containers.

    Accept exact built-in str/int/bool/None and finite floats. Mapping keys must be exact strings; mappings become MappingProxyType over new dictionaries. Sequences other than strings/bytes/bytearray/memoryview become tuples. Unsupported objects and non-finite numbers are rejected. Cycles have no special detection and can exhaust recursion.

    Example:
        >>> value = _freeze_column_option_value({"sizes": [1, 2]}, path="options")
        >>> value["sizes"]
        (1, 2)


    :param value: JSON-like scalar, mapping or non-text sequence to freeze.
    :param path: Diagnostic path extended with mapping keys and sequence indexes.
    :return: An accepted scalar, a read-only mapping copy, or a tuple of recursively frozen values.
    :raises TypeError: A key or value has an unsupported type.
    :raises ValueError: A float is NaN or infinite.
    """

    if value is None or type(value) in {str, int, bool}:
        return cast(str | int | bool | None, value)
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError(f"{path} must not contain NaN or infinity")
        return value
    if isinstance(value, Mapping):
        mapping = cast(Mapping[object, object], value)
        frozen: dict[str, ColumnOptionValue] = {}
        for key, item in mapping.items():
            if type(key) is not str:
                raise TypeError(f"{path} keys must be strings")
            frozen[key] = _freeze_column_option_value(
                item,
                path=f"{path}.{key}",
            )
        return MappingProxyType(frozen)
    if isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray, memoryview),
    ):
        return tuple(
            _freeze_column_option_value(item, path=f"{path}[{index}]")
            for index, item in enumerate(value)
        )
    raise TypeError(
        f"{path} contains unsupported value type {type(value).__name__!r}"
    )


def freeze_column_options(
    options: object | None,
    *,
    field_name: str = "options",
) -> ColumnOptions:
    """
    Validate an option mapping and deeply copy its containers into immutable forms.

    Nested dictionaries and sequences are recursively copied, so subsequent mutation of supplied containers does not change the result. Scalar subclasses, non-string keys, sets and non-finite floats are rejected by the recursive validator.

    Example:
        >>> raw = {"sizes": [1, 2]}
        >>> frozen = freeze_column_options(raw)
        >>> raw["sizes"].append(3)
        >>> frozen["sizes"]
        (1, 2)


    :param options: Option mapping or None, which means an empty mapping.
    :param field_name: Root name included in nested validation error messages.
    :return: Read-only string-keyed mapping containing only accepted immutable option values.
    :raises TypeError: The root is not a mapping or a nested key/value type is unsupported.
    :raises ValueError: A nested float is NaN or infinite.
    """

    if options is None:
        options = {}
    if not isinstance(options, Mapping):
        raise TypeError(f"{field_name} must be a mapping")
    frozen = _freeze_column_option_value(
        cast(Mapping[object, object], options),
        path=field_name,
    )
    assert isinstance(frozen, Mapping)
    return frozen


def _mutable_column_option_value(value: ColumnOptionValue) -> object:
    """
    Convert frozen option containers to ordinary JSON-serializable dictionaries and lists.

    Mappings recurse into dictionaries and tuples recurse into lists. This helper does not validate unsupported values; callers freeze the input first.

    Example:
        >>> _mutable_column_option_value(freeze_column_options({"sizes": [1, 2]}))
        {'sizes': [1, 2]}


    :param value: Previously validated frozen option value.
    :return: New dictionaries/lists for containers; scalar values are returned unchanged.
    """

    if isinstance(value, Mapping):
        return {
            key: _mutable_column_option_value(item)
            for key, item in value.items()
        }
    if isinstance(value, tuple):
        return [_mutable_column_option_value(item) for item in value]
    return value


def column_options_to_json(options: Mapping[str, object] | None) -> str:
    """
    Validate and serialize an option mapping as compact, deterministic Unicode JSON.

    Freeze the input first, convert frozen containers to JSON containers, then encode with allow_nan=False. This rejects unsupported objects rather than stringifying them.

    Example:
        >>> column_options_to_json({"width": 42, "label": "Title"})
        '{"label":"Title","width":42}'


    :param options: Option mapping or None, interpreted as an empty object.
    :return: JSON object text with sorted keys, no formatting whitespace and unescaped Unicode characters.
    :raises TypeError: The input or a nested value is not an accepted option type.
    :raises ValueError: An option float is not finite.
    """

    frozen = freeze_column_options(options)
    payload = _mutable_column_option_value(frozen)
    return json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def column_options_from_json(
    value: object,
    *,
    field_name: str = "options",
) -> ColumnOptions:
    """
    Decode an option object and freeze its nested containers.

    Decode byte-like inputs as UTF-8 and strings with json.loads. An already-decoded mapping is also accepted. The resulting root must be an object: JSON null, arrays and scalars are rejected even though a direct None input means empty options.

    Example:
        >>> column_options_from_json('{"sizes":[1,2]}')["sizes"]
        (1, 2)


    :param value: JSON text, UTF-8 bytes/bytearray/memoryview, an existing mapping, or None.
    :param field_name: Root name included in validation diagnostics.
    :return: A deeply immutable option mapping; None produces an empty mapping.
    :raises ValueError: JSON is malformed, its root is not an object, or an option float is non-finite.
    :raises TypeError: Decoded options contain an unsupported key/value type.
    :raises UnicodeDecodeError: A byte-like input is not valid UTF-8.
    """

    if value is None:
        return freeze_column_options({}, field_name=field_name)
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, (bytes, bytearray)):
        value = bytes(value).decode("utf-8")
    if isinstance(value, str):
        decoded = cast(object, json.loads(value))
    else:
        decoded = value
    if not isinstance(decoded, Mapping):
        raise ValueError(f"{field_name} JSON must contain an object")
    return freeze_column_options(cast(object, decoded), field_name=field_name)


class ColumnSemanticRole(str, Enum):
    """
    Classify the application meaning of a physical column.

    Members are string-valued policy labels. The enum itself performs no normalization, merge or validation operation on column data.

    Example:
        >>> ColumnSemanticRole("title") is ColumnSemanticRole.TITLE
        True
    """

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
    """
    Name the normalization policy used before column-value comparison.

    Members are string-valued policy labels. The enum itself performs no normalization, merge or validation operation on column data.

    Example:
        >>> ColumnNormalizationProfile("unicode_nfc") is ColumnNormalizationProfile.UNICODE_NFC
        True
    """

    NONE = "none"
    UNICODE_NFC = "unicode_nfc"
    UNICODE_NFC_TRIM_CASEFOLD = "unicode_nfc_trim_casefold"
    TAG_SEARCH_TERM = "tag_search_term"
    TITLE_SEARCH_TERM = "title_search_term"


class ColumnEmptyValuePolicy(str, Enum):
    """
    Describe how missing or blank values are treated by a column policy.

    Members are string-valued policy labels. The enum itself performs no normalization, merge or validation operation on column data.

    Example:
        >>> ColumnEmptyValuePolicy("null_or_blank_is_missing") is ColumnEmptyValuePolicy.NULL_OR_BLANK_IS_MISSING
        True
    """

    NULL_IS_MISSING = "null_is_missing"
    NULL_OR_BLANK_IS_MISSING = "null_or_blank_is_missing"
    PRESERVE = "preserve"


class ColumnMergePolicy(str, Enum):
    """
    Name the policy for combining an incoming and existing column value.

    Members are string-valued policy labels. The enum itself performs no normalization, merge or validation operation on column data.

    Example:
        >>> ColumnMergePolicy("preserve_existing") is ColumnMergePolicy.PRESERVE_EXISTING
        True
    """

    REPLACE = "replace"
    SET_UNION = "set_union"
    APPEND = "append"
    PRESERVE_EXISTING = "preserve_existing"


class ColumnValidationProfile(str, Enum):
    """
    Name the semantic validation policy assigned to a column.

    Members are string-valued policy labels. The enum itself performs no normalization, merge or validation operation on column data.

    Example:
        >>> ColumnValidationProfile("identifier") is ColumnValidationProfile.IDENTIFIER
        True
    """

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
    """
    Hold the semantic and writer policy for one physical database column.

    This frozen, slotted dataclass stores table/column names, case sensitivity, role, normalization/comparison, empty-value, merge and validation policies. Construction deep-freezes both option maps; it does not independently validate the other fields, compare values or enforce database constraints.

    Example:
        >>> metadata = default_column_metadata("tags", "tag")
        >>> metadata.semantic_role is ColumnSemanticRole.TAXONOMY_TERM
        True
    """

    table: str
    column: str
    case_sensitive: bool
    semantic_role: ColumnSemanticRole
    normalization_profile: ColumnNormalizationProfile
    comparison_column: str | None
    empty_value_policy: ColumnEmptyValuePolicy
    merge_policy: ColumnMergePolicy
    validation_profile: ColumnValidationProfile
    formatting_options: ColumnOptions = field(
        default_factory=lambda: MappingProxyType({})
    )
    display_options: ColumnOptions = field(
        default_factory=lambda: MappingProxyType({})
    )

    def __post_init__(self) -> None:
        """
        Replace both presentation option maps with validated immutable copies.

        Use object.__setattr__ during frozen-dataclass initialization. Invalid nested values fail construction; no validation of the other policy fields occurs here.

        Example:
            >>> from dataclasses import replace
            >>> metadata = replace(default_column_metadata("works", "work_title"), display_options={"sizes": [1, 2]})
            >>> metadata.display_options["sizes"]
            (1, 2)


        :return: None; formatting_options is frozen first, followed by display_options.
        :raises TypeError: An option map contains an unsupported key/value type.
        :raises ValueError: An option map contains a non-finite float.
        """

        object.__setattr__(
            self,
            "formatting_options",
            freeze_column_options(
                self.formatting_options,
                field_name="formatting_options",
            ),
        )
        object.__setattr__(
            self,
            "display_options",
            freeze_column_options(
                self.display_options,
                field_name="display_options",
            ),
        )

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
    ("backup_policies", "backup_policy_name"): (
        "backup_policy_name_norm",
        ColumnNormalizationProfile.UNICODE_NFC_TRIM_CASEFOLD,
    ),
    ("custom_columns", "custom_column_label"): (
        "custom_column_label_norm",
        ColumnNormalizationProfile.UNICODE_NFC_TRIM_CASEFOLD,
    ),
    ("custom_columns", "custom_column_name"): (
        "custom_column_name_norm",
        ColumnNormalizationProfile.UNICODE_NFC_TRIM_CASEFOLD,
    ),
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
        "subject_phash",
        ColumnNormalizationProfile.TITLE_SEARCH_TERM,
    ),
    ("series", "series"): (
        "series_name_norm",
        ColumnNormalizationProfile.TITLE_SEARCH_TERM,
    ),
    ("replication_policies", "replication_policy_name"): (
        "replication_policy_name_norm",
        ColumnNormalizationProfile.UNICODE_NFC_TRIM_CASEFOLD,
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
    """
    Select the display role for a configured column pair.

    Check title, label, sort key, taxonomy, verbatim and resource-name groups in that order. This helper does not validate that the pair belongs to DISPLAY_COLUMNS.

    Example:
        >>> _semantic_role(("tags", "tag")).value
        'taxonomy_term'


    :param key: Physical (table, column) pair, matched exactly against the configured registries.
    :return: The first matching display-role enum, defaulting to DISPLAY_NAME.
    """

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
    """
    Select a comparison override or the configured display normalization fallback.

    A COMPARISON_COLUMNS entry takes precedence over case-sensitivity and verbatim defaults. This only chooses a policy; it does not transform text.

    Example:
        >>> _normalization_profile(("tags", "tag")).value
        'tag_search_term'


    :param key: Physical (table, column) pair, matched exactly against the configured registries.
    :return: Configured comparison profile, case-insensitive NFC/trim/casefold, verbatim NFC, or NONE.
    """

    comparison = COMPARISON_COLUMNS.get(key)
    if comparison is not None:
        return comparison[1]
    if key in CASE_INSENSITIVE_DISPLAY_COLUMNS:
        return ColumnNormalizationProfile.UNICODE_NFC_TRIM_CASEFOLD
    if key in VERBATIM_DISPLAY_COLUMNS:
        return ColumnNormalizationProfile.UNICODE_NFC
    return ColumnNormalizationProfile.NONE


def _merge_policy(key: tuple[str, str]) -> ColumnMergePolicy:
    """
    Select set-union, append or replace behavior from display-column registries.

    Example:
        >>> _merge_policy(("comments", "comment")).value
        'append'


    :param key: Physical (table, column) pair, matched exactly against the configured registries.
    :return: SET_UNION for its registry, APPEND for its registry, otherwise REPLACE.
    """

    if key in SET_UNION_DISPLAY_COLUMNS:
        return ColumnMergePolicy.SET_UNION
    if key in APPEND_DISPLAY_COLUMNS:
        return ColumnMergePolicy.APPEND
    return ColumnMergePolicy.REPLACE


def _validation_profile(
    semantic_role: ColumnSemanticRole,
) -> ColumnValidationProfile:
    """
    Choose the validation label associated with a display semantic role.

    This helper supplies display defaults. Machine-role validation uses _machine_validation_profile instead; no value is validated here.

    Example:
        >>> _validation_profile(ColumnSemanticRole.TITLE).value
        'display_text'


    :param semantic_role: Role enum used for identity comparisons.
    :return: Taxonomy, verbatim or resource-name validation for those roles; DISPLAY_TEXT otherwise.
    """

    if semantic_role is ColumnSemanticRole.TAXONOMY_TERM:
        return ColumnValidationProfile.TAXONOMY_TERM
    if semantic_role is ColumnSemanticRole.VERBATIM_TEXT:
        return ColumnValidationProfile.VERBATIM_TEXT
    if semantic_role is ColumnSemanticRole.RESOURCE_NAME:
        return ColumnValidationProfile.RESOURCE_NAME
    return ColumnValidationProfile.DISPLAY_TEXT


def _display_column_metadata(key: tuple[str, str]) -> ColumnMetadata:
    """
    Build a complete display-policy record from the configured registries.

    Case sensitivity is membership in CASE_SENSITIVE_DISPLAY_COLUMNS; semantic, normalization and merge helpers determine the other policies. The optional comparison column comes from COMPARISON_COLUMNS. Intended for registered display pairs; unregistered input is not rejected.

    Example:
        >>> _display_column_metadata(("tags", "tag")).comparison_column
        'tag_phash'


    :param key: Physical (table, column) pair, matched exactly against the configured registries.
    :return: New ColumnMetadata with blank/null treated as missing and empty presentation options.
    """

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
    """
    Reduce the first SQL type token to a known family or a lowercase fallback.

    Stringify, trim and uppercase the declaration, then stop at whitespace or an opening parenthesis. Classification uses explicit token sets, not SQLite affinity substring rules; for example DECIMAL is not in the numeric token set.

    Example:
        >>> _declared_type_family("VARCHAR(80)")
        'text'
        >>> _declared_type_family("DECIMAL(8,2)")
        'decimal'


    :param declared_type: SQL type declaration, or None/empty input.
    :return: number, boolean, date_time, blob, text, an unrecognized lowercase token, or an empty string.
    """

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
    """
    Infer a machine column role using ordered schema and naming heuristics.

    Precedence is scratch, primary/conventional/identifier names, relationship keys, date/time, boolean names, ordering, hashes, normalized keys, locators, structured suffixes, code tokens, declared boolean/number types, then provenance. Thus a naming rule can override declared type or foreign-key evidence. Declared text/blob alone does not imply a display role.

    Example:
        >>> _machine_role("works", "work_id", "INTEGER", is_primary_key=False, is_foreign_key=False).value
        'identifier'


    :param table: Table name used for conventional primary-key and last-read-position recognition.
    :param column: Column name casefolded for token/suffix rules.
    :param declared_type: SQL declaration used for fallback type-family rules.
    :param is_primary_key: Whether schema inspection identifies this as a primary key.
    :param is_foreign_key: Whether schema inspection identifies this as a foreign key.
    :return: The first matching ColumnSemanticRole; MACHINE_VALUE when no rule matches.
    """

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
    """
    Recognize a table-name stem followed by the column suffix _id.

    Try the original table name, ies-to-y, removal of es after ch/sh/ss/x/z endings, and removal of a final s. This is a naming heuristic and does not inspect schema constraints or irregular plurals.

    Example:
        >>> _looks_like_conventional_primary_key("policies", "policy_id")
        True
        >>> _looks_like_conventional_primary_key("works", "agent_id")
        False


    :param table: Table name stringified and casefolded before simple suffix rules.
    :param column: Column name stringified and casefolded; must end in _id.
    :return: True if the column stem matches the full table name or a supported singularized form.
    """

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
    """
    Map an inferred machine role to its validation-policy enum.

    Both identifiers and relationship keys use identifier validation; ordering shares number validation, and structured data uses JSON. This returns a label without checking any stored value.

    Example:
        >>> _machine_validation_profile(ColumnSemanticRole.RELATIONSHIP_KEY).value
        'identifier'


    :param role: Machine semantic role used as a mapping key.
    :return: Matching validation profile, or NONE for an unmapped role.
    """

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
    """
    Return an explicit display override or infer a machine-column default policy.

    Exact display overrides take precedence over all supplied schema facts. Machine defaults are case-sensitive with no normalization/comparison column and empty option maps. Scratch preserves empty values; other machine roles treat null as missing. Identifiers, tables ending exactly in _events and columns containing created_timestamp or source_created_datestamp preserve existing values; other inferred columns replace. No database existence check or value normalization occurs.

    Example:
        >>> metadata = infer_column_metadata("works", "work_id", "INTEGER", is_primary_key=True)
        >>> metadata.merge_policy.value
        'preserve_existing'


    :param table: Physical table name, stringified for lookup and stored metadata.
    :param column: Physical column name, stringified for lookup and stored metadata.
    :param declared_type: Optional SQL type declaration for machine-role inference.
    :param is_primary_key: Primary-key fact passed to the ordered machine-role rules.
    :param is_foreign_key: Foreign-key fact passed to the ordered machine-role rules.
    :return: Existing configured ColumnMetadata for an exact override, otherwise a new inferred record.
    """

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
    """
    Return a column default using only its table and column names.

    Example:
        >>> default_column_metadata("notes", "note").merge_policy.value
        'append'


    :param table: Physical table name passed to infer_column_metadata.
    :param column: Physical column name passed to infer_column_metadata.
    :return: Configured display metadata or machine metadata inferred without SQL type/key facts.
    """

    return infer_column_metadata(table, column)


def default_column_case_sensitive(table: str, column: str) -> bool:
    """
    Read the case-sensitivity flag from the canonical fallback policy.

    Example:
        >>> default_column_case_sensitive("tags", "tag")
        False


    :param table: Physical table name.
    :param column: Physical column name.
    :return: Configured display flag, or True for an inferred machine column.
    """

    return default_column_metadata(table, column).case_sensitive


def is_display_column(table: str, column: str) -> bool:
    """
    Check exact membership in the explicit display-column registry.

    This does not infer display status from SQL type or naming patterns.

    Example:
        >>> is_display_column("works", "work_title")
        True
        >>> is_display_column("WORKS", "work_title")
        False


    :param table: Table name stringified without case normalization.
    :param column: Column name stringified without case normalization.
    :return: True for a registered pair, otherwise False.
    """

    return (str(table), str(column)) in DISPLAY_COLUMNS


def iter_column_case_sensitivity_defaults() -> Iterator[tuple[str, str, bool]]:
    """
    Yield configured display case-sensitivity rows in sorted table/column order.

    Machine-column inferred defaults are absent. Iteration reads the current COLUMN_CASE_SENSITIVITY_DEFAULTS registry.

    Example:
        >>> rows = list(iter_column_case_sensitivity_defaults())
        >>> rows == sorted(rows)
        True


    :return: Iterator of (table, column, case_sensitive) tuples for explicit defaults only.
    """

    for (table, column), case_sensitive in sorted(COLUMN_CASE_SENSITIVITY_DEFAULTS.items()):
        yield table, column, case_sensitive


def iter_column_metadata_defaults() -> Iterator[ColumnMetadata]:
    """
    Yield explicit display-policy records in sorted table/column order.

    These are display overrides only. Schema generators must call infer_column_metadata for other physical columns to obtain machine defaults.

    Example:
        >>> rows = list(iter_column_metadata_defaults())
        >>> [(row.table, row.column) for row in rows] == sorted(DISPLAY_COLUMNS)
        True


    :return: Iterator of existing configured ColumnMetadata objects, without copying them.
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
