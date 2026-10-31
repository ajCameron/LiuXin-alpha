"""Declare schema-independent value policies used by metadata consumers.

This module owns the shared policy vocabulary and defaults for names, titles,
taxonomy terms, aliases and prose. DisplayValueKind distinguishes meanings that
share a presentation role but have different merge policies, such as a name and
its aliases, or a verbatim annotation and an appended comment.

These immutable declarations do not normalize, validate, merge or persist values.
Text transformations live in metadata.text_identity; Catalog chooses and applies
rules during semantic writes. Databases maps these kinds to physical columns and
stores policy records, including database-specific overrides. No schema names,
database imports or Catalog imports belong here.

The Column-prefixed enum names and their serialized values are retained for API
and persisted-policy compatibility. New consumers should import them here.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
from types import MappingProxyType


class ColumnSemanticRole(str, Enum):
    """
    Classify a value by its application meaning.

    Members are string-valued policy labels. The enum itself performs no normalization,
    merge or validation operation on column data.

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
    Name the normalization policy used before value comparison.

    Members are string-valued policy labels. The enum itself performs no normalization,
    merge or validation operation on column data.

    Example:
        >>> ColumnNormalizationProfile(
        ...     "unicode_nfc"
        ... ) is ColumnNormalizationProfile.UNICODE_NFC
        True
    """

    NONE = "none"
    UNICODE_NFC = "unicode_nfc"
    UNICODE_NFC_TRIM_CASEFOLD = "unicode_nfc_trim_casefold"
    UNICODE_NFKC_WHITESPACE_CASEFOLD = "unicode_nfkc_whitespace_casefold"
    TAG_SEARCH_TERM = "tag_search_term"
    TITLE_SEARCH_TERM = "title_search_term"


class ColumnEmptyValuePolicy(str, Enum):
    """
    Describe how missing or blank values are treated by a value policy.

    Members are string-valued policy labels. The enum itself performs no normalization,
    merge or validation operation on column data.

    Example:
        >>> ColumnEmptyValuePolicy(
        ...     "null_or_blank_is_missing"
        ... ) is ColumnEmptyValuePolicy.NULL_OR_BLANK_IS_MISSING
        True
    """

    NULL_IS_MISSING = "null_is_missing"
    NULL_OR_BLANK_IS_MISSING = "null_or_blank_is_missing"
    PRESERVE = "preserve"


class ColumnMergePolicy(str, Enum):
    """
    Name the policy for combining an incoming and existing value.

    Members are string-valued policy labels. The enum itself performs no normalization,
    merge or validation operation on column data.

    Example:
        >>> ColumnMergePolicy(
        ...     "preserve_existing"
        ... ) is ColumnMergePolicy.PRESERVE_EXISTING
        True
    """

    REPLACE = "replace"
    SET_UNION = "set_union"
    APPEND = "append"
    PRESERVE_EXISTING = "preserve_existing"


class ColumnValidationProfile(str, Enum):
    """
    Name the semantic validation policy assigned to a value.

    Members are string-valued policy labels. The enum itself performs no normalization,
    merge or validation operation on column data.

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


class DisplayValueKind(str, Enum):
    """Distinguish display values without referring to their storage location."""

    NAME = "name"
    TITLE = "title"
    LABEL = "label"
    SORT_KEY = "sort_key"
    TAXONOMY_TERM = "taxonomy_term"
    VERBATIM_TEXT = "verbatim_text"
    RESOURCE_NAME = "resource_name"
    ALIASES = "aliases"
    COMMENT = "comment"
    NOTE = "note"
    SYNOPSIS = "synopsis"


@dataclass(frozen=True, slots=True)
class DisplayValuePolicy:
    """Describe comparison, emptiness, merge and validation for a display kind.

    A policy has no table names, comparison-column names or presentation options.
    Its normalization profile is a default; a stored comparison-key declaration
    can select a different profile without rewriting the original display text.
    """

    case_sensitive: bool
    semantic_role: ColumnSemanticRole
    normalization_profile: ColumnNormalizationProfile
    empty_value_policy: ColumnEmptyValuePolicy
    merge_policy: ColumnMergePolicy
    validation_profile: ColumnValidationProfile


_NAME_POLICY = DisplayValuePolicy(
    case_sensitive=False,
    semantic_role=ColumnSemanticRole.DISPLAY_NAME,
    normalization_profile=ColumnNormalizationProfile.UNICODE_NFC_TRIM_CASEFOLD,
    empty_value_policy=ColumnEmptyValuePolicy.NULL_OR_BLANK_IS_MISSING,
    merge_policy=ColumnMergePolicy.REPLACE,
    validation_profile=ColumnValidationProfile.DISPLAY_TEXT,
)
_VERBATIM_POLICY = replace(
    _NAME_POLICY,
    case_sensitive=True,
    semantic_role=ColumnSemanticRole.VERBATIM_TEXT,
    normalization_profile=ColumnNormalizationProfile.UNICODE_NFC,
    validation_profile=ColumnValidationProfile.VERBATIM_TEXT,
)
_APPEND_PROSE_POLICY = replace(_VERBATIM_POLICY, merge_policy=ColumnMergePolicy.APPEND)

# Case is presentation for names and terms, but can carry meaning in prose or
# resource names. Taxonomy terms and aliases form sets; comments, notes and
# synopses declare append behavior. These labels do not execute a merge.
_DISPLAY_VALUE_POLICIES = MappingProxyType({
    DisplayValueKind.NAME: _NAME_POLICY,
    DisplayValueKind.TITLE: replace(
        _NAME_POLICY, semantic_role=ColumnSemanticRole.TITLE,
    ),
    DisplayValueKind.LABEL: replace(
        _NAME_POLICY, semantic_role=ColumnSemanticRole.LABEL,
    ),
    DisplayValueKind.SORT_KEY: replace(
        _NAME_POLICY, semantic_role=ColumnSemanticRole.SORT_KEY,
    ),
    DisplayValueKind.TAXONOMY_TERM: replace(
        _NAME_POLICY,
        semantic_role=ColumnSemanticRole.TAXONOMY_TERM,
        merge_policy=ColumnMergePolicy.SET_UNION,
        validation_profile=ColumnValidationProfile.TAXONOMY_TERM,
    ),
    DisplayValueKind.VERBATIM_TEXT: _VERBATIM_POLICY,
    DisplayValueKind.RESOURCE_NAME: replace(
        _VERBATIM_POLICY,
        semantic_role=ColumnSemanticRole.RESOURCE_NAME,
        normalization_profile=ColumnNormalizationProfile.NONE,
        validation_profile=ColumnValidationProfile.RESOURCE_NAME,
    ),
    DisplayValueKind.ALIASES: replace(
        _NAME_POLICY, merge_policy=ColumnMergePolicy.SET_UNION,
    ),
    DisplayValueKind.COMMENT: _APPEND_PROSE_POLICY,
    DisplayValueKind.NOTE: _APPEND_PROSE_POLICY,
    DisplayValueKind.SYNOPSIS: _APPEND_PROSE_POLICY,
})


def display_value_policy(kind: DisplayValueKind) -> DisplayValuePolicy:
    """Return immutable defaults for a known display kind; reject unknown kinds.

    Example:
        >>> display_value_policy(DisplayValueKind.COMMENT).merge_policy.value
        'append'
        >>> display_value_policy(DisplayValueKind.TITLE).case_sensitive
        False

    :param kind: Schema-independent meaning of the display value.
    :return: Shared immutable policy for that kind.
    :raises KeyError: The kind has no declared display policy.
    """
    return _DISPLAY_VALUE_POLICIES[kind]


__all__ = [
    "ColumnEmptyValuePolicy",
    "ColumnMergePolicy",
    "ColumnNormalizationProfile",
    "ColumnSemanticRole",
    "ColumnValidationProfile",
    "DisplayValueKind",
    "DisplayValuePolicy",
    "display_value_policy",
]
