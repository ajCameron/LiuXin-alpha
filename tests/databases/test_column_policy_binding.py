"""Check the boundary between semantic defaults and physical column policies."""

from __future__ import annotations

import pytest

from LiuXin_alpha.databases import column_metadata
from LiuXin_alpha.metadata import value_policies
from LiuXin_alpha.metadata.text_identity import normalize_identity_text


def test_historical_enum_imports_are_aliases_of_metadata_types() -> None:
    """Existing adapters and serialized labels must share one enum identity."""
    for name in (
        "ColumnEmptyValuePolicy",
        "ColumnMergePolicy",
        "ColumnNormalizationProfile",
        "ColumnSemanticRole",
        "ColumnValidationProfile",
    ):
        assert getattr(column_metadata, name) is getattr(value_policies, name)


@pytest.mark.parametrize(
    ("table", "column", "role", "merge"),
    [
        ("agents", "agent_canonical_name", "display_name", "replace"),
        ("agents", "agent_aliases", "display_name", "set_union"),
        ("comments", "comment", "verbatim_text", "append"),
        ("notes", "note", "verbatim_text", "append"),
        ("synopses", "synopsis", "verbatim_text", "append"),
        ("annotations", "annotation_note_text", "verbatim_text", "replace"),
        ("files", "file_name", "resource_name", "replace"),
    ],
)
def test_schema_binds_distinct_meanings_that_share_a_display_role(
    table: str, column: str, role: str, merge: str,
) -> None:
    policy = column_metadata.default_column_metadata(table, column)
    assert policy.semantic_role.value == role
    assert policy.merge_policy.value == merge


def test_comparison_column_overrides_generic_taxonomy_normalization() -> None:
    """A key's explicit profile takes precedence over its display-kind default."""
    generic = value_policies.display_value_policy(
        value_policies.DisplayValueKind.TAXONOMY_TERM
    )
    bound = column_metadata.default_column_metadata("tags", "tag")
    text = "  Ｓｃｉ   Fi! "
    assert generic.normalization_profile.value == "unicode_nfc_trim_casefold"
    assert bound.comparison_column == "tag_phash"
    assert bound.normalization_profile.value == "unicode_nfkc_whitespace_casefold"
    assert normalize_identity_text(text, bound.normalization_profile.value) == "sci fi!"
    assert generic.normalization_profile is not bound.normalization_profile
    assert column_metadata.default_column_metadata(
        "genres", "genre_full"
    ).normalization_profile is generic.normalization_profile
