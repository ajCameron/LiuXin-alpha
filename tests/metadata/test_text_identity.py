"""Identity keys retain distinctions that approximate search may discard."""

from __future__ import annotations

import pytest

from LiuXin_alpha.metadata.text_identity import normalize_identity_text


def test_full_text_keys_preserve_words_punctuation_and_accents() -> None:
    profile = "unicode_nfkc_whitespace_casefold"
    values = ("Sci-Fi & Fantasy", "Sci-Fi & Horror", "Sci Fi & Fantasy", "Café", "Cafe")
    keys = tuple(normalize_identity_text(value, profile) for value in values)
    assert keys == (
        "sci-fi & fantasy",
        "sci-fi & horror",
        "sci fi & fantasy",
        "café",
        "cafe",
    )
    assert len(set(keys)) == len(values)
    assert (
        normalize_identity_text("  Ｃａｆｅ\u0301\tStraße  東京\n", profile)
        == "café strasse 東京"
    )


@pytest.mark.parametrize(
    ("profile", "expected"),
    (("tag_search_term", "sci-fi&fantasy"), ("title_search_term", "sci")),
)
def test_historical_profiles_keep_their_declared_contract(
    profile: str, expected: str
) -> None:
    assert normalize_identity_text("Sci-Fi & Fantasy", profile) == expected


def test_unknown_identity_policy_is_rejected() -> None:
    with pytest.raises(ValueError, match="Unsupported text identity profile"):
        normalize_identity_text("Example", "unknown")
