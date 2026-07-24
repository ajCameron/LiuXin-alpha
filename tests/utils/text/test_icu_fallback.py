"""Contract tests for the source-checkout ICU fallback."""

from __future__ import annotations

from LiuXin_alpha.utils.text import icu
from LiuXin_alpha.utils.text import icu_fallback


def test_fallback_case_normalization_and_character_helpers() -> None:
    assert icu_fallback._text(None) == ""
    assert icu_fallback._text(b"\xff") == "\ufffd"
    assert icu_fallback.set_default_encoding("utf-8") is None
    assert icu_fallback.set_filesystem_encoding(b"utf-8") is None
    assert icu_fallback.change_case("Straße", icu_fallback.UPPER_CASE) == "STRASSE"
    assert icu_fallback.change_case("TITLE", icu_fallback.LOWER_CASE) == "title"
    assert icu_fallback.change_case("two words", icu_fallback.TITLE_CASE) == "Two Words"
    assert icu_fallback.swap_case("AbC") == "aBc"
    assert icu_fallback.normalize(icu_fallback.UNORM_NFC, "e\u0301") == "é"
    assert icu_fallback.normalize(None, "e\u0301") == "é"
    assert icu_fallback.character_name("A") == "LATIN CAPITAL LETTER A"
    assert icu_fallback.character_name_from_code(ord("A")) == "LATIN CAPITAL LETTER A"
    assert icu_fallback.character_name_from_code(-1) == ""
    assert icu_fallback.chr(0x1F600) == "\U0001F600"
    assert icu_fallback.string_length("A\U0001F600") == 2
    assert icu_fallback.utf16_length("A\U0001F600") == 3


def test_fallback_collator_matches_compiled_call_conventions() -> None:
    collator = icu_fallback.Collator("en")
    collator.strength = icu_fallback.UCOL_PRIMARY

    assert collator.contains("cafe", "A CAFÉ table")
    assert collator.find("cafe", "A CAFÉ table") == 2
    assert collator.startswith("cafe", "CAFÉ table")
    assert collator.strcmp("alpha", "beta") < 0
    assert collator.collation_order("Alpha") == (ord("a"), 1)
    assert collator.contractions() == ()

    collator.numeric = True
    assert collator.sort_key("item2") < collator.sort_key("item10")
    clone = collator.clone()
    assert clone.locale == "en"
    assert clone.strength == collator.strength
    assert clone.numeric is True

    case_sensitive = icu_fallback.Collator("en")
    case_sensitive.strength = 99
    case_sensitive.upper_first = True
    assert case_sensitive.sort_key("Alpha") < case_sensitive.sort_key("alpha")


def test_public_icu_wrapper_works_without_the_compiled_extension() -> None:
    assert icu.primary_contains("needle", "Hay NEEDLE stack")
    assert icu.primary_find("cafe", "A CAFÉ table") == 2
    assert icu.primary_startswith("cafe", "CAFÉ table")
    assert icu.upper("mixed") == "MIXED"
    assert icu.lower("MIXED") == "mixed"
    assert icu.title_case("two words") == "Two Words"
    assert sorted(["item10", "item2"], key=icu.numeric_sort_key) == [
        "item2",
        "item10",
    ]
