from __future__ import annotations

from tests.support.file_format_unicode import (
    COMMON_TEXT_FRAGMENTS,
    MULTISCRIPT_CASES,
    MULTISCRIPT_TEXT,
    assert_fragments_present,
    assert_no_replacement_chars,
    case_ids,
    deterministic_unicode_fuzz,
    encoded_unicode_cases,
    strip_known_bom,
)


def test_multiscript_corpus_has_stable_case_ids_and_fragments() -> None:
    assert case_ids(MULTISCRIPT_CASES) == (
        "latin",
        "latin_diacritics",
        "greek",
        "cyrillic",
        "arabic",
        "hebrew",
        "devanagari",
        "thai",
        "cjk",
        "emoji",
        "combining",
        "bidi_zwj",
    )
    assert_fragments_present(MULTISCRIPT_TEXT)
    assert_no_replacement_chars(MULTISCRIPT_TEXT)


def test_deterministic_unicode_fuzz_is_repeatable() -> None:
    first = deterministic_unicode_fuzz(seed=6801, length=256)
    second = deterministic_unicode_fuzz(seed=6801, length=256)
    different_seed = deterministic_unicode_fuzz(seed=6802, length=256)

    assert first == second
    assert first != different_seed
    assert_no_replacement_chars(first)


def test_encoded_unicode_cases_decode_to_the_same_corpus_after_bom_strip() -> None:
    assert case_ids(encoded_unicode_cases()) == (
        "utf_8",
        "utf_8_bom",
        "utf_16_le_bom",
        "utf_16_be_bom",
    )

    for case in encoded_unicode_cases():
        decoded = strip_known_bom(case.payload).decode(case.encoding, "replace")
        assert decoded == case.text
        assert_fragments_present(decoded, COMMON_TEXT_FRAGMENTS, context=case.case_id)
        assert_no_replacement_chars(decoded, context=case.case_id)
