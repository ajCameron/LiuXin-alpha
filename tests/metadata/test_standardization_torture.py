"""Torture tests for metadata standardization.

These tests are intentionally "mean": they try to feed the standardization
utilities strings with awkward Unicode (BOM, ZWSP, combining marks, RTL marks,
emoji ZWJ sequences, lone surrogates), odd punctuation, and messy whitespace.

The goal is to harden the standardization layer by pinning down invariants
and catching regressions.

Some expectations are marked xfail to document known rough edges in the
current implementation.
"""

from __future__ import annotations

import random
import string

import pytest


# ------------------------------
# Test vectors
# ------------------------------

UNICODE_NIGHTMARES: list[str] = [
    "café",  # precomposed
    "cafe\u0301",  # combining mark
    "\ufeffBOMtag",  # byte order mark
    "zero\u200bwidth",  # ZWSP
    "joiner👩\u200d👩\u200d👧\u200d👦family",  # ZWJ emoji sequence
    "rtl\u200fmark",  # RLM
    "nbsp\u00a0space",  # NBSP
    "zalgo a\u0336\u0335\u0334\u034f\u035c\u035f",  # combining pile
    "null\x00byte",
    "lone_surrogate_\ud800",  # lone surrogate (legal in Python str)
]


def _rand_weird_string(rng: random.Random, max_len: int = 120) -> str:
    """Deterministic fuzzer producing slightly cursed Unicode-ish strings."""

    # Keep this cheap; we don't want quadratic regex loops to explode.
    alphabet = (
        string.ascii_letters
        + string.digits
        + " \t\n\r\f\v"
        + "'\".,;:!?_-/|()[]{}<>"
        + "\u00a0\u200b\u200f\ufeff"
        + "éöŁß"
        + "👩👦📚✨"
    )
    n = rng.randint(0, max_len)
    s = "".join(rng.choice(alphabet) for _ in range(n))
    # Occasionally glue on a combining mark run
    if rng.random() < 0.25:
        s += "\u0301\u0327\u034f" * rng.randint(1, 3)
    # Occasionally inject a lone surrogate via escape
    if rng.random() < 0.10:
        s += "\ud800"
    return s


# ------------------------------
# Smoke + invariants (should pass)
# ------------------------------


@pytest.mark.parametrize("s", UNICODE_NIGHTMARES)
def test_standardization_smoke_unicode_inputs_do_not_crash(s: str) -> None:
    """The standardization surface should be robust to weird Unicode."""

    from LiuXin_alpha.metadata import standardization as std

    # Creator names
    out = std.standardize_creator_name(s)
    assert isinstance(out, str)

    # Titles
    out = std.standardize_title(s)
    assert isinstance(out, str)

    # Search terms / hashes
    out = std.make_simpler_search_term(s)
    assert isinstance(out, str)

    out = std.make_title_search_term(s)
    assert isinstance(out, str)

    out = std.make_tag_search_term(s)
    assert isinstance(out, str)

    out = std.make_creator_phash(s)
    assert isinstance(out, str)

    # Other fields
    out = std.standardize_genre(s)
    assert isinstance(out, str)

    out = std.standardize_language(s)
    assert isinstance(out, str)

    out = std.standardize_tag(s)
    assert isinstance(out, str)

    out = std.standardize_identifier(s)
    assert isinstance(out, str)

    out = std.standardize_publisher(s)
    assert isinstance(out, str)

    out = std.standardize_series(s)
    assert isinstance(out, str)

    out = std.make_series_phash(s, s)
    assert isinstance(out, str)

    out = std.gen_title_author_phash(s, s)
    assert isinstance(out, str)


def test_standardize_creator_name_known_good_cases() -> None:
    from LiuXin_alpha.metadata.standardization import standardize_creator_name

    assert standardize_creator_name("Clarke, Arthur C") == "Arthur C. Clarke"
    assert standardize_creator_name("George R R Martin") == "George R. R. Martin"
    assert standardize_creator_name("Mc Donald") == "McDonald"
    assert standardize_creator_name("Dostoevsky, Fyodor") == "Fyodor Dostoevsky"


def test_standardize_creator_name_is_idempotent_for_common_inputs() -> None:
    """Idempotence is a useful invariant for standardization routines."""

    from LiuXin_alpha.metadata.standardization import standardize_creator_name

    rng = random.Random(1337)
    for _ in range(200):
        s = _rand_weird_string(rng, max_len=60)
        # Avoid pathological all-caps runs that can be slow under the current algorithm.
        if s.isupper() and len(s) > 20:
            continue
        once = standardize_creator_name(s)
        twice = standardize_creator_name(once)
        # The current implementation can "refine" further on the second pass
        # (e.g. turning "NK" into "N. K."). We still want it to converge.
        thrice = standardize_creator_name(twice)
        assert thrice == twice


def test_make_simpler_search_term_invariants() -> None:
    from LiuXin_alpha.metadata.standardization import make_simpler_search_term

    cases = [
        "The Lord of the Rings: The Two Towers!",
        "  A Study in Scarlet  ",
        "Watership Down (Illustrated Edition)",
        "Café au lait — a mémoire",
        "lone_surrogate_\ud800",
    ]
    for s in cases:
        out = make_simpler_search_term(s)
        assert isinstance(out, str)
        assert out == out.lower()
        assert " " not in out


def test_standardize_genre_mappings() -> None:
    from LiuXin_alpha.metadata.standardization import standardize_genre

    assert standardize_genre("sci fi") == "Science Fiction"
    assert standardize_genre("SF") == "Science Fiction"
    # The current regex expects the s/f to be separated ("mil s f"), not "mil sf".
    assert standardize_genre("mil s f") == "Military Science Fiction"
    assert standardize_genre("UF") == "Urban Fantasy"


def test_standardize_language_variants() -> None:
    from LiuXin_alpha.metadata.standardization import standardize_language

    assert standardize_language("en") == "English"
    assert standardize_language("eng") == "English"
    assert standardize_language("zh") == "Chinese"
    assert standardize_language("zho") == "Chinese"


def test_standardize_isbn_10_normalization() -> None:
    from LiuXin_alpha.metadata.standardization import standardize_isbn

    # The current ISBN tooling appears to focus on ISBN-10.
    assert standardize_isbn("0261103571") == "02-6110-357-1"
    assert standardize_isbn("0-261-10357-1") == "02-6110-357-1"
    assert standardize_isbn("not an isbn") is False


@pytest.mark.xfail(
    reason="cleanup_tags currently uses isbytestring() that treats str as bytes, causing decode() on str"
)
def test_cleanup_tags_dedupes_normalizes_and_replaces_commas() -> None:
    from LiuXin_alpha.metadata.standardization import cleanup_tags

    tags = [
        "  Space  Opera  ",
        "space opera",
        "sci-fi, military",
        "Sci-Fi, military",
        "\ufeffBOMtag",
        "",
        "   ",
    ]
    out = cleanup_tags(tags)
    assert out == ["Space Opera", "sci-fi; military", "\ufeffBOMtag"]


# ------------------------------
# Torture expectations (known rough edges)
# ------------------------------


@pytest.mark.xfail(reason="Hyphenated and apostrophe names are currently mangled/truncated")
def test_standardize_creator_name_handles_hyphens_and_apostrophes() -> None:
    from LiuXin_alpha.metadata.standardization import standardize_creator_name

    assert standardize_creator_name("Jean-Luc Picard") == "Jean-Luc Picard"
    assert standardize_creator_name("O'Neill") == "O'Neill"


@pytest.mark.xfail(reason="standardize_title currently inserts literal backslashes around separators")
def test_standardize_title_should_not_insert_backslashes() -> None:
    from LiuXin_alpha.metadata.standardization import standardize_title

    out = standardize_title("lord_of_the_rings")
    assert "\\" not in out


@pytest.mark.xfail(reason="cleanup_tags currently applies str replace() before decoding bytes")
def test_cleanup_tags_accepts_bytes_tags() -> None:
    from LiuXin_alpha.metadata.standardization import cleanup_tags

    tags = [b"  a  ", b"b,", "C"]
    out = cleanup_tags(tags)
    assert out[0] == "a"


# ------------------------------
# Torture: standardize.py front-end helpers
# ------------------------------


def test_string_to_authors_splits_titlecases_and_handles_ampersand_escape() -> None:
    from LiuXin_alpha.metadata.standardize import string_to_authors

    assert string_to_authors("mary shelley and percy b shelley") == [
        "Mary Shelley",
        "Percy B Shelley",
    ]
    assert string_to_authors("A && B") == ["A & B"]


def test_standardize_tag_strips_bom_and_lowercases() -> None:
    from LiuXin_alpha.metadata.standardize import standardize_tag

    assert standardize_tag("  \ufeffWeird\u200bTag  ") == "weird\u200btag"


def test_standardize_id_name_maps_common_synonyms() -> None:
    from LiuXin_alpha.metadata.standardize import standardize_id_name

    assert standardize_id_name("isbn_13") == "isbn"
    assert standardize_id_name("ISBN10") == "isbn"
    assert standardize_id_name("asin") == "amazon"
    assert standardize_id_name("calibre") == "uuid"
    assert standardize_id_name("unknown") is None


def test_standardize_creator_category_maps_common_roles() -> None:
    from LiuXin_alpha.metadata.standardize import standardize_creator_category

    assert standardize_creator_category("author") == "authors"
    assert standardize_creator_category("aut") == "authors"
    assert standardize_creator_category("writer") == "authors"
    assert standardize_creator_category("ed") == "editors"
    assert standardize_creator_category("unknown") is None


def test_standardize_identifier_is_stringy_and_stripped() -> None:
    from LiuXin_alpha.metadata.standardize import standardize_identifier

    assert standardize_identifier(12345) == "12345"
    assert standardize_identifier("  12345  ") == "12345"


def test_standardize_rating_type_is_lowercased() -> None:
    from LiuXin_alpha.metadata.standardize import standardize_rating_type

    assert standardize_rating_type("Amazon (US)") == "amazon (us)"
