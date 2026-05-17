from __future__ import annotations

import importlib
from dataclasses import dataclass, field

import pytest

from LiuXin_alpha.metadata import standardize
from LiuXin_alpha.metadata import standardization
from LiuXin_alpha.preferences import preferences


@dataclass
class _LogCapture:
    calls: list[tuple[object, ...]] = field(default_factory=list)

    def log_variables(self, *args: object) -> None:
        self.calls.append(args)


def test_identifier_and_creator_mapping_logging_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    capture = _LogCapture()
    monkeypatch.setattr(standardize, "default_log", capture)

    assert standardize.standardize_id_name("ISBN10") == "isbn"
    assert standardize.standardize_id_name("mobi-asin") == "amazon"
    assert standardize.standardize_id_name("goodreads") == "goodreads"
    assert standardize.standardize_id_name("unknown-id", logging=True) is None

    assert standardize.standardize_internal_id_name("calibre") == "uuid"
    assert standardize.standardize_internal_id_name("uuid") == "uuid"
    assert standardize.standardize_internal_id_name("external-only", logging=True) is None

    assert standardize.standardize_creator_category("authors") == "authors"
    assert standardize.standardize_creator_category("AUTHOR") == "authors"
    assert standardize.standardize_creator_category("ed") == "editors"
    assert standardize.standardize_creator_category("not-a-role", logging=True) is None

    assert len(capture.calls) == 3


def test_title_search_and_hash_helpers_handle_unicode_and_separators() -> None:
    assert standardize.standardize_title(None) == ""
    assert standardize.standardize_title("the_lord-of:the;rings|return") == (
        "The : Lord - of - the - Rings - Return"
    )
    assert "\\" not in standardize.standardize_title("東京_普通话-日本語")
    assert standardize.standardize_title("東京_普通话-日本語") == "東京 : 普通话 - 日本語"

    assert standardize.make_simpler_search_term("The Lord of the Rings: Return-of_the King!") == (
        "lord_rings_return"
    )
    assert standardize.make_title_search_term("A Study in Scarlet") == "study_in_scarlet"
    assert standardize.gen_title_author_phash("Tolkien, J R R & Other", "The Lord of the Rings") == (
        "tolkien_lord_rings"
    )
    assert standardize.make_series_phash("", "The Wheel of Time") == "_wheel_time"
    assert standardize.make_creator_phash(" 李   白 ") == "李白"


def test_field_standardizers_cover_language_genre_identifier_and_series_paths() -> None:
    assert standardize.standardize_lang("zh") == "Chinese"
    assert standardize.standardize_lang("Mandarin") is None
    assert standardize.standardize_language("zho") == "Chinese"
    assert standardize.standardize_language("not a language") == "Not a Language"

    assert standardize.standardize_genre("high fantasy") == "High Fantasy"
    assert standardize.standardize_genre("space opera") == "Space Opera"

    assert standardize.standardize_identifier(12345) == "12345"
    assert standardize.standardize_identifier_value("0-261-10357-1") == "02-6110-357-1"
    assert standardize.standardize_identifier_value("abc-123") == "abc-123"

    assert standardize.standardize_isbn("not an isbn") is False
    assert standardize.standardize_publisher(None) == ""
    assert standardize.standardize_publisher("small press") == "Small Press"
    assert standardize.standardize_series(None) == ""
    assert standardize.standardize_series("wheel of time") == "Wheel of Time"
    assert standardize.standardize_rating_type("Amazon (US)") == "amazon (us)"


def test_cleanup_tags_accepts_text_bytes_none_and_non_string_values() -> None:
    assert standardize.cleanup_tags(
        [
            "  Space  Opera  ",
            "space opera",
            "sci-fi, military",
            "Sci-Fi, military",
            b" bytes,tag ",
            bytearray("普通话, 日本語", "utf-8"),
            123,
            None,
            "",
            "   ",
        ]
    ) == [
        "Space Opera",
        "sci-fi; military",
        "bytes;tag",
        "普通话; 日本語",
        "123",
    ]


def test_creator_name_current_supported_shapes_and_unicode_safety() -> None:
    assert standardize.string_to_authors("mary shelley and percy b shelley") == [
        "Mary Shelley",
        "Percy B Shelley",
    ]
    assert standardize.string_to_authors("A && B") == ["A & B"]
    assert standardize.string_to_authors("") == []

    assert standardize.standardize_creator_name("Clarke, Arthur C") == "Arthur C. Clarke"
    assert standardize.standardize_creator_name("George R R Martin") == "George R. R. Martin"
    assert standardize.standardize_creator_name("Mc Donald") == "McDonald"
    assert standardize.standardize_creator_name("Jean-Luc Picard") == "Jean-Luc Picard"
    assert standardize.standardize_creator_name("O'Neill") == "O'Neill"

    # Non-Latin names should not crash or lose characters, even though the
    # legacy Roman-name standardizer is not semantically correct for them.
    assert "李" in standardize.standardize_creator_name("李 白")
    assert "山田" in standardize.standardize_creator_name("山田 太郎")


def test_string_to_authors_falls_back_when_author_regex_preference_is_invalid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import LiuXin_alpha.metadata.standardize as standardize_module
    import LiuXin_alpha.utils.logging as logging_module

    warnings = []
    with monkeypatch.context() as patch_context:
        patch_context.setattr(
            logging_module,
            "LiuXin_warning_print",
            lambda *args: warnings.append(args),
        )
        patch_context.setitem(preferences, "authors_split_regex", "(")
        reloaded = importlib.reload(standardize_module)

        assert reloaded.string_to_authors("ada lovelace and grace hopper") == [
            "Ada Lovelace",
            "Grace Hopper",
        ]
        assert warnings
        assert "using default" in " ".join(str(part) for part in warnings[0])

    importlib.reload(standardize_module)


@pytest.mark.parametrize(
    ("raw_name", "expected"),
    [
        ("JRR Tolkien", "J. R. R. Tolkien"),
        ("j r r tolkien", "J. R. R. Tolkien"),
        ("ArthurCClarke", "Arthur C. Clarke"),
        ("john mc donald", "John McDonald"),
        ("mac donald", "MacDonald"),
        ("Rowling, J, K", "Rowling, J, K."),
    ],
)
def test_creator_name_initial_spacing_and_comma_edges_are_pinned(
    raw_name: str,
    expected: str,
) -> None:
    assert standardize.standardize_creator_name(raw_name) == expected
    assert standardization.standardize_creator_name(raw_name) == expected


@pytest.mark.parametrize(
    ("function_name", "args"),
    [
        ("standardize_title", ("'the.old,man' - sea",)),
        ("standardize_title", (None,)),
        ("gen_title_author_phash", ("Le Guin, Ursula K & Someone", "A Wizard of Earthsea")),
        ("make_title_search_term", ("The Left Hand of Darkness",)),
        ("make_simpler_search_term", ("The Left Hand of Darkness: A Novel!",)),
        ("standardize_language", ("zho",)),
        ("standardize_isbn", ("0-261-10357-1",)),
        ("standardize_publisher", (None,)),
        ("standardize_publisher", ("small press",)),
        ("standardize_series", (None,)),
        ("standardize_series", ("earthsea cycle",)),
        ("make_series_phash", ("", "The Expanse")),
        ("make_series_phash", ("Le Guin, Ursula K", "Earthsea Cycle")),
        ("make_creator_phash", (" 李   白 ",)),
        ("make_tag_search_term", ("  Space  Opera  ",)),
        ("cleanup_tags", ([None, 123, b"a,b", bytearray("普通话,日本語", "utf-8")],)),
    ],
)
def test_legacy_standardize_modules_match_for_shared_api(
    function_name: str,
    args: tuple[object, ...],
) -> None:
    assert getattr(standardize, function_name)(*args) == getattr(standardization, function_name)(*args)


def test_identifier_normalization_compatibility_aliases() -> None:
    for raw, expected in [
        ("0-261-10357-1", "02-6110-357-1"),
        ("abc-123", "abc-123"),
    ]:
        assert standardize.standardize_identifier_value(raw) == expected
        assert standardization.standardize_identifier(raw) == expected


def test_standardization_module_genre_classifier_and_none_paths() -> None:
    assert standardization.standardize_genre(None) == ""
    assert standardization.standardize_genre("space opera") == "Space Opera"

    classification = standardization.classify_fiction_genre("space opera")

    assert classification.branch == "Science Fiction"
    assert classification.leaf == "Space Opera"
    assert classification.leaves == frozenset({"Space Opera"})
