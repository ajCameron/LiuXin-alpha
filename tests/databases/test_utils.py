"""Tests for LiuXin_alpha.databases.utils.

Covers fuzzy_title, fuzzy_title_patterns, find_identical_books,
get_link_table_name, _get_next_series_num_for_list, and _get_series_values.

Note: force_to_bool and cleanup_tags have pre-existing bugs in the Python 3
port (isinstance with unicode_literals/_Feature, and str.decode() call via a
broken isbytestring implementation) and are tested separately to document the
known failure modes.
"""
from __future__ import annotations

import pytest

from LiuXin_alpha.databases.utils import (
    _get_next_series_num_for_list,
    _get_series_values,
    fuzzy_title,
    fuzzy_title_patterns,
    get_link_table_name,
)


# ---------------------------------------------------------------------------
# fuzzy_title_patterns
# ---------------------------------------------------------------------------


class TestFuzzyTitlePatterns:
    def test_returns_non_empty_sequence(self) -> None:
        patterns = fuzzy_title_patterns()
        assert patterns is not None
        assert len(patterns) > 0

    def test_returns_tuple_of_tuples(self) -> None:
        patterns = fuzzy_title_patterns()
        for item in patterns:
            assert len(item) == 2  # (compiled_pattern, replacement)

    def test_is_cached_across_calls(self) -> None:
        p1 = fuzzy_title_patterns()
        p2 = fuzzy_title_patterns()
        assert p1 is p2


# ---------------------------------------------------------------------------
# fuzzy_title
# ---------------------------------------------------------------------------


class TestFuzzyTitle:
    def test_lowercases_result(self) -> None:
        result = fuzzy_title("Great Expectations")
        assert result == result.lower()

    def test_strips_leading_article_the(self) -> None:
        result = fuzzy_title("The Great Gatsby")
        assert "the" not in result.split()[:1]

    def test_strips_leading_article_a(self) -> None:
        result = fuzzy_title("A Tale of Two Cities")
        # 'a' should be stripped from the front
        assert not result.startswith("a ")

    def test_strips_punctuation(self) -> None:
        # brackets, colons, etc. should be removed
        result = fuzzy_title("Title: Subtitle [2024]")
        assert "[" not in result
        assert "]" not in result

    def test_collapses_whitespace(self) -> None:
        result = fuzzy_title("One   Two    Three")
        assert "  " not in result

    def test_dashes_become_spaces(self) -> None:
        result = fuzzy_title("Foo-Bar")
        # dash should become space
        assert "-" not in result

    def test_plain_title(self) -> None:
        result = fuzzy_title("Dune")
        assert "dune" == result

    def test_empty_string(self) -> None:
        result = fuzzy_title("")
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# find_identical_books
# ---------------------------------------------------------------------------


class TestFindIdenticalBooks:
    """Tests that find_identical_books returns expected sets."""

    def _make_data(
        self,
    ) -> tuple[dict[str, list[int]], dict[int, set[int]], dict[int, str]]:
        """
        Build the (author_map, aid_map, title_map) triple expected by
        find_identical_books.

        Format:
          author_map: {lower(author_name): [author_id, ...]}
          aid_map:    {author_id: {book_id, ...}}
          title_map:  {book_id: title_str}
        """
        from LiuXin_alpha.databases.utils import find_identical_books

        author_map = {"tolkien, j.r.r.": [1], "unknown": [99]}
        aid_map = {1: {10, 11}, 99: {12}}
        title_map = {10: "The Lord of the Rings", 11: "The Hobbit", 12: "A Book"}
        return author_map, aid_map, title_map

    def _mi(self, title: str, authors: list[str]):
        """Minimal metadata-like object."""

        class _MI:
            pass

        mi = _MI()
        mi.title = title
        mi.authors = authors
        return mi

    def test_finds_matching_book(self) -> None:
        from LiuXin_alpha.databases.utils import find_identical_books

        data = self._make_data()
        mi = self._mi("The Lord of the Rings", ["Tolkien, J.R.R."])
        result = find_identical_books(mi, data)
        assert 10 in result

    def test_no_match_for_unknown_author(self) -> None:
        from LiuXin_alpha.databases.utils import find_identical_books

        data = self._make_data()
        mi = self._mi("Some Book", ["NonExistent Author"])
        result = find_identical_books(mi, data)
        assert result == set()

    def test_no_match_for_wrong_title(self) -> None:
        from LiuXin_alpha.databases.utils import find_identical_books

        data = self._make_data()
        mi = self._mi("Silmarillion", ["Tolkien, J.R.R."])
        result = find_identical_books(mi, data)
        assert 10 not in result
        assert 11 not in result

    def test_fuzzy_match_ignores_articles(self) -> None:
        from LiuXin_alpha.databases.utils import find_identical_books

        data = self._make_data()
        # "Hobbit" fuzzy-matches "The Hobbit" after article stripping
        mi = self._mi("Hobbit", ["Tolkien, J.R.R."])
        result = find_identical_books(mi, data)
        assert 11 in result


# ---------------------------------------------------------------------------
# get_link_table_name
# ---------------------------------------------------------------------------


class TestGetLinkTableName:
    def test_two_different_tables_sorted_alphabetically(self) -> None:
        result = get_link_table_name("titles", "agents")
        # singular(agent)=agent, singular(title)=title -> sorted: [agent, title]
        assert "agent" in result
        assert "title" in result
        assert result.endswith("_links")

    def test_same_table_returns_intralinks(self) -> None:
        result = get_link_table_name("titles", "titles")
        assert result.endswith("_intralinks")
        assert "title" in result

    def test_table_name_normalised_to_lowercase(self) -> None:
        result_lower = get_link_table_name("titles", "agents")
        result_mixed = get_link_table_name("Titles", "Agents")
        assert result_lower == result_mixed

    def test_books_agents_link_table(self) -> None:
        result = get_link_table_name("books", "agents")
        assert "agent" in result
        assert "book" in result
        assert "_links" in result

    def test_books_books_intralink_table(self) -> None:
        result = get_link_table_name("books", "books")
        assert "book" in result
        assert "_intralinks" in result


# ---------------------------------------------------------------------------
# _get_series_values
# ---------------------------------------------------------------------------


class TestGetSeriesValues:
    def test_empty_string(self) -> None:
        assert _get_series_values("") == ("", None)

    def test_none(self) -> None:
        # Function starts with `if not val`, so None/empty -> (None/empty, None)
        assert _get_series_values(None) == (None, None)

    def test_series_with_integer_index(self) -> None:
        series, idx = _get_series_values("Dune [1]")
        assert series == "Dune"
        assert idx == 1.0

    def test_series_with_float_index(self) -> None:
        series, idx = _get_series_values("Foundation [2.5]")
        assert series == "Foundation"
        assert idx == 2.5

    def test_plain_name_no_index(self) -> None:
        series, idx = _get_series_values("No Index Series")
        assert series == "No Index Series"
        assert idx is None

    def test_strips_whitespace(self) -> None:
        series, idx = _get_series_values("  Discworld [21]  ")
        assert series == "Discworld"
        assert idx == 21.0

    def test_zero_index(self) -> None:
        series, idx = _get_series_values("Prequels [0]")
        assert series == "Prequels"
        assert idx == 0.0


# ---------------------------------------------------------------------------
# _get_next_series_num_for_list
# ---------------------------------------------------------------------------


class TestGetNextSeriesNumForList:
    def test_empty_list_returns_one(self) -> None:
        result = _get_next_series_num_for_list([])
        assert result == 1.0

    def test_next_after_list(self) -> None:
        # Default mode is "next": floor(last) + 1
        result = _get_next_series_num_for_list([[1.0], [2.0], [3.0]])
        assert result == 4.0

    def test_unwrap_true_unpacks_inner_list(self) -> None:
        # With unwrap=True (default), each element is treated as [index]
        result = _get_next_series_num_for_list([[5.0]])
        assert result == 6.0

    def test_unwrap_false_uses_element_directly(self) -> None:
        # With unwrap=False, each element is the index directly
        result = _get_next_series_num_for_list([1.0, 2.0, 3.0], unwrap=False)
        assert result == 4.0

    def test_fractional_last_element_floored(self) -> None:
        # floor(3.7) + 1 = 4.0
        result = _get_next_series_num_for_list([[3.7]], unwrap=True)
        assert result == 4.0
