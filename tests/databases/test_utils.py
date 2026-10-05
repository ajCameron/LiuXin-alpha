"""
Check boolean coercion, fuzzy titles, book matching, link-table names, and series helpers.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/test_utils.py
"""
from __future__ import annotations

import pytest

from LiuXin_alpha.databases.utils import (
    _get_next_series_num_for_list,
    _get_series_values,
    force_to_bool,
    fuzzy_title,
    fuzzy_title_patterns,
    get_link_table_name,
)


# ---------------------------------------------------------------------------
# force_to_bool
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        (False, False),
        (True, True),
        (0, False),
        (1, True),
        (2, True),
        ("", None),
        ("0", False),
        ("1", True),
        ("false", False),
        ("no", False),
        ("unchecked", False),
        ("true", True),
        ("yes", True),
        ("checked", True),
        ("not-a-bool", None),
    ],
)
def test_force_to_bool_handles_typed_and_text_values(
    value: object,
    expected: object,
) -> None:
    """
    Check boolean coercion against the parametrized typed, textual, and unknown values.

    Example:
        >>> test_force_to_bool_handles_typed_and_text_values('checked', True)
        >>> test_force_to_bool_handles_typed_and_text_values('not-a-bool', None)


    :param value: Input value for this boolean-coercion case.
    :param expected: Expected result, compared by equality: True, False, or None.
    :return: None; failed expectations raise AssertionError.
    """
    assert force_to_bool(value) == expected


# ---------------------------------------------------------------------------
# fuzzy_title_patterns
# ---------------------------------------------------------------------------


class TestFuzzyTitlePatterns:
    """
    Check pattern availability, pair lengths, and cached object identity.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_utils.py
    """
    def test_returns_non_empty_sequence(self) -> None:
        """
        Check that fuzzy-title patterns are non-None and nonempty.

        Example:
            >>> TestFuzzyTitlePatterns().test_returns_non_empty_sequence()


        :return: None; failed expectations raise AssertionError.
        """
        patterns = fuzzy_title_patterns()
        assert patterns is not None
        assert len(patterns) > 0

    def test_returns_tuple_of_tuples(self) -> None:
        """
        Check that every pattern entry has two elements.

        Example:
            >>> TestFuzzyTitlePatterns().test_returns_tuple_of_tuples()


        :return: None; failed expectations raise AssertionError.
        """
        patterns = fuzzy_title_patterns()
        for item in patterns:
            assert len(item) == 2  # (compiled_pattern, replacement)

    def test_is_cached_across_calls(self) -> None:
        """
        Check that repeated calls return the identical pattern object.

        Example:
            >>> TestFuzzyTitlePatterns().test_is_cached_across_calls()


        :return: None; failed expectations raise AssertionError.
        """
        p1 = fuzzy_title_patterns()
        p2 = fuzzy_title_patterns()
        assert p1 is p2


# ---------------------------------------------------------------------------
# fuzzy_title
# ---------------------------------------------------------------------------


class TestFuzzyTitle:
    """
    Check observed normalization results for articles, case, whitespace, brackets, and dashes.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_utils.py
    """
    def test_lowercases_result(self) -> None:
        """
        Check that the normalized title equals its lowercase form.

        Example:
            >>> TestFuzzyTitle().test_lowercases_result()


        :return: None; failed expectations raise AssertionError.
        """
        result = fuzzy_title("Great Expectations")
        assert result == result.lower()

    def test_strips_leading_article_the(self) -> None:
        """
        Check that the normalized first word is not the article the.

        Example:
            >>> TestFuzzyTitle().test_strips_leading_article_the()


        :return: None; failed expectations raise AssertionError.
        """
        result = fuzzy_title("The Great Gatsby")
        assert "the" not in result.split()[:1]

    def test_strips_leading_article_a(self) -> None:
        """
        Check that the normalized title does not start with the article a followed by a space.

        Example:
            >>> TestFuzzyTitle().test_strips_leading_article_a()


        :return: None; failed expectations raise AssertionError.
        """
        result = fuzzy_title("A Tale of Two Cities")
        # 'a' should be stripped from the front
        assert not result.startswith("a ")

    def test_strips_punctuation(self) -> None:
        # brackets, colons, etc. should be removed
        """
        Check that square brackets are absent from the normalized title.

        Example:
            >>> TestFuzzyTitle().test_strips_punctuation()


        :return: None; failed expectations raise AssertionError.
        """
        result = fuzzy_title("Title: Subtitle [2024]")
        assert "[" not in result
        assert "]" not in result

    def test_collapses_whitespace(self) -> None:
        """
        Check that the normalized title contains no doubled spaces.

        Example:
            >>> TestFuzzyTitle().test_collapses_whitespace()


        :return: None; failed expectations raise AssertionError.
        """
        result = fuzzy_title("One   Two    Three")
        assert "  " not in result

    def test_dashes_become_spaces(self) -> None:
        """
        Check that the normalized title contains no dash.

        Example:
            >>> TestFuzzyTitle().test_dashes_become_spaces()


        :return: None; failed expectations raise AssertionError.
        """
        result = fuzzy_title("Foo-Bar")
        # dash should become space
        assert "-" not in result

    def test_plain_title(self) -> None:
        """
        Check that Dune normalizes exactly to dune.

        Example:
            >>> TestFuzzyTitle().test_plain_title()


        :return: None; failed expectations raise AssertionError.
        """
        result = fuzzy_title("Dune")
        assert "dune" == result

    def test_empty_string(self) -> None:
        """
        Check that normalizing an empty input returns a string.

        Example:
            >>> TestFuzzyTitle().test_empty_string()


        :return: None; failed expectations raise AssertionError.
        """
        result = fuzzy_title("")
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# find_identical_books
# ---------------------------------------------------------------------------


class TestFindIdenticalBooks:
    """
    Check title/author matching against a small in-memory book index.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_utils.py
    """

    def _make_data(
        self,
    ) -> tuple[dict[str, list[int]], dict[int, set[int]], dict[int, str]]:
        """
        Build fresh author-name, author-book, and book-title mappings for three books.

        Example:
            >>> data = TestFindIdenticalBooks()._make_data()
            >>> len(data)
            3


        :return: Tuple of the three mutable lookup dictionaries.
        """
        from LiuXin_alpha.databases.utils import find_identical_books

        author_map = {"tolkien, j.r.r.": [1], "unknown": [99]}
        aid_map = {1: {10, 11}, 99: {12}}
        title_map = {10: "The Lord of the Rings", 11: "The Hobbit", 12: "A Book"}
        return author_map, aid_map, title_map

    def _mi(self, title: str, authors: list[str]):
        """
        Create a minimal metadata carrier and assign its title and authors without copying them.

        Example:
            >>> authors = ['Tolkien, J.R.R.']
            >>> mi = TestFindIdenticalBooks()._mi('The Hobbit', authors)
            >>> (mi.title, mi.authors is authors)
            ('The Hobbit', True)


        :param title: Title value assigned to the carrier.
        :param authors: Author collection assigned by reference.
        :return: A fresh local _MI instance holding the supplied attributes.
        """

        class _MI:
            """
            Allow dynamic title and authors attributes on the local metadata double.

            Example:
                Run the owning tests with pytest::

                    python -m pytest -q tests/databases/test_utils.py
            """
            pass

        mi = _MI()
        mi.title = title
        mi.authors = authors
        return mi

    def test_finds_matching_book(self) -> None:
        """
        Check that the known author and matching title include book ten.

        Example:
            >>> TestFindIdenticalBooks().test_finds_matching_book()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.utils import find_identical_books

        data = self._make_data()
        mi = self._mi("The Lord of the Rings", ["Tolkien, J.R.R."])
        result = find_identical_books(mi, data)
        assert 10 in result

    def test_no_match_for_unknown_author(self) -> None:
        """
        Check that an absent author produces an empty match set.

        Example:
            >>> TestFindIdenticalBooks().test_no_match_for_unknown_author()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.utils import find_identical_books

        data = self._make_data()
        mi = self._mi("Some Book", ["NonExistent Author"])
        result = find_identical_books(mi, data)
        assert result == set()

    def test_no_match_for_wrong_title(self) -> None:
        """
        Check that a different title excludes the two indexed Tolkien books.

        Example:
            >>> TestFindIdenticalBooks().test_no_match_for_wrong_title()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.utils import find_identical_books

        data = self._make_data()
        mi = self._mi("Silmarillion", ["Tolkien, J.R.R."])
        result = find_identical_books(mi, data)
        assert 10 not in result
        assert 11 not in result

    def test_fuzzy_match_ignores_articles(self) -> None:
        """
        Check that the article-free Hobbit title matches book eleven.

        Example:
            >>> TestFindIdenticalBooks().test_fuzzy_match_ignores_articles()


        :return: None; failed expectations raise AssertionError.
        """
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
    """
    Check link/intralink suffixes, component names, and case normalization.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_utils.py
    """
    def test_two_different_tables_sorted_alphabetically(self) -> None:
        """
        Check that a title/agent link name contains both components and ends in _links.

        Example:
            >>> TestGetLinkTableName().test_two_different_tables_sorted_alphabetically()


        :return: None; failed expectations raise AssertionError.
        """
        result = get_link_table_name("titles", "agents")
        # singular(agent)=agent, singular(title)=title -> sorted: [agent, title]
        assert "agent" in result
        assert "title" in result
        assert result.endswith("_links")

    def test_same_table_returns_intralinks(self) -> None:
        """
        Check that a title self-link name contains title and ends in _intralinks.

        Example:
            >>> TestGetLinkTableName().test_same_table_returns_intralinks()


        :return: None; failed expectations raise AssertionError.
        """
        result = get_link_table_name("titles", "titles")
        assert result.endswith("_intralinks")
        assert "title" in result

    def test_table_name_normalised_to_lowercase(self) -> None:
        """
        Check that mixed-case and lowercase table inputs yield the same link name.

        Example:
            >>> TestGetLinkTableName().test_table_name_normalised_to_lowercase()


        :return: None; failed expectations raise AssertionError.
        """
        result_lower = get_link_table_name("titles", "agents")
        result_mixed = get_link_table_name("Titles", "Agents")
        assert result_lower == result_mixed

    def test_books_agents_link_table(self) -> None:
        """
        Check that a book/agent link name contains both components and the link suffix.

        Example:
            >>> TestGetLinkTableName().test_books_agents_link_table()


        :return: None; failed expectations raise AssertionError.
        """
        result = get_link_table_name("books", "agents")
        assert "agent" in result
        assert "book" in result
        assert "_links" in result

    def test_books_books_intralink_table(self) -> None:
        """
        Check that a book self-link name contains book and the intralink suffix.

        Example:
            >>> TestGetLinkTableName().test_books_books_intralink_table()


        :return: None; failed expectations raise AssertionError.
        """
        result = get_link_table_name("books", "books")
        assert "book" in result
        assert "_intralinks" in result


# ---------------------------------------------------------------------------
# _get_series_values
# ---------------------------------------------------------------------------


class TestGetSeriesValues:
    """
    Check series names and optional numeric indices, including empty and zero values.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_utils.py
    """
    def test_empty_string(self) -> None:
        """
        Check that an empty input returns an empty name and no index.

        Example:
            >>> TestGetSeriesValues().test_empty_string()


        :return: None; failed expectations raise AssertionError.
        """
        assert _get_series_values("") == ("", None)

    def test_none(self) -> None:
        # Function starts with `if not val`, so None/empty -> (None/empty, None)
        """
        Check that None returns no name and no index.

        Example:
            >>> TestGetSeriesValues().test_none()


        :return: None; failed expectations raise AssertionError.
        """
        assert _get_series_values(None) == (None, None)

    def test_series_with_integer_index(self) -> None:
        """
        Check that Dune with index one returns its name and 1.0.

        Example:
            >>> TestGetSeriesValues().test_series_with_integer_index()


        :return: None; failed expectations raise AssertionError.
        """
        series, idx = _get_series_values("Dune [1]")
        assert series == "Dune"
        assert idx == 1.0

    def test_series_with_float_index(self) -> None:
        """
        Check that Foundation retains its fractional index of 2.5.

        Example:
            >>> TestGetSeriesValues().test_series_with_float_index()


        :return: None; failed expectations raise AssertionError.
        """
        series, idx = _get_series_values("Foundation [2.5]")
        assert series == "Foundation"
        assert idx == 2.5

    def test_plain_name_no_index(self) -> None:
        """
        Check that a plain series name has no parsed index.

        Example:
            >>> TestGetSeriesValues().test_plain_name_no_index()


        :return: None; failed expectations raise AssertionError.
        """
        series, idx = _get_series_values("No Index Series")
        assert series == "No Index Series"
        assert idx is None

    def test_strips_whitespace(self) -> None:
        """
        Check that surrounding whitespace is removed from Discworld and its index twenty-one.

        Example:
            >>> TestGetSeriesValues().test_strips_whitespace()


        :return: None; failed expectations raise AssertionError.
        """
        series, idx = _get_series_values("  Discworld [21]  ")
        assert series == "Discworld"
        assert idx == 21.0

    def test_zero_index(self) -> None:
        """
        Check that a zero series index is retained as 0.0.

        Example:
            >>> TestGetSeriesValues().test_zero_index()


        :return: None; failed expectations raise AssertionError.
        """
        series, idx = _get_series_values("Prequels [0]")
        assert series == "Prequels"
        assert idx == 0.0


# ---------------------------------------------------------------------------
# _get_next_series_num_for_list
# ---------------------------------------------------------------------------


class TestGetNextSeriesNumForList:
    """
    Check next-series numbering for empty, nested, flat, and fractional inputs.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_utils.py
    """
    def test_empty_list_returns_one(self) -> None:
        """
        Check that an empty sequence produces series number 1.0.

        Example:
            >>> TestGetNextSeriesNumForList().test_empty_list_returns_one()


        :return: None; failed expectations raise AssertionError.
        """
        result = _get_next_series_num_for_list([])
        assert result == 1.0

    def test_next_after_list(self) -> None:
        # Default mode is "next": floor(last) + 1
        """
        Check that nested indices one through three produce 4.0.

        Example:
            >>> TestGetNextSeriesNumForList().test_next_after_list()


        :return: None; failed expectations raise AssertionError.
        """
        result = _get_next_series_num_for_list([[1.0], [2.0], [3.0]])
        assert result == 4.0

    def test_unwrap_true_unpacks_inner_list(self) -> None:
        # With unwrap=True (default), each element is treated as [index]
        """
        Check that the default nested-value form unwraps five and produces 6.0.

        Example:
            >>> TestGetNextSeriesNumForList().test_unwrap_true_unpacks_inner_list()


        :return: None; failed expectations raise AssertionError.
        """
        result = _get_next_series_num_for_list([[5.0]])
        assert result == 6.0

    def test_unwrap_false_uses_element_directly(self) -> None:
        # With unwrap=False, each element is the index directly
        """
        Check that unwrap=False accepts flat indices and produces 4.0.

        Example:
            >>> TestGetNextSeriesNumForList().test_unwrap_false_uses_element_directly()


        :return: None; failed expectations raise AssertionError.
        """
        result = _get_next_series_num_for_list([1.0, 2.0, 3.0], unwrap=False)
        assert result == 4.0

    def test_fractional_last_element_floored(self) -> None:
        # floor(3.7) + 1 = 4.0
        """
        Check that a final fractional index of 3.7 produces 4.0.

        Example:
            >>> TestGetNextSeriesNumForList().test_fractional_last_element_floored()


        :return: None; failed expectations raise AssertionError.
        """
        result = _get_next_series_num_for_list([[3.7]], unwrap=True)
        assert result == 4.0
