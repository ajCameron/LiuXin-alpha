"""
Verify test search core behavior against the public catalog contracts.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test search core through its owning regression module::

        python -m pytest -q tests/catalog/test_search_core.py
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest

import LiuXin_alpha.catalog.search as search_module
from LiuXin_alpha.catalog.search import (
    CONTAINS_MATCH,
    EQUALS_MATCH,
    REGEXP_MATCH,
    KeyPairSearch,
    LRUCache,
    Parser,
    SavedSearchQueries,
    Search,
    _match,
    _matchkind,
)
from LiuXin_alpha.utils.search_query_parser import ParseException


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("Title", (CONTAINS_MATCH, "title")),
        (r"\=Literal", (CONTAINS_MATCH, "=literal")),
        ("=Title", (EQUALS_MATCH, "title")),
        (r"~T\S+", (REGEXP_MATCH, r"T\S+")),
    ],
)
def test_matchkind_parses_prefixes_and_preserves_regex_case(
    query: str,
    expected: tuple[int, str],
) -> None:
    """
    Verify matchkind parses prefixes and preserves regex case.

    Example:
        Exercise test matchkind parses prefixes and preserves regex case through its owning regression module::

            python -m pytest -q tests/catalog/test_search_core.py


    :param query: Parsed or textual catalog query to evaluate.
    :param expected: Value supplied for expected under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
    assert _matchkind(query) == expected


@pytest.mark.parametrize(
    ("query", "values", "match_kind", "use_primary", "expected"),
    [
        ("needle", ("hay Needle stack",), CONTAINS_MATCH, True, True),
        ("needle", ("hay needle stack",), CONTAINS_MATCH, False, True),
        ("missing", ("haystack",), CONTAINS_MATCH, False, False),
        ("title", ("Title",), EQUALS_MATCH, True, True),
        ("..books", (".books",), EQUALS_MATCH, True, True),
        ("..target", ("first.parts", "other.target"), EQUALS_MATCH, True, True),
        (".books", ("books.fiction",), EQUALS_MATCH, True, True),
        (
            ".books",
            ("other", "bookstore", "books.fiction"),
            EQUALS_MATCH,
            True,
            True,
        ),
        ("..fiction", ("books.fiction",), EQUALS_MATCH, True, True),
        (r"^Ti\w+$", ("Title",), REGEXP_MATCH, True, True),
        (r"^target$", ("miss", "target"), REGEXP_MATCH, True, True),
        ("[", ("Title",), REGEXP_MATCH, True, False),
        ("unused", ("first", "second"), 99, True, False),
    ],
)
def test_match_supports_contains_equals_hierarchies_and_regex(
    query: str,
    values: tuple[str, ...],
    match_kind: int,
    use_primary: bool,
    expected: bool,
) -> None:
    """
    Verify match supports contains equals hierarchies and regex.

    Example:
        Exercise test match supports contains equals hierarchies and regex through its owning regression module::

            python -m pytest -q tests/catalog/test_search_core.py


    :param query: Parsed or textual catalog query to evaluate.
    :param values: Values to normalize, compare or write in stable order.
    :param match_kind: Value supplied for match kind under the catalog contract.
    :param use_primary: Value supplied for use primary under the catalog contract.
    :param expected: Value supplied for expected under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
    assert (
        _match(
            query,
            values,
            match_kind,
            use_primary_find_in_search=use_primary,
        )
        is expected
    )


def test_key_pair_search_matches_keys_values_and_presence() -> None:
    """
    Verify key pair search matches keys values and presence.

    Example:
        Exercise test key pair search matches keys values and presence through its owning regression module::

            python -m pytest -q tests/catalog/test_search_core.py


    :return: None; the function records state or raises through its assertions.
    """
    values = [
        ({"isbn": "123", "asin": "ABC"}, {1}),
        ({"other": "123"}, {2}),
        ({}, {3}),
    ]
    field_iter = lambda: iter(values)
    candidates = {1, 2, 3, 4}
    search = KeyPairSearch()

    assert search("isbn:123", field_iter, candidates, False) == {1}
    assert search("123", field_iter, candidates, False) == {1, 2}
    assert search("isbn:true", field_iter, candidates, False) == {1}
    assert search("isbn:false", field_iter, candidates, False) == {2, 3, 4}
    assert search("true", field_iter, candidates, False) == {1, 2}
    assert search("false", field_iter, candidates, False) == {3, 4}
    assert search("missing:value", field_iter, candidates, False) == set()


class _PreferenceDatabase:
    """
    Provide the PreferenceDatabase test fixture or double with explicit deterministic behavior.

    Example:
        Exercise PreferenceDatabase through its owning regression module::

            python -m pytest -q tests/catalog/test_search_core.py
    """
    def __init__(self) -> None:
        """
        Initialize the PreferenceDatabase test double.

        Example:
            Exercise PreferenceDatabase.init through its owning regression module::

                python -m pytest -q tests/catalog/test_search_core.py


        :return: None; the function records state or raises through its assertions.
        """
        self.preferences = {"saved": {"Existing": "title:old"}}
        self.writes: list[tuple[str, dict[str, str]]] = []
        self.private_writes: list[tuple[str, dict[str, str]]] = []

    def pref(self, name: str, default: dict[str, str]) -> dict[str, str]:
        """
        Perform the pref test-helper operation with deterministic inputs.

        Example:
            Exercise PreferenceDatabase.pref through its owning regression module::

                python -m pytest -q tests/catalog/test_search_core.py


        :param name: Value supplied for name under the catalog contract.
        :param default: Value supplied for default under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """
        return dict(self.preferences.get(name, default))

    def set_pref(self, name: str, value: dict[str, str]) -> None:
        """
        Perform the set pref test-helper operation with deterministic inputs.

        Example:
            Exercise PreferenceDatabase.set pref through its owning regression module::

                python -m pytest -q tests/catalog/test_search_core.py


        :param name: Value supplied for name under the catalog contract.
        :param value: Public or stored value to normalize, compare or write.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.writes.append((name, dict(value)))

    def _set_pref(self, name: str, value: dict[str, str]) -> None:
        """
        Perform the set pref test-helper operation with deterministic inputs.

        Example:
            Exercise PreferenceDatabase.set pref through its owning regression module::

                python -m pytest -q tests/catalog/test_search_core.py


        :param name: Value supplied for name under the catalog contract.
        :param value: Public or stored value to normalize, compare or write.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.private_writes.append((name, dict(value)))


def test_saved_search_queries_round_trip_and_mutate_preferences() -> None:
    """
    Verify saved search queries round trip and mutate preferences.

    Example:
        Exercise test saved search queries round trip and mutate preferences through its owning regression module::

            python -m pytest -q tests/catalog/test_search_core.py


    :return: None; the function records state or raises through its assertions.
    """
    db = _PreferenceDatabase()
    saved = SavedSearchQueries(db, "saved")

    assert saved.db is db
    assert saved.lookup("Existing") == "title:old"

    saved.add(b"New", b"  author:Le Guin  ")
    assert saved.lookup("New") == "author:Le Guin"
    assert db.writes[-1] == (
        "saved",
        {"Existing": "title:old", "New": "author:Le Guin"},
    )

    saved.rename("New", "Renamed")
    assert saved.lookup("New") is None
    assert saved.lookup("Renamed") == "author:Le Guin"
    assert db.private_writes[-1][0] == "saved"

    saved.delete("Existing")
    assert saved.lookup("Existing") is None
    assert db.writes[-1][0] == "saved"

    saved.set_all({"Zulu": "z", "alpha": "a"})
    assert saved.names() == ["alpha", "Zulu"]
    assert db.private_writes[-1] == ("saved", {"Zulu": "z", "alpha": "a"})


def test_saved_search_queries_without_database_are_inert() -> None:
    """
    Verify saved search queries without database remain inert.

    Example:
        Exercise test saved search queries without database are inert through its owning regression module::

            python -m pytest -q tests/catalog/test_search_core.py


    :return: None; the function records state or raises through its assertions.
    """
    saved = SavedSearchQueries(None, "saved")

    assert saved.db is None
    assert saved.queries == {}
    saved.add("name", "value")
    saved.delete("name")
    saved.rename("old", "new")
    saved.set_all({"ignored": "value"})
    assert saved.queries == {}


def test_lru_cache_refreshes_age_evicts_and_removes_entries() -> None:
    """
    Verify lru cache refreshes age evicts and removes entries.

    Example:
        Exercise test lru cache refreshes age evicts and removes entries through its owning regression module::

            python -m pytest -q tests/catalog/test_search_core.py


    :return: None; the function records state or raises through its assertions.
    """
    cache = LRUCache(limit=2)
    cache.add("first", {1})
    cache["second"] = {2}

    assert cache.get("first") == {1}
    cache.add("third", {3})
    assert "first" in cache
    assert "second" not in cache
    assert cache["missing"] is None

    cache.add("first", {99})
    assert cache["first"] == {1}
    assert len(cache) == 2

    cache.pop("missing")
    cache.pop("first")
    assert list(cache) == [("third", {3})]

    cache.clear()
    assert len(cache) == 0


class _SearchDatabase:
    """
    Provide the SearchDatabase test fixture or double with explicit deterministic behavior.

    Example:
        Exercise SearchDatabase through its owning regression module::

            python -m pytest -q tests/catalog/test_search_core.py
    """
    def __init__(self, ids: set[int]) -> None:
        """
        Initialize the SearchDatabase test double.

        Example:
            Exercise SearchDatabase.init through its owning regression module::

                python -m pytest -q tests/catalog/test_search_core.py


        :param ids: Value supplied for ids under the catalog contract.
        :return: None; the function records state or raises through its assertions.
        """
        self.ids = ids

    def _all_book_ids(self, type: type[set[int]]) -> set[int]:
        """
        Perform the all book ids test-helper operation with deterministic inputs.

        Example:
            Exercise SearchDatabase.all book ids through its owning regression module::

                python -m pytest -q tests/catalog/test_search_core.py


        :param type: Value supplied for type under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """
        return type(self.ids)


class _ParserStub:
    """
    Provide the ParserStub test fixture or double with explicit deterministic behavior.

    Example:
        Exercise ParserStub through its owning regression module::

            python -m pytest -q tests/catalog/test_search_core.py
    """
    def __init__(
        self,
        responses: dict[str, set[int] | Exception],
    ) -> None:
        """
        Initialize the ParserStub test double.

        Example:
            Exercise ParserStub.init through its owning regression module::

                python -m pytest -q tests/catalog/test_search_core.py


        :param responses: Value supplied for responses under the catalog contract.
        :return: None; the function records state or raises through its assertions.
        """
        self.responses = responses
        self.all_book_ids: set[int] = set()
        self.virtual_field_used = False
        self.calls: list[tuple[str, set[int]]] = []
        self.dbcache: Any = object()
        self.lookup_saved_search: Any = object()

    def parse(self, query: str) -> set[int]:
        """
        Perform the parse test-helper operation with deterministic inputs.

        Example:
            Exercise ParserStub.parse through its owning regression module::

                python -m pytest -q tests/catalog/test_search_core.py


        :param query: Parsed or textual catalog query to evaluate.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.calls.append((query, set(self.all_book_ids)))
        response = self.responses[query]
        if isinstance(response, Exception):
            raise response
        self.virtual_field_used = query.startswith("virtual")
        return set(response).intersection(self.all_book_ids)


class _FieldMetadata(dict[str, dict[str, Any]]):
    """
    Provide the FieldMetadata test fixture or double with explicit deterministic behavior.

    Example:
        Exercise FieldMetadata through its owning regression module::

            python -m pytest -q tests/catalog/test_search_core.py
    """
    def __init__(
        self,
        fields: dict[str, dict[str, Any]],
        aliases: dict[str, str | list[str]] | None = None,
    ) -> None:
        """
        Initialize the FieldMetadata test double.

        Example:
            Exercise FieldMetadata.init through its owning regression module::

                python -m pytest -q tests/catalog/test_search_core.py


        :param fields: Value supplied for fields under the catalog contract.
        :param aliases: Value supplied for aliases under the catalog contract.
        :return: None; the function records state or raises through its assertions.
        """
        super().__init__(fields)
        self.aliases = aliases or {}

    def search_term_to_field_key(self, term: str) -> str | list[str]:
        """
        Perform the search term to field key test-helper operation with deterministic inputs.

        Example:
            Exercise FieldMetadata.search term to field key through its owning regression module::

                python -m pytest -q tests/catalog/test_search_core.py


        :param term: Value supplied for term under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """
        return self.aliases.get(term, term)


class _SearchField:
    """
    Provide the SearchField test fixture or double with explicit deterministic behavior.

    Example:
        Exercise SearchField through its owning regression module::

            python -m pytest -q tests/catalog/test_search_core.py
    """
    def __init__(
        self,
        values: list[tuple[Any, set[int]]],
        *,
        is_many: bool = False,
        counts: list[tuple[int, set[int]]] | None = None,
    ) -> None:
        """
        Initialize the SearchField test double.

        Example:
            Exercise SearchField.init through its owning regression module::

                python -m pytest -q tests/catalog/test_search_core.py


        :param values: Values to normalize, compare or write in stable order.
        :param is_many: Value supplied for is many under the catalog contract.
        :param counts: Value supplied for counts under the catalog contract.
        :return: None; the function records state or raises through its assertions.
        """
        self.values = values
        self.is_many = is_many
        self.counts = counts or []

    @staticmethod
    def _within_candidates(
        values: list[tuple[Any, set[int]]],
        candidates: set[int],
    ) -> Iterator[tuple[Any, set[int]]]:
        """
        Perform the within candidates test-helper operation with deterministic inputs.

        Example:
            Exercise SearchField.within candidates through its owning regression module::

                python -m pytest -q tests/catalog/test_search_core.py


        :param values: Values to normalize, compare or write in stable order.
        :param candidates: Optional candidate identities restricting the search universe.
        :return: The deterministic value, row, identity or collection described above.
        """
        for value, book_ids in values:
            matched = set(book_ids).intersection(candidates)
            if matched:
                yield value, matched

    def iter_searchable_values(
        self,
        _get_metadata: Any,
        candidates: set[int],
    ) -> Iterator[tuple[Any, set[int]]]:
        """
        Perform the iter searchable values test-helper operation with deterministic inputs.

        Example:
            Exercise SearchField.iter searchable values through its owning regression module::

                python -m pytest -q tests/catalog/test_search_core.py


        :param _get_metadata: Value supplied for get metadata under the catalog contract.
        :param candidates: Optional candidate identities restricting the search universe.
        :return: The deterministic value, row, identity or collection described above.
        """
        return self._within_candidates(self.values, candidates)

    def iter_counts(self, candidates: set[int]) -> Iterator[tuple[int, set[int]]]:
        """
        Perform the iter counts test-helper operation with deterministic inputs.

        Example:
            Exercise SearchField.iter counts through its owning regression module::

                python -m pytest -q tests/catalog/test_search_core.py


        :param candidates: Optional candidate identities restricting the search universe.
        :return: The deterministic value, row, identity or collection described above.
        """
        return self._within_candidates(self.counts, candidates)


def _field_metadata(
    datatype: str,
    *,
    is_multiple: bool = False,
    is_csp: bool = False,
) -> dict[str, Any]:
    """
    Perform the field metadata test-helper operation with deterministic inputs.

    Example:
        Exercise field metadata through its owning regression module::

            python -m pytest -q tests/catalog/test_search_core.py


    :param datatype: Value supplied for datatype under the catalog contract.
    :param is_multiple: Value supplied for is multiple under the catalog contract.
    :param is_csp: Value supplied for is csp under the catalog contract.
    :return: The deterministic value, row, identity or collection described above.
    """
    return {
        "datatype": datatype,
        "display": {},
        "is_multiple": {"cache_to_list": ","} if is_multiple else {},
        "is_csp": is_csp,
        "search_terms": ["field"],
    }


class _CatalogSearchDatabase(_SearchDatabase):
    """
    Provide the CatalogSearchDatabase test fixture or double with explicit deterministic behavior.

    Example:
        Exercise CatalogSearchDatabase through its owning regression module::

            python -m pytest -q tests/catalog/test_search_core.py
    """
    def __init__(self) -> None:
        """
        Initialize the CatalogSearchDatabase test double.

        Example:
            Exercise CatalogSearchDatabase.init through its owning regression module::

                python -m pytest -q tests/catalog/test_search_core.py


        :return: None; the function records state or raises through its assertions.
        """
        super().__init__({1, 2, 3})
        self.field_metadata = _FieldMetadata(
            {
                "title": _field_metadata("text"),
                "pubdate": _field_metadata("datetime"),
                "date": _field_metadata("datetime"),
                "rating": _field_metadata("rating"),
                "flag": _field_metadata("bool"),
                "tags": _field_metadata("text", is_multiple=True),
                "identifiers": _field_metadata("text", is_csp=True),
                "id": _field_metadata("int"),
                "languages": _field_metadata("text"),
                "score": _field_metadata("float"),
                "pages": _field_metadata("int"),
                "cover": _field_metadata("text"),
                "notes": _field_metadata("text"),
                "uuid": _field_metadata("text"),
                "@ignored": _field_metadata("text"),
            },
            aliases={"isbn": "identifiers"},
        )
        self.fields = {
            "title": _SearchField(
                [("Dune", {1}), ("Foundation", {2}), ("Other", {3})]
            ),
            "pubdate": _SearchField(
                [
                    ("2024-05-06", {1}),
                    ("2023-01-01", {2}),
                    (None, {3}),
                ]
            ),
            "timestamp": _SearchField(
                [
                    ("2024-05-06", {1}),
                    ("2023-01-01", {2}),
                    (None, {3}),
                ]
            ),
            "rating": _SearchField(
                [(2, {1}), (8, {1}), (4, {2}), (None, {3})]
            ),
            "flag": _SearchField([(True, {1}), (False, {2}), (None, {3})]),
            "tags": _SearchField(
                [("science fiction", {1}), ("classic", {2})],
                is_many=True,
                counts=[(2, {1}), (1, {2}), (0, {3})],
            ),
            "identifiers": _SearchField(
                [
                    ({"isbn": "123", "asin": "ABC"}, {1}),
                    ({"isbn": "456"}, {2}),
                ],
                is_many=True,
            ),
            "languages": _SearchField(
                [("eng", {1}), ("fra", {2}), ("deu", {3})]
            ),
            "score": _SearchField([(1.5, {3}), (2.5, {3}), (4.0, {3})]),
            "pages": _SearchField([(1, {1}), (4, {2}), (8, {3})]),
            "cover": _SearchField([(None, {1}), ("", {2}), ("present", {3})]),
            "notes": _SearchField([(None, {1}), ("memo", {2})]),
        }
        self.preferences: dict[str, Any] = {
            "grouped_search_terms": {},
            "bools_are_tristate": False,
            "user_categories": {},
        }

    def _pref(self, name: str) -> Any:
        """
        Perform the pref test-helper operation with deterministic inputs.

        Example:
            Exercise CatalogSearchDatabase.pref through its owning regression module::

                python -m pytest -q tests/catalog/test_search_core.py


        :param name: Value supplied for name under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """
        return self.preferences[name]

    def _get_proxy_metadata(self, book_id: int) -> dict[str, int]:
        """
        Return proxy metadata from deterministic test state.

        Example:
            Exercise CatalogSearchDatabase.get proxy metadata through its owning regression module::

                python -m pytest -q tests/catalog/test_search_core.py


        :param book_id: Catalog record identity addressed by the operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return {"id": book_id}


_CATALOG_SEARCH_LOCATIONS = (
    "all",
    "title",
    "pubdate",
    "date",
    "rating",
    "flag",
    "tags",
    "identifiers",
    "isbn",
    "id",
    "languages",
    "score",
    "pages",
    "cover",
    "notes",
)


def test_parser_dispatches_catalog_field_types_without_mutating_candidates() -> None:
    """
    Verify parser dispatches catalog field types without mutating candidates.

    Example:
        Exercise test parser dispatches catalog field types without mutating candidates through its owning regression module::

            python -m pytest -q tests/catalog/test_search_core.py


    :return: None; the function records state or raises through its assertions.
    """
    db = _CatalogSearchDatabase()
    search = Search(
        None,
        "saved",
        all_search_locations=_CATALOG_SEARCH_LOCATIONS,
    )
    parser = search.create_parser(db)
    parser.all_book_ids = {1, 2, 3}
    candidates = {1, 2, 3}

    assert parser.get_matches("title", "dune", candidates) == {1}
    assert parser.get_matches("pubdate", ">=2024-01-01", candidates) == {1}
    assert parser.get_matches("date", "2024", candidates) == {1}
    assert parser.get_matches("rating", ">=4", candidates) == {1}
    assert parser.get_matches("flag", "true", candidates) == {1}
    assert parser.get_matches("tags", "#>=2", candidates) == {1}
    assert parser.get_matches("identifiers", "asin:ABC", candidates) == {1}
    assert parser.get_matches("isbn", "123", candidates) == {1}
    assert parser.get_matches("id", ">=2", candidates) == {2, 3}
    assert parser.get_matches("all", "foundation", candidates) == {2}
    assert parser.get_matches("missing", "anything", candidates) == set()
    assert parser.get_matches("title", " ", candidates) == set()
    assert candidates == {1, 2, 3}


def test_parser_grouped_searches_support_alias_inversion_and_recursion_guards() -> None:
    """
    Verify parser grouped searches support alias inversion and recursion guards.

    Example:
        Exercise test parser grouped searches support alias inversion and recursion guards through its owning regression module::

            python -m pytest -q tests/catalog/test_search_core.py


    :return: None; the function records state or raises through its assertions.
    """
    db = _CatalogSearchDatabase()
    db.field_metadata.aliases.update(
        {
            "group": ["title", "tags"],
            "tag_group": ["tags"],
        }
    )
    db.preferences["grouped_search_terms"] = {
        "group": ["title", "tags"],
        "tag_group": ["tags"],
    }
    search = Search(
        None,
        "saved",
        all_search_locations=_CATALOG_SEARCH_LOCATIONS
        + ("group", "tag_group", "@group", "@tag_group"),
    )
    parser = search.create_parser(db)
    parser.all_book_ids = {1, 2, 3}

    assert parser.get_matches("title", "dune") == {1}
    assert parser.get_matches("group", "dune", {1, 2, 3}) == {1}
    assert parser.get_matches("group", "true", {1, 2, 3}) == {1, 2, 3}
    assert parser.get_matches("@tag_group", "false", {1, 2, 3}) == {3}
    with pytest.raises(ParseException, match="Recursive query group"):
        parser.get_matches(
            "group",
            "dune",
            {1, 2, 3},
            allow_recursion=False,
        )


def test_parser_restricted_all_searches_only_configured_fields() -> None:
    """
    Verify parser restricted all searches only configured fields.

    Example:
        Exercise test parser restricted all searches only configured fields through its owning regression module::

            python -m pytest -q tests/catalog/test_search_core.py


    :return: None; the function records state or raises through its assertions.
    """
    db = _CatalogSearchDatabase()
    search = Search(
        None,
        "saved",
        all_search_locations=_CATALOG_SEARCH_LOCATIONS,
    )
    parser = search.create_parser(db)
    parser.all_book_ids = {1, 2, 3}
    parser.limit_search_columns = True
    parser.limit_search_columns_to = (
        " title ",
        "tags",
        "all",
        "",
        "missing",
    )

    assert parser.get_matches("all", "true", {1, 2, 3}) == {1, 2, 3}
    assert parser.get_matches("all", "dune", {1, 2, 3}) == {1}

    parser.limit_search_columns_to = ("all", "", "missing")
    assert parser.get_matches("all", "foundation", {1, 2, 3}) == {2}


def test_parser_user_categories_include_subcategories_and_invert_membership() -> None:
    """
    Verify parser user categories include subcategories and invert membership.

    Example:
        Exercise test parser user categories include subcategories and invert membership through its owning regression module::

            python -m pytest -q tests/catalog/test_search_core.py


    :return: None; the function records state or raises through its assertions.
    """
    db = _CatalogSearchDatabase()
    db.preferences["user_categories"] = {
        "favorites": [("Dune", "title", False)],
        "favorites.classics": [("classic", "tags", False)],
        "other": [("Foundation", "title", False)],
    }
    search = Search(
        None,
        "saved",
        all_search_locations=_CATALOG_SEARCH_LOCATIONS + ("@favorites",),
    )
    parser = search.create_parser(db)
    parser.all_book_ids = {1, 2, 3}

    assert parser.get_matches("@favorites", "x", {1, 2, 3}) == set()
    assert parser.get_matches("@favorites", "true", {1, 2, 3}) == {1}
    assert parser.get_matches("@favorites", ".true", {1, 2, 3}) == {1, 2}
    assert parser.get_matches("@favorites", "false", {1, 2, 3}) == {2, 3}


def test_parser_language_alias_and_generic_value_matching(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Verify parser language alias and generic value matching.

    Example:
        Exercise test parser language alias and generic value matching through its owning regression module::

            python -m pytest -q tests/catalog/test_search_core.py


    :param monkeypatch: Value supplied for monkeypatch under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
    db = _CatalogSearchDatabase()
    search = Search(
        None,
        "saved",
        all_search_locations=_CATALOG_SEARCH_LOCATIONS,
    )
    parser = search.create_parser(db)
    parser.all_book_ids = {1, 2, 3}
    candidates = {1, 2, 3}

    monkeypatch.setattr(search_module, "canonicalize_lang", lambda _query: "eng")
    assert parser.get_matches("languages", "English", candidates) == {1}

    monkeypatch.setattr(search_module, "canonicalize_lang", lambda _query: None)
    monkeypatch.setattr(
        search_module,
        "lang_map",
        lambda: {"eng": "English", "fra": "French", "deu": "German"},
    )
    assert parser.get_matches("languages", "English", candidates) == {1}

    assert parser.get_matches("cover", "true", candidates) == {3}
    assert parser.get_matches("cover", "false", candidates) == {1, 2}
    assert parser.get_matches("cover", "present", candidates) == set()
    assert parser.get_matches("notes", "memo", candidates) == {2}
    assert parser.get_matches("all", "4", candidates) == {1, 2, 3}


def test_parser_field_iteration_uses_real_and_virtual_fields() -> None:
    """
    Verify parser field iteration uses real and virtual fields.

    Example:
        Exercise test parser field iteration uses real and virtual fields through its owning regression module::

            python -m pytest -q tests/catalog/test_search_core.py


    :return: None; the function records state or raises through its assertions.
    """
    db = _CatalogSearchDatabase()
    virtual = _SearchField([("marked", {3})])
    parser = Parser(
        db,
        {1, 2, 3},
        {},
        object(),
        object(),
        object(),
        object(),
        False,
        (),
        ("title", "virtual"),
        {"virtual": virtual},
        lambda _name: None,
        LRUCache(),
    )

    assert parser.field_metadata is db.field_metadata
    assert parser.universal_set() == {1, 2, 3}
    assert list(parser.field_iter("title", {1})) == [("Dune", {1})]
    assert list(parser.field_iter("virtual", {3})) == [("marked", {3})]
    assert parser.virtual_field_used is True
    assert list(parser.iter_searchable_values()) == []

    marked_parser = Parser(
        db,
        {1, 2, 3},
        {},
        object(),
        object(),
        object(),
        object(),
        False,
        (),
        ("marked",),
        {"marked": virtual},
        lambda _name: None,
        LRUCache(),
    )
    assert marked_parser.virtual_fields["marked"] is virtual


def test_search_call_uses_real_parser_and_detaches_it_afterward(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Verify search call uses real parser and detaches it afterward.

    Example:
        Exercise test search call uses real parser and detaches it afterward through its owning regression module::

            python -m pytest -q tests/catalog/test_search_core.py


    :param monkeypatch: Value supplied for monkeypatch under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
    db = _CatalogSearchDatabase()
    search = Search(None, "saved", all_search_locations=("title",))
    parser = search.create_parser(db)
    monkeypatch.setattr(search, "create_parser", lambda *_args, **_kwargs: parser)

    assert search(db, "title:dune", "") == {1}
    assert parser.dbcache is None
    assert parser.lookup_saved_search is None


def test_search_update_or_clear_and_update_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Verify search update or clear and update cleanup.

    Example:
        Exercise test search update or clear and update cleanup through its owning regression module::

            python -m pytest -q tests/catalog/test_search_core.py


    :param monkeypatch: Value supplied for monkeypatch under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
    search = Search(None, "saved")
    search.cache.add("query", {1})
    calls: list[tuple[str, Any]] = []
    monkeypatch.setattr(
        search,
        "update_caches",
        lambda dbcache, book_ids: calls.append(("update", (dbcache, book_ids))),
    )
    monkeypatch.setattr(
        search,
        "clear_caches",
        lambda: calls.append(("clear", None)),
    )

    search.update_or_clear("db", {1})
    search.update_or_clear("db", None)
    assert calls == [("update", ("db", {1})), ("clear", None)]
    assert search.get_saved_searches() is search.saved_searches

    cleanup_search = Search(None, "saved")
    cleanup_search.cache.add("query", {1})
    parser = _ParserStub({"query": {2}})
    monkeypatch.setattr(cleanup_search, "create_parser", lambda _dbcache: parser)
    cleanup_search.update_caches("db", {1, 2})
    assert parser.dbcache is None
    assert parser.lookup_saved_search is None


def test_search_caches_full_library_queries_and_decodes_bytes() -> None:
    """
    Verify search caches full library queries and decodes bytes.

    Example:
        Exercise test search caches full library queries and decodes bytes through its owning regression module::

            python -m pytest -q tests/catalog/test_search_core.py


    :return: None; the function records state or raises through its assertions.
    """
    search = Search(None, "saved")
    db = _SearchDatabase({1, 2, 3})
    parser = _ParserStub({"title:test": {1, 3}})

    result = search._do_search(parser, b"title:test", b"", db)
    assert result == {1, 3}
    assert search.cache.get("title:test") == {1, 3}

    parser.responses["title:test"] = {2}
    assert search._do_search(parser, "title:test", "", db) == {1, 3}
    assert parser.calls == [("title:test", {1, 2, 3})]

    whitespace_restriction_parser = _ParserStub({})
    assert search._do_search(
        whitespace_restriction_parser,
        "title:test",
        " ",
        db,
    ) == {1, 3}
    assert whitespace_restriction_parser.calls == []


def test_search_applies_and_caches_restrictions() -> None:
    """
    Verify search applies and caches restrictions.

    Example:
        Exercise test search applies and caches restrictions through its owning regression module::

            python -m pytest -q tests/catalog/test_search_core.py


    :return: None; the function records state or raises through its assertions.
    """
    search = Search(None, "saved")
    db = _SearchDatabase({1, 2, 3})
    parser = _ParserStub(
        {
            "tag:kept": {1, 2},
            "title:test": {2, 3},
        }
    )

    assert search._do_search(parser, "title:test", "tag:kept", db) == {2}
    assert search.cache.get("tag:kept") == {1, 2}
    assert "title:test" not in search.cache
    assert parser.calls == [
        ("tag:kept", {1, 2, 3}),
        ("title:test", {1, 2}),
    ]

    cached_parser = _ParserStub({})
    assert search._do_search(
        cached_parser,
        "",
        "tag:kept",
        db,
        book_ids={2, 3},
    ) == {2}
    assert cached_parser.calls == []

    no_subset_parser = _ParserStub({})
    assert search._do_search(
        no_subset_parser,
        "",
        "tag:kept",
        db,
    ) == {1, 2}
    assert no_subset_parser.calls == []

    subset_search = Search(None, "saved")
    subset_parser = _ParserStub({"tag:kept": {1, 2}})
    assert subset_search._do_search(
        subset_parser,
        "",
        "tag:kept",
        db,
        book_ids={2, 3},
    ) == {2}
    assert subset_search.cache.get("tag:kept") is None


def test_search_does_not_cache_subset_or_virtual_field_results() -> None:
    """
    Verify search does not cache subset or virtual field results.

    Example:
        Exercise test search does not cache subset or virtual field results through its owning regression module::

            python -m pytest -q tests/catalog/test_search_core.py


    :return: None; the function records state or raises through its assertions.
    """
    search = Search(None, "saved")
    db = _SearchDatabase({1, 2, 3})
    subset_parser = _ParserStub({"title:test": {1, 2}})

    assert search._do_search(
        subset_parser,
        "title:test",
        "",
        db,
        book_ids={2, 3},
    ) == {2}
    assert "title:test" not in search.cache

    virtual_parser = _ParserStub({"virtual:test": {3}})
    assert search._do_search(virtual_parser, "virtual:test", "", db) == {3}
    assert "virtual:test" not in search.cache


def test_search_cache_maintenance_updates_discards_and_removes_bad_queries() -> None:
    """
    Verify search cache maintenance updates discards and removes bad queries.

    Example:
        Exercise test search cache maintenance updates discards and removes bad queries through its owning regression module::

            python -m pytest -q tests/catalog/test_search_core.py


    :return: None; the function records state or raises through its assertions.
    """
    search = Search(None, "saved")
    search.cache.add("good", {1, 3, 4})
    search.cache.add("broken", {1})
    parser = _ParserStub(
        {
            "good": {2},
            "broken": ParseException("invalid saved query"),
        }
    )

    search._update_caches(parser, {1, 2})
    assert search.cache["good"] == {2, 3, 4}
    assert "broken" not in search.cache

    search.discard_books({3, 99})
    assert search.cache["good"] == {2, 4}


def test_search_location_changes_clear_both_caches() -> None:
    """
    Verify search location changes clear both caches.

    Example:
        Exercise test search location changes clear both caches through its owning regression module::

            python -m pytest -q tests/catalog/test_search_core.py


    :return: None; the function records state or raises through its assertions.
    """
    search = Search(None, "saved", all_search_locations=("title",))
    search.cache.add("query", {1})
    search.parse_cache.add("parsed", object())

    search.change_locations(("title",))
    assert len(search.cache) == 1
    assert len(search.parse_cache) == 1

    search.change_locations(("authors",))
    assert len(search.cache) == 0
    assert len(search.parse_cache) == 0
    assert search.all_search_locations == ("authors",)


def test_populate_all_locations_replaces_all_and_flattens_columns() -> None:
    """
    Verify populate all locations replaces all and flattens columns.

    Example:
        Exercise test populate all locations replaces all and flattens columns through its owning regression module::

            python -m pytest -q tests/catalog/test_search_core.py


    :return: None; the function records state or raises through its assertions.
    """
    locations = Search.populate_all_locations(
        {
            "all": ("stale",),
            "title": "work_title",
            "agents": ("agent_name", "agent_sort"),
        }
    )

    assert locations["all"] == ("agent_name", "agent_sort", "work_title")
    assert Search.populate_all_locations({"numeric": 7})["all"] == (7,)
