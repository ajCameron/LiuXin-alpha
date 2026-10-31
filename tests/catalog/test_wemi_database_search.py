"""Exercise semantic search across complete, partial and shared WEMI paths."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from LiuXin_alpha.catalog.api import CatalogNotFoundError, CatalogSearchAPI
from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.catalog.relationships import CatalogRelationships
from LiuXin_alpha.catalog.search.database_search import CatalogSearch
from LiuXin_alpha.utils.search_query_parser import ParseException


@pytest.fixture
def collection(db):
    catalog = Catalog(db)
    links = CatalogRelationships(db)
    novel = catalog.works.create({"title": "Searchfixture Novel"})
    intro = catalog.works.create({"title": "Searchfixture Introduction"})
    orphan = catalog.works.create({"title": "Searchfixture Unpublished"})
    original = catalog.expressions.create({"label": "Searchfixture Original"})
    translation = catalog.expressions.create({"label": "Searchfixture Translation"})
    introduction = catalog.expressions.create({"label": "Searchfixture Intro text"})
    for work, expression in (
        (novel, original),
        (novel, translation),
        (intro, introduction),
    ):
        links.link("works", work, "expressions", expression)
    omnibus = catalog.manifestations.create(
        {"edition_statement": "Searchfixture Omnibus"}
    )
    translated = catalog.manifestations.create(
        {"edition_statement": "Searchfixture Translated"}
    )
    for expression, manifestation in (
        (original, omnibus),
        (introduction, omnibus),
        (translation, translated),
    ):
        links.link("expressions", expression, "manifestations", manifestation)
    copy = catalog.items.create(
        {"manifestation_id": omnibus, "inventory_code": "SEARCH-COPY"}
    )
    other_copy = catalog.items.create(
        {"manifestation_id": translated, "inventory_code": "SEARCH-TRANSLATED"}
    )
    for name, work in (("Novel Author", novel), ("Introduction Author", intro)):
        agent = catalog.agents.create({"name": name})
        catalog.agents.link_to_wemi(
            agent_id=agent, level="work", entity_id=work, role="aut"
        )
    translator = catalog.agents.create({"name": "Search Translator"})
    catalog.agents.link_to_wemi(
        agent_id=translator, level="expression", entity_id=translation, role="trl"
    )
    tag = catalog.tags.create({"tag": "Searchfixture IntroOnly"})
    links.link("works", intro, "tags", tag)
    return SimpleNamespace(
        catalog=catalog,
        novel=novel,
        intro=intro,
        orphan=orphan,
        original=original,
        translation=translation,
        omnibus=omnibus,
        translated=translated,
        copy=copy,
        other_copy=other_copy,
    )


def ids(rows, level="work"):
    return {row[f"{level}_id"] for row in rows}


def test_construction_and_zero_limit_do_not_read_database():
    search = CatalogSearch(None, None, None)
    assert isinstance(search, CatalogSearchAPI)
    assert search.find("work.title:Example", limit=0) == ()


def test_result_limit_does_not_limit_initial_candidates():
    rows = tuple({"work_id": i, "work_title": "No match"} for i in range(105))
    rows += ({"work_id": 105, "work_title": "Late needle"},)
    macros = SimpleNamespace(get_rows=lambda table, **kwargs: rows)
    search = CatalogSearch(SimpleNamespace(macros=macros), None, None)
    assert ids(search.find("work.title:needle", limit=1)) == {105}


@pytest.mark.parametrize("query", ["title:", "title:=", "(", "title:example and"])
def test_bad_queries_fail_before_database_access(query):
    with pytest.raises(ParseException):
        CatalogSearch(None, None, None).find(query)


@pytest.mark.parametrize(
    "kwargs,error",
    [
        ({"level": "book"}, ValueError),
        ({"limit": -1}, ValueError),
        ({"offset": -1}, ValueError),
        ({"limit": True}, TypeError),
        ({"offset": None}, TypeError),
    ],
)
def test_bad_search_options_fail_before_database_access(kwargs, error):
    with pytest.raises(error):
        CatalogSearch(None, None, None).find("title:Example", **kwargs)


def test_all_paths_and_all_four_result_levels(collection):
    c = collection
    search = c.catalog.search
    assert ids(search.find('work.title:"Searchfixture Novel"')) == {c.novel}
    assert ids(
        search.find('work.title:"Searchfixture Novel"', level="expression"),
        "expression",
    ) == {c.original, c.translation}
    assert ids(
        search.find('work.title:"Searchfixture Novel"', level="manifestation"),
        "manifestation",
    ) == {c.omnibus, c.translated}
    assert ids(
        search.find('work.title:"Searchfixture Novel"', level="item"), "item"
    ) == {c.copy, c.other_copy}
    assert ids(
        search.find('work.title:"Searchfixture Introduction"', level="item"), "item"
    ) == {c.copy}
    assert ids(search.find('expression:"Searchfixture Translation"')) == {c.novel}
    assert ids(search.find('item:"SEARCH-COPY"')) == {c.novel, c.intro}


def test_shared_edition_does_not_leak_sibling_work_metadata(collection):
    c = collection
    search = c.catalog.search
    assert ids(search.find('author:"Introduction Author"')) == {c.intro}
    assert ids(search.find("work.tag:IntroOnly")) == {c.intro}
    assert search.find('work.title:Novel and author:"Introduction Author"') == ()
    assert ids(
        search.find('author:"Introduction Author"', level="manifestation"),
        "manifestation",
    ) == {c.omnibus}
    assert ids(search.find('translator:"Search Translator"')) == {c.novel}
    assert search.find('author:"Search Translator"') == ()


def test_boolean_queries_retain_partial_paths_and_literal_text(collection):
    c = collection
    search = c.catalog.search
    assert ids(search.find('work.title:Searchfixture and not item:"SEARCH"')) == {
        c.orphan
    }
    assert ids(
        search.find(
            "(work.title:Novel or work.title:Unpublished) and not work.tag:IntroOnly"
        )
    ) == {c.novel, c.orphan}
    assert ids(search.find('work.title:"=Searchfixture Novel"')) == {c.novel}
    assert search.find('work.title:"=Novel"') == ()
    title = "  Ｃａｆｅ\u0301   O'Reilly %_ : Draft  "
    special = c.catalog.works.create({"title": title})
    assert ids(search.find('work.title:"=Café O\'Reilly %_ : Draft"')) == {special}
    assert search.find('work.title:"\' OR 1=1 --"') == ()
    assert search.find('work.title:"%_missing"') == ()


def test_pagination_and_fresh_reads_do_not_drop_late_matches(db):
    catalog = Catalog(db)
    first = catalog.works.create({"title": "Searchpaging first"})
    second = catalog.works.create({"title": "Searchpaging second"})
    third = catalog.works.create({"title": "Searchpaging third"})
    assert ids(catalog.search.find("work.title:Searchpaging", limit=1, offset=1)) == {
        second
    }
    assert [
        row["work_id"]
        for row in catalog.search.find("work.title:Searchpaging", limit=None)
    ] == [first, second, third]
    catalog.works.update(third, {"title": "Searchpaging changed"})
    rows = catalog.search.find("work.title:changed")
    assert ids(rows) == {third}
    rows[0]["work_title"] = "Mutated result"
    assert catalog.works.require(third)["work_title"] == "Searchpaging changed"
    assert catalog.search.find("work.title:third") == ()


def test_attached_metadata_languages_and_item_identifiers(collection):
    c = collection
    language = c.catalog.languages.exact("French").entity_id
    c.catalog.expressions.update(c.translation, {"language_id": language})
    c.catalog.notes.add_for_wemi(
        level="work", entity_id=c.orphan, data={"text": "Searchfixture private note"}
    )
    c.catalog.item_identifiers.create(
        {"item_id": c.copy, "scheme": "isbn13", "value": "9780306406157"}
    )
    assert ids(c.catalog.search.find("language:French")) == {c.novel}
    assert ids(c.catalog.search.find('note:"Searchfixture private"')) == {c.orphan}
    assert ids(c.catalog.search.find("item.identifier:9780306406157")) == {
        c.novel,
        c.intro,
    }
    assert ids(c.catalog.search.find('"Searchfixture private"')) == {c.orphan}


def test_search_is_read_only_and_propagates_read_errors(collection, monkeypatch):
    c = collection

    def forbidden_write(*args, **kwargs):
        raise AssertionError("Search attempted a write")

    for method in ("insert_row", "update_row", "delete_row"):
        monkeypatch.setattr(c.catalog.db.macros, method, forbidden_write)
    assert ids(c.catalog.search.find('author:"Novel Author"')) == {c.novel}

    def failed_read(*args, **kwargs):
        raise RuntimeError("Backend unavailable")

    monkeypatch.setattr(c.catalog.repositories.agents, "list_for_wemi", failed_read)
    with pytest.raises(RuntimeError, match="Backend unavailable"):
        c.catalog.search.find("author:anyone")

    original_get_row = c.catalog.db.macros.get_row

    def missing_tag(table, entity_id):
        return None if table == "tags" else original_get_row(table, entity_id)

    monkeypatch.setattr(c.catalog.db.macros, "get_row", missing_tag)
    with pytest.raises(CatalogNotFoundError, match="Missing linked tags row"):
        c.catalog.search.find("work.tag:IntroOnly")
