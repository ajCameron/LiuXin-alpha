"""Independent query algebra, snapshot, identity and concurrency regressions."""

import random
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

import pytest

from LiuXin_alpha.metadata.calibre_conversion import CalibreBookRef
from LiuXin_alpha.utils.calibre_compat.caches.search.adapter import (
    CalibreSearchAdapter,
    SearchDefinitions,
    TemplateExecutionWarning,
    TrustedTemplateExecution,
)
from LiuXin_alpha.utils.calibre_compat.caches.search.upstream_parser import (
    ParseException,
)


@pytest.mark.parametrize("seed", range(4))
@pytest.mark.parametrize("cache_size", (0, 3))
def test_random_search_algebra_with_restrictions(seed, cache_size):
    rng = random.Random(seed)
    rows = {
        CalibreBookRef(i * 17): {
            "title": f"Book {i}",
            "rating": 2 * (i % 6),
            "tags": ("fiction", "space") if i % 2 else ("history",),
            "#score": i % 7,
            "#read": (True, False, None)[i % 3],
        }
        for i in range(1, 65)
    }
    adapter = CalibreSearchAdapter(
        rows,
        fields={"#score": {"datatype": "int"}, "#read": {"datatype": "bool"}},
        plan_cache_size=cache_size,
        result_cache_size=cache_size,
    )
    terms = [
        ("tags:fiction", lambda r: "fiction" in r["tags"]),
        ("rating:>=3", lambda r: r["rating"] >= 6),
        ("#score:>4", lambda r: r["#score"] > 4),
        ("#read:_yes", lambda r: r["#read"] is True),
        ("#read:_no", lambda r: r["#read"] is False),
        ("#read:_empty", lambda r: r["#read"] is None),
        ("tags:#=2", lambda r: len(r["tags"]) == 2),
        ('title:"~[135]$"', lambda r: r["title"][-1] in "135"),
    ]
    for case in range(100):
        a, b, c = (rng.choice(terms) for _ in range(3))
        selected = frozenset(ref for ref in rows if rng.random() < 0.7)
        query = f"({a[0]} or {b[0]}) and not ({c[0]})"
        restriction, accepts = rng.choice(terms)
        expected = frozenset(
            ref
            for ref, row in rows.items()
            if ref in selected
            and accepts(row)
            and (a[1](row) or b[1](row))
            and not c[1](row)
        )
        for _ in range(2):
            assert (
                adapter.search(query, restriction, references=selected) == expected
            ), (seed, case, query)
        assert len(adapter._plans) <= cache_size and len(adapter._results) <= cache_size


def test_duplicate_expression_contexts_keep_distinct_identity():
    refs = (
        CalibreBookRef(700, work_id=10),
        CalibreBookRef(700, work_id=20),
        CalibreBookRef(900),
    )
    adapter = CalibreSearchAdapter(
        dict(
            zip(
                refs,
                (
                    {"title": "One", "sort": "One"},
                    {"title": "Two", "sort": "Two"},
                    {"title": "Other", "sort": "Other"},
                ),
                strict=True,
            )
        )
    )
    assert adapter.search("id:700") == set(refs[:2])
    assert adapter.search("id:700", references=(refs[1],)) == {refs[1]}
    assert adapter.search("title:two") == {refs[1]}
    assert adapter.multisort([]) == refs
    assert adapter.search("", references=()) == set()


def test_provider_values_are_copied_into_snapshot():
    ref = CalibreBookRef(1)
    values = {ref: ["fiction"]}
    adapter = CalibreSearchAdapter({ref: {"title": "One"}}, providers={"tags": values})
    assert adapter.search("tags:fiction") == {ref}
    values[ref][:] = ["history"]
    # Use a cold expression so a result hit cannot hide a mutated snapshot.
    assert adapter.search("tags:history") == set()
    assert adapter.search("tags:=fiction") == {ref}


def test_public_field_values_cannot_mutate_search_snapshot():
    ref = CalibreBookRef(1)
    adapter = CalibreSearchAdapter(
        {ref: {"tags": ["fiction"], "identifiers": {"isbn": "123"}}}
    )
    assert adapter.search("tags:fiction") == {ref}
    adapter.fields["tags"].value(1).append("history")
    adapter.fields["identifiers"].book_value_map[1]["isbn"] = "456"
    assert adapter.search("tags:history") == set()
    assert adapter.search("isbn:123") == {ref}
    assert adapter.search("isbn:456") == set()


def test_casefold_alias_collisions_are_rejected():
    with pytest.raises(ValueError, match="Ambiguous"):
        CalibreSearchAdapter(
            {},
            fields={
                "#a": {"search_terms": ["Code"]},
                "#b": {"search_terms": ["code"]},
            },
        )


def test_inferred_field_names_are_case_insensitive_and_unambiguous():
    ref = CalibreBookRef(1)
    adapter = CalibreSearchAdapter({ref: {"#Rank": 5}})
    assert adapter.search("#rank:>2") == {ref}
    assert adapter.search("#RANK:>2") == {ref}
    with pytest.raises(ValueError, match="Ambiguous"):
        CalibreSearchAdapter({ref: {"#Rank": 5, "#rank": 3}})


@pytest.mark.parametrize("field", ("series", "#reading_order"))
def test_series_sort_preserves_zero_index(field):
    refs = tuple(CalibreBookRef(i) for i in (30, 20, 10))
    adapter = CalibreSearchAdapter(
        {
            ref: {field: "A series", field + "_index": index}
            for ref, index in zip(refs, (0.0, 0.5, None), strict=True)
        },
        fields={field: {"datatype": "series"}},
    )
    assert adapter.multisort([(field, True)]) == refs
    assert adapter.multisort([(field, False)]) == tuple(reversed(refs))


@pytest.mark.parametrize(
    "kind,definitions,query",
    (
        (
            "saved",
            {"saved_searches": {"A": "vl:B"}, "virtual_libraries": {"B": "search:A"}},
            "search:A",
        ),
        ("group", {"grouped_search_terms": {"a": ["b"], "b": ["a"]}}, "a:true"),
        (
            "category",
            {"user_categories": {"a": [["x", "@b", 0]], "b": [["y", "@a", 0]]}},
            "@a:true",
        ),
    ),
)
def test_transitive_cycles_are_rejected_even_for_empty_candidates(
    kind, definitions, query
):
    adapter = CalibreSearchAdapter({}, definitions=definitions)
    for _ in range(2):
        with pytest.raises(ParseException, match="Recursive"):
            adapter.search("title:absent and " + query, references=())


def test_definition_edits_and_deletes_never_reuse_stale_results():
    prefs = {}
    definitions = SearchDefinitions(prefs)
    ref = CalibreBookRef(1)
    adapter = CalibreSearchAdapter({ref: {"title": "One"}}, definitions=definitions)
    for value, expected in (
        ("title:one", {ref}),
        ("title:absent", set()),
        ("title:one", {ref}),
    ):
        definitions.set("saved_searches", "Selected", value)
        assert adapter.search("search:selected") == expected
        assert adapter.search("search:Selected") == expected
    definitions.delete("saved_searches", "SELECTED")
    with pytest.raises(ParseException, match="Unknown"):
        adapter.search("search:selected")


def test_concurrent_queries_keep_clocks_and_candidates_isolated():
    today = datetime(2026, 11, 17, tzinfo=UTC)
    yesterday = datetime(2026, 11, 16, tzinfo=UTC)
    refs = (CalibreBookRef(1), CalibreBookRef(2))
    adapters = [
        CalibreSearchAdapter(
            {
                refs[0]: {"title": "One", "pubdate": today},
                refs[1]: {"title": "Two", "pubdate": yesterday},
            }
        )
        for _ in range(4)
    ]

    def run(i):
        now, expected = (today, refs[0]) if i % 2 else (yesterday, refs[1])
        return adapters[i % 4].search("pubdate:today", now=now), {expected}

    with ThreadPoolExecutor(max_workers=8) as pool:
        for actual, expected in pool.map(run, range(120)):
            assert actual == expected


def test_shared_template_capability_can_only_authorize_one_concurrent_request():
    ref = CalibreBookRef(1)
    adapters = [CalibreSearchAdapter({ref: {"title": "One"}}) for _ in range(8)]
    permission = TrustedTemplateExecution(
        trusted_local=True, acknowledge_arbitrary_code_execution=True
    )

    def run(adapter):
        try:
            return adapter.search(
                'template:"{title}#@#:t:=One"', template_execution=permission
            )
        except PermissionError:
            return None

    with (
        pytest.warns(TemplateExecutionWarning),
        ThreadPoolExecutor(max_workers=8) as pool,
    ):
        results = list(pool.map(run, adapters))
    assert results.count(frozenset({ref})) == 1
    assert results.count(None) == 7
    for adapter in adapters:
        assert not adapter._template_authorized
        with pytest.raises(PermissionError):
            adapter.search('template:"{title}#@#:t:=One"')


@pytest.mark.parametrize(
    "query",
    (
        "(",
        ")",
        "title:one and",
        "title:one or )",
        "search:Missing",
        'title:"~["',
        "#score:>oops",
        "pubdate:>not-a-date",
    ),
)
def test_invalid_queries_do_not_poison_later_requests(query):
    ref = CalibreBookRef(1)
    adapter = CalibreSearchAdapter(
        {
            ref: {
                "title": "One",
                "#score": 3,
                "pubdate": datetime(2026, 11, 17, tzinfo=UTC),
            }
        },
        fields={"#score": {"datatype": "int"}},
    )
    for _ in range(2):
        with pytest.raises(ParseException):
            adapter.search(query)
        assert adapter.search("title:one") == {ref}
        assert not adapter._active_queries
        assert not adapter._template_authorized


@pytest.mark.parametrize("ascending", (False, True))
def test_numeric_multisort_is_stable_and_keeps_missing_last(ascending):
    rng = random.Random(918)
    refs = [CalibreBookRef(i) for i in range(1, 101)]
    rng.shuffle(refs)
    rows = {
        ref: {
            "#rank": (None, 0, 1, 2)[ref.expression_id % 4],
            "#second": ref.expression_id % 7,
        }
        for ref in refs
    }
    adapter = CalibreSearchAdapter(
        rows, fields={"#rank": {"datatype": "int"}, "#second": {"datatype": "int"}}
    )
    expected = sorted(refs, key=lambda ref: ref.expression_id)
    expected.sort(key=lambda ref: rows[ref]["#second"], reverse=True)
    present = [ref for ref in expected if rows[ref]["#rank"] is not None]
    missing = [ref for ref in expected if rows[ref]["#rank"] is None]
    present.sort(key=lambda ref: rows[ref]["#rank"], reverse=not ascending)
    assert adapter.multisort([("#rank", ascending), ("#second", False)]) == tuple(
        present + missing
    )
