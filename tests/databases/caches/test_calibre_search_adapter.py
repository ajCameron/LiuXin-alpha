"""Pinned Calibre search examples, native identities and template boundaries."""

from datetime import UTC, datetime

import pytest

from LiuXin_alpha.caches.api import UnsupportedCacheQueryError
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


@pytest.fixture
def adapter():
    refs = [CalibreBookRef(i) for i in (1, 2, 3)]
    rows = [
        {
            "title": "Dune",
            "sort": "Dune",
            "authors": ("Frank Herbert",),
            "author_sort": "Herbert, Frank",
            "tags": ("Fiction", "Space"),
            "rating": 8,
            "pubdate": datetime(1965, 8, 1, tzinfo=UTC),
            "identifiers": {"isbn": "123"},
            "#read": True,
            "#score": 3.5,
        },
        {
            "title": "Café Noir",
            "sort": "Café Noir",
            "authors": ("Other",),
            "author_sort": "Other",
            "tags": ("Fiction",),
            "rating": 2,
            "pubdate": datetime(2020, 1, 1, tzinfo=UTC),
            "identifiers": {},
            "#read": False,
            "#score": 0.0,
        },
        {
            "title": "History",
            "sort": "History",
            "authors": (),
            "author_sort": None,
            "tags": (),
            "rating": None,
            "pubdate": None,
            "identifiers": {},
            "#read": None,
            "#score": None,
        },
    ]
    return CalibreSearchAdapter(
        dict(zip(refs, rows, strict=True)),
        fields={"#read": {"datatype": "bool"}, "#score": {"datatype": "float"}},
        settings={"bools_are_tristate": True},
    )


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("(title:dune or tags:fiction) and not rating:<3", {1}),
        ("author:herbert", {1}),
        ('title:"~^d.*e$"', {1}),
        ("title:=Dune", {1}),
        ("title:cafe", {2}),
        ("tags:#>=2", {1}),
        ("tags:false", {3}),
        ("identifiers:=isbn:=123", {1}),
        ("isbn:123", {1}),
        ("rating:>=4", {1}),
        ("pubdate:=1965", {1}),
        ("pubdate:false", {3}),
        ("#read:_yes", {1}),
        ("#read:_no", {2}),
        ("#read:_empty", {3}),
        ("#score:>3", {1}),
        ("#score:=0", {2}),
        ("Dune or History", {1, 3}),
        ("not (tags:space or title:cafe)", {3}),
    ],
)
def test_metadata_query_vectors(adapter, query, expected):
    assert adapter.search(query) == frozenset(CalibreBookRef(i) for i in expected)


def test_definitions_restrictions_and_recursion(adapter):
    prefs = {}
    definitions = SearchDefinitions(prefs)
    definitions.set("saved_searches", "Fiction", "tags:fiction")
    definitions.set("virtual_libraries", "Good", "rating:>=4")
    adapter._definitions = definitions
    assert adapter.search("search:Fiction and vl:Good") == {CalibreBookRef(1)}
    definitions.set("saved_searches", "Fiction", "title:history")
    assert adapter.search("search:fiction") == {CalibreBookRef(3)}
    assert adapter.search("", restriction="search:Fiction") == {CalibreBookRef(3)}
    definitions.set("saved_searches", "Loop", "search:Loop")
    with pytest.raises(ParseException, match="Recursive"):
        adapter.search("search:Loop")


def test_grouped_categories_and_missing_providers(adapter):
    adapter._definitions = {
        "grouped_search_terms": {"names": ["title", "authors"]},
        "user_categories": {"Chosen": [["Fiction", "tags", 0]]},
    }
    assert adapter.search("names:herbert") == {CalibreBookRef(1)}
    assert adapter.search("@Chosen:true") == {CalibreBookRef(1), CalibreBookRef(2)}
    with pytest.raises(UnsupportedCacheQueryError, match="ondevice"):
        adapter.search("ondevice:true")


def test_templates_are_transitively_gated_and_capabilities_are_one_use(adapter):
    query = 'template:"{title}#@#:t:=Dune"'
    adapter._definitions = {"saved_searches": {"hidden": query}}
    with pytest.raises(PermissionError, match="DANGER"):
        adapter.search("title:nonexistent and search:hidden")
    capability = TrustedTemplateExecution(
        trusted_local=True, acknowledge_arbitrary_code_execution=True
    )
    with pytest.warns(TemplateExecutionWarning, match="not a sandbox"):
        assert adapter.search("search:hidden", template_execution=capability) == {
            CalibreBookRef(1)
        }
    with pytest.raises(PermissionError, match="already been used"):
        adapter.search(query, template_execution=capability)
    capability = TrustedTemplateExecution(
        trusted_local=True, acknowledge_arbitrary_code_execution=True
    )
    with pytest.raises(PermissionError, match="Remote"):
        adapter.search(query, template_execution=capability, remote=True)


def test_projection_copy_sort_and_cache(adapter):
    assert adapter.multisort([("title", True)]) == (
        CalibreBookRef(2),
        CalibreBookRef(1),
        CalibreBookRef(3),
    )
    assert adapter.search("tags:fiction", references=(CalibreBookRef(2),)) == {
        CalibreBookRef(2)
    }
    adapter.search("title:dune")
    assert adapter._results
    assert adapter.search("title:dune") == {CalibreBookRef(1)}


def test_relative_date_uses_supplied_clock():
    ref = CalibreBookRef(1)
    adapter = CalibreSearchAdapter(
        {ref: {"pubdate": datetime(2026, 11, 17, tzinfo=UTC)}}
    )
    assert adapter.search("pubdate:today", now=datetime(2026, 11, 17, tzinfo=UTC)) == {
        ref
    }
    assert (
        adapter.search("pubdate:today", now=datetime(2026, 11, 18, tzinfo=UTC)) == set()
    )
    assert not adapter._results


def test_display_identity_is_not_internal_candidate_number():
    ref = CalibreBookRef(131, 3, 105)
    adapter = CalibreSearchAdapter({ref: {"id": 131, "title": "A"}})
    assert adapter.search("id:131") == {ref}
    assert adapter.search("id:1") == set()


def test_current_template_language_and_composite_recursion():
    ref = CalibreBookRef(13)
    fields = {
        "#computed": {
            "datatype": "composite",
            "display": {
                "composite_template": 'program: if field("title") == "Dune" then "yes" else "no" fi'
            },
        }
    }
    adapter = CalibreSearchAdapter({ref: {"title": "Dune"}}, fields=fields)
    with pytest.raises(PermissionError):
        adapter.search("#computed:yes")
    cap = TrustedTemplateExecution(
        trusted_local=True, acknowledge_arbitrary_code_execution=True
    )
    with pytest.warns(TemplateExecutionWarning):
        assert adapter.search("#computed:yes", template_execution=cap) == {ref}
    fields["#computed"]["display"]["composite_template"] = "{#computed}"
    cyclic = CalibreSearchAdapter({ref: {"title": "Dune"}}, fields=fields)
    cap = TrustedTemplateExecution(
        trusted_local=True, acknowledge_arbitrary_code_execution=True
    )
    with (
        pytest.warns(TemplateExecutionWarning),
        pytest.raises(ParseException, match="Recursive composite"),
    ):
        cyclic.search("#computed:true", template_execution=cap)


def test_current_template_builtins_and_metadata_queries(adapter):
    for template, expected in (
        ('program: list_count("one,two", ",")', {1, 2, 3}),
        ('program: book_count("tags:fiction", 0)', {1, 2, 3}),
    ):
        query = 'template:"""' + template + '#@#:n:=2"""'
        cap = TrustedTemplateExecution(
            trusted_local=True, acknowledge_arbitrary_code_execution=True
        )
        with pytest.warns(TemplateExecutionWarning):
            assert adapter.search(query, template_execution=cap) == {
                CalibreBookRef(i) for i in expected
            }
    with pytest.raises(PermissionError):
        adapter.search('template:"{title}#@#:t:Dune"')


def test_composite_sort_and_field_access_require_permission():
    refs = [CalibreBookRef(13), CalibreBookRef(14)]
    adapter = CalibreSearchAdapter(
        dict(zip(refs, ({"title": "10"}, {"title": "2"}), strict=True)),
        fields={
            "#computed": {
                "datatype": "composite",
                "display": {
                    "composite_template": "{title}",
                    "composite_sort": "number",
                },
            }
        },
    )
    with pytest.raises(PermissionError):
        adapter.multisort([("#computed", True)])
    with pytest.raises(PermissionError):
        adapter.fields["#computed"].value(1)
    cap = TrustedTemplateExecution(
        trusted_local=True, acknowledge_arbitrary_code_execution=True
    )
    with pytest.warns(TemplateExecutionWarning):
        assert adapter.multisort(
            [("#computed", True)], template_execution=cap
        ) == tuple(reversed(refs))
    with pytest.raises(PermissionError):
        adapter.fields["#computed"].value(1)

    from concurrent.futures import ThreadPoolExecutor, TimeoutError
    from threading import Event

    entered = Event()

    def external_read():
        entered.set()
        return adapter.fields["#computed"].value(1)

    with ThreadPoolExecutor(max_workers=1) as pool:
        with adapter._lock:
            # Model the permission state while a different thread owns a request.
            adapter._template_authorized = True
            try:
                future = pool.submit(external_read)
                assert entered.wait(2)
                with pytest.raises(TimeoutError):
                    future.result(timeout=0.1)
            finally:
                adapter._template_authorized = False
        with pytest.raises(PermissionError):
            future.result(timeout=2)


def test_injected_template_function_is_explicit_and_gated():
    from LiuXin_alpha.utils.calibre_compat.caches.search.upstream_functions import (
        BuiltinFormatterFunction,
    )

    calls = []

    class Function(BuiltinFormatterFunction):
        name = "application_value"
        arg_count = 0

        def evaluate(self, formatter, kwargs, mi, locals, *args):
            calls.append(mi.title)
            return "trusted value"

    ref = CalibreBookRef(13)
    adapter = CalibreSearchAdapter(
        {ref: {"title": "A"}}, template_functions={"application_value": Function()}
    )
    query = 'template:"program: application_value()#@#:t:trusted"'
    with pytest.raises(PermissionError):
        adapter.search("title:impossible and " + query)
    assert not calls
    cap = TrustedTemplateExecution(
        trusted_local=True, acknowledge_arbitrary_code_execution=True
    )
    with pytest.warns(TemplateExecutionWarning):
        assert adapter.search(query, template_execution=cap) == {ref}
    assert calls == ["A"]
    nested = (
        'template:"""program: template("program: application_value()")#@#:t:trusted"""'
    )
    cap = TrustedTemplateExecution(
        trusted_local=True, acknowledge_arbitrary_code_execution=True
    )
    with pytest.warns(TemplateExecutionWarning):
        assert adapter.search(nested, template_execution=cap) == {ref}
    assert calls == ["A", "A"]


def test_input_settings_and_descriptors_cannot_change_cached_answers():
    ref = CalibreBookRef(13)
    rows = {ref: {"title": "Dune"}}
    settings = {"case_sensitive": False}
    adapter = CalibreSearchAdapter(rows, settings=settings)
    assert adapter.search("title:DUNE") == {ref}
    rows[ref]["title"] = "Changed"
    settings["case_sensitive"] = True
    adapter.field_metadata["title"]["datatype"] = "int"
    assert adapter.search("title:DUNE") == {ref}


def test_copied_sources_keep_licence_and_modification_records():
    import json
    from pathlib import Path

    import LiuXin_alpha.utils.calibre_compat.caches.search.adapter as implementation

    directory = Path(implementation.__file__).parent
    manifest = json.loads((directory / "provenance.json").read_text())
    assert manifest["licence"] == "GPL-3.0-only"
    assert manifest["revision"] == "5f53fce641f180cfaf0e976e703302d596e4fb2a"
    for source in manifest["files"]:
        header = (directory / source["local_file"]).read_text()[:1000]
        assert "Kovid Goyal" in header and "GPLv3" in header
        assert manifest["revision"] in header
        assert manifest["adapted_on"] in header
        assert len(source["upstream_sha256"]) == 64
