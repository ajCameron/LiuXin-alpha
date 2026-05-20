from __future__ import annotations

import queue
from datetime import datetime
from threading import Event


class _Log:
    def __init__(self) -> None:
        self.events = []

    def __call__(self, *parts):
        self.events.append(("call", parts))

    def info(self, *parts):
        self.events.append(("info", parts))

    def warning(self, *parts):
        self.events.append(("warning", parts))

    def error(self, *parts):
        self.events.append(("error", parts))

    def exception(self, *parts):
        self.events.append(("exception", parts))


def _claim(value):
    return {"mainsnak": {"datavalue": {"value": value}}}


def _entity_value(qid: str) -> dict:
    return {"entity-type": "item", "numeric-id": int(qid[1:]), "id": qid}


def _sample_entity(qid: str = "Q123") -> dict:
    return {
        "id": qid,
        "labels": {"en": {"language": "en", "value": "Sample Book"}},
        "descriptions": {"en": {"language": "en", "value": "A useful Wikidata description."}},
        "claims": {
            "P31": [_claim(_entity_value("Q571"))],
            "P1476": [_claim({"text": "Sample Book", "language": "en"})],
            "P50": [_claim(_entity_value("Q42"))],
            "P123": [_claim(_entity_value("Q500"))],
            "P577": [_claim({"time": "+1937-09-21T00:00:00Z", "precision": 11})],
            "P407": [_claim(_entity_value("Q1860"))],
            "P136": [_claim(_entity_value("Q8261"))],
            "P921": [_claim(_entity_value("Q1"))],
            "P212": [_claim("9780306406157")],
            "P957": [_claim("0306406152")],
            "P1144": [_claim("2020 123456")],
            "P243": [_claim("ocn123-456")],
        },
    }


def _entities_payload(*entities: dict) -> dict:
    return {"entities": {entity["id"]: entity for entity in entities}}


def _label_payload() -> dict:
    return _entities_payload(
        {"id": "Q42", "labels": {"en": {"value": "Douglas Adams"}}},
        {"id": "Q500", "labels": {"en": {"value": "Example Press"}}},
        {"id": "Q8261", "labels": {"en": {"value": "novel"}}},
        {"id": "Q1", "labels": {"en": {"value": "universe"}}},
        {"id": "Q1860", "labels": {"en": {"value": "English"}}},
    )


def test_web_sources_wikidata_import_smoke() -> None:
    import LiuXin_alpha.metadata.web_sources.wikidata as wikidata

    assert wikidata is not None


def test_wikidata_helper_edges() -> None:
    import LiuXin_alpha.metadata.web_sources.wikidata as wd

    class BadString:
        def __str__(self):
            raise RuntimeError("bad")

    assert wd._as_text(b"hello") == "hello"
    assert wd._as_text(None) == ""
    assert wd._as_text(BadString()) == ""
    assert wd._first({"a": "b"}) == "b"
    assert wd._first(iter(["one"])) == "one"
    assert wd._as_list("x") == ["x"]
    assert wd._safe_isbn({"isbn13": ["9780306406157"]}) == "9780306406157"
    assert wd._normalize_qid("https://www.wikidata.org/wiki/q42") == "Q42"
    assert wd._normalize_qid("not-a-qid") is None
    assert wd._wikidata_id_from_identifiers({"wd": {"Q123"}}) == "Q123"
    assert wd._label_from_entity({"labels": {"mul": {"value": "Fallback"}}}) == "Fallback"
    assert wd._description_from_entity({"descriptions": {"en": {"value": "Desc"}}}) == "Desc"


def test_wikidata_url_and_query_builders() -> None:
    from LiuXin_alpha.metadata.web_sources.wikidata import Wikidata

    plugin = Wikidata()
    assert plugin.get_book_url({}) is None
    assert plugin.get_book_url({"wikidata": "Q123"}) == ("wikidata", "Q123", "https://www.wikidata.org/wiki/Q123")
    assert plugin.id_from_url("https://www.wikidata.org/wiki/Q123") == ("wikidata", "Q123")
    assert plugin.id_from_url("https://www.wikidata.org/entity/Q123") == ("wikidata", "Q123")
    assert plugin.id_from_url("https://example.org/wiki/Q123") is None

    assert plugin.create_query(identifiers={"qid": "Q123"})[0][0] == "entities"
    assert "wbgetentities" in plugin.create_query(identifiers={"qid": "Q123"})[0][1]
    assert plugin.create_query(identifiers={"isbn": "9780306406157"})[0][0] == "sparql"
    title_query = plugin.create_query(title="The Great Gatsby", authors=["F. Scott Fitzgerald"], identifiers={})
    assert title_query[0][0] == "search"
    assert "wbsearchentities" in title_query[0][1]
    assert "Great+Gatsby" in title_query[0][1]
    assert plugin.create_query(title=None, authors=None, identifiers={}) == []


def test_wikidata_payload_parsers() -> None:
    from LiuXin_alpha.metadata.web_sources.wikidata import Wikidata

    assert Wikidata._qids_from_search_payload({"search": [{"id": "Q1"}, {"id": "Q1"}, {"id": "Q2"}]}) == ["Q1", "Q2"]
    assert Wikidata._qids_from_search_payload([]) == []
    sparql_payload = {"results": {"bindings": [{"item": {"value": "http://www.wikidata.org/entity/Q123"}}]}}
    assert Wikidata._qids_from_sparql_payload(sparql_payload) == ["Q123"]
    assert Wikidata._qids_from_sparql_payload({}) == []
    entities = Wikidata._entities_from_payload({"entities": {"Q1": _sample_entity("Q1"), "Q2": {"missing": True}}})
    assert sorted(entities) == ["Q1"]
    labels = Wikidata._label_map_from_payload(_label_payload())
    assert labels["Q42"] == "Douglas Adams"


def test_wikidata_claim_and_date_helpers(monkeypatch) -> None:
    import LiuXin_alpha.metadata.web_sources.wikidata as wd

    monkeypatch.setattr(wd, "parse_only_date", lambda raw: datetime.fromisoformat({"1937-09-21": "1937-09-21", "1937-09": "1937-09-15", "1937": "1937-01-15"}[raw]))

    entity = _sample_entity()
    assert wd._entity_ids_from_claim(entity, "P50") == ["Q42"]
    assert wd._string_values_from_claim(entity, "P212") == ["9780306406157"]
    assert wd._best_monolingual_text(entity, "P1476") == "Sample Book"
    assert wd._wikidata_time_to_date({"time": "+1937-09-21T00:00:00Z", "precision": 11}) == datetime(1937, 9, 21)
    assert wd._wikidata_time_to_date({"time": "+1937-09-00T00:00:00Z", "precision": 10}) == datetime(1937, 9, 15)
    assert wd._wikidata_time_to_date({"time": "+1937-00-00T00:00:00Z", "precision": 9}) == datetime(1937, 1, 15)
    assert wd._wikidata_time_to_date({"time": "bad", "precision": 11}) is None


def test_wikidata_metadata_from_entity(monkeypatch) -> None:
    import LiuXin_alpha.metadata.web_sources.wikidata as wd

    plugin = wd.Wikidata()
    monkeypatch.setattr(wd, "parse_only_date", lambda raw: datetime(1937, 9, 21))
    label_map = {
        "Q42": "Douglas Adams",
        "Q500": "Example Press",
        "Q8261": "novel",
        "Q1": "universe",
        "Q1860": "English",
    }

    mi = plugin._metadata_from_entity(_sample_entity(), label_map=label_map, relevance=3)
    assert mi.source_relevance == 3
    assert mi.title == "Sample Book"
    assert mi.authors == ["Douglas Adams"]
    assert mi.comments == "A useful Wikidata description."
    assert mi.publisher == "Example Press"
    assert mi.pubdate == datetime(1937, 9, 21)
    assert mi.language == "en"
    assert mi.tags == ["novel", "universe"]
    assert mi.get_identifiers()["wikidata"] == "Q123"
    assert mi.get_identifiers()["isbn"] == "9780306406157"
    assert mi.get_identifiers()["lccn"] == "2020123456"
    assert mi.get_identifiers()["oclc"] == "123456"


def test_wikidata_metadata_fallbacks_and_postprocess() -> None:
    from LiuXin_alpha.metadata.web_sources.wikidata import Wikidata

    plugin = Wikidata()
    entity = {"id": "Q999", "labels": {"en": {"value": "Label Only"}}, "claims": {"P2093": [_claim("String Author")]}}
    mi = plugin._metadata_from_entity(entity)
    assert mi.title == "Label Only"
    assert mi.authors == ["String Author"]
    assert mi.get_identifiers()["wikidata"] == "Q999"
    assert Wikidata._entity_is_bookish(entity) is False

    mi = plugin._metadata_from_entity(_sample_entity())
    out = plugin._postprocess_downloaded_metadata(mi, relevance=7)
    assert out is mi
    assert out.source_relevance == 7
    assert plugin.cached_isbn_to_identifier("9780306406157") == "Q123"
    assert plugin._postprocess_downloaded_metadata(None) is None


def test_wikidata_request_failure_is_logged(monkeypatch) -> None:
    from LiuXin_alpha.metadata.web_sources.wikidata import Wikidata

    plugin = Wikidata()
    monkeypatch.setattr(plugin, "_request_json_with_backoff", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
    log = _Log()
    assert plugin._request_json_or_none(log, Event(), "https://www.wikidata.org/w/api.php", 1, "test") is None
    assert any(level == "warning" and "failed" in parts[0] for level, parts in log.events)


def test_wikidata_identify_direct_search_and_sparql(monkeypatch) -> None:
    from LiuXin_alpha.metadata.web_sources.wikidata import Wikidata

    plugin = Wikidata()
    calls = []

    def _request(log, abort, url, timeout, context):
        del log, abort, timeout
        calls.append((url, context))
        if context == "Wikidata entities":
            return _entities_payload(_sample_entity())
        if context == "Wikidata label lookup":
            return _label_payload()
        raise AssertionError(context)

    monkeypatch.setattr(plugin, "_request_json_or_none", _request)
    out = queue.Queue()
    plugin.identify(log=_Log(), result_queue=out, abort=Event(), identifiers={"wikidata": "Q123"})
    assert out.get_nowait().get_identifiers()["wikidata"] == "Q123"
    assert calls[0][1] == "Wikidata entities"

    plugin = Wikidata()
    calls = []

    def _search_request(log, abort, url, timeout, context):
        del log, abort, timeout
        calls.append((url, context))
        if context == "Wikidata search":
            return {"search": [{"id": "Q123"}]}
        if context == "Wikidata entity lookup":
            return _entities_payload(_sample_entity())
        if context == "Wikidata label lookup":
            return _label_payload()
        raise AssertionError(context)

    monkeypatch.setattr(plugin, "_request_json_or_none", _search_request)
    out = queue.Queue()
    plugin.identify(log=_Log(), result_queue=out, abort=Event(), title="Sample Book", authors=["Douglas Adams"])
    assert out.get_nowait().title == "Sample Book"
    assert calls[0][1] == "Wikidata search"

    plugin = Wikidata()
    calls = []

    def _sparql_request(log, abort, url, timeout, context):
        del log, abort, timeout
        calls.append((url, context))
        if context == "Wikidata sparql":
            return {"results": {"bindings": [{"item": {"value": "http://www.wikidata.org/entity/Q123"}}]}}
        if context == "Wikidata entity lookup":
            return _entities_payload(_sample_entity())
        if context == "Wikidata label lookup":
            return _label_payload()
        raise AssertionError(context)

    monkeypatch.setattr(plugin, "_request_json_or_none", _sparql_request)
    out = queue.Queue()
    plugin.identify(log=_Log(), result_queue=out, abort=Event(), identifiers={"isbn": "9780306406157"})
    assert out.get_nowait().get_identifiers()["isbn"] == "9780306406157"
    assert calls[0][1] == "Wikidata sparql"


def test_wikidata_identify_empty_abort_and_parse_failure(monkeypatch) -> None:
    from LiuXin_alpha.metadata.web_sources.wikidata import Wikidata

    plugin = Wikidata()
    called = {"request": False}
    monkeypatch.setattr(plugin, "_request_json_or_none", lambda **kwargs: called.__setitem__("request", True))
    abort = Event()
    abort.set()
    out = queue.Queue()
    plugin.identify(log=_Log(), result_queue=out, abort=abort, title="Title")
    assert called["request"] is False
    assert out.empty()

    plugin = Wikidata()
    out = queue.Queue()
    plugin.identify(log=_Log(), result_queue=out, abort=Event(), identifiers={})
    assert out.empty()

    plugin = Wikidata()
    monkeypatch.setattr(plugin, "_request_json_or_none", lambda *args, **kwargs: _entities_payload(_sample_entity()))
    monkeypatch.setattr(plugin, "_metadata_from_entity", lambda entity, label_map=None, relevance=0: (_ for _ in ()).throw(RuntimeError("bad parse")))
    log = _Log()
    out = queue.Queue()
    plugin.identify(log=log, result_queue=out, abort=Event(), identifiers={"wikidata": "Q123"})
    assert out.empty()
    assert any(level == "exception" for level, _parts in log.events)


def test_wikidata_imports_from_known_modules() -> None:
    from LiuXin_alpha.metadata.web_sources import import_web_source_module, iter_known_web_source_modules

    assert "wikidata" in iter_known_web_source_modules()
    mod = import_web_source_module("wikidata")
    assert hasattr(mod, "Wikidata")
