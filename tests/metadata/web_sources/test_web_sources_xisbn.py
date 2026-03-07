from __future__ import annotations

import json


def test_web_sources_xisbn_import_smoke() -> None:
    import LiuXin_alpha.metadata.web_sources.xisbn as xisbn_mod

    assert xisbn_mod is not None
    assert xisbn_mod.xisbn is not None


def test_xisbn_purify_strips_non_isbn_chars() -> None:
    from LiuXin_alpha.metadata.web_sources.xisbn import xISBN

    x = xISBN()
    assert x.purify(" 978-0-306-40615-7 ") == "9780306406157"
    assert x.purify("x-1 2.3") == "X123"
    assert x.purify(None) == ""


def test_xisbn_fetch_data_disabled_by_default() -> None:
    from LiuXin_alpha.metadata.web_sources.xisbn import xISBN

    x = xISBN()
    assert x.service_available is False
    assert x.fetch_data("9780306406157") == []


def test_xisbn_fetch_data_filters_non_book_forms(monkeypatch) -> None:
    from LiuXin_alpha.metadata.web_sources.xisbn import xISBN

    x = xISBN(enable_network=True)
    payload = {
        "stat": "ok",
        "list": [
            {"isbn": ["111"], "form": ["BA"], "year": "2000"},
            {"isbn": ["222"], "form": ["CD"], "year": "2001"},
            {"isbn": ["333"], "form": ["DA", "CD"], "year": "2002"},
        ],
    }
    monkeypatch.setattr(x, "_fetch_raw", lambda _isbn, timeout=20: json.dumps(payload).encode("utf-8"))

    data = x.fetch_data("9780306406157")
    assert len(data) == 2
    assert {rec["isbn"][0] for rec in data} == {"111", "333"}


def test_xisbn_get_data_caches_and_reuses_related_isbn_mapping(monkeypatch) -> None:
    from LiuXin_alpha.metadata.web_sources.xisbn import xISBN

    x = xISBN(enable_network=True)
    calls = {"count": 0}

    def _fetch_data(_isbn):
        calls["count"] += 1
        return [{"isbn": ["9780306406157", "0306406152"], "form": ["BA"], "year": "1980"}]

    monkeypatch.setattr(x, "fetch_data", _fetch_data)

    first = x.get_data("978-0-306-40615-7")
    second = x.get_data("0-306-40615-2")

    assert calls["count"] == 1
    assert first == second


def test_xisbn_pool_and_associated_isbns() -> None:
    from LiuXin_alpha.metadata.web_sources.xisbn import xISBN

    x = xISBN()
    x._data = [
        [
            {"isbn": ["9780306406157", "0306406152"], "year": "1980"},
            {"isbn": ["9781861972712"], "year": "2001"},
            {"isbn": ["bad"], "year": "unknown"},
        ]
    ]
    x._map = {"9780306406157": 0}

    associated = x.get_associated_isbns("9780306406157")
    assert associated == {"9780306406157", "0306406152", "9781861972712"}

    pool, min_year = x.get_isbn_pool("9780306406157")
    assert pool == frozenset({"9780306406157", "0306406152", "9781861972712"})
    assert min_year == 1980
