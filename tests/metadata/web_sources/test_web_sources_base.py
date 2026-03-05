from __future__ import annotations

from LiuXin_alpha.metadata.utils import calibreMetaInformation


def test_web_sources_base_import_smoke() -> None:
    import LiuXin_alpha.metadata.web_sources.base as base

    assert base is not None


def test_cleanup_title_and_fixauthors_behavior() -> None:
    from LiuXin_alpha.metadata.web_sources.base import cleanup_title, fixauthors

    assert cleanup_title("The  Book (Omnibus)") == "book"
    assert fixauthors(["mcdonald", "j.k."]) == ["McDonald", "J. K."]


def test_source_cache_roundtrip_and_job_split() -> None:
    from LiuXin_alpha.metadata.web_sources.base import Source

    source = Source()
    source.cache_isbn_to_identifier("9780306406157", "id-1")
    source.cache_identifier_to_cover_url("id-1", "https://example.invalid/cover.jpg")

    assert source.cached_isbn_to_identifier("9780306406157") == "id-1"
    assert source.cached_identifier_to_cover_url("id-1") == "https://example.invalid/cover.jpg"
    assert list(source.get_related_isbns("id-1")) == ["9780306406157"]

    groups = source.split_jobs([1, 2, 3, 4, 5], 2)
    assert len(groups) == 2
    assert sorted([x for g in groups for x in g]) == [1, 2, 3, 4, 5]


def test_source_identify_results_keygen_prefers_exact_title() -> None:
    from LiuXin_alpha.metadata.web_sources.base import Source

    source = Source()
    exact = calibreMetaInformation("Exact Match", ["Author"])
    fuzzy = calibreMetaInformation("Completely Different", ["Author"])

    keygen = source.identify_results_keygen(title="Exact Match", authors=["Author"], identifiers={})
    ordered = sorted([fuzzy, exact], key=keygen)

    assert ordered[0].title == "Exact Match"


def test_random_user_agent_can_rotate_with_env(monkeypatch) -> None:
    import LiuXin_alpha.metadata.web_sources.base as base

    monkeypatch.setenv("LIUXIN_WEB_SOURCES_RANDOM_UA", "1")
    monkeypatch.setattr(base.random, "choice", lambda seq: seq[-1])
    assert base.random_user_agent() == base.random_user_agent(index=2)


def test_source_browser_adds_rich_headers_when_enabled(monkeypatch) -> None:
    from LiuXin_alpha.metadata.web_sources.base import Source

    monkeypatch.setenv("LIUXIN_WEB_SOURCES_RICH_HEADERS", "1")
    source = Source()
    br = source.browser()
    headers = {k: v for k, v in br.addheaders}

    assert "User-Agent" in headers
    assert headers.get("Accept-Language")
    assert headers.get("DNT") == "1"
    assert headers.get("Upgrade-Insecure-Requests") == "1"


def test_source_browser_rotates_user_agent_when_enabled(monkeypatch) -> None:
    import LiuXin_alpha.metadata.web_sources.base as base

    monkeypatch.setenv("LIUXIN_WEB_SOURCES_RANDOM_UA", "1")

    class _SeqChoice:
        def __init__(self):
            self.idx = 0

        def __call__(self, seq):
            item = seq[self.idx % len(seq)]
            self.idx += 1
            return item

    monkeypatch.setattr(base.random, "choice", _SeqChoice())

    source = base.Source()
    first = {k: v for k, v in source.browser().addheaders}.get("User-Agent")
    second = {k: v for k, v in source.browser().addheaders}.get("User-Agent")

    assert first is not None and second is not None
    assert first != second
