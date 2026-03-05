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
