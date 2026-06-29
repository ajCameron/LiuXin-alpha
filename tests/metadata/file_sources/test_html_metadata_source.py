from __future__ import annotations

import io
from collections.abc import Mapping
from pathlib import Path

import pytest


def _values(raw):
    if raw is None:
        return []
    if isinstance(raw, Mapping):
        return list(raw.keys())
    if isinstance(raw, str):
        return [raw]
    try:
        return list(raw)
    except TypeError:
        return [raw]


def _first(raw):
    vals = _values(raw)
    return vals[0] if vals else None


def _series_index_for(md, series_name: str):
    raw = getattr(md, "series_index", None)
    if isinstance(raw, Mapping):
        return raw.get(series_name)
    return None


def test_html_metadata_module_import_smoke() -> None:
    import LiuXin_alpha.metadata.file_sources.html as html_md

    assert html_md is not None


def test_html_hashed_fixture_reader_smoke(md_test_fixtures_for_ext) -> None:
    from LiuXin_alpha.metadata.file_sources.html import get_metadata

    # Fixtures include both .html and .htm.
    fixtures = md_test_fixtures_for_ext(file_ext="html", verify_hash=True)
    fixtures += md_test_fixtures_for_ext(file_ext="htm", verify_hash=False)
    assert fixtures

    for fixture in fixtures:
        md = get_metadata(fixture)
        assert md is not None
        assert md.title


def test_html_legacy_fixture1_expectations(md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources.html import get_metadata

    fixture = md_test_fixture(file_ext="html", file_num=1, verify_hash=True)
    metadata = get_metadata(fixture)

    assert metadata.title == "The Project Gutenberg eBook of Twenty Thousand Leagues Under the Sea, by Jules Verne"
    assert _first(metadata.authors) == "Unknown"
    assert not _values(metadata.tags)


def test_html_reader_plugin_is_available(md_test_fixture) -> None:
    from LiuXin_alpha.customize.builtins.metadata_readers import get_metadata_reader_plugins

    fixture = md_test_fixture(file_ext="html", file_num=1, verify_hash=True)
    plugins = get_metadata_reader_plugins()
    html_cls = next((p for p in plugins if p.__name__ == "HTMLMetadataReader"), None)
    assert html_cls is not None

    reader = html_cls(None)
    with fixture.open("rb") as stream:
        metadata = reader.get_metadata(stream=stream, ftype="html")
        assert stream.tell() == 0

    inplace = reader.get_metadata_inplace(file_path=str(fixture), ftype="html")
    assert metadata.title == inplace.title


def test_html_meta_and_comment_precedence_and_rich_fields() -> None:
    from LiuXin_alpha.metadata.file_sources.html import get_metadata_

    src = """
    <html><head>
      <title>Fallback Title</title>
      <meta name="dc:title" content="Meta Title" />
      <meta name="author" content="Meta One" />
      <meta name="author" content="Meta Two" />
      <meta name="publisher" content="Meta Pub" />
      <meta name="dc.language" content="en" />
      <meta name="dcterms.language" content="fr" />
      <meta name="dc.date.issued" content="2020-01-02" />
      <meta name="dcterms.created" content="2019-06-07" />
      <meta name="series" content="Meta Series [2.5]" />
      <meta name="rating" content="8" />
      <meta name="tags" content="tag-a, tag-b" />
      <meta name="subject" content="tag-c" />
      <meta name="dc.identifier.url" content="https://example.invalid/book" />
      <meta name="dc.identifier" scheme="isbn" content="9781402894626" />
      <!-- TITLE="Comment Title" -->
      <!-- AUTHOR="Comment One and Comment Two" -->
      <!-- PUBLISHER="Comment Pub" -->
      <!-- COMMENTS="Comment block" -->
      <!-- TAGS="comment-tag" -->
    </head><body></body></html>
    """

    md = get_metadata_(src)

    # Comments override meta tags where present.
    assert md.title == "Comment Title"
    assert _values(md.authors) == ["Comment One", "Comment Two"]
    assert _first(md.publisher) == "Comment Pub"
    assert _first(md.comments) == "Comment block"
    assert _values(md.tags) == ["comment-tag"]

    # Meta-only fields should still be preserved.
    assert _first(md.language) == "en"
    assert _values(md.languages) == ["en", "fr"]
    assert getattr(md.pubdate, "year", None) == 2020
    assert getattr(md.timestamp, "year", None) == 2019
    assert _first(md.series) == "Meta Series"
    assert float(_series_index_for(md, "Meta Series")) == 2.5
    assert float(_first(md.rating)) == 4.0
    assert _first(md.isbn) == "9781402894626"


def test_html_title_fallback_when_no_comment_or_meta_title() -> None:
    from LiuXin_alpha.metadata.file_sources.html import get_metadata_

    md = get_metadata_("<html><head><title>Title Only</title></head><body></body></html>")

    assert md.title == "Title Only"
    assert _first(md.authors) == "Unknown"


def test_html_parse_meta_and_comment_helpers() -> None:
    from LiuXin_alpha.metadata.file_sources.html import parse_comment_tags, parse_meta_tags

    src = """
    <html><head>
      <meta name='dc:title' content='A Meta Title' />
      <meta content='A Meta Author' name='author' />
      <!-- TITLE="A Comment Title" -->
      <!-- AUTHOR="A Comment Author" -->
    </head></html>
    """

    meta = parse_meta_tags(src)
    comments = parse_comment_tags(src)

    assert meta["title"] == "A Meta Title"
    assert meta["authors"] == "A Meta Author"
    assert comments["title"] == "A Comment Title"
    assert comments["authors"] == "A Comment Author"


def test_html_handles_malformed_input_gracefully() -> None:
    from LiuXin_alpha.metadata.file_sources.html import get_metadata_

    src = "<html><head><meta name='title' content='Broken'><title>X"
    md = get_metadata_(src)

    assert md.title in {"Broken", "X", "Unknown"}


def test_html_reads_stream_and_rewinds() -> None:
    from LiuXin_alpha.metadata.file_sources.html import get_metadata

    stream = io.BytesIO(b"<html><head><title>Stream Title</title></head><body></body></html>")
    md = get_metadata(stream)

    assert stream.tell() == 0
    assert md.title == "Stream Title"


def test_html_handles_cp1251_bytes() -> None:
    from LiuXin_alpha.metadata.file_sources.html import get_metadata_

    src = "<html><head><meta name='title' content='Привет мир'/><meta name='author' content='Иван Петров'/></head></html>"
    raw = src.encode("cp1251")

    md = get_metadata_(raw, encoding="cp1251")
    assert md.title == "Привет мир"
    assert _values(md.authors) == ["Иван Петров"]


def test_html_pathlike_input(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.html import get_metadata

    path = tmp_path / "demo.html"
    path.write_text("<html><head><title>Path Title</title></head><body></body></html>", encoding="utf-8")

    md = get_metadata(path)
    assert md.title == "Path Title"


def test_html_unicode_torture_multiscript_roundtrip() -> None:
    from LiuXin_alpha.metadata.file_sources.html import get_metadata_

    src = """
    <html><head>
      <meta name="title" content="naïve café — Καλημέρα — Привет — こんにちは — مرحبا — नमस्ते — 😀 — e&#x0301;" />
      <meta name="author" content="Renée Faßbinder and 李白" />
      <meta name="author" content="Александр Пушкин" />
      <meta name="dc.language" content="fr" />
      <meta name="dcterms.language" content="ja" />
      <meta name="tags" content="δοκιμή, テスト, اختبار, परीक्षण, tag😀" />
      <meta name="comments" content="RTL: &#x202e;abc&#x202c; / combining: a&#x0300;e&#x0301;" />
    </head></html>
    """

    md = get_metadata_(src)
    assert "naïve" in md.title
    assert "😀" in md.title
    assert _values(md.authors) == ["Renée Faßbinder", "李白", "Александр Пушкин"]
    assert _values(md.languages) == ["fr", "ja"]
    assert "tag😀" in _values(md.tags)
    assert "combining" in _first(md.comments)


def test_html_comment_parser_handles_multiple_pairs_and_mixed_quotes() -> None:
    from LiuXin_alpha.metadata.file_sources.html import get_metadata_

    src = """
    <html><head>
      <!-- title='Commented Title' author="Alice &amp; Bob" tags='x, y' comments="From comment" -->
    </head></html>
    """

    md = get_metadata_(src)
    assert md.title == "Commented Title"
    assert _values(md.authors) == ["Alice", "Bob"]
    assert _values(md.tags) == ["x", "y"]
    assert _first(md.comments) == "From comment"


def test_html_identifier_variants_are_extracted() -> None:
    from LiuXin_alpha.metadata.file_sources.html import get_metadata_

    src = """
    <html><head>
      <meta name="dc.identifier.url" content="https://example.invalid/book" />
      <meta name="dcterms.identifier.doi" content="10.1000/xyz" />
      <meta name="dc.identifier" scheme="asin" content="B00TESTING" />
    </head></html>
    """

    md = get_metadata_(src)
    identifiers = md.identifiers
    assert "doi" in identifiers
    assert "10.1000/xyz" in identifiers["doi"]
    # `asin` is normalized by metadata identifier standardization.
    assert "amazon" in identifiers
    assert "B00TESTING" in identifiers["amazon"]


def test_html_rating_normalization_ranges() -> None:
    from LiuXin_alpha.metadata.file_sources.html import get_metadata_

    low = get_metadata_("<html><head><meta name='rating' content='-2'/></head></html>")
    mid = get_metadata_("<html><head><meta name='rating' content='8'/></head></html>")
    high = get_metadata_("<html><head><meta name='rating' content='11'/></head></html>")

    assert float(_first(low.rating)) == 0.0
    assert float(_first(mid.rating)) == 4.0
    assert float(_first(high.rating)) == 0.0


def test_html_series_index_from_separate_meta_field() -> None:
    from LiuXin_alpha.metadata.file_sources.html import get_metadata_

    md = get_metadata_(
        """
        <html><head>
          <meta name="series" content="Chronicles" />
          <meta name="seriesnumber" content="3.25" />
        </head></html>
        """
    )

    assert _first(md.series) == "Chronicles"
    assert float(_series_index_for(md, "Chronicles")) == 3.25


def test_html_authors_split_and_stable_dedupe() -> None:
    from LiuXin_alpha.metadata.file_sources.html import get_metadata_

    md = get_metadata_(
        """
        <html><head>
          <meta name="author" content="A. One and B. Two" />
          <meta name="author" content="A. One" />
          <meta name="author" content="B. Two with C. Three" />
        </head></html>
        """
    )

    assert _values(md.authors) == ["A. One", "B. Two", "C. Three"]


def test_html_invalid_dates_are_ignored() -> None:
    from LiuXin_alpha.metadata.file_sources.html import get_metadata_

    md = get_metadata_(
        """
        <html><head>
          <meta name="pubdate" content="not-a-date" />
          <meta name="timestamp" content="2020-02-30" />
        </head></html>
        """
    )

    assert md.is_null("pubdate")
    assert md.is_null("timestamp")


def test_html_stream_rewinds_to_original_nonzero_position() -> None:
    from LiuXin_alpha.metadata.file_sources.html import get_metadata

    payload = b"<html><head><title>Original Position</title></head><body></body></html>"
    stream = io.BytesIO(payload)
    stream.seek(7)
    md = get_metadata(stream)

    assert md.title == "Original Position"
    assert stream.tell() == 7


def test_html_title_fallback_decodes_entities_and_charrefs() -> None:
    from LiuXin_alpha.metadata.file_sources.html import get_metadata_

    md = get_metadata_("<html><head><title>A &amp; B &#169; &#x1F600;</title></head></html>")
    assert md.title == "A & B © 😀"


def test_html_mixed_case_meta_names_and_identifier_scheme() -> None:
    from LiuXin_alpha.metadata.file_sources.html import get_metadata_

    md = get_metadata_(
        """
        <html><head>
          <meta NAME="DC:TITLE" CONTENT="Casefolded Title" />
          <meta name="DCTERMS:IDENTIFIER" scheme="DOI" content="10.1000/case-test" />
        </head></html>
        """
    )

    assert md.title == "Casefolded Title"
    assert "doi" in md.identifiers
    assert "10.1000/case-test" in md.identifiers["doi"]


def test_html_date_parsing_supports_compact_and_year_only_formats() -> None:
    from LiuXin_alpha.metadata.file_sources.html import get_metadata_

    md = get_metadata_(
        """
        <html><head>
          <meta name="pubdate" content="1999" />
          <meta name="timestamp" content="20240203" />
        </head></html>
        """
    )

    assert (md.pubdate.year, md.pubdate.month, md.pubdate.day) == (1999, 6, 2)
    assert (md.timestamp.year, md.timestamp.month, md.timestamp.day) == (2024, 2, 3)


def test_html_invalid_utf8_bytes_are_replaced_without_crashing() -> None:
    from LiuXin_alpha.metadata.file_sources.html import get_metadata_

    raw = (
        b"<html><head>"
        b"<meta name='title' content='Bad \xff\xfe \xf0\x28\x8c\xbc'/>"
        b"<meta name='author' content='Alice and Bob'/>"
        b"</head></html>"
    )
    md = get_metadata_(raw, encoding="utf-8")

    assert md.title.startswith("Bad")
    assert _values(md.authors) == ["Alice", "Bob"]


def test_html_get_metadata_accepts_bytes_payload_directly() -> None:
    from LiuXin_alpha.metadata.file_sources.html import get_metadata

    raw = "<html><head><title>Bytes Payload — 世界</title></head></html>".encode("utf-8")
    md = get_metadata(raw)

    assert md.title == "Bytes Payload — 世界"


def test_html_binary_signatures_return_safe_default() -> None:
    from LiuXin_alpha.metadata.file_sources.html import get_metadata_

    for payload in (
        b"\x89PNG\r\n\x1a\n" + b"\x00" * 32,
        b"%PDF-1.7\n<title>Not HTML</title>",
        b"PK\x03\x04" + b"\x00" * 20,
    ):
        md = get_metadata_(payload)
        assert md.title == "Unknown"
        assert _values(md.authors) == ["Unknown"]


def test_html_parser_internal_errors_return_safe_default(monkeypatch) -> None:
    import LiuXin_alpha.metadata.file_sources.html as html_md

    class _BrokenParser:
        comment_tags = {}
        meta_tags = {}
        meta_identifiers = {}
        title_text = ""

        def feed(self, _src):
            raise RuntimeError("parser failed")

        def close(self):
            raise AssertionError("unreachable")

    monkeypatch.setattr(html_md, "_HTMLMetadataParser", _BrokenParser)

    md = html_md.get_metadata_("<html><head><title>Ignored</title></head></html>")

    assert md.title == "Unknown"
    assert _values(md.authors) == ["Unknown"]


def test_html_input_scan_limit_is_predictable() -> None:
    from LiuXin_alpha.metadata.file_sources.html import get_metadata_

    late_metadata = "<html><head>" + (" " * 260000) + "<meta name='title' content='Late' /></head></html>"
    early_metadata = "<html><head><meta name='title' content='Early' /></head>" + (" " * 260000)

    md_late = get_metadata_(late_metadata)
    md_early = get_metadata_(early_metadata)

    assert md_late.title == "Unknown"
    assert md_early.title == "Early"


def test_html_rejects_non_path_non_stream_inputs() -> None:
    from LiuXin_alpha.metadata.file_sources.html import get_metadata

    with pytest.raises(TypeError):
        get_metadata(object())
