from __future__ import annotations

import importlib
import sys

from lxml.html import document_fromstring

import pytest


class _Log:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []

    def debug(self, *parts) -> None:
        self.messages.append(("debug", " ".join(str(x) for x in parts)))

    def info(self, *parts) -> None:
        self.messages.append(("info", " ".join(str(x) for x in parts)))

    def warning(self, *parts) -> None:
        self.messages.append(("warning", " ".join(str(x) for x in parts)))

    def warn(self, *parts) -> None:
        self.warning(*parts)

    def error(self, *parts) -> None:
        self.messages.append(("error", " ".join(str(x) for x in parts)))

    def exception(self, *parts) -> None:
        self.messages.append(("exception", " ".join(str(x) for x in parts)))


def _sample_article_html() -> bytes:
    body = (
        "<html><head><title>Noise | Real Story Title Here</title></head>"
        "<body>"
        "<div id='header'>top nav</div>"
        "<div class='article'>"
        "<h1>Real Story Title Here</h1>"
        "<p>This paragraph has enough text, with commas, to exceed threshold, and provide meaningful content for extraction.</p>"
        "<p>Second paragraph includes unicode: café, Ελληνικά, हिन्दी, 日本語, العربية, emoji 😀, and combining e\u0301 marks.</p>"
        "</div>"
        "<div class='footer sidebar'>ad links links links</div>"
        "</body></html>"
    )
    return body.encode("utf-8")


def test_readability_modules_import_smoke() -> None:
    modules = (
        "LiuXin_alpha.file_formats.readability",
        "LiuXin_alpha.file_formats.readability.cleaners",
        "LiuXin_alpha.file_formats.readability.debug",
        "LiuXin_alpha.file_formats.readability.htmls",
        "LiuXin_alpha.file_formats.readability.readability",
    )
    for module_name in modules:
        importlib.import_module(module_name)


def test_clean_attributes_strips_style_and_event_handlers() -> None:
    cleaners = importlib.import_module("LiuXin_alpha.file_formats.readability.cleaners")

    raw = '<p style="color:red" width="99" onclick="boom()">ok</p>'
    cleaned = cleaners.clean_attributes(raw)
    assert "style=" not in cleaned
    assert "width=" not in cleaned
    assert "onclick=" not in cleaned
    assert "<p" in cleaned and "ok" in cleaned


def test_fallback_cleaner_removes_scripts_links_styles_and_comments() -> None:
    cleaners = importlib.import_module("LiuXin_alpha.file_formats.readability.cleaners")
    cleaner = cleaners._FallbackCleaner()
    doc = document_fromstring(
        "<html><head><style>x</style><link rel='x' href='x'/></head><body><!--c--><script>x</script><p>ok</p></body></html>"
    )
    out = cleaner.clean_html(doc)
    serialized = out.text_content()
    assert "ok" in serialized
    assert not out.xpath(".//script")
    assert not out.xpath(".//style")
    assert not out.xpath(".//link")
    assert not out.xpath("//comment()")


def test_htmls_title_shortening_and_entity_normalization() -> None:
    htmls = importlib.import_module("LiuXin_alpha.file_formats.readability.htmls")
    doc = htmls.build_doc(
        b"<html><head><title>Example &mdash; Long Story Title Here</title></head>"
        b"<body><h1>Long Story Title Here</h1></body></html>"
    )
    assert htmls.get_title(doc) == "Example - Long Story Title Here"
    assert htmls.shorten_title(doc) == "Long Story Title Here"


def test_htmls_shorten_title_without_cssselect(monkeypatch: pytest.MonkeyPatch) -> None:
    htmls = importlib.import_module("LiuXin_alpha.file_formats.readability.htmls")
    doc = htmls.build_doc(
        b"<html><head><title>Prefix | A Meaningful Long Title For Story</title></head>"
        b"<body><div id='title'>A Meaningful Long Title For Story</div></body></html>"
    )
    monkeypatch.setitem(sys.modules, "cssselect", None)
    assert htmls.shorten_title(doc) == "A Meaningful Long Title For Story"


def test_htmls_get_body_removes_script_style_link() -> None:
    htmls = importlib.import_module("LiuXin_alpha.file_formats.readability.htmls")
    doc = htmls.build_doc(
        b"<html><head><style>.x{}</style><link rel='x' href='x'/></head>"
        b"<body><script>alert(1)</script><p>Hello</p></body></html>"
    )
    body = htmls.get_body(doc)
    assert "alert(" not in body
    assert "<style" not in body
    assert "<link" not in body
    assert "Hello" in body


def test_js_re_replacement_order() -> None:
    htmls = importlib.import_module("LiuXin_alpha.file_formats.readability.htmls")
    assert htmls.js_re("abc123", r"\d+", 0, "NUM") == "abcNUM"


def test_document_title_short_title_and_content_smoke() -> None:
    readability = importlib.import_module("LiuXin_alpha.file_formats.readability.readability")
    doc = readability.Document(_sample_article_html(), _Log())
    assert doc.title() == "Noise | Real Story Title Here"
    assert doc.short_title() == "Real Story Title Here"
    body = doc.content()
    assert "Real Story Title Here" in body
    assert "emoji" in body


def test_document_summary_extracts_main_content_and_strips_noise() -> None:
    readability = importlib.import_module("LiuXin_alpha.file_formats.readability.readability")
    summary = readability.Document(_sample_article_html(), _Log()).summary()
    assert "Real Story Title Here" in summary
    assert "Second paragraph includes unicode" in summary
    assert "footer sidebar" not in summary


def test_document_keep_elements_preserves_selected_node() -> None:
    readability = importlib.import_module("LiuXin_alpha.file_formats.readability.readability")
    html = (
        "<html><head><title>T</title></head><body>"
        "<div class='sidebar' id='keepme'>Important retained block, with commas, and enough words to stay visible.</div>"
        "<div class='article'><p>Main body text, with commas, and enough words for extraction.</p></div>"
        "</body></html>"
    ).encode("utf-8")
    doc = readability.Document(html, _Log(), keep_elements='//*[@id="keepme"]')
    summary = doc.summary()
    assert "Important retained block" in summary


def test_document_summary_handles_invalid_utf8_bytes() -> None:
    readability = importlib.import_module("LiuXin_alpha.file_formats.readability.readability")
    broken = (
        b"<html><head><title>Bad\xffTitle</title></head><body>"
        b"<div><p>Broken text \xe2(\xa1 with valid tail, and commas, for scoring.</p></div>"
        b"</body></html>"
    )
    summary = readability.Document(broken, _Log()).summary()
    assert isinstance(summary, str)
    assert "Broken text" in summary


def test_document_summary_is_deterministic_for_same_input() -> None:
    readability = importlib.import_module("LiuXin_alpha.file_formats.readability.readability")
    raw = _sample_article_html()
    first = readability.Document(raw, _Log()).summary()
    second = readability.Document(raw, _Log()).summary()
    assert first == second


def test_document_summary_wraps_parse_failures_as_unparseable(monkeypatch: pytest.MonkeyPatch) -> None:
    readability = importlib.import_module("LiuXin_alpha.file_formats.readability.readability")

    def _boom(self, _input):
        raise ValueError("explode")

    monkeypatch.setattr(readability.Document, "_parse", _boom)
    with pytest.raises(readability.Unparseable):
        readability.Document(b"<html></html>", _Log()).summary()


def test_readability_debug_save_to_file(tmp_path) -> None:
    debug = importlib.import_module("LiuXin_alpha.file_formats.readability.debug")
    out = tmp_path / "debug.html"
    debug.save_to_file("<p>hello</p>", str(out))
    data = out.read_text(encoding="utf-8")
    assert "<p>hello</p>" in data
