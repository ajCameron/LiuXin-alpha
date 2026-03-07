from __future__ import annotations

import importlib
import random


def _textile(text: str) -> str:
    mod = importlib.import_module("LiuXin_alpha.file_formats.textile.functions")
    return mod.textile(text)


def _textile_restricted(text: str) -> str:
    mod = importlib.import_module("LiuXin_alpha.file_formats.textile.functions")
    return mod.textile_restricted(text)


def test_textile_unicode_torture_multiscript_blocks_and_links() -> None:
    src = (
        "h1. Ωμέγα 世界\n\n"
        "p. مرحبا שלום नमस्ते दुनिया\n\n"
        '"参照":https://example.com/路径?x=✓\n\n'
        "!images/图像.png(封面)!"
    )
    out = _textile(src)
    assert "<h1>Ωμέγα 世界</h1>" in out
    assert "مرحبا שלום नमस्ते दुनिया" in out
    assert '<a href="https://example.com/路径?x=✓">参照</a>' in out
    assert '<img src="images/图像.png" title="封面" alt="封面"' in out


def test_textile_preserves_combining_marks_without_normalizing() -> None:
    nfd = "Cafe\u0301 co\u0308operate A\u030A"
    out = _textile(f"h2. {nfd}")
    assert "<h2>" in out and "</h2>" in out
    assert nfd in out
    assert "\u0301" in out and "\u0308" in out and "\u030A" in out


def test_textile_handles_bidi_zwj_and_emoji_sequences() -> None:
    payload = "h3. RTL \u200fمرحبا\u200f / ZWJ A\u200dB / Emoji 👩🏽‍💻🧪📚"
    out = _textile(payload)
    assert "\u200fمرحبا\u200f" in out
    assert "A\u200dB" in out
    assert "👩🏽‍💻🧪📚" in out


def test_textile_notextile_block_preserves_unicode_markup_literals() -> None:
    src = "<notextile>*粗體* _курсив_ 👩🏽‍💻</notextile>"
    out = _textile(src)
    assert "*粗體* _курсив_ 👩🏽‍💻" in out
    assert "<strong>粗體</strong>" not in out
    assert "<em>курсив</em>" not in out


def test_textile_double_equals_no_textile_region_preserved() -> None:
    out = _textile("==*世界* مرحبا==")
    assert "*世界* مرحبا" in out
    assert "<strong>世界</strong>" not in out


def test_textile_restricted_unicode_torture_escapes_html_and_adds_nofollow() -> None:
    src = 'سلام <b>粗體</b> "资料":https://example.com/路径?鍵=值 !封面.png!'
    out = _textile_restricted(src)
    assert "&#60;b&#62;粗體&#60;/b&#62;" in out
    assert 'rel="nofollow"' in out
    assert '<a href="https://example.com/路径?鍵=值"' in out
    assert "!封面.png!" in out


def test_textile_unicode_reference_links_resolve() -> None:
    src = "[资料]https://example.com/路径?x=✓\n\n\"点击\":资料"
    out = _textile(src)
    assert '<a href="https://example.com/路径?x=✓">点击</a>' in out


def test_textile_smart_quotes_and_dash_with_unicode_content() -> None:
    out = _textile('p. "Καλημέρα" -- "世界"')
    assert "&#8220;Καλημέρα&#8221;" in out
    assert "&#8220;世界&#8221;" in out
    assert "&#8212;" in out


def test_textile_deterministic_output_under_unicode_fuzz() -> None:
    rng = random.Random(20260303)
    alphabet = (
        "abcXYZ"
        "ΩЖשלוםمرحباनमस्ते世界"
        "👩🏽‍💻🧪📚"
        " _*+-=~^%|!\"':;,.?/[](){}"
        "\u200d\u200f"
        "\u0301\u0308"
    )
    fuzz = "".join(rng.choice(alphabet) for _ in range(400))
    src = f"h4. Fuzz\n\n{fuzz}"
    out_a = _textile(src)
    out_b = _textile(src)
    assert out_a == out_b
    assert len(out_a) > 0


def test_convert_textile_processor_unicode_torture_wrapper() -> None:
    mod = importlib.import_module("LiuXin_alpha.file_formats.txt.processor")
    src = (
        "h1. 多言語\n\n"
        "Arabic مرحبا / Hebrew שלום / Hindi नमस्ते / Emoji 👩🏽‍💻\n\n"
        '"参照":https://example.com/路径'
    )
    out = mod.convert_textile(src, title="Тест")
    assert out.startswith("<html>")
    assert "<title>Тест " in out
    assert "<h1>多言語</h1>" in out
    assert "Emoji 👩🏽‍💻" in out
    assert '<a href="https://example.com/路径">参照</a>' in out
