from __future__ import annotations

import time

import pytest


def test_monotonic_increases() -> None:
    from LiuXin_alpha.utils.plugins.fallbacks import monotonic

    a = monotonic.monotonic()
    time.sleep(0.01)
    b = monotonic.monotonic()
    assert isinstance(a, float)
    assert b >= a


def test_matcher_ratio_basic_properties() -> None:
    from LiuXin_alpha.utils.plugins.fallbacks import matcher

    assert matcher.ratio("abc", "abc") == 1.0
    r = matcher.ratio("abc", "abd")
    assert 0.0 <= r <= 1.0


def test_tokenizer_as_css_accepts_various_token_shapes() -> None:
    from LiuXin_alpha.utils.plugins.fallbacks import tokenizer

    assert tokenizer.as_css(None) == ""
    assert tokenizer.as_css("a") == "a"
    assert tokenizer.as_css(b"a") == "a"
    assert tokenizer.as_css(["a", "b"]) == "ab"
    assert tokenizer.as_css([(1, "a"), (2, "b")]) == "ab"
    assert tokenizer.as_css([b"a", 1, ("x", "y")]) == "a1y"


def test_progress_indicator_tracks_fraction() -> None:
    from LiuXin_alpha.utils.plugins.fallbacks import progress_indicator

    p = progress_indicator.ProgressIndicator()
    assert p.fraction == 0.0
    p.set_fraction(0.25)
    assert p.fraction == 0.25
    p.set_fraction("nope")
    assert p.fraction == 0.0
    p.reset()
    assert p.fraction == 0.0


def test_html_fallback_is_importable_and_noops() -> None:
    from LiuXin_alpha.utils.plugins.fallbacks import html as mod

    assert mod.init() is None
    assert mod.check_spelling("anything") == []

    t = mod.Tag("x")
    t2 = t.copy()
    assert t2 is not t
    assert t2.name == "x"

    s = mod.State({"k": "v"})
    s2 = s.copy()
    assert s2 is not s


def test_freetype_face_contract() -> None:
    from LiuXin_alpha.utils.plugins.fallbacks import freetype

    face = freetype.load_font(b"dummyfont", index=0)
    assert face.supports_text("Hello") is True
    assert face.glyph_id("A") == ord("A")
    assert face.glyph_id("") == 0

    with pytest.raises(TypeError):
        freetype.load_font("not-bytes")  # type: ignore[arg-type]


def test_hunspell_dictionary_minimal_behaviour() -> None:
    from LiuXin_alpha.utils.plugins.fallbacks import hunspell

    aff = b"SET UTF-8\n"
    # Hunspell dic often starts with an integer count; we accept it.
    dic = b"2\ncolour\ncolor\n"
    d = hunspell.Dictionary(dic, aff)

    assert d.recognized("colour") is True
    assert d.recognized("Colour") is True  # case-insensitive check
    assert d.recognized("colr") is False

    sugg = d.suggest("colr")
    assert isinstance(sugg, list)
    # Might be empty depending on difflib cutoff, but should never error.

    d.add("colr")
    assert d.recognized("colr") is True
    d.remove("colr")
    assert d.recognized("colr") is False


def test_winutil_strftime_passthrough() -> None:
    from LiuXin_alpha.utils.plugins.fallbacks import winutil

    assert isinstance(winutil.strftime("%Y"), str)


def test_usbobserver_date_format_is_reasonable() -> None:
    from LiuXin_alpha.utils.plugins.fallbacks import usbobserver

    fmt = usbobserver.date_format()
    assert "%Y" in fmt
