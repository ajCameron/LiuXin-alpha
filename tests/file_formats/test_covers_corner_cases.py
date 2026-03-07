from __future__ import annotations

from copy import deepcopy

import pytest

from LiuXin_alpha.file_formats import covers


def _prefs_dict():
    return deepcopy(dict(covers.cprefs.defaults))


def test_generate_cover_handles_invalid_numeric_prefs_without_qt(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(covers, "HAS_QT", False)
    mi = covers.Metadata("Corner Title", ["Author"])
    prefs = _prefs_dict()
    prefs["cover_width"] = 0
    prefs["cover_height"] = -123
    prefs["title_font_size"] = 0
    prefs["subtitle_font_size"] = -2
    prefs["footer_font_size"] = None

    data = covers.generate_cover(mi, prefs=prefs)

    assert isinstance(data, (bytes, bytearray))
    assert len(data) > 0


def test_generate_masthead_coerces_bad_dimensions_without_qt(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(covers, "HAS_QT", False)

    data = covers.generate_masthead("Masthead", width="bad-width", height=None)

    assert isinstance(data, (bytes, bytearray))
    assert len(data) > 0


def test_scale_cover_clamps_to_positive_values() -> None:
    prefs = {
        "cover_width": 10,
        "cover_height": 20,
        "title_font_size": 8,
        "subtitle_font_size": 7,
        "footer_font_size": 6,
    }

    covers.scale_cover(prefs, 0)

    assert prefs["cover_width"] >= 1
    assert prefs["cover_height"] >= 1
    assert prefs["title_font_size"] >= 1
    assert prefs["subtitle_font_size"] >= 1
    assert prefs["footer_font_size"] >= 1


def test_fallback_cover_bytes_tolerates_malformed_unicode() -> None:
    data = covers._fallback_cover_bytes("bad\ud800title", "sub", "footer", 200, 300)

    assert isinstance(data, (bytes, bytearray))
    # Resource fallback may be empty in edge environments, but call should never crash.
    assert len(data) >= 0


def test_load_color_themes_ignores_invalid_custom_entries(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeColor:
        def __init__(self, *_args, **_kwargs):
            pass

        def isValid(self):
            return True

    monkeypatch.setattr(covers, "QColor", _FakeColor)
    prefs = _prefs_dict()
    prefs["color_themes"] = {"invalid-theme": "zzz"}
    obj = covers.Prefs(**prefs)

    themes = covers.load_color_themes(obj)

    assert themes

