from __future__ import annotations

import LiuXin_alpha.metadata.book.render as legacy_render

from LiuXin_alpha.metadata.book.base import calibreMetadata
from LiuXin_alpha.surfaces.renderers.calibre_metadata import (
    calibre_metadata_to_html,
    search_href,
)


def test_calibre_metadata_renderer_matches_compat_method() -> None:
    metadata = calibreMetadata("Renderer Book", ["Author One"])
    metadata.publisher = "Publisher"
    metadata.tags = ["tag two", "tag one"]
    metadata.series = "Saga"
    metadata.series_index = 2

    rendered = calibre_metadata_to_html(metadata)

    assert rendered == metadata.to_html()
    assert "Renderer Book" in rendered
    assert "Author One" in rendered
    assert "Saga #2" in rendered


def test_metadata_book_render_delegates_to_surface_renderer(monkeypatch) -> None:
    calls = {}

    def fake_renderer(*args, **kwargs):
        calls["args"] = args
        calls["kwargs"] = kwargs
        return "<table>delegated</table>", ["comment"]

    monkeypatch.setattr(
        "LiuXin_alpha.surfaces.renderers.calibre_metadata.mi_to_html",
        fake_renderer,
    )

    sentinel = object()
    result = legacy_render.mi_to_html(
        sentinel,
        field_list=[("title", True)],
        default_author_link="search-calibre",
        use_roman_numbers=False,
        rating_font="Test Font",
    )

    assert result == ("<table>delegated</table>", ["comment"])
    assert calls["args"] == (sentinel,)
    assert calls["kwargs"] == {
        "field_list": [("title", True)],
        "default_author_link": "search-calibre",
        "use_roman_numbers": False,
        "rating_font": "Test Font",
    }


def test_search_href_hex_encodes_text_for_calibre_search_links() -> None:
    href = search_href("authors", 'Author "Quoted"')

    assert href.startswith("search:")
    assert "617574686f7273" in href
    assert isinstance(href, str)
