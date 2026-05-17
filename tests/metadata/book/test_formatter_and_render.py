from __future__ import annotations

import pytest

from LiuXin_alpha.metadata.book.base import calibreMetadata
from LiuXin_alpha.metadata.book.formatter import SafeFormat
from LiuXin_alpha.metadata.book import render


def test_safe_format_get_value_resolves_standard_identifier_and_custom_fields() -> None:
    metadata = calibreMetadata("Formatter Book", ["Author"])
    metadata.rating = 8
    metadata.set_identifier("isbn", "9780000000001")
    metadata.set_user_metadata(
        "#score",
        {
            "name": "Score",
            "datatype": "int",
            "is_multiple": {},
            "display": {},
            "#value#": None,
        },
    )

    formatter = SafeFormat()
    formatter.book = metadata

    assert formatter.get_value("", (), {}) == ""
    assert formatter.get_value("TITLE", (), {}) == "Formatter Book"
    assert formatter.get_value("isbn", (), {}) == "9780000000001"
    assert formatter.get_value("rating", (), {}) == "4"
    assert formatter.get_value("#score", (), {}) == ""

    with pytest.raises(ValueError, match="unknown field"):
        formatter.get_value("definitely_missing", (), {})


def test_render_module_delegates_legacy_wrappers(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, tuple[object, ...], dict[str, object]]] = []

    def fake(name: str):
        def _inner(*args: object, **kwargs: object) -> str:
            calls.append((name, args, kwargs))
            return name + "-result"

        return _inner

    monkeypatch.setattr(
        "LiuXin_alpha.surfaces.renderers.calibre_metadata.field_sort",
        fake("field_sort"),
    )
    monkeypatch.setattr(
        "LiuXin_alpha.surfaces.renderers.calibre_metadata.displayable_field_keys",
        fake("displayable_field_keys"),
    )
    monkeypatch.setattr(
        "LiuXin_alpha.surfaces.renderers.calibre_metadata.get_field_list",
        fake("get_field_list"),
    )
    monkeypatch.setattr(
        "LiuXin_alpha.surfaces.renderers.calibre_metadata.search_href",
        fake("search_href"),
    )
    monkeypatch.setattr(
        "LiuXin_alpha.surfaces.renderers.calibre_metadata.mi_to_html",
        fake("mi_to_html"),
    )

    metadata = object()

    assert render.field_sort(metadata, "title") == "field_sort-result"
    assert render.displayable_field_keys(metadata) == "displayable_field_keys-result"
    assert render.get_field_list(metadata) == "get_field_list-result"
    assert render.search_href("authors", "Name") == "search_href-result"
    assert (
        render.mi_to_html(
            metadata,
            field_list=[("title", True)],
            default_author_link="search",
            use_roman_numbers=False,
            rating_font="Font",
        )
        == "mi_to_html-result"
    )

    assert calls == [
        ("field_sort", (metadata, "title"), {}),
        ("displayable_field_keys", (metadata,), {}),
        ("get_field_list", (metadata,), {}),
        ("search_href", ("authors", "Name"), {}),
        (
            "mi_to_html",
            (metadata,),
            {
                "field_list": [("title", True)],
                "default_author_link": "search",
                "use_roman_numbers": False,
                "rating_font": "Font",
            },
        ),
    ]
