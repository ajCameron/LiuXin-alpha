"""
Verify metadata template formatting and human-readable rendering.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test formatter and render through its owning regression module::

        python -m pytest -q tests/metadata/book/test_formatter_and_render.py
"""
from __future__ import annotations

import pytest

from LiuXin_alpha.metadata.book.base import calibreMetadata
from LiuXin_alpha.metadata.book.formatter import SafeFormat
from LiuXin_alpha.metadata.book import render


def test_safe_format_get_value_resolves_standard_identifier_and_custom_fields() -> None:
    """
    Verify safe format get value resolves standard identifier and custom fields.

    Example:
        Exercise test safe format get value resolves standard identifier and custom fields through its owning regression module::

            python -m pytest -q tests/metadata/book/test_formatter_and_render.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify render module delegates legacy wrappers.

    Example:
        Exercise test render module delegates legacy wrappers through its owning regression module::

            python -m pytest -q tests/metadata/book/test_formatter_and_render.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    calls: list[tuple[str, tuple[object, ...], dict[str, object]]] = []

    def fake(name: str):
        """
        Perform the fake test-helper operation with deterministic inputs.

        Example:
            Exercise test render module delegates legacy wrappers.fake through its owning regression module::

                python -m pytest -q tests/metadata/book/test_formatter_and_render.py


        :param name: Value supplied for name in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        def _inner(*args: object, **kwargs: object) -> str:
            """
            Perform the inner test-helper operation with deterministic inputs.

            Example:
                Exercise test render module delegates legacy wrappers.fake.inner through its owning regression module::

                    python -m pytest -q tests/metadata/book/test_formatter_and_render.py


            :param args: Positional values forwarded by the test double.
            :param kwargs: Keyword values forwarded by the test double.
            :return: The deterministic value, row, identity or collection described above.
            """
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
