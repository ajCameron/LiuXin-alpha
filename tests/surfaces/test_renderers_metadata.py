from __future__ import annotations

import importlib
import sys
from collections import OrderedDict

import pytest

from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import (
    CalibreLikeLiuXinBookMetaData,
)
import LiuXin_alpha.surfaces.renderers as renderers
import LiuXin_alpha.surfaces.renderers.metadata as metadata_renderer
from LiuXin_alpha.surfaces.renderers.metadata import (
    metadata_to_html,
    series_index_to_text,
)


def test_metadata_renderer_renders_calibre_like_metadata_to_html() -> None:
    metadata = CalibreLikeLiuXinBookMetaData(title="Renderer Title", authors=["Author"])
    metadata.tag = "Space Opera"

    html = metadata_to_html(metadata)

    assert html.startswith("<table>")
    assert "Renderer Title" in html
    assert "Space Opera" in html


def test_series_index_renderer_handles_explicit_and_metadata_values() -> None:
    metadata = CalibreLikeLiuXinBookMetaData(title="Series Title", authors=["Author"])
    metadata.series = "Saga"
    metadata.series_index = ("Saga", 3)

    class MissingSeries:
        series = {}
        series_index = {"Saga": 3}

    assert series_index_to_text("2.0") == "2"
    assert series_index_to_text(metadata=metadata) == "3"
    assert series_index_to_text(metadata=MissingSeries()) == "1"
    assert series_index_to_text("not-a-number") == "1"


def test_metadata_renderer_renders_mapping_field_shapes(monkeypatch) -> None:
    def fake_standardize_id_name(field: str, logging: bool = False) -> str | None:
        if field == "isbn":
            return "isbn"
        return None

    def fake_standardize_internal_id_name(field: str, logging: bool = False) -> str | None:
        if field == "internal_id":
            return "uuid"
        return None

    monkeypatch.setattr(
        metadata_renderer,
        "standardize_id_name",
        fake_standardize_id_name,
    )
    monkeypatch.setattr(
        metadata_renderer,
        "standardize_internal_id_name",
        fake_standardize_internal_id_name,
    )
    metadata = OrderedDict(
        [
            ("authors", OrderedDict([("Creator One", "primary")])),
            ("isbn", {"9780000000001"}),
            ("internal_id", {"internal-1"}),
            ("ordered_values", OrderedDict([("first", 1), ("second", 2)])),
            ("set_values", {"set entry"}),
        ]
    )

    html = metadata_to_html(metadata)

    assert "Creator_Role" in html
    assert "Creator One" in html
    assert "Identifier Type" in html
    assert "9780000000001" in html
    assert "internal-1" in html
    assert "first , second" in html
    assert "set entry" in html


def test_metadata_renderer_accepts_to_mapping_and_rejects_bad_sources() -> None:
    class MappingSource:
        def to_mapping(self) -> dict[str, str]:
            return {"title": "Mapped Title"}

    class MissingMapping:
        pass

    class BadData:
        _data = ["not", "a", "mapping"]

    assert "Mapped Title" in metadata_to_html(MappingSource())
    with pytest.raises(TypeError, match="expects a mapping"):
        metadata_to_html(MissingMapping())
    with pytest.raises(TypeError, match="_data must be a mapping"):
        metadata_to_html(BadData())


def test_renderers_package_lazy_loader() -> None:
    assert "metadata" in dir(renderers)
    assert renderers.__getattr__("metadata") is metadata_renderer
    with pytest.raises(AttributeError):
        renderers.__getattr__("missing")


def test_metadata_api_does_not_import_surface_renderers() -> None:
    for module_name in tuple(sys.modules):
        if module_name.startswith("LiuXin_alpha.surfaces.renderers"):
            sys.modules.pop(module_name)

    importlib.import_module("LiuXin_alpha.metadata.api")

    assert "LiuXin_alpha.surfaces.renderers" not in sys.modules
    assert "LiuXin_alpha.surfaces.renderers.metadata" not in sys.modules
