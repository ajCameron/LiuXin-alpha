from __future__ import annotations

import importlib
import sys

from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import (
    CalibreLikeLiuXinBookMetaData,
)
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

    assert series_index_to_text("2.0") == "2"
    assert series_index_to_text(metadata=metadata) == "3"
    assert series_index_to_text("not-a-number") == "1"


def test_metadata_api_does_not_import_surface_renderers() -> None:
    for module_name in tuple(sys.modules):
        if module_name.startswith("LiuXin_alpha.surfaces.renderers"):
            sys.modules.pop(module_name)

    importlib.import_module("LiuXin_alpha.metadata.api")

    assert "LiuXin_alpha.surfaces.renderers" not in sys.modules
    assert "LiuXin_alpha.surfaces.renderers.metadata" not in sys.modules
