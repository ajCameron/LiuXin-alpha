from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace
from xml.etree import ElementTree as ET

import pytest


class _Log:
    def __call__(self, *args, **kwargs):
        return None

    def info(self, *args, **kwargs):
        return None

    def debug(self, *args, **kwargs):
        return None

    def warning(self, *args, **kwargs):
        return None

    warn = warning

    def exception(self, *args, **kwargs):
        return None


def test_lrf_modules_import_smoke() -> None:
    import importlib

    importlib.import_module("LiuXin_alpha.file_formats.lrf")
    importlib.import_module("LiuXin_alpha.file_formats.lrf.input")
    importlib.import_module("LiuXin_alpha.file_formats.lrf.meta")
    importlib.import_module("LiuXin_alpha.file_formats.lrf.lrfparser")
    importlib.import_module("LiuXin_alpha.file_formats.lrf.html.convert_from")
    importlib.import_module("LiuXin_alpha.file_formats.lrf.html.table")
    importlib.import_module("LiuXin_alpha.file_formats.lrf.html.table_as_image")
    importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.lrf_input")
    importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.lrf_output")


def _lrf_paths(md_test_files_by_ext: dict[str, list[Path]]) -> list[Path]:
    paths = list(md_test_files_by_ext.get("lrf", []))
    if not paths:
        pytest.skip("No .lrf fixtures found in optional LiuXin_alpha_data corpus")
    return paths


def test_lrf_input_converts_fixture_to_opf(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    md_test_files_by_ext: dict[str, list[Path]],
) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.lrf_input import LRFInput

    fixture = next((p for p in _lrf_paths(md_test_files_by_ext) if p.name == "lrf_md_test_file_1.lrf"), None)
    if fixture is None:
        fixture = _lrf_paths(md_test_files_by_ext)[0]

    monkeypatch.chdir(tmp_path)
    plugin = LRFInput(None)
    options = SimpleNamespace(verbose=0)

    with fixture.open("rb") as stream:
        out = plugin.convert(stream, options, "lrf", _Log(), accelerators={})

    out_path = Path(out) if os.path.isabs(out) else tmp_path / out
    assert out_path.exists()
    ET.parse(out_path)
    assert out_path.stat().st_size > 0


def test_table_as_image_raises_clear_error_when_qt_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    import LiuXin_alpha.file_formats.lrf.html.table_as_image as table_as_image

    monkeypatch.setattr(table_as_image, "_QT_IMPORT_ERROR", RuntimeError("missing qt"), raising=False)

    with pytest.raises(RuntimeError, match="PyQt5"):
        table_as_image.HTMLTableRenderer("<html></html>", ".", 100, 100, 166, 1.0)
