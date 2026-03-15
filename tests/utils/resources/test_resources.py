# tests/utils/test_resources.py
from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path

import pytest


@pytest.fixture()
def resource_modules(monkeypatch: pytest.MonkeyPatch):
    for key in ("LIUXIN_BASE_DIR", "LIUXIN_PREFS_DIR", "LIUXIN_CONFIG_DIR"):
        monkeypatch.delenv(key, raising=False)

    for name in (
        "LiuXin_alpha.constants.paths",
        "LiuXin_alpha.utils.resources",
    ):
        sys.modules.pop(name, None)

    import LiuXin_alpha.constants.paths as paths
    import LiuXin_alpha.utils.resources as resources

    paths = importlib.reload(paths)
    resources = importlib.reload(resources)
    return paths, resources


def test_get_path_resolves_calibre_mime_types(resource_modules) -> None:
    """P/get_path should resolve known calibre resources (e.g. mime.types)."""
    paths, resources = resource_modules

    expected = Path(paths.LiuXin_calibre_resources_folder) / "mime.types"
    assert expected.is_file(), f"Expected calibre mime.types at {expected}"

    got = Path(resources.get_path("mime.types"))
    assert got.is_file(), got
    assert got.samefile(expected)

    data = resources.get_path("mime.types", data=True)
    assert isinstance(data, (bytes, bytearray))
    assert data == expected.read_bytes()

    assert Path(resources.resource_to_path("mime.types")).samefile(expected)
    assert Path(resources.P("mime.types")).samefile(expected)


def test_get_image_path_resolves_under_images(resource_modules) -> None:
    paths, resources = resource_modules

    expected_images_dir = Path(paths.LiuXin_calibre_resources_folder) / "images"
    assert expected_images_dir.is_dir(), f"Expected images dir at {expected_images_dir}"

    images_dir = Path(resources.get_image_path(""))
    assert images_dir.is_dir()
    assert images_dir.samefile(expected_images_dir)

    blank = Path(resources.get_image_path("blank.png"))
    assert blank.is_file()
    assert blank.samefile(expected_images_dir / "blank.png")

    blank_bytes = resources.get_image_path("blank.png", data=True)
    assert isinstance(blank_bytes, (bytes, bytearray))
    assert blank_bytes == (expected_images_dir / "blank.png").read_bytes()
