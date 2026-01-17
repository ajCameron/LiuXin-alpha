# tests/utils/test_resources.py
from __future__ import annotations

from pathlib import Path


def test_get_path_resolves_calibre_mime_types() -> None:
    """P/get_path should resolve known calibre resources (e.g. mime.types)."""

    from LiuXin_alpha.constants import paths
    from LiuXin_alpha.utils import resources

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


def test_get_image_path_resolves_under_images() -> None:
    from LiuXin_alpha.constants import paths
    from LiuXin_alpha.utils import resources

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
