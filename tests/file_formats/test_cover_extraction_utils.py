"""
Provide test cover extraction utils utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test cover extraction utils through a consuming regression::

        python -m pytest -q tests/file_formats/test_cover_extraction_utils.py
"""
from __future__ import annotations

import importlib
import sys
import types

import pytest


PNG_BYTES = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"


@pytest.fixture(params=["LiuXin_alpha.file_formats", "LiuXin_alpha.file_formats.utils"])
def cover_module(request):
    """
    Perform the cover module operation under explicit file-format and conversion rules.

    Example:
        Exercise cover module through a consuming regression::

            python -m pytest -q tests/file_formats/test_cover_extraction_utils.py


    :param request: Value supplied for request under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return importlib.import_module(request.param)


def _write_cover(tmp_path):
    """
    Write cover under the format's safety and compatibility rules.

    Example:
        Exercise  write cover through a consuming regression::

            python -m pytest -q tests/file_formats/test_cover_extraction_utils.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    image_dir = tmp_path / "images"
    image_dir.mkdir()
    cover = image_dir / "cover.png"
    cover.write_bytes(PNG_BYTES)
    return cover


def test_return_raster_image_reads_supported_images_only(cover_module, tmp_path) -> None:
    """
    Perform the test return raster image reads supported images only operation under explicit file-format and conversion rules.

    Example:
        Exercise test return raster image reads supported images only through a consuming regression::

            python -m pytest -q tests/file_formats/test_cover_extraction_utils.py


    :param cover_module: Value supplied for cover module under the utility contract.
    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    cover = _write_cover(tmp_path)
    text = tmp_path / "not-an-image.txt"
    text.write_text("not an image", encoding="utf-8")

    assert cover_module.return_raster_image(str(cover)) == PNG_BYTES
    assert cover_module.return_raster_image(str(text)) is None
    assert cover_module.return_raster_image(str(tmp_path / "missing.png")) is None


def test_extract_cover_from_embedded_svg_reads_nested_raster(cover_module, tmp_path) -> None:
    """
    Perform the test extract cover from embedded svg reads nested raster operation under explicit file-format and conversion rules.

    Example:
        Exercise test extract cover from embedded svg reads nested raster through a consuming regression::

            python -m pytest -q tests/file_formats/test_cover_extraction_utils.py


    :param cover_module: Value supplied for cover module under the utility contract.
    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    _write_cover(tmp_path)
    raw = b"""\
    <svg xmlns="http://www.w3.org/2000/svg"
         xmlns:xlink="http://www.w3.org/1999/xlink">
      <image xlink:href="images/cover.png" />
    </svg>
    """

    assert cover_module.extract_cover_from_embedded_svg(raw, str(tmp_path), log=None) == PNG_BYTES


def test_extract_calibre_cover_accepts_cover_alt_image(cover_module, tmp_path) -> None:
    """
    Perform the test extract calibre cover accepts cover alt image operation under explicit file-format and conversion rules.

    Example:
        Exercise test extract calibre cover accepts cover alt image through a consuming regression::

            python -m pytest -q tests/file_formats/test_cover_extraction_utils.py


    :param cover_module: Value supplied for cover module under the utility contract.
    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    _write_cover(tmp_path)
    raw = b'<html><body><img alt="cover" src="images/cover.png" /></body></html>'

    assert cover_module.extract_calibre_cover(raw, str(tmp_path), log=None) == PNG_BYTES


def test_extract_calibre_cover_accepts_body_with_single_image_and_no_text(cover_module, tmp_path) -> None:
    """
    Perform the test extract calibre cover accepts body with single image and no text operation under explicit file-format and conversion rules.

    Example:
        Exercise test extract calibre cover accepts body with single image and no text through a consuming regression::

            python -m pytest -q tests/file_formats/test_cover_extraction_utils.py


    :param cover_module: Value supplied for cover module under the utility contract.
    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    _write_cover(tmp_path)
    raw = b'<html><body> \n <img src="images/cover.png" /> \n </body></html>'

    assert cover_module.extract_calibre_cover(raw, str(tmp_path), log=None) == PNG_BYTES


def test_extract_calibre_cover_ignores_body_with_text(cover_module, tmp_path) -> None:
    """
    Perform the test extract calibre cover ignores body with text operation under explicit file-format and conversion rules.

    Example:
        Exercise test extract calibre cover ignores body with text through a consuming regression::

            python -m pytest -q tests/file_formats/test_cover_extraction_utils.py


    :param cover_module: Value supplied for cover module under the utility contract.
    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    _write_cover(tmp_path)
    raw = b'<html><body>Text before image <img src="images/cover.png" /></body></html>'

    assert cover_module.extract_calibre_cover(raw, str(tmp_path), log=None) is None


def test_render_html_svg_workaround_prefers_static_svg_cover(cover_module, tmp_path) -> None:
    """
    Perform the test render html svg workaround prefers static svg cover operation under explicit file-format and conversion rules.

    Example:
        Exercise test render html svg workaround prefers static svg cover through a consuming regression::

            python -m pytest -q tests/file_formats/test_cover_extraction_utils.py


    :param cover_module: Value supplied for cover module under the utility contract.
    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    _write_cover(tmp_path)
    html = tmp_path / "cover.xhtml"
    html.write_bytes(
        b"""\
        <html><body>
          <svg xmlns="http://www.w3.org/2000/svg"
               xmlns:xlink="http://www.w3.org/1999/xlink">
            <image xlink:href="images/cover.png" />
          </svg>
        </body></html>
        """
    )

    assert cover_module.render_html_svg_workaround(str(html), log=None) == PNG_BYTES


def test_render_html_svg_workaround_uses_qt_renderer_when_no_static_cover(
    cover_module,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """
    Perform the test render html svg workaround uses qt renderer when no static cover operation under explicit file-format and conversion rules.

    Example:
        Exercise test render html svg workaround uses qt renderer when no static cover through a consuming regression::

            python -m pytest -q tests/file_formats/test_cover_extraction_utils.py


    :param cover_module: Value supplied for cover module under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    html = tmp_path / "chapter.xhtml"
    html.write_text("<html><body><p>Chapter text</p></body></html>", encoding="utf-8")
    fake_gui = types.ModuleType("LiuXin_alpha.surfaces.gui2")
    fake_gui.is_ok_to_use_qt = lambda: True
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.surfaces.gui2", fake_gui)
    monkeypatch.setattr(
        cover_module,
        "render_html_data",
        lambda path, width, height: f"{path}:{width}x{height}".encode("utf-8"),
    )

    assert cover_module.render_html_svg_workaround(str(html), log=None, width=123, height=456).endswith(b":123x456")


def test_gui2_compatibility_module_is_importable() -> None:
    """
    Perform the test gui2 compatibility module is importable operation under explicit file-format and conversion rules.

    Example:
        Exercise test gui2 compatibility module is importable through a consuming regression::

            python -m pytest -q tests/file_formats/test_cover_extraction_utils.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    gui2 = importlib.import_module("LiuXin_alpha.surfaces.gui2")

    assert isinstance(gui2.config, dict)
    assert gui2.is_ok_to_use_qt() is False
    assert gui2.must_use_qt() is False
    assert gui2.pixmap_to_data(None) == b""
