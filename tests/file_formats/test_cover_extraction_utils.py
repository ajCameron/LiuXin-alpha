from __future__ import annotations

import importlib
import sys
import types

import pytest


PNG_BYTES = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"


@pytest.fixture(params=["LiuXin_alpha.file_formats", "LiuXin_alpha.file_formats.utils"])
def cover_module(request):
    return importlib.import_module(request.param)


def _write_cover(tmp_path):
    image_dir = tmp_path / "images"
    image_dir.mkdir()
    cover = image_dir / "cover.png"
    cover.write_bytes(PNG_BYTES)
    return cover


def test_return_raster_image_reads_supported_images_only(cover_module, tmp_path) -> None:
    cover = _write_cover(tmp_path)
    text = tmp_path / "not-an-image.txt"
    text.write_text("not an image", encoding="utf-8")

    assert cover_module.return_raster_image(str(cover)) == PNG_BYTES
    assert cover_module.return_raster_image(str(text)) is None
    assert cover_module.return_raster_image(str(tmp_path / "missing.png")) is None


def test_extract_cover_from_embedded_svg_reads_nested_raster(cover_module, tmp_path) -> None:
    _write_cover(tmp_path)
    raw = b"""\
    <svg xmlns="http://www.w3.org/2000/svg"
         xmlns:xlink="http://www.w3.org/1999/xlink">
      <image xlink:href="images/cover.png" />
    </svg>
    """

    assert cover_module.extract_cover_from_embedded_svg(raw, str(tmp_path), log=None) == PNG_BYTES


def test_extract_calibre_cover_accepts_cover_alt_image(cover_module, tmp_path) -> None:
    _write_cover(tmp_path)
    raw = b'<html><body><img alt="cover" src="images/cover.png" /></body></html>'

    assert cover_module.extract_calibre_cover(raw, str(tmp_path), log=None) == PNG_BYTES


def test_extract_calibre_cover_accepts_body_with_single_image_and_no_text(cover_module, tmp_path) -> None:
    _write_cover(tmp_path)
    raw = b'<html><body> \n <img src="images/cover.png" /> \n </body></html>'

    assert cover_module.extract_calibre_cover(raw, str(tmp_path), log=None) == PNG_BYTES


def test_extract_calibre_cover_ignores_body_with_text(cover_module, tmp_path) -> None:
    _write_cover(tmp_path)
    raw = b'<html><body>Text before image <img src="images/cover.png" /></body></html>'

    assert cover_module.extract_calibre_cover(raw, str(tmp_path), log=None) is None


def test_render_html_svg_workaround_prefers_static_svg_cover(cover_module, tmp_path) -> None:
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
