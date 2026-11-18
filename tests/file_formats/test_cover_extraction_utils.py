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
    fake_gui = types.ModuleType("LiuXin_alpha.file_formats.rendering")
    fake_gui.is_ok_to_use_qt = lambda: True
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.rendering", fake_gui)
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
    rendering = importlib.import_module("LiuXin_alpha.file_formats.rendering")
    assert gui2.pixmap_to_data is rendering.pixmap_to_data

    assert isinstance(gui2.config, dict)
    assert gui2.is_ok_to_use_qt() is False
    assert gui2.must_use_qt() is False
    assert gui2.pixmap_to_data(None) == b""


def _svg_page(href: str) -> bytes:
    """
    Make an SVG cover page showing one image.

    Example:
        >>> b"cover.png" in _svg_page("cover.png")
        True


    :param href: The image's href.
    :return: The page.
    """
    return (
        '<html xmlns="http://www.w3.org/1999/xhtml"><body><svg xmlns="http://www.w3.org/2000/svg"'
        f' xmlns:xlink="http://www.w3.org/1999/xlink"><image xlink:href="{href}"/></svg></body></html>'
    ).encode("utf-8")


def _html_page(src: str) -> bytes:
    """
    Make an HTML cover page holding only a cover image.

    Example:
        >>> b'alt="cover"' in _html_page("cover.png")
        True


    :param src: The image's src.
    :return: The page.
    """
    return f'<html><body><img src="{src}" alt="cover"/></body></html>'.encode("utf-8")


@pytest.mark.parametrize("make_page", [_svg_page, _html_page], ids=["svg", "html"])
def test_cover_page_images_stay_inside_the_book(cover_module, tmp_path, make_page) -> None:
    """
    Read a cover page's image only from inside the book, as calibre does.

    Example:
        >>> test_cover_page_images_stay_inside_the_book(None, None, _svg_page)  # doctest: +SKIP


    :param cover_module: The module whose helpers are checked.
    :param tmp_path: Folder for the book and an image outside it.
    :param make_page: Makes the cover page.
    :return: None; the assertions are the check.
    """
    book = tmp_path / "book"
    text = book / "text"
    text.mkdir(parents=True)
    _write_cover(book)
    (tmp_path / "private.png").write_bytes(PNG_BYTES)
    outside = (tmp_path / "private.png").as_posix()
    extract = (
        cover_module.extract_cover_from_embedded_svg
        if make_page is _svg_page
        else cover_module.extract_calibre_cover
    )

    assert extract(make_page("../images/cover.png"), str(text), None, root=str(book)) == PNG_BYTES
    assert extract(make_page("../../private.png"), str(text), None, root=str(book)) is None
    assert extract(make_page(outside), str(text), None, root=str(book)) is None
    # Without a root, the page's own folder is the limit
    assert extract(make_page("../images/cover.png"), str(text), None) is None
    assert extract(make_page("../images/cover.png"), str(book / "images" / ".."), None) is None


def test_render_passes_the_book_folder_on(cover_module, tmp_path) -> None:
    """
    Keep images to the book folder given to ``render_html_svg_workaround``.

    Example:
        >>> test_render_passes_the_book_folder_on(None, None)  # doctest: +SKIP


    :param cover_module: The module whose helpers are checked.
    :param tmp_path: Folder for the book and an image outside it.
    :return: None; the assertions are the check.
    """
    book = tmp_path / "book"
    text = book / "text"
    text.mkdir(parents=True)
    _write_cover(book)
    (tmp_path / "private.png").write_bytes(PNG_BYTES)
    inside = text / "inside.xhtml"
    inside.write_bytes(_svg_page("../images/cover.png"))
    escaping = text / "escaping.xhtml"
    escaping.write_bytes(_svg_page("../../private.png"))

    assert cover_module.render_html_svg_workaround(str(inside), None, root=str(book)) == PNG_BYTES
    assert cover_module.extract_cover_from_embedded_svg(
        escaping.read_bytes(), str(text), None, root=str(book)
    ) is None


def test_is_existing_subpath_resolves_links(tmp_path) -> None:
    """
    Follow links before deciding whether a file is inside a folder.

    Example:
        >>> test_is_existing_subpath_resolves_links(None)  # doctest: +SKIP


    :param tmp_path: Folder for the files.
    :return: None; the assertions are the check.
    """
    from LiuXin_alpha.file_formats.utils import is_existing_subpath

    book = tmp_path / "book"
    book.mkdir()
    private = tmp_path / "private.png"
    private.write_bytes(PNG_BYTES)
    link = book / "cover.png"
    try:
        link.symlink_to(private)
    except OSError:
        pytest.skip("symbolic links are not available here")

    assert is_existing_subpath(str(private), str(tmp_path))
    assert not is_existing_subpath(str(link), str(book))
    assert not is_existing_subpath(str(book / "missing.png"), str(book))
