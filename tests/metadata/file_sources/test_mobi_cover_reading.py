"""
Check that a MOBI cover is read as the book holds it, or as calibre gives it, as the setting says.

calibre re-encodes every MOBI cover as a JPEG no larger than 1650x2200. LiuXin keeps
the image the book holds by default; the ``read_mobi_covers_as`` preference can ask
for calibre's cover, for older devices.

Example:
    Run the checks::

        python -m pytest -q tests/metadata/file_sources/test_mobi_cover_reading.py
"""

from __future__ import annotations

import io
from collections import ChainMap
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

from LiuXin_alpha.configuration import current_preferences, use_preferences
from LiuXin_alpha.metadata.file_sources import mobi
from LiuXin_alpha.metadata.file_sources.mobi_covers import (
    CALIBRE_MAXIMUM_COVER_SIZE,
    MOBI_COVERS_PREFERENCE,
    read_mobi_cover,
)
from LiuXin_alpha.metadata.upstream_calibre import mobi as calibre_mobi
from tests.file_formats.providers.test_ported_mobi_reader import EXTH, book_bytes

Image = pytest.importorskip("PIL.Image")

HTML = b"<html><body><p>Words</p></body></html>"


@contextmanager
def covers(reading: str) -> Iterator[None]:
    """
    Set the ``read_mobi_covers_as`` preference for a block, keeping every other preference.

    Example:
        >>> with covers("calibre"):
        ...     pass


    :param reading: ``original`` or ``calibre``.
    :return: An iterator that yields once, inside the block.
    """
    with use_preferences(ChainMap({MOBI_COVERS_PREFERENCE: reading}, current_preferences())):
        yield


def image(size: tuple[int, int], fmt: str, mode: str = "RGB") -> bytes:
    """
    Draw an image of a size and format.

    Example:
        >>> image((4, 4), "PNG")[:4]
        b'\\x89PNG'


    :param size: Width and height.
    :param fmt: Pillow's format name.
    :param mode: Pillow's mode; ``RGBA`` for transparency.
    :return: The image's bytes.
    """
    colour: Any = (200, 30, 30, 128) if mode == "RGBA" else (200, 30, 30)
    out = io.BytesIO()
    Image.new(mode, size, colour).save(out, fmt)
    return out.getvalue()


def opened(data: bytes) -> Any:
    """
    Open image bytes with Pillow.

    Example:
        >>> opened(image((4, 4), "PNG")).size
        (4, 4)


    :param data: The image.
    :return: The Pillow image.
    """
    return Image.open(io.BytesIO(data))


def test_the_original_is_kept_by_default() -> None:
    """
    Give the image the book holds, byte for byte, in its own format.

    Example:
        >>> test_the_original_is_kept_by_default()


    :return: None; the assertions are the check.
    """
    for fmt, name in (("PNG", "png"), ("GIF", "gif"), ("JPEG", "jpg")):
        data = image((3000, 4000) if fmt == "JPEG" else (40, 60), fmt)
        assert read_mobi_cover(data) == (name, data)


def test_calibre_s_cover_is_a_jpeg_within_1650_by_2200() -> None:
    """
    Re-encode as calibre does: a PNG with transparency becomes an opaque JPEG; a large JPEG is scaled down.

    Example:
        >>> test_calibre_s_cover_is_a_jpeg_within_1650_by_2200()


    :return: None; the assertions are the check.
    """
    fmt, data = read_mobi_cover(image((40, 60), "PNG", "RGBA"), "calibre")
    assert fmt == "jpg" and opened(data).format == "JPEG" and opened(data).mode != "RGBA"
    assert opened(data).size == (40, 60)

    fmt, data = read_mobi_cover(image((2000, 3000), "JPEG"), "calibre")
    width, height = opened(data).size
    assert fmt == "jpg" and width <= CALIBRE_MAXIMUM_COVER_SIZE[0] and height == CALIBRE_MAXIMUM_COVER_SIZE[1]


def test_calibre_leaves_a_small_jpeg_as_it_is() -> None:
    """
    Keep a JPEG already within 1650x2200 byte for byte, as calibre does.

    Example:
        >>> test_calibre_leaves_a_small_jpeg_as_it_is()


    :return: None; the assertions are the check.
    """
    small = image((600, 800), "JPEG")
    assert read_mobi_cover(small, "calibre") == ("jpg", small)


def test_an_image_that_cannot_be_decoded_is_kept() -> None:
    """
    Keep the original when calibre's cover cannot be made from it.

    Example:
        >>> test_an_image_that_cannot_be_decoded_is_kept()


    :return: None; the assertions are the check.
    """
    broken = b"\x89PNG\r\n\x1a\n" + b"not an image"
    assert read_mobi_cover(broken, "calibre") == ("png", broken)


@pytest.mark.parametrize("reading", ["original", "calibre"])
def test_both_mobi_readers_follow_the_setting(tmp_path: Path, reading: str) -> None:
    """
    Give the same cover from LiuXin's MOBI reader and calibre's port, as the setting says.

    Example:
        >>> test_both_mobi_readers_follow_the_setting(Path("/tmp"), "original")  # doctest: +SKIP


    :param tmp_path: A folder to write the book in; the port reads a named file.
    :param reading: The preference's value.
    :return: None; the assertions are the check.
    """
    png = image((40, 60), "PNG", "RGBA")
    path = tmp_path / "book.mobi"
    path.write_bytes(book_bytes(HTML, exth=EXTH, image=png))
    with covers(reading), path.open("rb") as first, path.open("rb") as second:
        liuxin = mobi.get_metadata(first)
        port = calibre_mobi.get_metadata(second)
    liuxin_cover = liuxin.to_calibre().cover_data if hasattr(liuxin, "to_calibre") else liuxin.cover_data
    assert liuxin_cover == port.cover_data
    if reading == "original":
        assert port.cover_data == ("png", png)
    else:
        assert port.cover_data[0] == "jpg" and opened(port.cover_data[1]).format == "JPEG"
