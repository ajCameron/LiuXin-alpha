"""
Provide test img pillow fallback utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test img pillow fallback through a consuming regression::

        python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py
"""
from __future__ import annotations

from io import BytesIO

import pytest

PIL = pytest.importorskip("PIL", reason="Pillow (PIL) not installed; skipping Pillow-specific image tests")


def _make_rgba_png_bytes(size=(64, 48)) -> bytes:
    """
    Perform the make rgba png bytes utility operation under explicit compatibility rules.

    Example:
        Exercise  make rgba png bytes through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param size: Value supplied for size under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from PIL import Image

    im = Image.new("RGBA", size, (0, 0, 0, 0))
    # draw a solid rectangle inside to make border trimming meaningful
    for x in range(10, size[0] - 10):
        for y in range(8, size[1] - 8):
            im.putpixel((x, y), (200, 10, 10, 255))
    bio = BytesIO()
    im.save(bio, format="PNG")
    return bio.getvalue()


def test_import_does_not_require_pyqt5() -> None:
    # PyQt5 is intentionally not installed in CI for LiuXin-alpha
    """
    Perform the test import does not require pyqt5 utility operation under explicit compatibility rules.

    Example:
        Exercise test import does not require pyqt5 through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    import importlib.util

    assert importlib.util.find_spec("PyQt5") is None
    from LiuXin_alpha.utils.image_tools import img as img_mod

    assert hasattr(img_mod, "save_cover_data_to")


def test_scale_image_returns_expected_dimensions_and_bytes() -> None:
    """
    Perform the test scale image returns expected dimensions and bytes utility operation under explicit compatibility rules.

    Example:
        Exercise test scale image returns expected dimensions and bytes through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.image_tools.img import scale_image

    data = _make_rgba_png_bytes((200, 100))
    w, h, out = scale_image(data, width=60, height=60, as_png=True)
    assert w <= 60 and h <= 60
    assert isinstance(out, (bytes, bytearray))
    assert out[:8] == b"\x89PNG\r\n\x1a\n"


def test_save_cover_data_to_blends_alpha_and_converts_to_jpeg() -> None:
    """
    Perform the test save cover data to blends alpha and converts to jpeg utility operation under explicit compatibility rules.

    Example:
        Exercise test save cover data to blends alpha and converts to jpeg through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.image_tools.img import save_cover_data_to

    data = _make_rgba_png_bytes((80, 80))
    out = save_cover_data_to(data, path=None, data_fmt="jpeg", return_data=True)
    assert out is not None
    # JPEG magic bytes
    assert out[:2] == b"\xff\xd8"


def test_remove_borders_from_image_trims() -> None:
    """
    Perform the test remove borders from image trims utility operation under explicit compatibility rules.

    Example:
        Exercise test remove borders from image trims through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.image_tools.img import image_from_data, remove_borders_from_image

    data = _make_rgba_png_bytes((100, 90))
    im = image_from_data(data)
    trimmed = remove_borders_from_image(im, fuzz=5)
    # We created a 10/8px border, so expect smaller
    assert trimmed.size[0] < im.size[0]
    assert trimmed.size[1] < im.size[1]


def test_quantize_image_does_not_crash() -> None:
    """
    Perform the test quantize image does not crash utility operation under explicit compatibility rules.

    Example:
        Exercise test quantize image does not crash through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.image_tools.img import image_from_data, quantize_image

    data = _make_rgba_png_bytes((64, 48))
    im = image_from_data(data)
    q = quantize_image(im, max_colors=16)
    # Pillow quantize returns mode 'P'
    assert getattr(q, "mode", None) in ("P", "RGB")
