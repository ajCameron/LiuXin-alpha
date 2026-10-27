"""
Provide test imageops pillow fallback utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test imageops pillow fallback through a consuming regression::

        python -m pytest -q tests/utils/plugins/test_imageops_pillow_fallback.py
"""
from __future__ import annotations

from io import BytesIO

import pytest

PIL = pytest.importorskip("PIL", reason="Pillow (PIL) not installed; skipping Pillow-specific image tests")


def _png_bytes(size=(48, 32)) -> bytes:
    """
    Perform the png bytes utility operation under explicit compatibility rules.

    Example:
        Exercise  png bytes through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_imageops_pillow_fallback.py


    :param size: Value supplied for size under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from PIL import Image

    im = Image.new("RGBA", size, (0, 0, 0, 0))
    for x in range(8, size[0] - 8):
        for y in range(6, size[1] - 6):
            im.putpixel((x, y), (0, 180, 40, 255))
    bio = BytesIO()
    im.save(bio, format="PNG")
    return bio.getvalue()


def _force_pillow_backend(monkeypatch: pytest.MonkeyPatch):
    """
    Perform the force pillow backend utility operation under explicit compatibility rules.

    Example:
        Exercise  force pillow backend through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_imageops_pillow_fallback.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from LiuXin_alpha.utils.plugins.fallbacks import imageops

    # Disable external backends so we exercise the Pillow "last resort".
    monkeypatch.setattr(imageops, "_wand_image_cls", lambda: None)
    monkeypatch.setattr(imageops, "_magick_convert", lambda: None)
    return imageops


def test_imageops_pillow_fallback_remove_borders_and_grayscale(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the test imageops pillow fallback remove borders and grayscale utility operation under explicit compatibility rules.

    Example:
        Exercise test imageops pillow fallback remove borders and grayscale through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_imageops_pillow_fallback.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    imageops = _force_pillow_backend(monkeypatch)

    data = _png_bytes((64, 48))
    trimmed = imageops.remove_borders(data, fuzz=0.1)
    assert isinstance(trimmed, (bytes, bytearray))
    assert trimmed[:8] == b"\x89PNG\r\n\x1a\n"
    from PIL import Image

    with Image.open(BytesIO(trimmed)) as result:
        assert result.size == (48, 36)

    gray = imageops.grayscale(trimmed)
    assert isinstance(gray, (bytes, bytearray))
    assert gray[:8] == b"\x89PNG\r\n\x1a\n"
    with Image.open(BytesIO(gray)) as result:
        r, g, b = result.convert("RGB").getpixel((0, 0))
        assert r == g == b


def test_imageops_overlay_mutates_bytearray_canvas(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the test imageops overlay mutates bytearray canvas utility operation under explicit compatibility rules.

    Example:
        Exercise test imageops overlay mutates bytearray canvas through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_imageops_pillow_fallback.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    imageops = _force_pillow_backend(monkeypatch)

    from PIL import Image

    def encode(color, size):
        """Use solid pixels so an unchanged canvas cannot pass this regression."""
        output = BytesIO()
        Image.new("RGBA", size, color).save(output, format="PNG")
        return output.getvalue()

    base = bytearray(encode((0, 0, 255, 255), (8, 8)))
    stamp = encode((255, 0, 0, 128), (2, 2))
    assert imageops.overlay(stamp, base, 3, 4) is None
    with Image.open(BytesIO(base)) as result:
        assert result.getpixel((3, 4)) == (128, 0, 127, 255)
        assert result.getpixel((0, 0)) == (0, 0, 255, 255)


@pytest.mark.parametrize("operation", ["grayscale", "trim", "overlay"])
def test_imageops_cli_failure_falls_back_to_pillow(monkeypatch: pytest.MonkeyPatch, operation: str) -> None:
    """
    Perform the test imageops cli failure falls back to pillow utility operation under explicit compatibility rules.

    Example:
        Exercise test imageops cli failure falls back to pillow through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_imageops_pillow_fallback.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.plugins.fallbacks import imageops

    # Force Wand off, but pretend CLI exists and then fails at runtime.
    monkeypatch.setattr(imageops, "_wand_image_cls", lambda: None)
    monkeypatch.setattr(imageops, "_magick_convert", lambda: "/usr/bin/magick")

    class CP:
        """
        Provide the CP utility contract with explicit state and cleanup behavior.

        Example:
            Exercise test imageops cli failure falls back to pillow.CP through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_imageops_pillow_fallback.py
        """
        returncode = 1
        stdout = b""
        stderr = b"boom"

    monkeypatch.setattr(imageops.subprocess, "run", lambda *a, **k: CP())

    data = _png_bytes((64, 48))
    if operation == "grayscale":
        out = imageops.grayscale(data)
    elif operation == "trim":
        out = imageops.remove_borders(data, 0.1)
    else:
        out = imageops.overlay(data, data, 0, 0)
    assert isinstance(out, (bytes, bytearray))
    assert out[:8] == b"\x89PNG\r\n\x1a\n"


def test_invalid_overlay_preserves_caller_canvas(monkeypatch: pytest.MonkeyPatch) -> None:
    """Failed decoding must leave the caller's original encoded image intact."""
    from LiuXin_alpha.utils.image_tools.img_fallback import NotImage

    imageops = _force_pillow_backend(monkeypatch)
    original = _png_bytes()
    canvas = bytearray(original)
    with pytest.raises(NotImage):
        imageops.overlay(b"not an image", canvas, 0, 0)
    assert canvas == original
