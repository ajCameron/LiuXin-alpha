# -*- coding: utf-8 -*-
"""
Provide imageops alt fallback (3) utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise imageops alt fallback (3) through a consuming regression::

        python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
"""

from __future__ import annotations

import shutil
import subprocess
from typing import Optional


def _convert_cmd() -> Optional[list]:
    """
    Generate the base of the conversion command.

    Example:
        Exercise  convert cmd through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    magick = shutil.which("magick")
    if magick:
        return [magick, "convert"]
    convert = shutil.which("convert")
    if convert:
        return [convert]
    return None


def _is_qimage(obj: Any) -> bool:
    """
    Perform the is qimage utility operation under explicit compatibility rules.

    Example:
        Exercise  is qimage through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param obj: Value supplied for obj under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return hasattr(obj, "isNull") and hasattr(obj, "save") and hasattr(obj, "width") and hasattr(obj, "height")


def _qimage_to_png_bytes(img: Any) -> bytes:
    # Avoid importing Qt modules; rely on duck typing.
    """
    Perform the qimage to png bytes utility operation under explicit compatibility rules.

    Example:
        Exercise  qimage to png bytes through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param img: Value supplied for img under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    bio = BytesIO()
    ok = False
    try:
        ok = bool(img.save(bio, "PNG"))
    except Exception:
        ok = False
    if not ok:
        raise ValueError("Failed to serialize QImage to PNG bytes")
    return bio.getvalue()


def _pil_resize_bytes(data: bytes, width: int, height: int, fmt: str) -> bytes:
    """
    Perform the pil resize bytes utility operation under explicit compatibility rules.

    Example:
        Exercise  pil resize bytes through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param data: Value supplied for data under the utility contract.
    :param width: Value supplied for width under the utility contract.
    :param height: Value supplied for height under the utility contract.
    :param fmt: Date, number or template format specification.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    try:
        from PIL import Image  # type: ignore
    except Exception as e:  # pragma: no cover
        raise RuntimeError("No ImageMagick backend found and Pillow is not installed") from e

    fmt2 = (fmt or "png").lower().strip()
    if fmt2 == "jpg":
        fmt2 = "jpeg"

    with Image.open(BytesIO(data)) as im:
        im.load()
        w = max(1, int(width))
        h = max(1, int(height))
        resample = getattr(getattr(Image, "Resampling", Image), "LANCZOS", getattr(Image, "LANCZOS", 1))
        im2 = im.resize((w, h), resample=resample)

        out = BytesIO()
        if fmt2 in ("jpeg", "jpe"):
            if im2.mode in ("RGBA", "LA") or (im2.mode == "P" and "transparency" in im2.info):
                bg = Image.new("RGB", im2.size, (255, 255, 255))
                bg.paste(im2.convert("RGBA"), mask=im2.convert("RGBA").split()[-1])
                im2 = bg
            else:
                im2 = im2.convert("RGB")
            im2.save(out, format="JPEG", quality=85, optimize=True, progressive=True)
        elif fmt2 in ("ppm", "pnm"):
            im2.convert("RGB").save(out, format="PPM")
        else:
            im2.save(out, format="PNG")
        return out.getvalue()


def resize(data: Any, width: int, height: int, fmt: str = "png") -> bytes:
    """
    Resize image bytes to (width, height) and return encoded bytes in ``fmt``.

    Example:
        Exercise resize through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param data: Value supplied for data under the utility contract.
    :param width: Value supplied for width under the utility contract.
    :param height: Value supplied for height under the utility contract.
    :param fmt: Date, number or template format specification.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if _is_qimage(data):
        data = _qimage_to_png_bytes(data)

    if not isinstance(data, (bytes, bytearray, memoryview)):
        raise TypeError("resize() expects bytes-like data or a QImage-like object")
    b = bytes(data)

    cmd = _convert_cmd()
    if not cmd:
        return _pil_resize_bytes(b, width, height, fmt)

    w = int(width)
    h = int(height)
    fmt2 = str(fmt).lower().strip() or "png"
    full = cmd + ["-resize", f"{w}x{h}", f"{fmt2}:-"]
    cp = subprocess.run(full, input=b, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    if cp.returncode != 0 or not (cp.stdout or b""):
        # Fall back to Pillow, keeping the original CLI error in case Pillow also fails.
        cli_msg = (cp.stderr or b"").decode("utf-8", "ignore").strip() if cp is not None else ""
        try:
            return _pil_resize_bytes(b, width, height, fmt)
        except Exception as e:
            raise RuntimeError(cli_msg or f"convert failed with code {cp.returncode}") from e

    return bytes(cp.stdout or b"")
