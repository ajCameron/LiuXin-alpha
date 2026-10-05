# -*- coding: utf-8 -*-
"""
Provide imageops pillow utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise imageops pillow through a consuming regression::

        python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
"""

from __future__ import annotations

from io import BytesIO
from typing import Any, Iterable, Optional, Sequence, Tuple, Union


BytesLike = Union[bytes, bytearray, memoryview]


def _pillow():
    """
    Perform the pillow utility operation under explicit compatibility rules.

    Example:
        Exercise  pillow through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    try:
        from PIL import Image  # type: ignore

        return Image
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "Pillow (PIL) is required for the pure-python imageops fallback. Install with: pip install pillow"
        ) from e


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


def _qimage_to_png_bytes(qimg: Any) -> bytes:
    """
    Perform the qimage to png bytes utility operation under explicit compatibility rules.

    Example:
        Exercise  qimage to png bytes through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param qimg: Value supplied for qimg under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    for qt_pkg in ("PyQt6", "PyQt5", "PySide6", "PySide2"):
        try:
            QtCore = __import__(f"{qt_pkg}.QtCore", fromlist=["QtCore"])
            QBuffer = QtCore.QBuffer
            QIODevice = QtCore.QIODevice
            buf = QBuffer()
            mode = QIODevice.OpenModeFlag.WriteOnly if hasattr(QIODevice, "OpenModeFlag") else QIODevice.WriteOnly
            buf.open(mode)
            ok = qimg.save(buf, "PNG")
            if not ok:
                raise ValueError("QImage.save() failed")
            data = bytes(buf.data())
            buf.close()
            return data
        except Exception:
            continue
    raise RuntimeError("Qt bindings not available to serialize QImage")


def _png_bytes_to_qimage(data: bytes) -> Any:
    """
    Perform the png bytes to qimage utility operation under explicit compatibility rules.

    Example:
        Exercise  png bytes to qimage through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param data: Value supplied for data under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    for qt_pkg in ("PyQt6", "PyQt5", "PySide6", "PySide2"):
        try:
            QtGui = __import__(f"{qt_pkg}.QtGui", fromlist=["QtGui"])
            QImage = QtGui.QImage
            img = QImage.fromData(data, "PNG")
            if img is None or img.isNull():
                raise ValueError("QImage.fromData failed")
            return img
        except Exception:
            continue
    raise RuntimeError("Qt bindings not available to deserialize QImage")


class _Coerced:
    """
    Provide the Coerced utility contract with explicit state and cleanup behavior.

    Example:
        Exercise  Coerced through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
    """
    __slots__ = ("kind", "data", "original")

    def __init__(self, kind: str, data: bytes, original: Any):
        """
        Initialize and validate the Coerced state.

        Example:
            Exercise  Coerced.  init   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :param kind: Value supplied for kind under the utility contract.
        :param data: Value supplied for data under the utility contract.
        :param original: Value supplied for original under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.kind = kind  # "qimage" | "bytes" | "bytearray"
        self.data = data
        self.original = original


def _coerce_in(image: Any) -> _Coerced:
    """
    Coerce in under the documented compatibility and safety rules.

    Example:
        Exercise  coerce in through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param image: Value supplied for image under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if _is_qimage(image):
        if image.isNull():
            raise ValueError("Cannot operate on null QImage")
        return _Coerced("qimage", _qimage_to_png_bytes(image), image)
    if isinstance(image, (bytes, bytearray, memoryview)):
        b = bytes(image)
        kind = "bytearray" if isinstance(image, bytearray) else "bytes"
        return _Coerced(kind, b, image)
    raise TypeError("image must be a QImage (PyQt/PySide) or encoded image bytes/bytearray")


def _restore_out(out_png: bytes, c: _Coerced) -> Any:
    """
    Perform the restore out utility operation under explicit compatibility rules.

    Example:
        Exercise  restore out through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param out_png: Value supplied for out png under the utility contract.
    :param c: Value supplied for c under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if c.kind == "qimage":
        return _png_bytes_to_qimage(out_png)
    if c.kind == "bytearray":
        ba: bytearray = c.original
        ba[:] = out_png
        return None
    return out_png


def _open(inp: bytes) -> Any:
    """
    Perform the open utility operation under explicit compatibility rules.

    Example:
        Exercise  open through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param inp: Value supplied for inp under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    Image = _pillow()
    im = Image.open(BytesIO(inp))
    im.load()
    return im


def _encode_png(im: Any) -> bytes:
    """
    Perform the encode png utility operation under explicit compatibility rules.

    Example:
        Exercise  encode png through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param im: Value supplied for im under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    out = BytesIO()
    im.save(out, format="PNG")
    return out.getvalue()


def _parse_color(s: str) -> Tuple[int, int, int]:
    """
    Parse color under the documented compatibility and safety rules.

    Example:
        Exercise  parse color through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param s: Value supplied for s under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    s = (s or "#ffffff").strip()
    if s.startswith("#"):
        s = s[1:]
    if len(s) == 3:
        s = "".join(ch * 2 for ch in s)
    if len(s) != 6:
        return (255, 255, 255)
    try:
        return (int(s[0:2], 16), int(s[2:4], 16), int(s[4:6], 16))
    except Exception:
        return (255, 255, 255)


def _has_alpha(im: Any) -> bool:
    """
    Perform the has alpha utility operation under explicit compatibility rules.

    Example:
        Exercise  has alpha through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param im: Value supplied for im under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if getattr(im, "mode", "") in ("RGBA", "LA"):
        return True
    if getattr(im, "mode", "") == "P" and "transparency" in getattr(im, "info", {}):
        return True
    return False


def _blend_alpha(im: Any, bgcolor: str = "#ffffff") -> Any:
    """
    Perform the blend alpha utility operation under explicit compatibility rules.

    Example:
        Exercise  blend alpha through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param im: Value supplied for im under the utility contract.
    :param bgcolor: Value supplied for bgcolor under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    Image = _pillow()
    if not _has_alpha(im):
        return im
    rgb = _parse_color(bgcolor)
    bg = Image.new("RGBA", im.size, rgb + (255,))
    bg.alpha_composite(im.convert("RGBA"))
    return bg.convert("RGB")


def _trim_borders(im: Any, fuzz: int) -> Any:
    """
    Perform the trim borders utility operation under explicit compatibility rules.

    Example:
        Exercise  trim borders through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param im: Value supplied for im under the utility contract.
    :param fuzz: Value supplied for fuzz under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    src = im.convert("RGBA") if _has_alpha(im) else im.convert("RGB")
    w, h = src.size
    if w <= 2 or h <= 2:
        return im
    px = src.load()
    corners = [px[0, 0], px[w - 1, 0], px[0, h - 1], px[w - 1, h - 1]]
    corners_rgb = [(c[0], c[1], c[2]) for c in corners]
    bg = tuple(int(sum(c[i] for c in corners_rgb) / 4) for i in range(3))

    def is_bg(p: Any) -> bool:
        """
        Return or update whether is bg holds for the compatibility value.

        Example:
            Exercise  trim borders.is bg through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :param p: Path-like value normalized or validated by the operation.
        :return: True when the documented condition holds; otherwise False.
        """
        r, g, b = p[0], p[1], p[2]
        return max(abs(r - bg[0]), abs(g - bg[1]), abs(b - bg[2])) <= fuzz

    top = 0
    for y in range(h):
        if any(not is_bg(px[x, y]) for x in range(w)):
            top = y
            break
    bottom = h - 1
    for y in range(h - 1, -1, -1):
        if any(not is_bg(px[x, y]) for x in range(w)):
            bottom = y
            break
    left = 0
    for x in range(w):
        if any(not is_bg(px[x, y]) for y in range(top, bottom + 1)):
            left = x
            break
    right = w - 1
    for x in range(w - 1, -1, -1):
        if any(not is_bg(px[x, y]) for y in range(top, bottom + 1)):
            right = x
            break

    if right <= left or bottom <= top:
        return im
    if left == 0 and top == 0 and right == w - 1 and bottom == h - 1:
        return im
    return im.crop((left, top, right + 1, bottom + 1))


# ---------------- public API ----------------


def remove_borders(image: Any, fuzz: float) -> Any:
    """
    Remove borders under the documented compatibility and safety rules.

    Example:
        Exercise remove borders through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param image: Value supplied for image under the utility contract.
    :param fuzz: Value supplied for fuzz under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    inp = _coerce_in(image)
    im = _open(inp.data)
    out = _trim_borders(im, max(0, int(fuzz)))
    return _restore_out(_encode_png(out), inp)


def grayscale(image: Any) -> Any:
    """
    Perform the grayscale utility operation under explicit compatibility rules.

    Example:
        Exercise grayscale through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param image: Value supplied for image under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from PIL import ImageOps  # type: ignore

    inp = _coerce_in(image)
    im = _open(inp.data)
    out = ImageOps.grayscale(im)
    return _restore_out(_encode_png(out), inp)


def gaussian_sharpen(image: Any, radius: int, sigma: float, high_quality: bool) -> Any:
    """
    Perform the gaussian sharpen utility operation under explicit compatibility rules.

    Example:
        Exercise gaussian sharpen through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param image: Value supplied for image under the utility contract.
    :param radius: Value supplied for radius under the utility contract.
    :param sigma: Value supplied for sigma under the utility contract.
    :param high_quality: Value supplied for high quality under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from PIL import ImageFilter  # type: ignore

    inp = _coerce_in(image)
    im = _open(inp.data)
    r = max(0, int(radius) if radius else int(max(1, round(float(sigma)))))
    out = im.filter(ImageFilter.UnsharpMask(radius=r, percent=150 if high_quality else 80, threshold=3))
    return _restore_out(_encode_png(out), inp)


def gaussian_blur(image: Any, radius: int, sigma: float) -> Any:
    """
    Perform the gaussian blur utility operation under explicit compatibility rules.

    Example:
        Exercise gaussian blur through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param image: Value supplied for image under the utility contract.
    :param radius: Value supplied for radius under the utility contract.
    :param sigma: Value supplied for sigma under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from PIL import ImageFilter  # type: ignore

    inp = _coerce_in(image)
    im = _open(inp.data)
    r = float(sigma) if radius in (-1, 0) else float(radius)
    out = im.filter(ImageFilter.GaussianBlur(radius=r))
    return _restore_out(_encode_png(out), inp)


def despeckle(image: Any) -> Any:
    """
    Perform the despeckle utility operation under explicit compatibility rules.

    Example:
        Exercise despeckle through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param image: Value supplied for image under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from PIL import ImageFilter  # type: ignore

    inp = _coerce_in(image)
    im = _open(inp.data)
    out = im.filter(ImageFilter.MedianFilter(size=3))
    return _restore_out(_encode_png(out), inp)


def overlay(img: Any, canvas: Any, left: int, top: int) -> None:
    """
    Overlay img onto canvas. Mutates *canvas* if it is a QImage or bytearray.

    Example:
        Exercise overlay through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param img: Value supplied for img under the utility contract.
    :param canvas: Value supplied for canvas under the utility contract.
    :param left: Value supplied for left under the utility contract.
    :param top: Value supplied for top under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    a = _coerce_in(img)
    b = _coerce_in(canvas)
    base = _open(b.data)
    src = _open(a.data)

    left, top = int(left), int(top)
    if _has_alpha(src):
        base = base.convert("RGBA")
        base.alpha_composite(src.convert("RGBA"), (left, top))
        out = base
    else:
        base.paste(src.convert("RGB"), (left, top))
        out = base

    out_png = _encode_png(out)
    _restore_out(out_png, b)
    return None


def normalize(image: Any) -> Any:
    """
    Perform the normalize utility operation under explicit compatibility rules.

    Example:
        Exercise normalize through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param image: Value supplied for image under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from PIL import ImageOps  # type: ignore

    inp = _coerce_in(image)
    im = _open(inp.data)
    out = ImageOps.autocontrast(im)
    return _restore_out(_encode_png(out), inp)


def oil_paint(image: Any, radius: int, high_quality: bool) -> Any:
    """
    Perform the oil paint utility operation under explicit compatibility rules.

    Example:
        Exercise oil paint through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param image: Value supplied for image under the utility contract.
    :param radius: Value supplied for radius under the utility contract.
    :param high_quality: Value supplied for high quality under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from PIL import ImageFilter  # type: ignore

    inp = _coerce_in(image)
    im = _open(inp.data)
    r = 3 if radius in (-1, 0) else max(1, int(radius))
    out = im.filter(ImageFilter.ModeFilter(size=r))
    return _restore_out(_encode_png(out), inp)


def quantize(image: Any, max_colors: int, dither: bool, palette: Sequence[int]) -> Any:
    """
    Perform the quantize utility operation under explicit compatibility rules.

    Example:
        Exercise quantize through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param image: Value supplied for image under the utility contract.
    :param max_colors: Value supplied for max colors under the utility contract.
    :param dither: Value supplied for dither under the utility contract.
    :param palette: Value supplied for palette under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    Image = _pillow()
    inp = _coerce_in(image)
    im = _open(inp.data)
    im = _blend_alpha(im)
    colors = int(max(2, min(256, max_colors)))
    dither_flag = Image.Dither.FLOYDSTEINBERG if dither else Image.Dither.NONE

    if palette:
        pal_img = Image.new("P", (1, 1))
        # palette is a list of ints (Qt rgb()) in calibre; interpret as 0xAARRGGBB or 0x00RRGGBB.
        rgb_list: list[int] = []
        for qrgb in palette:
            rr = (qrgb >> 16) & 0xFF
            gg = (qrgb >> 8) & 0xFF
            bb = (qrgb >> 0) & 0xFF
            rgb_list.extend([rr, gg, bb])
        rgb_list.extend([0] * (768 - len(rgb_list)))
        pal_img.putpalette(rgb_list[:768])
        out = im.convert("RGB").quantize(palette=pal_img, dither=dither_flag)
    else:
        method = getattr(getattr(Image, "Quantize", None), "MEDIANCUT", 0)
        out = im.convert("RGB").quantize(colors=colors, method=method, dither=dither_flag)

    return _restore_out(_encode_png(out), inp)


def has_transparent_pixels(image: Any) -> bool:
    """
    Return or update whether has transparent pixels holds for the compatibility value.

    Example:
        Exercise has transparent pixels through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param image: Value supplied for image under the utility contract.
    :return: True when the documented condition holds; otherwise False.
    """
    inp = _coerce_in(image)
    im = _open(inp.data)
    if not _has_alpha(im):
        return False
    a = im.convert("RGBA").getchannel("A")
    lo, hi = a.getextrema()
    return lo < 255


def set_opacity(image: Any, alpha: float) -> Any:
    """
    Set opacity under the documented compatibility and safety rules.

    Example:
        Exercise set opacity through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param image: Value supplied for image under the utility contract.
    :param alpha: Value supplied for alpha under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    inp = _coerce_in(image)
    im = _open(inp.data)
    if not _has_alpha(im):
        im = im.convert("RGBA")
    a = im.getchannel("A")
    a = a.point(lambda v: int(max(0, min(255, round(v * float(alpha))))))
    im.putalpha(a)
    return _restore_out(_encode_png(im), inp)


def texture_image(canvas: Any, texture: Any) -> Any:
    """
    Perform the texture image utility operation under explicit compatibility rules.

    Example:
        Exercise texture image through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param canvas: Value supplied for canvas under the utility contract.
    :param texture: Value supplied for texture under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    a = _coerce_in(canvas)
    b = _coerce_in(texture)
    base = _open(a.data)
    tex = _open(b.data)
    base = _blend_alpha(base)
    tex_rgba = tex.convert("RGBA") if _has_alpha(tex) else tex.convert("RGB")

    bw, bh = base.size
    tw, th = tex_rgba.size
    if tw <= 0 or th <= 0:
        return _restore_out(_encode_png(base), a)

    out = base.convert("RGBA")
    for y in range(0, bh, th):
        for x in range(0, bw, tw):
            if tex_rgba.mode == "RGBA":
                out.alpha_composite(tex_rgba, (x, y))
            else:
                out.paste(tex_rgba, (x, y))
    return _restore_out(_encode_png(out.convert("RGB")), a)

def _encode_any(im: Any, fmt: str = "png", *, jpeg_quality: int = 85) -> bytes:
    """
    Encode a PIL image into the requested format.

    Example:
        Exercise  encode any through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param im: Value supplied for im under the utility contract.
    :param fmt: Date, number or template format specification.
    :param jpeg_quality: Value supplied for jpeg quality under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    fmt = (fmt or "png").lower().strip()
    if fmt == "jpg":
        fmt = "jpeg"
    out = BytesIO()
    if fmt in ("jpeg", "jpe"):
        # JPEG can't store alpha; blend onto white if needed.
        if _has_alpha(im):
            im = _blend_alpha(im, (255, 255, 255))
        im = im.convert("RGB")
        im.save(out, format="JPEG", quality=int(jpeg_quality), optimize=True, progressive=True)
    elif fmt in ("ppm", "pnm"):
        im = im.convert("RGB")
        im.save(out, format="PPM")
    else:
        # Default to PNG
        im.save(out, format="PNG")
    return out.getvalue()


def resize(data: Any, width: int, height: int, fmt: str = "png") -> Any:
    """
    Resize an image to (width, height). Accepts bytes or QImage.

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
    inp = _coerce_in(data)
    im = _open(inp.data)
    w = max(1, int(width))
    h = max(1, int(height))
    # Pillow compatibility for resampling constant naming
    Image = _pillow()
    resample = getattr(getattr(Image, "Resampling", Image), "LANCZOS", getattr(Image, "LANCZOS", 1))
    im2 = im.resize((w, h), resample=resample)
    out_bytes = _encode_any(im2, fmt)
    return _restore_out(out_bytes, inp)
