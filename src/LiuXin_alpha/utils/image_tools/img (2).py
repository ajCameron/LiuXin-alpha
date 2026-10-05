"""
Retain the alternate legacy image-processing implementation for compatibility review.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise img (2) through a consuming regression::

        python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from io import BytesIO
from typing import Any, Iterable, Optional, Sequence, Tuple, Union


BytesLike = Union[bytes, bytearray, memoryview]


class NotImage(ValueError):
    """
    Raised when supplied data cannot be parsed as an image.

    Example:
        Exercise NotImage through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py
    """


def _pillow() -> Any:
    """
    Perform the pillow utility operation under explicit compatibility rules.

    Example:
        Exercise  pillow through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    try:
        from PIL import Image  # type: ignore

        return Image
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "Pillow (PIL) is required for image operations when Qt is not available. "
            "Install with: pip install pillow"
        ) from e


def normalize_format_name(fmt: str) -> str:
    """
    Normalize an image format name.

    Example:
        Exercise normalize format name through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param fmt: Date, number or template format specification.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    fmt = (fmt or "").lower()
    if fmt == "jpg":
        fmt = "jpeg"
    return fmt


def fit_image(owidth: int, oheight: int, max_width: int, max_height: int) -> Tuple[bool, int, int]:
    """
    Compute a new size that fits within max_width/max_height, preserving aspect.

    Example:
        Exercise fit image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param owidth: Value supplied for owidth under the utility contract.
    :param oheight: Value supplied for oheight under the utility contract.
    :param max_width: Value supplied for max width under the utility contract.
    :param max_height: Value supplied for max height under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    if owidth <= 0 or oheight <= 0:
        return False, max(0, int(max_width)), max(0, int(max_height))
    max_width = int(max_width)
    max_height = int(max_height)
    if owidth <= max_width and oheight <= max_height:
        return False, int(owidth), int(oheight)
    ratio = min(max_width / float(owidth), max_height / float(oheight))
    nw = max(1, int(round(owidth * ratio)))
    nh = max(1, int(round(oheight * ratio)))
    return True, nw, nh


def _is_qimage(obj: Any) -> bool:
    # Avoid importing Qt unless needed.
    """
    Perform the is qimage utility operation under explicit compatibility rules.

    Example:
        Exercise  is qimage through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


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

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


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

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


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


def null_image() -> Any:
    """
    Create an invalid/empty image object (best-effort).

    Example:
        Exercise null image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    # Prefer a real QImage if Qt is present
    try:
        for qt_pkg in ("PyQt6", "PyQt5", "PySide6", "PySide2"):
            try:
                QtGui = __import__(f"{qt_pkg}.QtGui", fromlist=["QtGui"])
                return QtGui.QImage()
            except Exception:
                continue
    except Exception:
        pass

    # Pillow cannot represent a truly "null" image, so use a 1x1 transparent pixel.
    Image = _pillow()
    return Image.new("RGBA", (1, 1), (0, 0, 0, 0))


def _pil_open_bytes(data: bytes) -> Any:
    """
    Perform the pil open bytes utility operation under explicit compatibility rules.

    Example:
        Exercise  pil open bytes through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param data: Value supplied for data under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    Image = _pillow()
    try:
        im = Image.open(BytesIO(data))
        im.load()  # force decode now
        return im
    except Exception as e:
        raise NotImage("Not a valid image") from e


def image_from_data(data: Any) -> Any:
    """
    Create an image object from bytes/QImage/PIL.Image.

    Example:
        Exercise image from data through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param data: Value supplied for data under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    if _is_qimage(data):
        if data.isNull():
            raise NotImage("Not a valid image")
        return data
    # Pillow Image
    if hasattr(data, "save") and hasattr(data, "size") and hasattr(data, "mode"):
        return data
    if isinstance(data, (bytes, bytearray, memoryview)):
        return _pil_open_bytes(bytes(data))
    raise TypeError(f"Unknown image src type: {type(data)!r}")


def image_from_path(path: str) -> Any:
    """
    Perform the image from path utility operation under explicit compatibility rules.

    Example:
        Exercise image from path through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    with open(path, "rb") as f:
        return image_from_data(f.read())


def image_from_x(x: Any) -> Any:
    """
    Create an image from a bytestring or a path or a file-like object.

    Example:
        Exercise image from x through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param x: Value supplied for x under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    if isinstance(x, str):
        return image_from_path(x)
    if hasattr(x, "read"):
        return image_from_data(x.read())
    if isinstance(x, (bytes, bytearray, memoryview)) or _is_qimage(x):
        return image_from_data(x)
    # Pillow Image
    if hasattr(x, "save") and hasattr(x, "size") and hasattr(x, "mode"):
        return x
    raise TypeError(f"Unknown image src type: {type(x)!r}")


def image_and_format_from_data(data: BytesLike) -> Tuple[Any, str]:
    """
    Return (image_object, format_string).

    Example:
        Exercise image and format from data through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param data: Value supplied for data under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    im = _pil_open_bytes(bytes(data))
    fmt = normalize_format_name(getattr(im, "format", "") or "")
    return im, fmt


def _parse_color(bgcolor: str) -> Tuple[int, int, int]:
    """
    Parse color under the documented compatibility and safety rules.

    Example:
        Exercise  parse color through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param bgcolor: Value supplied for bgcolor under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    s = (bgcolor or "#ffffff").strip()
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


def _ensure_pil(img: Any) -> Any:
    """
    Ensure pil under the documented compatibility and safety rules.

    Example:
        Exercise  ensure pil through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if _is_qimage(img):
        return _pil_open_bytes(_qimage_to_png_bytes(img))
    if hasattr(img, "save") and hasattr(img, "size") and hasattr(img, "mode"):
        return img
    return image_from_data(img)


def _resample() -> int:
    """
    Perform the resample utility operation under explicit compatibility rules.

    Example:
        Exercise  resample through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    Image = _pillow()
    # Pillow 10 renamed enums; keep compatibility
    return getattr(Image, "Resampling", Image).LANCZOS if hasattr(getattr(Image, "Resampling", None), "LANCZOS") else Image.LANCZOS


def image_to_data(
    img: Any,
    compression_quality: int = 95,
    fmt: str = "JPEG",
    png_compression_level: int = 9,
    jpeg_optimized: bool = True,
    jpeg_progressive: bool = False,
) -> bytes:
    """
    Serialize image to bytes in the specified format.

    Example:
        Exercise image to data through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :param compression_quality: Value supplied for compression quality under the utility
        contract.
    :param fmt: Date, number or template format specification.
    :param png_compression_level: Value supplied for png compression level under the
        utility contract.
    :param jpeg_optimized: Value supplied for jpeg optimized under the utility contract.
    :param jpeg_progressive: Value supplied for jpeg progressive under the utility
        contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    Image = _pillow()
    im = _ensure_pil(img)
    fmt_u = (fmt or "JPEG").upper()
    out = BytesIO()

    # Handle alpha for JPEG
    if fmt_u in {"JPG", "JPEG"}:
        if im.mode in ("RGBA", "LA") or (im.mode == "P" and "transparency" in im.info):
            im = blend_image(im)
        save_kw = {
            "format": "JPEG",
            "quality": int(max(0, min(100, compression_quality))),
            "optimize": bool(jpeg_optimized),
            "progressive": bool(jpeg_progressive),
        }
        im = im.convert("RGB")
        im.save(out, **save_kw)

    elif fmt_u == "PNG":
        # Pillow uses 0-9 compression
        cl = int(max(0, min(9, png_compression_level)))
        im.save(out, format="PNG", compress_level=cl)

    elif fmt_u == "GIF":
        # "Close enough": let Pillow handle palette and transparency.
        im.save(out, format="GIF")

    else:
        # Try Pillow's format mapping
        try:
            im.save(out, format=fmt_u)
        except Exception as e:
            raise ValueError(f"Failed to export image as {fmt_u}") from e

    return out.getvalue()


def save_image(img: Any, path: str, **kw: Any) -> None:
    """
    Save image to path, inferring format from extension.

    Example:
        Exercise save image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :param kw: Value supplied for kw under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """

    fmt = os.path.splitext(path)[1].lstrip(".")
    kw["fmt"] = kw.get("fmt", fmt)
    with open(path, "wb") as f:
        f.write(image_to_data(image_from_data(img), **kw))


def save_cover_data_to(
    data: bytes,
    path: Optional[str] = None,
    bgcolor: str = "#ffffff",
    resize_to: Optional[Tuple[int, int]] = None,
    compression_quality: int = 90,
    minify_to: Optional[Tuple[int, int]] = None,
    grayscale: bool = False,
    data_fmt: str = "jpeg",
    return_data: bool = False,
) -> Optional[bytes]:
    """
    Save cover image data to *path* (or return bytes).

    Example:
        Exercise save cover data to through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param data: Value supplied for data under the utility contract.
    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :param bgcolor: Value supplied for bgcolor under the utility contract.
    :param resize_to: Value supplied for resize to under the utility contract.
    :param compression_quality: Value supplied for compression quality under the utility
        contract.
    :param minify_to: Value supplied for minify to under the utility contract.
    :param grayscale: Value supplied for grayscale under the utility contract.
    :param data_fmt: Value supplied for data fmt under the utility contract.
    :param return_data: Value supplied for return data under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    im, orig_fmt = image_and_format_from_data(data)
    orig_fmt = normalize_format_name(orig_fmt)
    out_fmt = normalize_format_name(data_fmt if path is None else os.path.splitext(path)[1][1:])
    changed = out_fmt != orig_fmt

    if resize_to is not None:
        changed = True
        im = resize_image(im, resize_to[0], resize_to[1])

    if minify_to is not None:
        owidth, oheight = im.size
        scaled, nwidth, nheight = fit_image(owidth, oheight, int(minify_to[0]), int(minify_to[1]))
        if scaled:
            changed = True
            im = resize_image(im, nwidth, nheight)

    if _pil_has_alpha(im):
        changed = True
        im = blend_image(im, bgcolor)

    if grayscale:
        from PIL import ImageOps  # type: ignore

        if im.mode != "L":
            changed = True
            im = ImageOps.grayscale(im)

    if path is None or return_data:
        return image_to_data(im, compression_quality=compression_quality, fmt=out_fmt.upper()) if changed else data

    with open(path, "wb") as f:
        f.write(image_to_data(im, compression_quality=compression_quality, fmt=out_fmt.upper()) if changed else data)
    return None


def _pil_has_alpha(im: Any) -> bool:
    """
    Perform the pil has alpha utility operation under explicit compatibility rules.

    Example:
        Exercise  pil has alpha through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param im: Value supplied for im under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if getattr(im, "mode", "") in ("RGBA", "LA"):
        return True
    if getattr(im, "mode", "") == "P" and "transparency" in getattr(im, "info", {}):
        return True
    return False


def blend_on_canvas(img: Any, width: int, height: int, bgcolor: str = "#ffffff") -> Any:
    """
    Perform the blend on canvas utility operation under explicit compatibility rules.

    Example:
        Exercise blend on canvas through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :param width: Value supplied for width under the utility contract.
    :param height: Value supplied for height under the utility contract.
    :param bgcolor: Value supplied for bgcolor under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    im = _ensure_pil(img)
    w, h = im.size
    scaled, nw, nh = fit_image(w, h, int(width), int(height))
    if scaled:
        im = resize_image(im, nw, nh)
        w, h = nw, nh
    canvas = create_canvas(width, height, bgcolor)
    overlay_image(im, canvas, (width - w) // 2, (height - h) // 2)
    return canvas


class Canvas:
    """
    Provide the Canvas utility contract with explicit state and cleanup behavior.

    Example:
        Exercise Canvas through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py
    """
    def __init__(self, width: int, height: int, bgcolor: str = "#ffffff") -> None:
        """
        Initialize and validate the Canvas state.

        Example:
            Exercise Canvas.  init   through a consuming regression::

                python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


        :param width: Value supplied for width under the utility contract.
        :param height: Value supplied for height under the utility contract.
        :param bgcolor: Value supplied for bgcolor under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.img = create_canvas(width, height, bgcolor)

    def __enter__(self) -> "Canvas":
        """
        Implement the resource's enter lifecycle operation.

        Example:
            Exercise Canvas.  enter   through a consuming regression::

                python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self

    def __exit__(self, *args: Any) -> None:
        """
        Implement the resource's exit lifecycle operation.

        Example:
            Exercise Canvas.  exit   through a consuming regression::

                python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


        :param args: Positional values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return None

    def compose(self, img: Any, x: int = 0, y: int = 0) -> None:
        """
        Perform the compose utility operation under explicit compatibility rules.

        Example:
            Exercise Canvas.compose through a consuming regression::

                python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


        :param img: Value supplied for img under the utility contract.
        :param x: Value supplied for x under the utility contract.
        :param y: Value supplied for y under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        overlay_image(img, self.img, x, y)

    def export(self, fmt: str = "JPEG", compression_quality: int = 95) -> bytes:
        """
        Perform the export utility operation under explicit compatibility rules.

        Example:
            Exercise Canvas.export through a consuming regression::

                python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


        :param fmt: Date, number or template format specification.
        :param compression_quality: Value supplied for compression quality under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return image_to_data(self.img, compression_quality=compression_quality, fmt=fmt)


def create_canvas(width: int, height: int, bgcolor: str = "#ffffff") -> Any:
    """
    Perform the create canvas utility operation under explicit compatibility rules.

    Example:
        Exercise create canvas through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param width: Value supplied for width under the utility contract.
    :param height: Value supplied for height under the utility contract.
    :param bgcolor: Value supplied for bgcolor under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    Image = _pillow()
    rgb = _parse_color(bgcolor)
    return Image.new("RGB", (int(width), int(height)), rgb)


def overlay_image(img: Any, canvas: Optional[Any] = None, left: int = 0, top: int = 0) -> Any:
    """
    Overlay *img* onto *canvas* at (left, top).

    Example:
        Exercise overlay image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :param canvas: Value supplied for canvas under the utility contract.
    :param left: Value supplied for left under the utility contract.
    :param top: Value supplied for top under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    base = _ensure_pil(canvas) if canvas is not None else create_canvas(*_ensure_pil(img).size)
    src = _ensure_pil(img)
    left, top = int(left), int(top)

    if src.mode in ("RGBA", "LA") or (src.mode == "P" and "transparency" in src.info):
        base = base.convert("RGBA")
        base.alpha_composite(src.convert("RGBA"), (left, top))
        return base.convert("RGB")
    base.paste(src.convert("RGB"), (left, top))
    return base


def texture_image(canvas: Any, texture: Any) -> Any:
    """
    Perform the texture image utility operation under explicit compatibility rules.

    Example:
        Exercise texture image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param canvas: Value supplied for canvas under the utility contract.
    :param texture: Value supplied for texture under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    base = _ensure_pil(canvas)
    tex = _ensure_pil(texture)
    if _pil_has_alpha(base):
        base = blend_image(base)
    return _tile_texture(base, tex)


def _tile_texture(base: Any, tex: Any) -> Any:
    """
    Perform the tile texture utility operation under explicit compatibility rules.

    Example:
        Exercise  tile texture through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param base: Value supplied for base under the utility contract.
    :param tex: Value supplied for tex under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    Image = _pillow()
    bw, bh = base.size
    tw, th = tex.size
    if tw <= 0 or th <= 0:
        return base
    tex = tex.convert("RGBA") if _pil_has_alpha(tex) else tex.convert("RGB")
    out = base.convert("RGBA")
    for y in range(0, bh, th):
        for x in range(0, bw, tw):
            if tex.mode == "RGBA":
                out.alpha_composite(tex, (x, y))
            else:
                out.paste(tex, (x, y))
    return out.convert("RGB")


def blend_image(img: Any, bgcolor: str = "#ffffff") -> Any:
    """
    Perform the blend image utility operation under explicit compatibility rules.

    Example:
        Exercise blend image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :param bgcolor: Value supplied for bgcolor under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    im = _ensure_pil(img)
    if not _pil_has_alpha(im):
        return im
    rgb = _parse_color(bgcolor)
    bg = _pillow().new("RGBA", im.size, rgb + (255,))
    out = bg
    out.alpha_composite(im.convert("RGBA"))
    return out.convert("RGB")


def add_borders_to_image(
    img: Any,
    left: int = 0,
    top: int = 0,
    right: int = 0,
    bottom: int = 0,
    border_color: str = "#ffffff",
) -> Any:
    """
    Add borders to image under the documented compatibility and safety rules.

    Example:
        Exercise add borders to image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :param left: Value supplied for left under the utility contract.
    :param top: Value supplied for top under the utility contract.
    :param right: Value supplied for right under the utility contract.
    :param bottom: Value supplied for bottom under the utility contract.
    :param border_color: Value supplied for border color under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    im = _ensure_pil(img).convert("RGBA") if _pil_has_alpha(_ensure_pil(img)) else _ensure_pil(img).convert("RGB")
    if not (left > 0 or right > 0 or top > 0 or bottom > 0):
        return im
    Image = _pillow()
    rgb = _parse_color(border_color)
    mode = "RGBA" if im.mode == "RGBA" else "RGB"
    fill = rgb + ((0,) if mode == "RGBA" else ())
    canvas = Image.new(mode, (im.size[0] + left + right, im.size[1] + top + bottom), fill)
    canvas.paste(im, (int(left), int(top)))
    return canvas


def remove_borders_from_image(img: Any, fuzz: Optional[int] = None) -> Any:
    """
    Attempt to auto-detect and remove borders.

    Example:
        Exercise remove borders from image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :param fuzz: Value supplied for fuzz under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    im = _ensure_pil(img)
    fuzz_i = 10 if fuzz is None else max(0, int(fuzz))
    trimmed = _trim_borders(im, fuzz_i)
    return trimmed


def _trim_borders(im: Any, fuzz: int) -> Any:
    # Convert to RGB for comparisons
    """
    Perform the trim borders utility operation under explicit compatibility rules.

    Example:
        Exercise  trim borders through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param im: Value supplied for im under the utility contract.
    :param fuzz: Value supplied for fuzz under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    src = im.convert("RGBA") if _pil_has_alpha(im) else im.convert("RGB")
    w, h = src.size
    if w <= 2 or h <= 2:
        return im

    # Sample corners and pick an average background colour
    px = src.load()
    corners = [px[0, 0], px[w - 1, 0], px[0, h - 1], px[w - 1, h - 1]]
    # Drop alpha if present
    corners_rgb = [(c[0], c[1], c[2]) for c in corners]
    bg = tuple(int(sum(c[i] for c in corners_rgb) / 4) for i in range(3))

    def is_bg(p: Any) -> bool:
        """
        Return or update whether is bg holds for the compatibility value.

        Example:
            Exercise  trim borders.is bg through a consuming regression::

                python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


        :param p: Path-like value normalized or validated by the operation.
        :return: True when the documented condition holds; otherwise False.
        """
        r, g, b = p[0], p[1], p[2]
        return max(abs(r - bg[0]), abs(g - bg[1]), abs(b - bg[2])) <= fuzz

    # Find top
    top = 0
    for y in range(h):
        if any(not is_bg(px[x, y]) for x in range(w)):
            top = y
            break
    # Find bottom
    bottom = h - 1
    for y in range(h - 1, -1, -1):
        if any(not is_bg(px[x, y]) for x in range(w)):
            bottom = y
            break
    # Find left
    left = 0
    for x in range(w):
        if any(not is_bg(px[x, y]) for y in range(top, bottom + 1)):
            left = x
            break
    # Find right
    right = w - 1
    for x in range(w - 1, -1, -1):
        if any(not is_bg(px[x, y]) for y in range(top, bottom + 1)):
            right = x
            break

    # Sanity
    if right <= left or bottom <= top:
        return im
    if left == 0 and top == 0 and right == w - 1 and bottom == h - 1:
        return im
    return im.crop((left, top, right + 1, bottom + 1))


def resize_image(img: Any, width: int, height: int) -> Any:
    """
    Perform the resize image utility operation under explicit compatibility rules.

    Example:
        Exercise resize image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :param width: Value supplied for width under the utility contract.
    :param height: Value supplied for height under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    im = _ensure_pil(img)
    return im.resize((int(width), int(height)), resample=_resample())


def resize_to_fit(img: Any, width: int, height: int) -> Tuple[bool, Any]:
    """
    Perform the resize to fit utility operation under explicit compatibility rules.

    Example:
        Exercise resize to fit through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :param width: Value supplied for width under the utility contract.
    :param height: Value supplied for height under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    im = _ensure_pil(img)
    resize_needed, nw, nh = fit_image(im.size[0], im.size[1], int(width), int(height))
    if resize_needed:
        im = resize_image(im, nw, nh)
    return resize_needed, im


def clone_image(img: Any) -> Any:
    """
    Perform the clone image utility operation under explicit compatibility rules.

    Example:
        Exercise clone image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    im = _ensure_pil(img)
    return im.copy()


def scale_image(
    data: bytes,
    width: int = 60,
    height: int = 80,
    compression_quality: int = 70,
    as_png: bool = False,
    preserve_aspect_ratio: bool = True,
) -> Tuple[int, int, bytes]:
    """
    Scale image data to the requested bounds while preserving its aspect ratio.

    Example:
        Exercise scale image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param data: Value supplied for data under the utility contract.
    :param width: Value supplied for width under the utility contract.
    :param height: Value supplied for height under the utility contract.
    :param compression_quality: Value supplied for compression quality under the utility
        contract.
    :param as_png: Value supplied for as png under the utility contract.
    :param preserve_aspect_ratio: Value supplied for preserve aspect ratio under the
        utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    im = _pil_open_bytes(data)
    if preserve_aspect_ratio:
        scaled, nwidth, nheight = fit_image(im.size[0], im.size[1], int(width), int(height))
        if scaled:
            im = resize_image(im, nwidth, nheight)
    else:
        if im.size != (int(width), int(height)):
            im = resize_image(im, int(width), int(height))
    fmt = "PNG" if as_png else "JPEG"
    w, h = im.size
    return int(w), int(h), image_to_data(im, compression_quality=compression_quality, fmt=fmt)


def crop_image(img: Any, x: int, y: int, width: int, height: int) -> Any:
    """
    Perform the crop image utility operation under explicit compatibility rules.

    Example:
        Exercise crop image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :param x: Value supplied for x under the utility contract.
    :param y: Value supplied for y under the utility contract.
    :param width: Value supplied for width under the utility contract.
    :param height: Value supplied for height under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    im = _ensure_pil(img)
    x, y = int(x), int(y)
    width = min(int(width), im.size[0] - x)
    height = min(int(height), im.size[1] - y)
    return im.crop((x, y, x + width, y + height))


def grayscale_image(img: Any) -> Any:
    """
    Perform the grayscale image utility operation under explicit compatibility rules.

    Example:
        Exercise grayscale image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from PIL import ImageOps  # type: ignore

    im = _ensure_pil(img)
    return ImageOps.grayscale(im)


def set_image_opacity(img: Any, alpha: float = 0.5) -> Any:
    """
    Set image opacity under the documented compatibility and safety rules.

    Example:
        Exercise set image opacity through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :param alpha: Value supplied for alpha under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    im = _ensure_pil(img)
    if not _pil_has_alpha(im):
        # Create an alpha channel
        im = im.convert("RGBA")
    a = im.getchannel("A")
    # Multiply existing alpha
    a = a.point(lambda v: int(max(0, min(255, round(v * float(alpha))))))
    im.putalpha(a)
    return im


def flip_image(img: Any, horizontal: bool = False, vertical: bool = False) -> Any:
    """
    Perform the flip image utility operation under explicit compatibility rules.

    Example:
        Exercise flip image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :param horizontal: Value supplied for horizontal under the utility contract.
    :param vertical: Value supplied for vertical under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    Image = _pillow()
    im = _ensure_pil(img)
    if horizontal:
        im = im.transpose(Image.Transpose.FLIP_LEFT_RIGHT)
    if vertical:
        im = im.transpose(Image.Transpose.FLIP_TOP_BOTTOM)
    return im


def image_has_transparent_pixels(img: Any) -> bool:
    """
    Perform the image has transparent pixels utility operation under explicit compatibility rules.

    Example:
        Exercise image has transparent pixels through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    im = _ensure_pil(img)
    if not _pil_has_alpha(im):
        return False
    a = im.convert("RGBA").getchannel("A")
    lo, hi = a.getextrema()
    return lo < 255


def rotate_image(img: Any, degrees: float) -> Any:
    """
    Perform the rotate image utility operation under explicit compatibility rules.

    Example:
        Exercise rotate image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :param degrees: Value supplied for degrees under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    im = _ensure_pil(img)
    # expand=True to avoid cropping
    return im.rotate(float(degrees), expand=True)


def gaussian_sharpen_image(img: Any, radius: int = 0, sigma: float = 3.0, high_quality: bool = True) -> Any:
    """
    Perform the gaussian sharpen image utility operation under explicit compatibility rules.

    Example:
        Exercise gaussian sharpen image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :param radius: Value supplied for radius under the utility contract.
    :param sigma: Value supplied for sigma under the utility contract.
    :param high_quality: Value supplied for high quality under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from PIL import ImageFilter  # type: ignore

    im = _ensure_pil(img)
    r = max(0, int(radius) if radius else int(max(1, round(float(sigma)))))
    # Unsharp mask is a decent approximation for "sharpen"
    return im.filter(ImageFilter.UnsharpMask(radius=r, percent=150 if high_quality else 80, threshold=3))


def gaussian_blur_image(img: Any, radius: int = -1, sigma: float = 3.0) -> Any:
    """
    Perform the gaussian blur image utility operation under explicit compatibility rules.

    Example:
        Exercise gaussian blur image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :param radius: Value supplied for radius under the utility contract.
    :param sigma: Value supplied for sigma under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from PIL import ImageFilter  # type: ignore

    im = _ensure_pil(img)
    r = float(sigma) if radius in (-1, 0) else float(radius)
    return im.filter(ImageFilter.GaussianBlur(radius=r))


def despeckle_image(img: Any) -> Any:
    """
    Perform the despeckle image utility operation under explicit compatibility rules.

    Example:
        Exercise despeckle image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from PIL import ImageFilter  # type: ignore

    im = _ensure_pil(img)
    return im.filter(ImageFilter.MedianFilter(size=3))


def oil_paint_image(img: Any, radius: int = -1, high_quality: bool = True) -> Any:
    """
    Perform the oil paint image utility operation under explicit compatibility rules.

    Example:
        Exercise oil paint image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :param radius: Value supplied for radius under the utility contract.
    :param high_quality: Value supplied for high quality under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from PIL import ImageFilter  # type: ignore

    im = _ensure_pil(img)
    r = 3 if radius in (-1, 0) else max(1, int(radius))
    # A crude but pleasant "paint" effect
    return im.filter(ImageFilter.ModeFilter(size=r))


def normalize_image(img: Any) -> Any:
    """
    Normalize image under the documented compatibility and safety rules.

    Example:
        Exercise normalize image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from PIL import ImageOps  # type: ignore

    im = _ensure_pil(img)
    return ImageOps.autocontrast(im)


def quantize_image(img: Any, max_colors: int = 256, dither: bool = True, palette: Union[str, Sequence[str]] = "") -> Any:
    """
    Perform the quantize image utility operation under explicit compatibility rules.

    Example:
        Exercise quantize image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :param max_colors: Value supplied for max colors under the utility contract.
    :param dither: Value supplied for dither under the utility contract.
    :param palette: Value supplied for palette under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    Image = _pillow()
    im = _ensure_pil(img)
    if _pil_has_alpha(im):
        im = blend_image(im)

    colors = int(max(2, min(256, max_colors)))
    dither_flag = Image.Dither.FLOYDSTEINBERG if dither else Image.Dither.NONE

    if palette:
        if isinstance(palette, str):
            pal = [p for p in palette.split() if p]
        else:
            pal = list(palette)

        # Build a palette image
        pal_img = Image.new("P", (1, 1))
        rgb_list: list[int] = []
        for col in pal:
            rgb_list.extend(_parse_color(col))
        # Pad to 768 entries
        rgb_list.extend([0] * (768 - len(rgb_list)))
        pal_img.putpalette(rgb_list[:768])
        return im.convert("RGB").quantize(palette=pal_img, dither=dither_flag)

    return im.convert("RGB").quantize(colors=colors, method=Image.Quantize.MEDIANCUT, dither=dither_flag)

def get_exe_path(name: str) -> str:
    """
    Return a best-effort path to an external helper executable.

    Example:
        Exercise get exe path through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param name: Field, file, function or resource name addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    import shutil

    exe = str(name)
    try:
        from LiuXin_alpha.file_formats.pdf.pdftohtml import PDFTOHTML  # type: ignore

        base = os.path.dirname(str(PDFTOHTML))
    except Exception:
        base = ""

    # Windows bundles sometimes used a suffixed name.
    if os.name == "nt":
        cand = exe + "-calibre.exe"
        if base:
            p = os.path.join(base, cand)
            if os.path.exists(p):
                return p
        q = shutil.which(cand)
        if q:
            return q

    if base:
        p = os.path.join(base, exe)
        if os.path.exists(p):
            return p

    q = shutil.which(exe)
    return q or exe


def _atomic_replace(src: str, dst: str) -> None:
    # os.replace is atomic on POSIX and Windows when src/dst are on same filesystem.
    """
    Perform the atomic replace utility operation under explicit compatibility rules.

    Example:
        Exercise  atomic replace through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param src: Value supplied for src under the utility contract.
    :param dst: Value supplied for dst under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    os.replace(src, dst)


def run_optimizer(file_path: str, cmd: Sequence[Any], as_filter: bool = False, input_data: Optional[bytes] = None) -> Optional[str]:
    """
    Run an external optimizer safely.

    Example:
        Exercise run optimizer through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param file_path: Value supplied for file path under the utility contract.
    :param cmd: Value supplied for cmd under the utility contract.
    :param as_filter: Value supplied for as filter under the utility contract.
    :param input_data: Value supplied for input data under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    import errno
    import shutil
    import subprocess
    import tempfile

    file_path = os.path.abspath(file_path)
    cwd = os.path.dirname(file_path) or "."
    ext = os.path.splitext(file_path)[1]
    if not ext or len(ext) > 10 or not ext.startswith("."):
        ext = ".jpg"

    fd, outfile = tempfile.mkstemp(dir=cwd, suffix=ext)
    os.close(fd)

    try:
        iname = os.path.basename(file_path)
        oname = os.path.basename(outfile)

        # Historically, commands used True/False sentinels for input/output filenames.
        cmd2 = list(cmd)
        if not as_filter:
            cmd2 = [iname if x is True else oname if x is False else x for x in cmd2]

        # Ensure everything is str for subprocess.
        cmd2s = [str(x) for x in cmd2]

        # Run
        if as_filter:
            if input_data is None:
                return "as_filter=True requires input_data"
            with open(outfile, "wb") as outfp:
                cp = subprocess.run(
                    cmd2s,
                    input=bytes(input_data),
                    stdout=outfp,
                    stderr=subprocess.PIPE,
                    cwd=cwd,
                    check=False,
                )
        else:
            cp = subprocess.run(
                cmd2s,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=cwd,
                check=False,
            )

        stderr = (cp.stderr or b"").decode("utf-8", "ignore").strip()
        if cp.returncode != 0:
            return stderr or f"{cmd2s[0]} failed with code {cp.returncode}"

        try:
            sz = os.path.getsize(outfile)
        except OSError:
            sz = 0
        if sz < 1:
            return f"{cmd2s[0]} returned a zero size image"

        try:
            shutil.copystat(file_path, outfile)
        except Exception:
            pass

        _atomic_replace(outfile, file_path)
        return None
    finally:
        try:
            os.remove(outfile)
        except OSError as err:
            if err.errno != errno.ENOENT:
                raise
        try:
            os.remove(outfile + ".bak")  # optipng sometimes creates these
        except OSError as err:
            if err.errno != errno.ENOENT:
                raise


def optimize_jpeg(file_path: str) -> Optional[str]:
    """
    Optimize a JPEG file in-place.

    Example:
        Exercise optimize jpeg through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param file_path: Value supplied for file path under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    import shutil

    exe = get_exe_path("jpegtran")
    have = shutil.which(exe) or (os.path.exists(exe) and exe)
    if have:
        cmd = [have] + "-copy none -optimize -progressive -maxmemory 100M -outfile".split() + [False, True]
        err = run_optimizer(file_path, cmd)
        if err is None:
            return None

    # Pillow fallback: re-save with optimize/progressive.
    try:
        from PIL import Image  # type: ignore

        with Image.open(file_path) as im:
            im.load()
            im = im.convert("RGB")
            im.save(file_path, format="JPEG", quality=85, optimize=True, progressive=True)
        return None
    except Exception as e:
        return str(e)


def optimize_png(file_path: str) -> Optional[str]:
    """
    Optimize a PNG file in-place.

    Example:
        Exercise optimize png through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param file_path: Value supplied for file path under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    import shutil

    exe = get_exe_path("optipng")
    have = shutil.which(exe) or (os.path.exists(exe) and exe)
    if have:
        cmd = [have] + "-fix -clobber -strip all -o7 -out".split() + [False, True]
        err = run_optimizer(file_path, cmd)
        if err is None:
            return None

    try:
        from PIL import Image  # type: ignore

        with Image.open(file_path) as im:
            im.load()
            im.save(file_path, format="PNG", optimize=True)
        return None
    except Exception as e:
        return str(e)


def encode_jpeg(file_path: str, quality: int = 80) -> Optional[str]:
    """
    Encode/re-encode the image at ``file_path`` as JPEG.

    Example:
        Exercise encode jpeg through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param file_path: Value supplied for file path under the utility contract.
    :param quality: Value supplied for quality under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    import shutil

    q = max(0, min(100, int(quality)))

    exe = get_exe_path("cjpeg")
    have = shutil.which(exe) or (os.path.exists(exe) and exe)
    if have:
        # Produce a simple PPM stream via Pillow.
        try:
            from PIL import Image  # type: ignore

            bio = BytesIO()
            with Image.open(file_path) as im:
                im.load()
                im.convert("RGB").save(bio, format="PPM")
            ppm = bio.getvalue()
            cmd = [have] + "-optimize -progressive -maxmemory 100M -quality".split() + [str(q)]
            err = run_optimizer(file_path, cmd, as_filter=True, input_data=ppm)
            if err is None:
                return None
        except Exception:
            # Fall through to Pillow fallback below.
            pass

    try:
        from PIL import Image  # type: ignore

        with Image.open(file_path) as im:
            im.load()
            im = im.convert("RGB")
            im.save(file_path, format="JPEG", quality=q, optimize=True, progressive=True)
        return None
    except Exception as e:
        return str(e)
