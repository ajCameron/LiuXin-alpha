# -*- coding: utf-8 -*-
"""
Pure-Python fallback for the compiled ``imageops`` extension.

The original calibre extension is a SIP wrapper around Qt's QImage and provides:
    remove_borders, grayscale, gaussian_sharpen, gaussian_blur, despeckle, overlay,
    normalize, oil_paint, quantize, has_transparent_pixels, set_opacity, texture_image

This fallback targets *practical* compatibility, while avoiding native compilation.

Strategy:
  1) Prefer Wand (Python bindings to ImageMagick) when available.
  2) Fall back to ImageMagick CLI (`magick`/`convert`) for a subset of operations.

Inputs:
  - Accepts QImage (if PyQt5/PyQt6 is installed) OR encoded image bytes.
  - For bytes, returns bytes (PNG by default).
  - For QImage, returns a QImage (except overlay(), which mutates the canvas and returns None).
  - For overlay(), if canvas is a bytearray, it is updated in-place (canvas[:] = new_bytes).

Notes:
  - Behaviour will not be pixel-identical to the original Qt implementation, but should be "good enough"
    for cover/thumbnail/texture style operations used by plugins.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass
from typing import Any, Iterable, Optional, Sequence, Tuple, Union


BytesLike = Union[bytes, bytearray, memoryview]


def _wand_image_cls() -> Optional[type]:
    try:
        from wand.image import Image as WandImage  # type: ignore[import-not-found]
        return WandImage
    except Exception:
        return None


def _which(*names: str) -> Optional[str]:
    for n in names:
        p = shutil.which(n)
        if p:
            return p
    return None


def _magick_convert() -> Optional[str]:
    # `magick` is the preferred entrypoint on Windows; `convert` exists on many Unix installs.
    return _which("magick", "convert")


def _is_qimage(obj: Any) -> bool:
    # Avoid importing Qt unless needed.
    return hasattr(obj, "isNull") and hasattr(obj, "save") and hasattr(obj, "width") and hasattr(obj, "height")


def _qimage_to_png_bytes(qimg: Any) -> bytes:
    # Support PyQt6 and PyQt5 (and potentially PySide, if API matches).
    for qt_pkg in ("PyQt6", "PyQt5", "PySide6", "PySide2"):
        try:
            QtCore = __import__(f"{qt_pkg}.QtCore", fromlist=["QtCore"])
            QtGui = __import__(f"{qt_pkg}.QtGui", fromlist=["QtGui"])
            QBuffer = QtCore.QBuffer
            QIODevice = QtCore.QIODevice
            buf = QBuffer()
            buf.open(QIODevice.OpenModeFlag.WriteOnly if hasattr(QIODevice, "OpenModeFlag") else QIODevice.WriteOnly)
            ok = qimg.save(buf, "PNG")
            if not ok:
                raise ValueError("QImage.save() failed")
            # In PyQt, buf.data() returns QByteArray
            data = bytes(buf.data())
            buf.close()
            return data
        except Exception:
            continue
    raise RuntimeError("Qt bindings not available to serialize QImage (install PyQt/PySide), or pass bytes instead")


def _png_bytes_to_qimage(data: bytes) -> Any:
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
    raise RuntimeError("Qt bindings not available to deserialize QImage (install PyQt/PySide)")


@dataclass(frozen=True)
class _Coerced:
    kind: str  # "qimage" | "bytes" | "bytearray"
    data: bytes
    original: Any


def _coerce_in(image: Any) -> _Coerced:
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
    if c.kind == "qimage":
        return _png_bytes_to_qimage(out_png)
    if c.kind == "bytearray":
        # update in-place and return None (overlay semantics)
        ba: bytearray = c.original
        ba[:] = out_png
        return None
    return out_png


def _wand_process_one(inp: _Coerced, fn) -> Any:
    WandImage = _wand_image_cls()
    if WandImage is None:
        raise RuntimeError("Wand not available")
    img = WandImage(blob=inp.data)
    try:
        fn(img)
        try:
            img.format = "PNG"
        except Exception:
            pass
        out = bytes(img.make_blob())
        return _restore_out(out, inp)
    finally:
        try:
            img.close()
        except Exception:
            pass


def _wand_process_two(a: _Coerced, b: _Coerced, fn) -> Any:
    WandImage = _wand_image_cls()
    if WandImage is None:
        raise RuntimeError("Wand not available")
    ia = WandImage(blob=a.data)
    ib = WandImage(blob=b.data)
    try:
        fn(ia, ib)
        try:
            ia.format = "PNG"
        except Exception:
            pass
        out = bytes(ia.make_blob())
        return _restore_out(out, a)
    finally:
        for im in (ia, ib):
            try:
                im.close()
            except Exception:
                pass


def _cli_convert(inp: _Coerced, args: Sequence[str]) -> Any:
    exe = _magick_convert()
    if not exe:
        raise RuntimeError("No ImageMagick backend found (install Wand or provide `magick`/`convert` on PATH)")
    base = os.path.basename(exe).lower()
    # magick convert - ... png:-
    if base == "magick":
        cmd = [exe, "convert", "-", *args, "png:-"]
    else:
        cmd = [exe, "-", *args, "png:-"]
    cp = subprocess.run(cmd, input=inp.data, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    if cp.returncode != 0:
        msg = (cp.stderr or b"").decode("utf-8", "ignore").strip()
        raise RuntimeError(msg or f"convert failed with code {cp.returncode}")
    return _restore_out(bytes(cp.stdout or b""), inp)


# ---------------- public API ----------------

def remove_borders(image: Any, fuzz: float) -> Any:
    inp = _coerce_in(image)
    WandImage = _wand_image_cls()
    if WandImage is not None:
        def op(img: Any) -> None:
            # Attempt to map fuzz to ImageMagick's "fuzz" concept.
            try:
                # If fuzz is small, treat as percentage.
                if fuzz is not None:
                    if 0.0 <= float(fuzz) <= 1.0:
                        img.fuzz = f"{float(fuzz) * 100.0}%"
                    else:
                        img.fuzz = float(fuzz)
            except Exception:
                pass
            try:
                img.trim()
            except Exception:
                # Some Wand versions accept trim(fuzz=...), try that
                try:
                    img.trim(fuzz=fuzz)
                except Exception:
                    pass
            # Remove any virtual canvas/page offsets if available
            try:
                img.reset_coords()
            except Exception:
                pass
        try:
            return _wand_process_one(inp, op)
        except Exception:
            # fall back to CLI below
            pass
    # CLI: -fuzz X% -trim +repage
    fuzz_arg = []
    try:
        if 0.0 <= float(fuzz) <= 1.0:
            fuzz_arg = ["-fuzz", f"{float(fuzz) * 100.0}%"]
        else:
            fuzz_arg = ["-fuzz", str(fuzz)]
    except Exception:
        fuzz_arg = []
    return _cli_convert(inp, [*fuzz_arg, "-trim", "+repage"])


def grayscale(image: Any) -> Any:
    inp = _coerce_in(image)
    WandImage = _wand_image_cls()
    if WandImage is not None:
        def op(img: Any) -> None:
            # Prefer a colorspace transform
            for meth, arg in (("transform_colorspace", "gray"), ("transform_colorspace", "GRAY")):
                try:
                    getattr(img, meth)(arg)
                    return
                except Exception:
                    pass
            # Fallback to type/colorspace properties
            for attr, val in (("colorspace", "gray"), ("type", "grayscale")):
                try:
                    setattr(img, attr, val)
                    return
                except Exception:
                    pass
        try:
            return _wand_process_one(inp, op)
        except Exception:
            pass
    return _cli_convert(inp, ["-colorspace", "Gray"])


def gaussian_sharpen(image: Any, radius: float, sigma: float, high_quality: bool = True) -> Any:
    inp = _coerce_in(image)
    WandImage = _wand_image_cls()
    if WandImage is not None:
        def op(img: Any) -> None:
            # ImageMagick sharpen is different from unsharp, but this is a reasonable approximation.
            try:
                img.sharpen(radius=float(radius), sigma=float(sigma))
                return
            except Exception:
                pass
            try:
                img.unsharp_mask(radius=float(radius), sigma=float(sigma), amount=1.0, threshold=0.0)
            except Exception:
                pass
        try:
            return _wand_process_one(inp, op)
        except Exception:
            pass
    return _cli_convert(inp, ["-sharpen", f"{float(radius)}x{float(sigma)}"])


def gaussian_blur(image: Any, radius: float, sigma: float) -> Any:
    inp = _coerce_in(image)
    WandImage = _wand_image_cls()
    if WandImage is not None:
        def op(img: Any) -> None:
            try:
                img.gaussian_blur(radius=float(radius), sigma=float(sigma))
                return
            except Exception:
                pass
            try:
                img.blur(radius=float(radius), sigma=float(sigma))
            except Exception:
                pass
        try:
            return _wand_process_one(inp, op)
        except Exception:
            pass
    return _cli_convert(inp, ["-gaussian-blur", f"{float(radius)}x{float(sigma)}"])


def despeckle(image: Any) -> Any:
    inp = _coerce_in(image)
    WandImage = _wand_image_cls()
    if WandImage is not None:
        def op(img: Any) -> None:
            try:
                img.despeckle()
            except Exception:
                pass
        try:
            return _wand_process_one(inp, op)
        except Exception:
            pass
    return _cli_convert(inp, ["-despeckle"])


def overlay(image: Any, canvas: Any, left: int, top: int) -> None:
    """
    Overlay `image` on top of `canvas` at (left, top).

    - If canvas is a QImage, we draw onto it in-place (returns None).
    - If canvas is a bytearray, we update it in-place with a new PNG (returns None).
    - If canvas is bytes, we return bytes (callers may choose to use it).
    """
    if _is_qimage(image) and _is_qimage(canvas):
        # Use Qt for real in-place overlay if available.
        for qt_pkg in ("PyQt6", "PyQt5", "PySide6", "PySide2"):
            try:
                QtGui = __import__(f"{qt_pkg}.QtGui", fromlist=["QtGui"])
                QPainter = QtGui.QPainter
                painter = QPainter(canvas)
                painter.drawImage(int(left), int(top), image)
                painter.end()
                return None
            except Exception:
                continue
        raise RuntimeError("Qt bindings not available for QImage overlay")

    a = _coerce_in(canvas)
    b = _coerce_in(image)

    WandImage = _wand_image_cls()
    if WandImage is not None:
        def op(base: Any, over: Any) -> None:
            try:
                base.composite(over, int(left), int(top))
            except Exception:
                try:
                    base.composite(over, int(left), int(top), operator="over")
                except Exception:
                    pass
        out = _wand_process_two(a, b, op)
        # If canvas was bytes, return bytes; if canvas was bytearray, updated in-place and returns None.
        return out  # type: ignore[return-value]

    # CLI fallback: composite
    exe = _magick_convert()
    if not exe:
        raise RuntimeError("No ImageMagick backend found (install Wand or provide `magick`/`convert` on PATH)")
    base = os.path.basename(exe).lower()
    if base == "magick":
        cmd = [exe, "convert", "png:-", "png:-", "-geometry", f"+{int(left)}+{int(top)}", "-composite", "png:-"]
    else:
        cmd = [exe, "png:-", "png:-", "-geometry", f"+{int(left)}+{int(top)}", "-composite", "png:-"]
    # Need both images: feed via temp files if CLI doesn't support two STDIN streams easily.
    import tempfile
    with tempfile.NamedTemporaryFile(prefix="liuxin_overlay_base_", suffix=".png", delete=False) as f1, \
         tempfile.NamedTemporaryFile(prefix="liuxin_overlay_over_", suffix=".png", delete=False) as f2:
        f1.write(a.data); f2.write(b.data)
        p1, p2 = f1.name, f2.name
    try:
        if base == "magick":
            cmd = [exe, "convert", p1, p2, "-geometry", f"+{int(left)}+{int(top)}", "-composite", "png:-"]
        else:
            cmd = [exe, p1, p2, "-geometry", f"+{int(left)}+{int(top)}", "-composite", "png:-"]
        cp = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
        if cp.returncode != 0:
            msg = (cp.stderr or b"").decode("utf-8", "ignore").strip()
            raise RuntimeError(msg or f"convert failed with code {cp.returncode}")
        _restore_out(bytes(cp.stdout or b""), a)
        return None
    finally:
        for p in (p1, p2):
            try:
                os.remove(p)
            except Exception:
                pass


def normalize(image: Any) -> Any:
    inp = _coerce_in(image)
    WandImage = _wand_image_cls()
    if WandImage is not None:
        def op(img: Any) -> None:
            for m in ("normalize", "auto_level", "contrast_stretch"):
                try:
                    getattr(img, m)()
                    return
                except Exception:
                    pass
        try:
            return _wand_process_one(inp, op)
        except Exception:
            pass
    return _cli_convert(inp, ["-normalize"])


def oil_paint(image: Any, radius: float = -1.0, high_quality: bool = True) -> Any:
    inp = _coerce_in(image)
    WandImage = _wand_image_cls()
    r = float(radius)
    if WandImage is not None:
        def op(img: Any) -> None:
            rr = 1.0 if r < 0 else r
            try:
                img.oil_paint(radius=rr)
            except Exception:
                try:
                    img.oil_paint(rr)
                except Exception:
                    pass
        try:
            return _wand_process_one(inp, op)
        except Exception:
            pass
    rr = 1.0 if r < 0 else r
    return _cli_convert(inp, ["-paint", str(rr)])


def quantize(image: Any, maximum_colors: int, dither: bool, palette: Any) -> Any:
    """
    Quantize to a maximum number of colors.

    The native Qt implementation supports a palette; here we primarily respect maximum_colors + dither.
    If a palette is supplied as an iterable of RGB tuples/ints, we attempt a remap.
    """
    inp = _coerce_in(image)
    maxc = int(maximum_colors)
    dith = bool(dither)

    WandImage = _wand_image_cls()
    if WandImage is not None:
        def op(img: Any) -> None:
            # Try palette remap if possible
            pal_colors: list[Tuple[int, int, int]] = []
            try:
                if palette is not None:
                    for c in palette:
                        if isinstance(c, int):
                            pal_colors.append(((c >> 16) & 0xFF, (c >> 8) & 0xFF, c & 0xFF))
                        elif isinstance(c, (tuple, list)) and len(c) >= 3:
                            pal_colors.append((int(c[0]) & 0xFF, int(c[1]) & 0xFF, int(c[2]) & 0xFF))
            except Exception:
                pal_colors = []

            if pal_colors:
                # Build a tiny palette image and remap.
                try:
                    from wand.color import Color  # type: ignore[import-not-found]
                    from wand.image import Image as WandImage2  # type: ignore[import-not-found]
                    w = max(1, len(pal_colors))
                    pal = WandImage2(width=w, height=1, background=Color("white"))
                    for i, (r, g, b) in enumerate(pal_colors):
                        pal[i, 0] = Color(f"rgb({r},{g},{b})")
                    try:
                        img.remap(pal, dither=dith)
                        pal.close()
                        return
                    except Exception:
                        pal.close()
                except Exception:
                    pass

            # Fallback to standard quantize
            try:
                img.quantize(number_colors=maxc, dither=dith)
            except Exception:
                try:
                    img.quantize(maxc)
                except Exception:
                    pass
        try:
            return _wand_process_one(inp, op)
        except Exception:
            pass

    # CLI fallback
    args = ["-colors", str(maxc)]
    if dith:
        args += ["-dither", "FloydSteinberg"]
    else:
        args += ["+dither"]
    return _cli_convert(inp, args)


def has_transparent_pixels(image: Any) -> bool:
    inp = _coerce_in(image)
    WandImage = _wand_image_cls()
    if WandImage is not None:
        img = WandImage(blob=inp.data)
        try:
            # Quick check: if no alpha channel, no transparent pixels
            try:
                if not bool(getattr(img, "alpha_channel", False)):
                    return False
            except Exception:
                pass

            # Histogram-based alpha scan
            try:
                hist = img.histogram
                for col in hist.keys():
                    a = getattr(col, "alpha", None)
                    if a is None:
                        continue
                    # Wand Color.alpha is typically 0..1, where 1 is fully opaque.
                    try:
                        if float(a) < 1.0:
                            return True
                    except Exception:
                        pass
                return False
            except Exception:
                # Fallback: sample a few pixels
                w = int(getattr(img, "width", 0))
                h = int(getattr(img, "height", 0))
                if w <= 0 or h <= 0:
                    return False
                for (x, y) in ((0, 0), (w - 1, 0), (0, h - 1), (w - 1, h - 1), (w // 2, h // 2)):
                    try:
                        col = img[x, y]
                        a = getattr(col, "alpha", 1.0)
                        if float(a) < 1.0:
                            return True
                    except Exception:
                        continue
                return False
        finally:
            try:
                img.close()
            except Exception:
                pass

    # CLI fallback (expensive but safe): extract alpha and check if min<max
    exe = _magick_convert()
    if not exe:
        raise RuntimeError("No ImageMagick backend found (install Wand or provide `magick`/`convert` on PATH)")
    base = os.path.basename(exe).lower()
    if base == "magick":
        cmd = [exe, "convert", "-", "-alpha", "extract", "-format", "%[fx:minima]!=%[fx:maxima]", "info:"]
    else:
        cmd = [exe, "-", "-alpha", "extract", "-format", "%[fx:minima]!=%[fx:maxima]", "info:"]
    cp = subprocess.run(cmd, input=inp.data, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    if cp.returncode != 0:
        msg = (cp.stderr or b"").decode("utf-8", "ignore").strip()
        raise RuntimeError(msg or f"convert failed with code {cp.returncode}")
    out = (cp.stdout or b"").decode("utf-8", "ignore").strip()
    return out == "1"


def set_opacity(image: Any, alpha: float) -> Any:
    inp = _coerce_in(image)
    a = float(alpha)

    WandImage = _wand_image_cls()
    if WandImage is not None:
        def op(img: Any) -> None:
            # Ensure alpha channel exists
            try:
                img.alpha_channel = True
            except Exception:
                pass
            # Multiply the alpha channel
            try:
                img.evaluate(operator="multiply", value=a, channel="alpha")
                return
            except Exception:
                pass
            try:
                img.evaluate("multiply", a, "alpha")
            except Exception:
                pass
        try:
            return _wand_process_one(inp, op)
        except Exception:
            pass

    # CLI fallback
    return _cli_convert(inp, ["-alpha", "set", "-channel", "A", "-evaluate", "multiply", str(a), "+channel"])


def texture_image(image: Any, texturei: Any) -> Any:
    """
    Apply a texture over an image.

    This is an approximation using ImageMagick compose operators.
    """
    a = _coerce_in(image)
    b = _coerce_in(texturei)

    WandImage = _wand_image_cls()
    if WandImage is not None:
        def op(base: Any, tex: Any) -> None:
            # Tile/resize texture to match base size
            try:
                tex.resize(int(base.width), int(base.height))
            except Exception:
                pass
            # Try a couple of blend modes
            for mode in ("overlay", "softlight", "multiply", "over"):
                try:
                    base.composite(tex, 0, 0, operator=mode)
                    return
                except Exception:
                    continue
            try:
                base.composite(tex, 0, 0)
            except Exception:
                pass
        try:
            return _wand_process_two(a, b, op)
        except Exception:
            pass

    # CLI fallback: convert base tex -compose overlay -composite
    exe = _magick_convert()
    if not exe:
        raise RuntimeError("No ImageMagick backend found (install Wand or provide `magick`/`convert` on PATH)")
    import tempfile
    with tempfile.NamedTemporaryFile(prefix="liuxin_tex_base_", suffix=".png", delete=False) as f1, \
         tempfile.NamedTemporaryFile(prefix="liuxin_tex_tex_", suffix=".png", delete=False) as f2:
        f1.write(a.data); f2.write(b.data)
        p1, p2 = f1.name, f2.name
    try:
        base_exe = os.path.basename(exe).lower()
        if base_exe == "magick":
            cmd = [exe, "convert", p1, p2, "-compose", "overlay", "-composite", "png:-"]
        else:
            cmd = [exe, p1, p2, "-compose", "overlay", "-composite", "png:-"]
        cp = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
        if cp.returncode != 0:
            msg = (cp.stderr or b"").decode("utf-8", "ignore").strip()
            raise RuntimeError(msg or f"convert failed with code {cp.returncode}")
        return _restore_out(bytes(cp.stdout or b""), a)
    finally:
        for p in (p1, p2):
            try:
                os.remove(p)
            except Exception:
                pass
