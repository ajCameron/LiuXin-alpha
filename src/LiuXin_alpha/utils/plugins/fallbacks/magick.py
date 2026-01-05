# -*- coding: utf-8 -*-
"""
Pure-python fallback for the compiled ``magick`` extension.

The compiled extension wraps ImageMagick APIs. This fallback shells out to the
ImageMagick CLI if available (`magick` or `identify`).

It only implements a small subset that is commonly needed:
    - Image(data_or_path).identify() -> dict

If the CLI is not available, identify() raises a RuntimeError.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from typing import Any, Dict, Optional, Union




#
# def _magick() -> Optional[str]:
#     return shutil.which("magick") or shutil.which("identify")
#
#
# @dataclass
# class Image:
#     _src: Union[bytes, str, os.PathLike[str]]
#
#     def __post_init__(self) -> None:
#         self._tmp_path: Optional[str] = None
#
#     def _ensure_path(self) -> str:
#         if isinstance(self._src, (str, os.PathLike)):
#             return os.fspath(self._src)
#         # bytes: spool to temp file
#         if self._tmp_path is None:
#             fd, path = tempfile.mkstemp(prefix="liuxin_magick_", suffix=".bin")
#             os.close(fd)
#             with open(path, "wb") as f:
#                 f.write(self._src)
#             self._tmp_path = path
#         return self._tmp_path
#
#     def identify(self) -> Dict[str, Any]:
#         exe = _magick()
#         if not exe:
#             raise RuntimeError("ImageMagick CLI not found (need `magick` or `identify` on PATH)")
#         path = self._ensure_path()
#
#         if os.path.basename(exe).lower() == "identify":
#             cmd = [exe, "-format", "%w %h %m %b", path]
#         else:
#             cmd = [exe, "identify", "-format", "%w %h %m %b", path]
#         cp = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
#         if cp.returncode != 0:
#             msg = (cp.stderr or b"").decode("utf-8", "ignore").strip()
#             raise RuntimeError(msg or f"identify failed with code {cp.returncode}")
#
#         out = (cp.stdout or b"").decode("utf-8", "ignore").strip()
#         parts = out.split()
#         if len(parts) < 4:
#             return {"raw": out}
#         w, h, fmt, size = parts[0], parts[1], parts[2], " ".join(parts[3:])
#         try:
#             w_i = int(w)
#             h_i = int(h)
#         except Exception:
#             w_i = None
#             h_i = None
#         return {"width": w_i, "height": h_i, "format": fmt, "size": size}
#
#     def close(self) -> None:
#         if self._tmp_path:
#             try:
#                 os.remove(self._tmp_path)
#             except Exception:
#                 pass
#             self._tmp_path = None
#
#     def __del__(self) -> None:
#         self.close()



import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from typing import Any, Dict, Optional, Union


def _wand_image_cls() -> Optional[type]:
    try:
        from wand.image import Image as WandImage  # type: ignore[import-not-found]
        return WandImage
    except Exception:
        return None


def _which(*names: str) -> Optional[str]:
    for name in names:
        p = shutil.which(name)
        if p:
            return p
    return None


def _magick_identify() -> Optional[str]:
    return _which("magick", "identify")


def _magick_convert() -> Optional[str]:
    return _which("magick", "convert")


def _safe_getsize(path: str) -> Optional[int]:
    try:
        return os.path.getsize(path)
    except Exception:
        return None


@dataclass
class Image:
    """Load an image from bytes or a filesystem path."""

    _src: Union[bytes, str, os.PathLike[str]]

    def __post_init__(self) -> None:
        self._tmp_path: Optional[str] = None
        self._wand_img: Optional[Any] = None

    def close(self) -> None:
        if self._wand_img is not None:
            try:
                self._wand_img.close()
            except Exception:
                pass
            self._wand_img = None

        if self._tmp_path:
            try:
                os.remove(self._tmp_path)
            except Exception:
                pass
            self._tmp_path = None

    def __del__(self) -> None:
        self.close()

    def __enter__(self) -> "Image":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        self.close()

    def _ensure_path(self) -> str:
        if isinstance(self._src, (str, os.PathLike)):
            return os.fspath(self._src)

        if self._tmp_path is None:
            fd, path = tempfile.mkstemp(prefix="liuxin_magick_", suffix=".bin")
            os.close(fd)
            with open(path, "wb") as f:
                f.write(self._src)
            self._tmp_path = path
        return self._tmp_path

    def _ensure_wand(self) -> Optional[Any]:
        if self._wand_img is not None:
            return self._wand_img

        WandImage = _wand_image_cls()
        if WandImage is None:
            return None

        try:
            if isinstance(self._src, (str, os.PathLike)):
                self._wand_img = WandImage(filename=os.fspath(self._src))
            else:
                self._wand_img = WandImage(blob=self._src)
        except Exception:
            self._wand_img = None
        return self._wand_img

    def identify(self) -> Dict[str, Any]:
        """Return a small metadata dictionary."""

        img = self._ensure_wand()
        if img is not None:
            fmt = getattr(img, "format", None)
            return {
                "width": int(getattr(img, "width", 0)),
                "height": int(getattr(img, "height", 0)),
                "format": fmt,
                "size": len(self._src) if isinstance(self._src, (bytes, bytearray)) else _safe_getsize(os.fspath(self._src)),
            }

        exe = _magick_identify()
        if not exe:
            raise RuntimeError(
                "No ImageMagick binding found (install `Wand` or provide `magick`/`identify` on PATH)"
            )

        path = self._ensure_path()
        base = os.path.basename(exe).lower()
        if base == "identify":
            cmd = [exe, "-format", "%w %h %m %b", path]
        else:
            cmd = [exe, "identify", "-format", "%w %h %m %b", path]

        cp = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
        if cp.returncode != 0:
            msg = (cp.stderr or b"").decode("utf-8", "ignore").strip()
            raise RuntimeError(msg or f"identify failed with code {cp.returncode}")

        out = (cp.stdout or b"").decode("utf-8", "ignore").strip()
        parts = out.split()
        if len(parts) < 4:
            return {"raw": out}

        w_s, h_s, fmt, size = parts[0], parts[1], parts[2], " ".join(parts[3:])
        try:
            w_i = int(w_s)
            h_i = int(h_s)
        except Exception:
            w_i = None
            h_i = None
        return {"width": w_i, "height": h_i, "format": fmt, "size": size}

    def to_bytes(self, *, format: str = "png") -> bytes:
        """Return the image encoded as bytes in the requested format."""

        img = self._ensure_wand()
        if img is not None:
            try:
                img.format = format.upper()
            except Exception:
                img.format = format
            return bytes(img.make_blob())

        exe = _magick_convert()
        if not exe:
            raise RuntimeError(
                "No ImageMagick binding found (install `Wand` or provide `magick`/`convert` on PATH)"
            )

        inpath = self._ensure_path()
        base = os.path.basename(exe).lower()
        outspec = f"{format}:-"
        if base == "magick":
            cmd = [exe, "convert", inpath, outspec]
        else:
            cmd = [exe, inpath, outspec]

        cp = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
        if cp.returncode != 0:
            msg = (cp.stderr or b"").decode("utf-8", "ignore").strip()
            raise RuntimeError(msg or f"convert failed with code {cp.returncode}")
        return bytes(cp.stdout or b"")

    def resize(self, width: int, height: int) -> "Image":
        """Resize to an exact width/height."""

        img = self._ensure_wand()
        if img is not None:
            img.resize(width, height)
            return self

        exe = _magick_convert()
        if not exe:
            raise RuntimeError(
                "No ImageMagick binding found (install `Wand` or provide `magick`/`convert` on PATH)"
            )

        data = self.to_bytes(format="png")
        with tempfile.NamedTemporaryFile(prefix="liuxin_magick_resize_", suffix=".png", delete=False) as f:
            tmp_in = f.name
            f.write(data)

        try:
            base = os.path.basename(exe).lower()
            outspec = "png:-"
            if base == "magick":
                cmd = [exe, "convert", tmp_in, "-resize", f"{width}x{height}!", outspec]
            else:
                cmd = [exe, tmp_in, "-resize", f"{width}x{height}!", outspec]

            cp = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
            if cp.returncode != 0:
                msg = (cp.stderr or b"").decode("utf-8", "ignore").strip()
                raise RuntimeError(msg or f"convert failed with code {cp.returncode}")

            self._src = bytes(cp.stdout or b"")
            self.close()
            return self
        finally:
            try:
                os.remove(tmp_in)
            except Exception:
                pass

    def thumbnail(self, max_width: int, max_height: int) -> "Image":
        """Constrain image to fit within max_width x max_height, preserving aspect ratio."""

        img = self._ensure_wand()
        if img is not None:
            img.transform(resize=f"{max_width}x{max_height}>")
            return self

        exe = _magick_convert()
        if not exe:
            raise RuntimeError(
                "No ImageMagick binding found (install `Wand` or provide `magick`/`convert` on PATH)"
            )

        data = self.to_bytes(format="png")
        with tempfile.NamedTemporaryFile(prefix="liuxin_magick_thumb_", suffix=".png", delete=False) as f:
            tmp_in = f.name
            f.write(data)

        try:
            base = os.path.basename(exe).lower()
            outspec = "png:-"
            if base == "magick":
                cmd = [exe, "convert", tmp_in, "-resize", f"{max_width}x{max_height}>", outspec]
            else:
                cmd = [exe, tmp_in, "-resize", f"{max_width}x{max_height}>", outspec]

            cp = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
            if cp.returncode != 0:
                msg = (cp.stderr or b"").decode("utf-8", "ignore").strip()
                raise RuntimeError(msg or f"convert failed with code {cp.returncode}")

            self._src = bytes(cp.stdout or b"")
            self.close()
            return self
        finally:
            try:
                os.remove(tmp_in)
            except Exception:
                pass

    def save(self, path: Union[str, os.PathLike[str]], *, format: Optional[str] = None) -> None:
        """Save the image to disk."""

        dst = os.fspath(path)

        img = self._ensure_wand()
        if img is not None:
            if format:
                try:
                    img.format = format.upper()
                except Exception:
                    img.format = format
            img.save(filename=dst)
            return

        if format is None:
            ext = os.path.splitext(dst)[1].lstrip(".") or "png"
            format = ext
        data = self.to_bytes(format=format)
        with open(dst, "wb") as f:
            f.write(data)
