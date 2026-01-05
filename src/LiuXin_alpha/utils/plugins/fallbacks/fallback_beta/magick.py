# -*- coding: utf-8 -*-
"""
Fallback layer: beta (CLI-only).

This avoids Wand entirely and shells out to ImageMagick command line tools.

Implements the same public surface as the base fallback:
    - Image(data_or_path).identify()
    - Image(...).to_bytes(format="png")
    - Image(...).resize(w, h)
    - Image(...).thumbnail(max_w, max_h)
    - Image(...).save(path, format=None)
"""

from __future__ import annotations

from dataclasses import dataclass
import os
import shutil
import subprocess
import tempfile
from typing import Any, Dict, Optional, Union


def _which(*names: str) -> Optional[str]:
    for n in names:
        p = shutil.which(n)
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


def __liuxin_plugin_probe__():
    exe = _magick_identify() or _magick_convert()
    if not exe:
        return False, "no ImageMagick CLI (magick/identify/convert) on PATH"
    try:
        subprocess.run([exe, "-version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False, timeout=0.5)
        return True, "cli ok"
    except Exception as e:
        return False, str(e)


@dataclass
class Image:
    _src: Union[bytes, str, os.PathLike[str]]
    _tmp_path: Optional[str] = None

    def close(self) -> None:
        if self._tmp_path is not None:
            try:
                os.remove(self._tmp_path)
            except OSError:
                pass
            self._tmp_path = None

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

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

    def identify(self) -> Dict[str, Any]:
        exe = _magick_identify()
        if not exe:
            raise RuntimeError("ImageMagick CLI not found (need `magick` or `identify` on PATH)")
        path = self._ensure_path()

        if os.path.basename(exe).lower() == "identify":
            cmd = [exe, "-format", "%w %h %m", path]
        else:
            cmd = [exe, "identify", "-format", "%w %h %m", path]
        cp = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
        if cp.returncode != 0:
            msg = (cp.stderr or b"").decode("utf-8", "ignore").strip()
            raise RuntimeError(msg or f"identify failed (code={cp.returncode})")

        parts = (cp.stdout or b"").decode("utf-8", "ignore").strip().split()
        width = int(parts[0]) if len(parts) > 0 else None
        height = int(parts[1]) if len(parts) > 1 else None
        fmt = parts[2] if len(parts) > 2 else None

        out: Dict[str, Any] = {"width": width, "height": height, "format": fmt}
        size = _safe_getsize(path)
        if size is not None:
            out["size"] = size
        return out

    def to_bytes(self, *, format: str = "png") -> bytes:
        exe = _magick_convert()
        if not exe:
            raise RuntimeError("ImageMagick CLI not found (need `magick` or `convert` on PATH)")
        path = self._ensure_path()

        if os.path.basename(exe).lower() == "convert":
            cmd = [exe, path, f"{format}:-"]
        else:
            cmd = [exe, "convert", path, f"{format}:-"]
        cp = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
        if cp.returncode != 0:
            msg = (cp.stderr or b"").decode("utf-8", "ignore").strip()
            raise RuntimeError(msg or f"convert failed (code={cp.returncode})")
        return cp.stdout or b""

    def resize(self, width: int, height: int) -> "Image":
        exe = _magick_convert()
        if not exe:
            raise RuntimeError("ImageMagick CLI not found (need `magick` or `convert` on PATH)")
        path = self._ensure_path()

        # write to a new temp file and replace _src
        fd, outp = tempfile.mkstemp(prefix="liuxin_magick_resize_", suffix=".png")
        os.close(fd)
        try:
            if os.path.basename(exe).lower() == "convert":
                cmd = [exe, path, "-resize", f"{width}x{height}!", outp]
            else:
                cmd = [exe, "convert", path, "-resize", f"{width}x{height}!", outp]
            cp = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
            if cp.returncode != 0:
                msg = (cp.stderr or b"").decode("utf-8", "ignore").strip()
                raise RuntimeError(msg or f"convert failed (code={cp.returncode})")

            # switch source to the output file; cleanup old temp
            self.close()
            self._src = outp
            self._tmp_path = None
            return self
        except Exception:
            try:
                os.remove(outp)
            except OSError:
                pass
            raise

    def thumbnail(self, max_width: int, max_height: int) -> "Image":
        exe = _magick_convert()
        if not exe:
            raise RuntimeError("ImageMagick CLI not found (need `magick` or `convert` on PATH)")
        path = self._ensure_path()

        fd, outp = tempfile.mkstemp(prefix="liuxin_magick_thumb_", suffix=".png")
        os.close(fd)
        try:
            # Use IM 'thumbnail' operator
            if os.path.basename(exe).lower() == "convert":
                cmd = [exe, path, "-thumbnail", f"{max_width}x{max_height}", outp]
            else:
                cmd = [exe, "convert", path, "-thumbnail", f"{max_width}x{max_height}", outp]
            cp = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
            if cp.returncode != 0:
                msg = (cp.stderr or b"").decode("utf-8", "ignore").strip()
                raise RuntimeError(msg or f"convert failed (code={cp.returncode})")
            self.close()
            self._src = outp
            self._tmp_path = None
            return self
        except Exception:
            try:
                os.remove(outp)
            except OSError:
                pass
            raise

    def save(self, path: Union[str, os.PathLike[str]], *, format: Optional[str] = None) -> None:
        exe = _magick_convert()
        if not exe:
            raise RuntimeError("ImageMagick CLI not found (need `magick` or `convert` on PATH)")
        src_path = self._ensure_path()
        dst = os.fspath(path)

        if format:
            dst = f"{format}:{dst}"

        if os.path.basename(exe).lower() == "convert":
            cmd = [exe, src_path, dst]
        else:
            cmd = [exe, "convert", src_path, dst]
        cp = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
        if cp.returncode != 0:
            msg = (cp.stderr or b"").decode("utf-8", "ignore").strip()
            raise RuntimeError(msg or f"convert failed (code={cp.returncode})")
