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


def _magick() -> Optional[str]:
    return shutil.which("magick") or shutil.which("identify")


@dataclass
class Image:
    _src: Union[bytes, str, os.PathLike[str]]

    def __post_init__(self) -> None:
        self._tmp_path: Optional[str] = None

    def _ensure_path(self) -> str:
        if isinstance(self._src, (str, os.PathLike)):
            return os.fspath(self._src)
        # bytes: spool to temp file
        if self._tmp_path is None:
            fd, path = tempfile.mkstemp(prefix="liuxin_magick_", suffix=".bin")
            os.close(fd)
            with open(path, "wb") as f:
                f.write(self._src)
            self._tmp_path = path
        return self._tmp_path

    def identify(self) -> Dict[str, Any]:
        exe = _magick()
        if not exe:
            raise RuntimeError("ImageMagick CLI not found (need `magick` or `identify` on PATH)")
        path = self._ensure_path()

        if os.path.basename(exe).lower() == "identify":
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
        w, h, fmt, size = parts[0], parts[1], parts[2], " ".join(parts[3:])
        try:
            w_i = int(w)
            h_i = int(h)
        except Exception:
            w_i = None
            h_i = None
        return {"width": w_i, "height": h_i, "format": fmt, "size": size}

    def close(self) -> None:
        if self._tmp_path:
            try:
                os.remove(self._tmp_path)
            except Exception:
                pass
            self._tmp_path = None

    def __del__(self) -> None:
        self.close()
