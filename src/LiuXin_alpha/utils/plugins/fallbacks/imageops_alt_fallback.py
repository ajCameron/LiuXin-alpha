# -*- coding: utf-8 -*-
"""
Pure-python fallback for the compiled ``imageops`` extension.

The compiled extension provides various image manipulation operations. This fallback
offers a tiny subset by shelling out to ImageMagick (if available).

Currently implemented:
    - resize(data: bytes, width: int, height: int, fmt: str = "png") -> bytes

If ImageMagick isn't available, these functions raise RuntimeError.
"""

from __future__ import annotations

import shutil
import subprocess
from typing import Optional


def _convert_cmd() -> Optional[list]:
    """
    Generate the base of the conversion command.

    :return:
    """
    magick = shutil.which("magick")
    if magick:
        return [magick, "convert"]
    convert = shutil.which("convert")
    if convert:
        return [convert]
    return None


def resize(data: bytes, width: int, height: int, fmt: str = "png") -> bytes:
    """
    Preform a resize operation on a image.

    :param data:
    :param width:
    :param height:
    :param fmt:
    :return:
    """
    cmd = _convert_cmd()
    if not cmd:
        raise RuntimeError("ImageMagick CLI not found (need `magick` or `convert` on PATH)")
    w = int(width)
    h = int(height)
    fmt = str(fmt).lower().strip() or "png"
    full = cmd + ["-resize", f"{w}x{h}", f"{fmt}:-"]
    cp = subprocess.run(full, input=data, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    if cp.returncode != 0:
        msg = (cp.stderr or b"").decode("utf-8", "ignore").strip()
        raise RuntimeError(msg or f"convert failed with code {cp.returncode}")
    return bytes(cp.stdout or b"")
