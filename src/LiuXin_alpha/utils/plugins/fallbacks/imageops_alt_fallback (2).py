# -*- coding: utf-8 -*-
"""
Provide imageops alt fallback (2) utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise imageops alt fallback (2) through a consuming regression::

        python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
"""

from __future__ import annotations

import shutil
import subprocess
from typing import Optional


def _convert_cmd() -> Optional[list]:
    """
    Return the base ImageMagick command.

    Example:
        Exercise  convert cmd through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    magick = shutil.which("magick")
    if magick:
        return [magick]
    convert = shutil.which("convert")
    if convert:
        return [convert]
    return None


def resize(data: bytes, width: int, height: int, fmt: str = "png") -> bytes:
    """
    Preform a resize operation on a image.

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
    cmd = _convert_cmd()
    if not cmd:
        raise RuntimeError("ImageMagick CLI not found (need `magick` or `convert` on PATH)")

    w = int(width)
    h = int(height)
    fmt = str(fmt).lower().strip() or "png"

    # Important: provide an explicit input spec, otherwise ImageMagick treats the
    # output spec (e.g. `ppm:-`) as the input and fails with "no images defined".
    in_spec = "-"
    if fmt in ("ppm", "pgm", "pbm"):
        in_spec = f"{fmt}:-"

    full = cmd + [in_spec, "-resize", f"{w}x{h}", f"{fmt}:-"]
    cp = subprocess.run(full, input=data, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    if cp.returncode != 0:
        msg = (cp.stderr or b"").decode("utf-8", "ignore").strip()
        raise RuntimeError(msg or f"convert failed with code {cp.returncode}")
    return bytes(cp.stdout or b"")
