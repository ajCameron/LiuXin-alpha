"""
Fallback layer: beta (more portable, lower feature tier).

For `imageops`, beta means "shell out to ImageMagick CLI" (convert/magick).
"""

from __future__ import annotations

from ..imageops_alt_fallback import *  # noqa: F403,F401

import shutil
import subprocess


def __liuxin_plugin_probe__():
    exe = shutil.which("magick") or shutil.which("convert")
    if not exe:
        return False, "no ImageMagick CLI (magick/convert) on PATH"
    try:
        # Fast sanity: version command should return quickly
        if exe.lower().endswith("magick"):
            cmd = [exe, "-version"]
        else:
            cmd = [exe, "-version"]
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False, timeout=0.5)
        return True, "cli ok"
    except Exception as e:
        return False, str(e)
