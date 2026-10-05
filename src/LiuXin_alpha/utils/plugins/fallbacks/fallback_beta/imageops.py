"""
Provide imageops utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise imageops through a consuming regression::

        python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
"""

from __future__ import annotations

from ..imageops_alt_fallback import *  # noqa: F403,F401

import shutil
import subprocess


def __liuxin_plugin_probe__():
    """
    Perform the liuxin plugin probe utility operation under explicit compatibility rules.

    Example:
        Exercise   liuxin plugin probe   through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
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
