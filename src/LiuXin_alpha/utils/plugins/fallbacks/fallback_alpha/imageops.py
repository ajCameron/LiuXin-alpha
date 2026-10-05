"""
Provide imageops utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise imageops through a consuming regression::

        python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
"""

from __future__ import annotations

from ..imageops import *  # noqa: F403,F401


def __liuxin_plugin_probe__():
    """
    Perform the liuxin plugin probe utility operation under explicit compatibility rules.

    Example:
        Exercise   liuxin plugin probe   through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    try:
        from wand.image import Image as WandImage  # type: ignore
        img = WandImage(width=1, height=1)  # type: ignore[call-arg]
        try:
            img.make_blob()  # type: ignore[attr-defined]
        finally:
            try:
                img.close()  # type: ignore[attr-defined]
            except Exception:
                pass
        return True, "wand ok"
    except Exception as e:
        return False, str(e)
