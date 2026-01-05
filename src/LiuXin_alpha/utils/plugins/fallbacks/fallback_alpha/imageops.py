"""
Fallback layer: alpha (prefer fast/featureful backends).

For `imageops`, alpha means "Wand bindings" (and then whatever the base
implementation chooses internally).
"""

from __future__ import annotations

from ..imageops import *  # noqa: F403,F401


def __liuxin_plugin_probe__():
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
