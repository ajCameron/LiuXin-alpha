"""
Fallback layer: alpha (prefer fast/featureful backends).

For `magick`, alpha means "Wand/ MagickWand bindings" — in-process calls
(if the underlying ImageMagick libs are available).
"""

from __future__ import annotations

from ..magick import *  # noqa: F403,F401


def __liuxin_plugin_probe__():
    try:
        from wand.image import Image as WandImage  # type: ignore
        # Tiny functional probe: create 1x1 and blob it (catches missing IM libs)
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
