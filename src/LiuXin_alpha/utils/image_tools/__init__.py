"""
Expose the supported image tools compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py
"""
