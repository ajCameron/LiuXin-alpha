"""
Expose the supported ebooks compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
"""
# Package marker for calibre compatibility shims.
