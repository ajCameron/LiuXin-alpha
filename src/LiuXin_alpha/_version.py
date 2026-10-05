"""
Expose the installed LiuXin version and build metadata.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise  version through a consuming regression::

        python -m pytest -q tests/surfaces/test_surface_read_errors.py
"""

LIUXIN_NUMERIC_VERSION = (0, 0, 8)
__version__ = "0.0.8"

__all__ = ["LIUXIN_NUMERIC_VERSION", "__version__"]
