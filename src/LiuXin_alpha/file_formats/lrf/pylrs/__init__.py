"""
Expose the supported pylrs compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
"""
from __future__ import annotations
