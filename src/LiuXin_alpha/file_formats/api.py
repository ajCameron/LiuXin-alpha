"""
Expose stable file-format discovery, metadata and conversion entry points.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise api through a consuming regression::

        python -m pytest -q tests/file_formats/test_top_level_file_formats_helpers.py
"""
from __future__ import annotations
