"""
Expose the supported maps compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/file_formats/lit/test_lit_modernized.py
"""
from __future__ import annotations

from LiuXin_alpha.file_formats.lit.maps.opf import MAP as OPF_MAP
from LiuXin_alpha.file_formats.lit.maps.html import MAP as HTML_MAP

__license__ = "GPL v3"
__copyright__ = "2008, Marshall T. Vandegrift <llasram@gmail.com>"

# OPF_MAP, HTML_MAP  # To make pyflakes shut up
assert OPF_MAP is not None
assert HTML_MAP is not None
