# -*- coding: utf-8 -*-

"""
Expose the supported azw4 compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/file_formats/azw4/test_azw4_reader_and_input.py
"""
from __future__ import annotations
from .reader import Reader, extract_embedded_pdf_bytes, unwrap

__all__ = ["Reader", "extract_embedded_pdf_bytes", "unwrap"]
