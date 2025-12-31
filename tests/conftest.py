"""Test configuration.

These tests are designed to run both when the project is installed (e.g.
`pip install -e .`) and when running directly from a source checkout.
"""

from __future__ import annotations

import sys
from pathlib import Path


def _ensure_src_on_path() -> None:
    root = Path(__file__).resolve().parents[1]
    src = root / "src"
    if src.is_dir():
        src_str = str(src)
        if src_str not in sys.path:
            # Prepend so local sources win over any globally installed version.
            sys.path.insert(0, src_str)


_ensure_src_on_path()
