#!/usr/bin/env python3
"""Compatibility wrapper for LiuXin's packaged mixed-ingest CLI."""

from __future__ import annotations

import sys

from pathlib import Path


EXAMPLES_ROOT = Path(__file__).resolve().parents[1]
if str(EXAMPLES_ROOT) not in sys.path:
    sys.path.insert(0, str(EXAMPLES_ROOT))

from _example_utils import bootstrap_src_path  # pyright: ignore[reportImplicitRelativeImport]


_ = bootstrap_src_path()

from LiuXin_alpha.surfaces.cli.storage import ingest_main


def main() -> int:
    return ingest_main()


if __name__ == "__main__":
    raise SystemExit(main())
