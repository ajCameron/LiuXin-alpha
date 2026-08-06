from __future__ import annotations

from typing import Optional


def main(argv: Optional[list[str]] = None) -> int:
    """Load the operational CLI only when it is actually invoked."""
    from LiuXin_alpha.surfaces.cli.squashfs import main as squashfs_main

    return squashfs_main(argv)


__all__ = ["main"]
