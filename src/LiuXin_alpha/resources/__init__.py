"""Locate package-owned runtime resources.

The compatibility layer still exposes filesystem paths to older callers, so
normal LiuXin installations must unpack the wheel. ``importlib.resources`` is
the authoritative package lookup rather than an inferred repository root.
"""

from __future__ import annotations

import os
from importlib.resources import files
from importlib.resources.abc import Traversable
from pathlib import Path


def calibre_resource_root() -> Traversable:
    """Return the package-owned Calibre resource tree."""

    return files(__package__).joinpath("calibre")


def calibre_resource_directory() -> Path:
    """Return the unpacked Calibre resource tree as a filesystem directory.

    LiuXin's inherited conversion APIs pass resource filenames to libraries
    which require real filesystem paths. Standard wheel installations are
    unpacked and satisfy that contract. A zip-import deployment receives a
    direct, actionable failure instead of a later file-not-found error.
    """

    root = calibre_resource_root()
    if isinstance(root, Path):
        return root
    try:
        return Path(os.fspath(root))
    except TypeError as exc:
        raise RuntimeError(
            "LiuXin Calibre resources require an unpacked package installation"
        ) from exc


__all__ = ["calibre_resource_directory", "calibre_resource_root"]
