"""
Locate package-owned runtime resources without inferring a repository checkout.

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
    """
    Locate the Calibre resource subtree through the installed package's resource loader.

    The returned handle is not necessarily a filesystem path. Callers needing a
    directory for inherited conversion APIs should use ``calibre_resource_directory``.

    Example:
        >>> calibre_resource_root().name
        'calibre'


    :return: Traversable handle for the package-owned Calibre resource subtree.
    """

    return files(__package__).joinpath("calibre")


def calibre_resource_directory() -> Path:
    """
    Return the unpacked Calibre resource tree as a filesystem directory.

    LiuXin's inherited conversion APIs pass resource filenames to libraries
    which require real filesystem paths. Standard wheel installations are
    unpacked and satisfy that contract. A zip-import deployment receives a
    direct, actionable failure instead of a later file-not-found error.

    This lookup does not extract archives or verify the inventory of resource files.

    Example:
        >>> calibre_resource_directory().name
        'calibre'


    :return: Filesystem path representing the package-owned resource directory.
    :raises RuntimeError: If the resource loader cannot expose a filesystem path.
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
