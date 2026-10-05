"""
Support ingest path planning and best-effort report-directory durability.

Containment is lexical and callers must resolve paths when physical containment
is needed. Existence and directory synchronization are point-in-time observations,
not reservations, locking, or a guarantee against later filesystem changes.
"""

from __future__ import annotations

import os
from pathlib import Path


def _path_is_within(path: Path, directory: Path) -> bool:
    """
    Test lexical Path.relative_to containment, including equality with the directory.

    Do not resolve symlinks or '..' components; caller-supplied path preparation
    determines whether this test corresponds to physical filesystem containment.

    Example:
        >>> _path_is_within(Path("source/book"), Path("source"))
        True
        >>> _path_is_within(Path("source/../outside"), Path("source"))
        True


    :param path: Candidate path whose lexical prefix is checked.
    :param directory: Directory prefix in the same absolute/relative coordinate system.
    :return: False only when relative_to raises ValueError; otherwise True.
    """
    try:
        path.relative_to(directory)
    except ValueError:
        return False
    return True


def _nearest_existing_parent(path: Path) -> Path:
    """
    Walk upwards from the supplied path until it exists or cannot ascend further.

    The starting path itself can be returned and need not be a directory. Path
    existence follows symlinks; the final root/anchor can still be nonexistent.

    Example:
        >>> str(_nearest_existing_parent(Path(".")))
        '.'


    :param path: Starting location, not expanded or resolved by this helper.
    :return: First observed existing candidate, or the final nonascending path.
    """
    candidate = path
    while not candidate.exists() and candidate != candidate.parent:
        candidate = candidate.parent
    return candidate


def _fsync_directory(path: Path) -> None:
    """
    Attempt to synchronize a report's directory entry without requiring fsync support.

    Ignore OSError from opening or syncing the directory, but always close an
    opened descriptor. A close error still propagates and durability is not proven.

    Example:
        >>> _fsync_directory(Path("report-directory"))  # doctest: +SKIP


    :param path: Parent directory opened read-only, with O_DIRECTORY when available.
    :return: None after best-effort synchronization and descriptor cleanup.
    """

    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)
