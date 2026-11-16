"""Encode filesystem paths for SQLite without losing Windows UNC filenames."""

from __future__ import annotations

from pathlib import PurePath
from typing import Literal


def sqlite_file_uri(path: PurePath, *, mode: Literal["ro", "rw", "rwc"]) -> str:
    """Keep drive and UNC paths under an empty URI authority with explicit access.

    Callers supply an absolute path. SQLite normally rejects the hostname
    authority produced by Windows UNC paths; its filename must retain the
    leading double slash instead.
    """
    uri = path.as_uri()
    if not uri.startswith("file:///"):
        uri = "file:////" + uri.removeprefix("file://")
    return f"{uri}?mode={mode}"
