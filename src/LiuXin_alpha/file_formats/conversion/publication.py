"""Publish a completed local conversion without replacing existing output."""

from __future__ import annotations

import os
import shutil
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any


def publish_conversion(
    destination: str,
    convert: Callable[[str], dict[str, Any]],
    *,
    checkpoint: Callable[[], None] = lambda: None,
) -> dict[str, Any]:
    """Stage beside the destination, fsync, and atomically claim an unused name.

    Failures before the link leave the destination untouched. A failure after the
    link may leave published output and must be inspected before retrying. Hard
    links are required so concurrent writers cannot silently replace each other.
    No converter interruption or automatic retry is implied.
    """
    output = Path(destination).expanduser().absolute()
    checkpoint()
    if output.exists() or output.is_symlink():
        raise FileExistsError(f"Conversion destination already exists: {output}")
    descriptor, staging_name = tempfile.mkstemp(
        prefix=".liuxin-conversion-", suffix=output.suffix, dir=output.parent
    )
    os.close(descriptor)
    staging = Path(staging_name)
    try:
        metadata = convert(str(staging))
        if not staging.is_file() or staging.is_symlink():
            raise ValueError("Converter did not produce a regular staged file")
        with staging.open("rb") as handle:
            size = os.fstat(handle.fileno()).st_size
            if size == 0:
                raise ValueError("Converter produced an empty staged file")
            os.fsync(handle.fileno())
        checkpoint()
        os.link(staging, output)
        if os.name == "posix":
            directory = os.open(output.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        return {
            **metadata,
            "output_path": str(output),
            "exists": True,
            "size_bytes": size,
            "publication": "committed",
        }
    finally:
        if staging.is_dir() and not staging.is_symlink():
            # The converter replaced our owned staging file with a directory.
            # Remove that private output without masking the regular-file error.
            shutil.rmtree(staging)
        else:
            staging.unlink(missing_ok=True)
