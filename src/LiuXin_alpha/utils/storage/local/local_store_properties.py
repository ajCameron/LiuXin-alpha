"""
Inspect capacity and path properties for local storage roots.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise local store properties through a consuming regression::

        python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py
"""
from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Union


def get_free_bytes(
    path: Union[str, os.PathLike[str]],
    include_reserved: bool = False,
) -> int:
    """
    Return free bytes on the filesystem that contains `path`, platform-independently.

    Example:
        Exercise get free bytes through a consuming regression::

            python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :param include_reserved: Value supplied for include reserved under the utility
        contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    p = Path(path)

    # Find nearest existing ancestor so callers can pass "future" paths
    cur = p
    while not cur.exists():
        parent = cur.parent
        if parent == cur:
            raise FileNotFoundError(f"No existing parent directory for: {path!r}")
        cur = parent

    if include_reserved and hasattr(os, "statvfs"):
        st = os.statvfs(cur)
        return int(st.f_frsize * st.f_bfree)

    return int(shutil.disk_usage(cur).free)
