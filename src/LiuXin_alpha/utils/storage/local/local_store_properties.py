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

    - Default (include_reserved=False): free bytes available in normal usage.
      This matches shutil.disk_usage(...).free (cross-platform).
    - On POSIX only, include_reserved=True: includes blocks reserved for root
      (uses statvfs f_bfree). On Windows, this flag has no effect.

    `path` may be a file or directory; if it doesn't exist yet, we walk up to the
    nearest existing parent directory.
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
