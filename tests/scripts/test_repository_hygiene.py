"""Keep reproducible benchmark scratch out of future repository snapshots."""

from __future__ import annotations

import subprocess
from pathlib import Path


def test_generated_scratch_is_not_tracked() -> None:
    """Ignore rules alone cannot prevent already tracked caches from returning."""
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        ["git", "ls-files", "-z", "--", ".tmp"],
        cwd=root,
        capture_output=True,
        check=True,
    )
    tracked = result.stdout.decode().strip("\0").split("\0") if result.stdout else []
    assert not tracked, f"Generated benchmark scratch is tracked: {tracked}"
