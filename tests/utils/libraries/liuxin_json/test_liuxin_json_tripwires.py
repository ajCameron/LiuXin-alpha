"""Tripwire tests for LiuXin's JSON fork.

These tests run a *nested* pytest invocation in a fresh subprocess to catch the
exact class of failure you previously hit:

  - LiuXin accidentally monkeypatches stdlib `json` (or `json.decoder`)
  - pytest's cacheprovider attempts to read a corrupted/empty cache JSON file
  - instead of a recoverable ValueError, something uncaught is raised and the
    pytest process crashes.

The subprocess approach makes these tests immune to import order / prior state
in the current test session.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest


def _project_root() -> Path:
    """Locate the project root by walking up until we find src/ and tests/."""
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "src").exists() and (parent / "tests").exists():
            return parent
    return Path.cwd().resolve()


@pytest.mark.parametrize(
    "corrupt_content",
    [
        "",          # empty file
        "{",         # truncated JSON
        "}{",        # malformed JSON
        "\ufeff",    # BOM only
    ],
)
def test_tripwire_pytest_cacheprovider_corrupt_lastfailed_does_not_crash(
    tmp_path: Path,
    corrupt_content: str,
) -> None:
    """Running pytest should *not* crash even if lastfailed cache is corrupt.

    This is the same failure mode you saw in the terminal: pytest calls
    Cache.get("cache/lastfailed", {}), which does json.load() on a cache file.
    JSONDecodeError is a ValueError and should be caught by pytest; any crash
    here strongly suggests stdlib json was modified.
    """
    # Minimal test project.
    (tmp_path / "test_dummy.py").write_text(
        "def test_ok():\n"
        "    assert True\n",
        encoding="utf-8",
    )

    # Worst-case: import + instantiate at collection time (conftest import).
    (tmp_path / "conftest.py").write_text(
        "from LiuXin_alpha.utils.libraries.liuxin_json import LiuXinJSON\n"
        "_ = LiuXinJSON()\n"
        "import json as _stdlib_json\n"
        "assert _stdlib_json.loads('{\"x\": 1}')['x'] == 1\n",
        encoding="utf-8",
    )

    # Create corrupted pytest cache file beforehand.
    cache_file = tmp_path / ".pytest_cache" / "v" / "cache" / "lastfailed"
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    cache_file.write_text(corrupt_content, encoding="utf-8")

    root = _project_root()
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root / "src") + os.pathsep + env.get("PYTHONPATH", "")

    # Avoid third-party plugin noise while keeping pytest core plugins enabled.
    env.setdefault("PYTEST_DISABLE_PLUGIN_AUTOLOAD", "1")

    proc = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "--disable-warnings"],
        cwd=str(tmp_path),
        env=env,
        text=True,
        capture_output=True,
    )

    assert proc.returncode == 0, (
        f"Nested pytest failed (returncode={proc.returncode})\n"
        f"--- stdout ---\n{proc.stdout}\n"
        f"--- stderr ---\n{proc.stderr}\n"
    )
