"""Locate bundled conversion tools without importing a converter or its plugins."""

from __future__ import annotations

import os
import sys


def bundled_executable_directory() -> str:
    """Return the legacy bundle directory, or empty text for normal PATH lookup."""
    platform = sys.platform.lower()
    if "darwin" in platform:
        return str(getattr(sys, "frameworks_dir", ""))
    if "win32" in platform or "win64" in platform:
        return os.path.dirname(sys.executable) if hasattr(sys, "frozen") else ""
    location = getattr(sys, "executables_location", None)
    if getattr(sys, "frozen", False) and location is not None:
        return os.path.join(str(location), "bin")
    return ""
