"""LiuXin path constants - resource and execution paths.

This module is imported very early during startup (often during module import
side-effects). Keep it lightweight and dependency-free.

Environment overrides (useful for tests and portable installs):

* ``LIUXIN_BASE_DIR``   : Base folder for top-level LiuXin folders.
* ``LIUXIN_PREFS_DIR``  : Overrides ``LiuXin_prefs_folder``.
* ``LIUXIN_CONFIG_DIR`` : Overrides ``config_dir`` / calibre-style config dir.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable, Optional


CONFIG_DIR_MODE = 0o0700


def rebuild_file_path(split_file_path: Iterable[str]) -> str:
    """Rebuild a filesystem path from parts.

    Historically this function lived here and returned ``False`` for empty
    input. Returning a boolean from a function annotated as ``str`` is a footgun,
    so the modern behaviour is to return an empty string.
    """

    parts = list(split_file_path)
    if not parts:
        return ""
    return os.path.join(*parts)


def _env_path(key: str) -> Optional[Path]:
    v = os.environ.get(key)
    if not v:
        return None
    return Path(v).expanduser()


def _resolve_base_dir() -> Path:
    """Resolve the LiuXin base folder.

    Preference order:
    1) Explicit env override.
    2) Project root (parent of a ``src`` directory containing this file).
    3) Nearest ancestor containing a typical project marker.
    4) Current working directory.
    """

    env = _env_path("LIUXIN_BASE_DIR")
    if env is not None:
        return env.resolve()

    here = Path(__file__).resolve()

    # Typical layout: <root>/src/LiuXin_alpha/constants/paths.py
    for parent in here.parents:
        if parent.name == "src":
            return parent.parent.resolve()

    markers = ("pyproject.toml", "setup.cfg", "setup.py", ".git")
    for parent in here.parents:
        if any((parent / m).exists() for m in markers):
            return parent.resolve()

    return Path.cwd().resolve()


def _makedirs(path: Path, mode: int = CONFIG_DIR_MODE) -> None:
    """Create a directory tree if needed.

    Uses ``os.makedirs`` so that the *mode* is applied on POSIX when possible.
    """

    os.makedirs(str(path), mode=mode, exist_ok=True)


# ---------------------------------------------------------------------------
# Base folders
# ---------------------------------------------------------------------------

_BASE_DIR = _resolve_base_dir()
_PREFS_DIR = _env_path("LIUXIN_PREFS_DIR") or (_BASE_DIR / "LiuXin_prefs")
_CONFIG_DIR = _env_path("LIUXIN_CONFIG_DIR") or (_PREFS_DIR / "calibre_config")

# Keep side effects small but sufficient: `Config` touches files under config_dir.
_makedirs(_PREFS_DIR, mode=CONFIG_DIR_MODE)
_makedirs(_CONFIG_DIR, mode=CONFIG_DIR_MODE)


# ---------------------------------------------------------------------------
# Public constants (strings for compatibility with legacy callers)
# ---------------------------------------------------------------------------

# The folder LiuXin considers its base.
LiuXin_base_folder = str(_BASE_DIR)

# Historical name; no longer depends on CWD.
LiuXin_path = str(Path(__file__).resolve().parent)

# Preferences and config folders
LiuXin_prefs_folder = str(_PREFS_DIR)
LiuXin_calibre_prefs_folder = str(Path(LiuXin_prefs_folder) / "calibre_prefs")
LiuXin_calibre_caches = str(Path(LiuXin_calibre_prefs_folder) / "caches")
LiuXin_calibre_config_folder = str(_CONFIG_DIR)

# Calibre compatibility constant name.
config_dir = LiuXin_calibre_config_folder

# Other operational folders
LiuXin_scratch_folder = str(Path(LiuXin_base_folder) / "LiuXin_scratch")
LiuXin_debug_folder = str(Path(LiuXin_base_folder) / "LiuXin_debug")

# Resources folder
LiuXin_resources_folder = str(Path(LiuXin_base_folder) / "LiuXin_resources")

# Path to the calibre resources folder (both names retained for compatibility).
# Prefer an existing resources tree so test isolation can point at either the
# modern LiuXin_resources layout or the older LiuXin_data/calibre_resources
# layout without leaking stale paths between reloads.
_CALIBRE_RESOURCES_CANDIDATES = (
    Path(LiuXin_resources_folder) / "calibre_resources",
    Path(LiuXin_base_folder) / "LiuXin_data" / "calibre_resources",
)
for _candidate in _CALIBRE_RESOURCES_CANDIDATES:
    if _candidate.exists():
        _calibre_resources_path = _candidate
        break
else:
    _calibre_resources_path = _CALIBRE_RESOURCES_CANDIDATES[0]

LiuXin_calibre_resources_folder = str(_calibre_resources_path)
LiuXin_calibre_resources = LiuXin_calibre_resources_folder

# Data folders
LiuXin_data_folder = str(Path(LiuXin_base_folder) / "LiuXin_data")

LiuXin_database_folder = str(Path(LiuXin_data_folder) / "databases")
LiuXin_default_database = str(Path(LiuXin_database_folder) / "LX_default_database.db")
LiuXin_local_covers_path = str(Path(LiuXin_data_folder) / "covers")
LiuXin_data_sources = str(Path(LiuXin_data_folder) / "data_sources")

# Programs
LiuXin_program_folder = str(Path(LiuXin_base_folder) / "LiuXin_programs")

# Import cache folders
LiuXin_import_cache = str(Path(LiuXin_base_folder) / "LiuXin_ic")
LiuXin_ic_new_books = str(Path(LiuXin_import_cache) / "new_books")
LiuXin_ic_compressed_files = str(Path(LiuXin_import_cache) / "compressed_files")


# ---------------------------------------------------------------------------
# Sundry path constants
# ---------------------------------------------------------------------------

LiuXin_utils_folder = str(Path(LiuXin_base_folder) / "utils")
LiuXin_plugins_folder = str(Path(LiuXin_base_folder) / "LiuXin_plugins")
LiuXin_calibre_portable = str(Path(LiuXin_plugins_folder) / "Calibre Portable")
LiuXin_calibre_plugins_folder = str(Path(LiuXin_calibre_portable) / "Calibre")

# Paths for the various plugins available to calibre portable.
LiuXin_pdftohtml_path = str(Path(LiuXin_calibre_plugins_folder) / "pdftohtml.exe")
LiuXin_ebook_meta_path = str(Path(LiuXin_calibre_plugins_folder) / "ebook-meta.exe")

# Todo: Rename to be less confusing
LiuXin_calibre_plugins_store = str(Path(LiuXin_plugins_folder) / "plugins from calibre")
