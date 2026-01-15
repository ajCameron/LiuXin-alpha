"""Calibre-compatibility shims for LiuXin.

This package hosts import-layer and lightweight compatibility objects so that
third-party calibre plugins can run inside LiuXin without the calibre runtime.

Entry point: :func:`install_calibre_shims`.
"""

from __future__ import annotations

from .install import install_calibre_shims
from .import_diagnostics import (
    calibre_import_failure_logging,
    install_calibre_import_failure_logging,
    install_calibre_meta_path_observer,
    reset_calibre_import_failure_dedupe,
    uninstall_calibre_import_failure_logging,
    uninstall_calibre_meta_path_observer,
)

__all__ = [
    "install_calibre_shims",
    "calibre_import_failure_logging",
    "install_calibre_import_failure_logging",
    "uninstall_calibre_import_failure_logging",
    "reset_calibre_import_failure_dedupe",
    "install_calibre_meta_path_observer",
    "uninstall_calibre_meta_path_observer",
]
