"""Calibre metadata implementations and import diagnostics.

Import implementations from LiuXin_alpha.utils.calibre_compat directly. This
package does not install aliases under the external calibre namespace.
"""

from __future__ import annotations

from .import_diagnostics import (
    calibre_import_failure_logging,
    install_calibre_import_failure_logging,
    install_calibre_meta_path_observer,
    reset_calibre_import_failure_dedupe,
    uninstall_calibre_import_failure_logging,
    uninstall_calibre_meta_path_observer,
)

__all__ = [
    "calibre_import_failure_logging",
    "install_calibre_import_failure_logging",
    "uninstall_calibre_import_failure_logging",
    "reset_calibre_import_failure_dedupe",
    "install_calibre_meta_path_observer",
    "uninstall_calibre_meta_path_observer",
]
