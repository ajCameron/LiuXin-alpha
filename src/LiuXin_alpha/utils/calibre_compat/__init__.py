"""
Expose the supported calibre compat compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
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
