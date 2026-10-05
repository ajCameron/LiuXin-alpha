"""
Expose the supported views compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/surfaces/test_tkinter_gui.py
"""

from __future__ import annotations

from .database_toolbar import DatabaseToolbar
from .inspector import DetailInspector
from .metadata_panel import MetadataPanel
from .row_grid import RowGrid
from .status_bar import StatusBar
from .table_sidebar import TableSidebar

__all__ = [
    "DatabaseToolbar",
    "DetailInspector",
    "MetadataPanel",
    "RowGrid",
    "StatusBar",
    "TableSidebar",
]
