"""
Expose the supported tkinter gui compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/surfaces/test_tkinter_gui.py
"""

from __future__ import annotations

from .app import build_arg_parser, config_from_args, main, run_tkinter_gui
from .backend import TkGuiBackend
from .controller import TkGuiApplication
from .session import TkGuiSession
from .state import RowPage, TableSchema, TableSummary, TkGuiConfig
from .tasks import TkGuiTaskHandle, TkGuiTaskResult, TkGuiTaskRunner

__all__ = [
    "RowPage",
    "TableSchema",
    "TableSummary",
    "TkGuiApplication",
    "TkGuiBackend",
    "TkGuiConfig",
    "TkGuiSession",
    "TkGuiTaskHandle",
    "TkGuiTaskResult",
    "TkGuiTaskRunner",
    "build_arg_parser",
    "config_from_args",
    "main",
    "run_tkinter_gui",
]
