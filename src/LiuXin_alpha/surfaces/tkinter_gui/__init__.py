"""Tkinter desktop surface for LiuXin."""

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
