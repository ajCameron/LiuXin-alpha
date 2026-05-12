"""Launcher and CLI for the LiuXin Tkinter GUI surface."""

from __future__ import annotations

import argparse

from pathlib import Path
from typing import Optional, Sequence

from .backend import TkGuiBackend
from .controller import TkGuiApplication, open_tk_modules
from .session import TkGuiSession
from .state import TableSchema, TkGuiConfig, coerce_positive_int


def run_tkinter_gui(config: TkGuiConfig) -> int:
    tk, _ttk, _filedialog, _messagebox = open_tk_modules()
    root = tk.Tk()
    app = TkGuiApplication(root, config=config)
    try:
        root.mainloop()
    finally:
        app.close()
    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the LiuXin Tkinter GUI.")
    parser.add_argument("--database", required=True, help="Path to a LiuXin database.")
    parser.add_argument("--db-type", default="sqlite", help="Database driver type. Default: sqlite.")
    parser.add_argument("--title", default="LiuXin", help="Window title.")
    parser.add_argument("--page-size", type=int, default=100, help="Rows shown per page.")
    parser.add_argument(
        "--enable-storage-manager",
        action="store_true",
        help="Bootstrap storage manager integration when opening the database. Slower startup.",
    )
    parser.add_argument(
        "--enable-maintenance",
        action="store_true",
        help="Start the background maintenance service when opening the database. Slower startup.",
    )
    parser.add_argument(
        "--repair-bootstrap-rows",
        action="store_true",
        help="Run rating/null-row bootstrap repairs while opening the database. May write to the database.",
    )
    return parser


def config_from_args(args: argparse.Namespace) -> TkGuiConfig:
    return TkGuiConfig(
        database=Path(args.database).expanduser(),
        db_type=str(args.db_type),
        title=str(args.title),
        page_size=coerce_positive_int(args.page_size, default=100, maximum=1000),
        enable_storage_manager=bool(args.enable_storage_manager),
        enable_maintenance=bool(args.enable_maintenance),
        repair_bootstrap_rows=bool(args.repair_bootstrap_rows),
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    return run_tkinter_gui(config_from_args(args))


__all__ = [
    "TkGuiApplication",
    "TkGuiBackend",
    "TkGuiConfig",
    "TkGuiSession",
    "TableSchema",
    "build_arg_parser",
    "config_from_args",
    "main",
    "run_tkinter_gui",
]
