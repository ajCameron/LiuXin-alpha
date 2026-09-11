"""
Parse terminal options and compose a Core session with scripted, plain, or curses browsing.

Command mode takes precedence over UI selection. Curses imports are deferred until
windowed execution; parser construction and plain/scripted entry points do not
load the curses adapter. Database creation uses the separate wizard owner.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from LiuXin_alpha.core import CoreClientAPI
from LiuXin_alpha.surfaces.core import (
    add_core_client_arguments,
    open_surface_core_from_args,
)
from LiuXin_alpha.surfaces.terminal.browser import TextDatabaseBrowser
from LiuXin_alpha.surfaces.terminal.database_creation import (
    create_database_from_wizard,
    run_database_creation_wizard,
)


def build_parser() -> argparse.ArgumentParser:
    """
    Build the terminal argument parser with Core connection, creation, paging, and UI options.

    Repeatable ``--command`` values are collected in order. Numeric options are
    converted by argparse but generally clamped by their runtime owners, not here.
    Parser construction neither opens a database nor loads the curses adapter.

    Example:
        >>> args = build_parser().parse_args(["--database", "example.sqlite", "--command", "tables"])
        >>> (args.ui_mode, args.page_size, args.command)
        ('plain', 20, ['tables'])


    :return: New argument parser for the terminal application entry point.
    """
    parser = argparse.ArgumentParser(description="LiuXin text browser")
    add_core_client_arguments(parser, database_help="Path to LiuXin database")
    parser.add_argument(
        "--db-type", default="SQLite", help="Database backend type (default: SQLite)"
    )
    parser.add_argument(
        "--create-new-db",
        action="store_true",
        help="Run an interactive wizard and create a new database before browsing.",
    )
    parser.add_argument(
        "--page-size", type=int, default=20, help="Default browse page size"
    )
    parser.add_argument(
        "--ui-mode",
        choices=("plain", "windowed"),
        default="plain",
        help="UI mode: plain line-based shell (default) or windowed split-pane curses UI.",
    )
    parser.add_argument(
        "--windowed-status-refresh-s",
        type=float,
        default=1.0,
        help="Windowed UI status board refresh interval in seconds (default: 1.0).",
    )
    parser.add_argument(
        "--windowed-status-height",
        type=int,
        default=9,
        help="Windowed UI status board height in terminal rows (default: 9).",
    )
    parser.add_argument(
        "--windowed-job-panel-height",
        type=int,
        default=10,
        help="Windowed UI dedicated job-output panel height in terminal rows (default: 10).",
    )
    parser.add_argument(
        "--windowed-telemetry-panel-height",
        type=int,
        default=9,
        help="Windowed UI dedicated telemetry panel height in terminal rows (default: 9).",
    )
    parser.add_argument(
        "--history-file",
        default=None,
        help=(
            "Optional readline history file path. "
            "Default: $XDG_STATE_HOME/liuxin_alpha/terminal_history "
            "or ~/.local/state/liuxin_alpha/terminal_history."
        ),
    )
    parser.add_argument(
        "--no-create-if-missing",
        action="store_true",
        help="Fail if the database path does not exist (default creates a new library database).",
    )
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
    parser.add_argument(
        "--command",
        action="append",
        default=[],
        help="Execute command(s) non-interactively; may be repeated.",
    )
    return parser


def run_windowed_text_browser(
    core: CoreClientAPI,
    *,
    page_size: int = 20,
    history_file: str | Path | None = None,
    status_refresh_s: float = 1.0,
    status_height: int = 9,
    job_panel_height: int = 10,
    telemetry_panel_height: int = 9,
) -> int:
    """
    Lazily load the curses adapter, normalize its configuration, and run it with the supplied Core client.

    Status height clamps to five rows and both auxiliary heights to four. Refresh
    seconds are converted to float; the drawing owner applies its minimum cadence.
    Import, configuration-conversion, and UI execution failures propagate.

    Example:
        >>> status = run_windowed_text_browser(core, page_size=25)  # doctest: +SKIP


    :param core: Core client used by the windowed browser; this helper does not open a session.
    :param page_size: Default row-page size forwarded to the browser composition.
    :param history_file: Optional path forwarded to windowed input/history management.
    :param status_refresh_s: Requested status/telemetry refresh interval in seconds.
    :param status_height: Requested status-pane height in terminal rows.
    :param job_panel_height: Requested job-output-pane height in terminal rows.
    :param telemetry_panel_height: Requested telemetry-pane height in terminal rows.
    :return: Exit status returned by the windowed browser runner.
    """
    from LiuXin_alpha.surfaces.terminal.windowed_ui import (
        WindowedUiConfig,
        run_windowed_browser,
    )

    config = WindowedUiConfig(
        status_refresh_s=float(status_refresh_s),
        status_height=max(5, int(status_height)),
        job_panel_height=max(4, int(job_panel_height)),
        telemetry_panel_height=max(4, int(telemetry_panel_height)),
    )
    return run_windowed_browser(
        core,
        page_size=page_size,
        history_file=history_file,
        config=config,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """
    Parse options, optionally create a database, then run the selected terminal mode in a Core session.

    Creation-wizard cancellation returns one; combining that wizard with an explicit
    ``--core-endpoint`` option is rejected. Missing database paths can trigger parent creation
    unless disabled. Repeated commands run before considering windowed/plain mode.
    The Core context closes after the chosen runner returns or raises.

    Operational ``Exception`` failures print an ERROR line to stderr and return two.
    Argument parsing occurs outside that handler, so argparse's SystemExit behavior
    remains intact; KeyboardInterrupt is not handled here either.

    Example:
        >>> status = main(["--database", "library.sqlite", "--command", "tables"])  # doctest: +SKIP


    :param argv: Argument tokens excluding the executable name, or ``None`` for process arguments.
    :return: Selected runner's status, one for wizard cancellation, or two for a caught operational error.
    """
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        database_path = args.database
        db_type = args.db_type

        if args.create_new_db:
            if args.core_endpoint:
                raise ValueError(
                    "--create-new-db cannot be combined with --core-endpoint."
                )
            wizard_config = run_database_creation_wizard(
                default_database_path=database_path,
                default_db_type=db_type,
                input_stream=sys.stdin,
                output_stream=sys.stdout,
            )
            if wizard_config is None:
                return 1
            created_path = create_database_from_wizard(wizard_config)
            database_path = str(created_path)
            db_type = wizard_config.db_type

        if database_path is not None:
            db_path = Path(database_path).expanduser()
            create_if_missing = bool(
                not args.no_create_if_missing and not db_path.exists()
            )
            if create_if_missing:
                db_path.parent.mkdir(parents=True, exist_ok=True)
        else:
            create_if_missing = False

        args.database = database_path
        args.db_type = db_type
        with open_surface_core_from_args(
            args,
            create=create_if_missing,
            enable_storage_manager=bool(args.enable_storage_manager),
            enable_maintenance=bool(args.enable_maintenance),
            repair_bootstrap_rows=bool(args.repair_bootstrap_rows),
        ) as core_session:
            if args.command:
                shell = TextDatabaseBrowser(
                    core_session.client,
                    page_size=args.page_size,
                    output=sys.stdout,
                    history_file=args.history_file,
                )
                return shell.run_commands(args.command)

            if args.ui_mode == "windowed":
                return run_windowed_text_browser(
                    core_session.client,
                    page_size=args.page_size,
                    history_file=args.history_file,
                    status_refresh_s=args.windowed_status_refresh_s,
                    status_height=args.windowed_status_height,
                    job_panel_height=args.windowed_job_panel_height,
                    telemetry_panel_height=args.windowed_telemetry_panel_height,
                )

            shell = TextDatabaseBrowser(
                core_session.client,
                page_size=args.page_size,
                output=sys.stdout,
                history_file=args.history_file,
            )
            return shell.run()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


__all__ = ["build_parser", "run_windowed_text_browser", "main"]
