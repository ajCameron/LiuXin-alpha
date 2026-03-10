"""Interactive text UI for browsing LiuXin database contents.

This module intentionally provides a read-only shell focused on schema and row
inspection while the broader interfaces layer is still being built out.
"""

from __future__ import annotations

import argparse
import shlex
import shutil
import sys

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence, TextIO

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.interfaces.terminal.commands import (
    IngestDiskCommand,
    LinkCommand,
    LinksCommand,
    NewCreatorWizardCommand,
    NewExpressionWizardCommand,
    NewGenreWizardCommand,
    NewItemWizardCommand,
    NewManifestationWizardCommand,
    NewNoteWizardCommand,
    NewOrganisationWizardCommand,
    NewPublisherWizardCommand,
    NewSeriesWizardCommand,
    NewStoreWizardCommand,
    NewSubjectWizardCommand,
    NewTagWizardCommand,
    NewTitleWizardCommand,
    NewWorkWizardCommand,
    NoteOnCommand,
    OffGenreCommand,
    OffLanguageCommand,
    OffNoteCommand,
    OffSeriesCommand,
    OffSubjectCommand,
    OffTagCommand,
    OnGenreCommand,
    OnLanguageCommand,
    OnNoteCommand,
    OnSeriesCommand,
    OnSubjectCommand,
    OnTagCommand,
    QuitCommand,
    ShowAllCommand,
    ShowGenresCommand,
    ShowLanguageCommand,
    ShowNotesCommand,
    ShowSeriesCommand,
    ShowSubjectsCommand,
    ShowTagsCommand,
    SummaryCommand,
    SyncStoreCommand,
    StoreFilesCommand,
    StoreListCommand,
    StoreShowCommand,
    TerminalCommandAPI,
    TopCommand,
    UnlinkCommand,
)
from LiuXin_alpha.interfaces.terminal.plugins import TerminalLifecyclePluginAPI

try:
    import readline as _readline
except Exception:  # pragma: no cover - platform dependent (e.g. minimal Windows builds)
    _readline = None


@dataclass
class DatabaseCreationWizardConfig:
    """Configuration collected from the interactive database creation wizard."""

    database_path: Path
    db_type: str
    backup_existing: bool
    enable_storage_manager: bool
    strict_storage_manager_bootstrap: bool
    storage_startup_on_add: bool


def _open_database(*, database_path: str, db_type: str, create_if_missing: bool = True) -> Database:
    db_path = Path(database_path).expanduser()
    should_create = bool(create_if_missing and not db_path.exists())
    if should_create:
        db_path.parent.mkdir(parents=True, exist_ok=True)
    return Database(
        metadata={"database_path": str(db_path)},
        db_type=db_type,
        create=should_create,
        backup=False,
    )


def _truncate(value: object, *, width: int = 80) -> str:
    text = repr(value)
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


def _stringify_table_cell(value: object, *, width: int = 60) -> str:
    if value is None:
        text = ""
    else:
        text = str(value)
    text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "\\n")
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


def _shorten_column_headers(headers: Sequence[str], *, table_name: Optional[str] = None) -> list[str]:
    originals = [str(h) for h in headers]
    table_name = (table_name or "").strip().lower()

    known_prefixes = {
        "agent_",
        "human_agent_",
        "org_agent_",
        "store_",
        "file_",
        "folder_",
        "note_",
        "synopsis_",
        "identifier_",
        "series_",
        "tag_",
        "genre_",
        "subject_",
        "image_",
        "language_",
        "book_",
        "title_",
        "work_",
        "expression_",
        "manifestation_",
        "item_",
    }
    if table_name:
        known_prefixes.add(table_name + "_")
        tokens = [token for token in table_name.split("_") if token]
        if tokens:
            tail = tokens[-1]
            if tail.endswith("s") and len(tail) > 1:
                singular_tokens = list(tokens)
                singular_tokens[-1] = tail[:-1]
                known_prefixes.add("_".join(singular_tokens) + "_")

    ordered_prefixes = sorted(known_prefixes, key=len, reverse=True)
    shortened: list[str] = []
    for original in originals:
        text = original
        changed = True
        while changed:
            changed = False
            for prefix in ordered_prefixes:
                if text.startswith(prefix) and len(text) > len(prefix):
                    text = text[len(prefix) :]
                    changed = True
                    break
        if not text:
            text = original
        shortened.append(text)

    # Keep headers unambiguous after shortening.
    counts: dict[str, int] = {}
    deduped: list[str] = []
    for idx, text in enumerate(shortened):
        seen = counts.get(text, 0)
        counts[text] = seen + 1
        if seen == 0:
            deduped.append(text)
        else:
            deduped.append("{}#{}".format(text, seen + 1))

    return deduped


def _render_ascii_table(
    headers: Sequence[str],
    rows: Sequence[Sequence[object]],
    *,
    max_cell_width: int = 60,
    max_table_width: Optional[int] = None,
) -> str:
    if not headers:
        return "(no columns)"

    normalized_headers = [str(h) for h in headers]
    normalized_rows: list[list[str]] = []
    for row in rows:
        cells = [_stringify_table_cell(value, width=max_cell_width) for value in row]
        if len(cells) < len(normalized_headers):
            cells.extend([""] * (len(normalized_headers) - len(cells)))
        elif len(cells) > len(normalized_headers):
            cells = cells[: len(normalized_headers)]
        normalized_rows.append(cells)

    widths = [len(h) for h in normalized_headers]
    for row in normalized_rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))

    if max_table_width is not None and max_table_width > 0:
        # Width model for this renderer:
        # total = 1 + sum(column_width + 3)
        preferred_min_widths = [min(max(6, len(h)), 14) for h in normalized_headers]

        def _table_width(width_values: Sequence[int]) -> int:
            return 1 + sum(w + 3 for w in width_values)

        # If preferred-readable columns won't fit, drop columns from the right.
        omitted_columns = 0
        while widths and _table_width(preferred_min_widths) > max_table_width:
            widths.pop()
            preferred_min_widths.pop()
            normalized_headers.pop()
            for row in normalized_rows:
                row.pop()
            omitted_columns += 1
        if omitted_columns:
            if normalized_headers:
                normalized_headers[-1] = normalized_headers[-1] + " (+{} cols)".format(omitted_columns)
                widths[-1] = max(widths[-1], len(normalized_headers[-1]))
            else:
                return "(table too wide to render in {} columns)".format(max_table_width)

        min_widths = [3 for _ in normalized_headers]

        # Shrink widest columns until table fits.
        while widths and _table_width(widths) > max_table_width:
            widest_idx = max(range(len(widths)), key=lambda idx: widths[idx])
            if widths[widest_idx] <= min_widths[widest_idx]:
                break
            widths[widest_idx] -= 1

        # Re-truncate headers and cells to final widths.
        for idx, header in enumerate(normalized_headers):
            if len(header) > widths[idx]:
                normalized_headers[idx] = _stringify_table_cell(header, width=widths[idx])
        for row in normalized_rows:
            for idx, cell in enumerate(row):
                if len(cell) > widths[idx]:
                    row[idx] = _stringify_table_cell(cell, width=widths[idx])

    divider = "+-" + "-+-".join("-" * width for width in widths) + "-+"
    lines = [divider]
    lines.append("| " + " | ".join(normalized_headers[idx].ljust(widths[idx]) for idx in range(len(widths))) + " |")
    lines.append(divider)
    for row in normalized_rows:
        lines.append("| " + " | ".join(row[idx].ljust(widths[idx]) for idx in range(len(widths))) + " |")
    lines.append(divider)
    return "\n".join(lines)


def _safe_int(value: str) -> Optional[int]:
    try:
        return int(value)
    except Exception:
        return None


def _looks_like_id_selector(token: str) -> bool:
    """Whether token looks like an id selector: `1`, `1,2`, `10-20`, or mixed."""
    text = str(token).strip()
    if not text:
        return False

    parts = text.split(",")
    if not parts:
        return False

    for raw_part in parts:
        part = raw_part.strip()
        if not part:
            return False
        if "-" in part:
            left, right = part.split("-", 1)
            if _safe_int(left.strip()) is None or _safe_int(right.strip()) is None:
                return False
        else:
            if _safe_int(part) is None:
                return False
    return True


def _ask_text(
    prompt: str,
    *,
    default: Optional[str] = None,
    input_stream: TextIO = sys.stdin,
    output_stream: TextIO = sys.stdout,
) -> str:
    suffix = ""
    if default is not None:
        suffix = " [{}]".format(default)
    output_stream.write("{}{}: ".format(prompt, suffix))
    output_stream.flush()
    raw = input_stream.readline()
    if raw == "":
        return default or ""
    value = raw.strip()
    if value == "" and default is not None:
        return default
    return value


def _ask_yes_no(
    prompt: str,
    *,
    default: bool,
    input_stream: TextIO = sys.stdin,
    output_stream: TextIO = sys.stdout,
) -> bool:
    hint = "Y/n" if default else "y/N"
    raw = _ask_text(
        "{} ({})".format(prompt, hint),
        default=None,
        input_stream=input_stream,
        output_stream=output_stream,
    ).strip().lower()
    if raw == "":
        return default
    if raw in {"y", "yes", "1", "true", "t"}:
        return True
    if raw in {"n", "no", "0", "false", "f"}:
        return False
    output_stream.write("Invalid response {!r}; using default {}\n".format(raw, "yes" if default else "no"))
    output_stream.flush()
    return default


def run_database_creation_wizard(
    *,
    default_database_path: str,
    default_db_type: str = "SQLite",
    input_stream: TextIO = sys.stdin,
    output_stream: TextIO = sys.stdout,
) -> Optional[DatabaseCreationWizardConfig]:
    """Interactively collect configuration for creating a new database."""
    output_stream.write("Database creation wizard\n")
    output_stream.write("------------------------\n")
    output_stream.flush()

    db_path_raw = _ask_text(
        "Target database path",
        default=str(Path(default_database_path).expanduser()),
        input_stream=input_stream,
        output_stream=output_stream,
    )
    db_path = Path(db_path_raw).expanduser()
    db_type = _ask_text(
        "Database backend type",
        default=default_db_type,
        input_stream=input_stream,
        output_stream=output_stream,
    ).strip() or default_db_type

    parent = db_path.parent
    if not parent.exists():
        make_parent = _ask_yes_no(
            "Create parent directory {}?".format(parent),
            default=True,
            input_stream=input_stream,
            output_stream=output_stream,
        )
        if not make_parent:
            output_stream.write("Wizard canceled: parent directory creation declined.\n")
            output_stream.flush()
            return None

    backup_existing = False
    if db_path.exists():
        recreate = _ask_yes_no(
            "Database file already exists. Recreate it?",
            default=False,
            input_stream=input_stream,
            output_stream=output_stream,
        )
        if not recreate:
            output_stream.write("Wizard canceled: existing database kept unchanged.\n")
            output_stream.flush()
            return None
        backup_existing = _ask_yes_no(
            "Backup existing database before recreate?",
            default=True,
            input_stream=input_stream,
            output_stream=output_stream,
        )

    enable_storage_manager = _ask_yes_no(
        "Enable storage manager integration?",
        default=True,
        input_stream=input_stream,
        output_stream=output_stream,
    )
    strict_storage_manager_bootstrap = _ask_yes_no(
        "Fail on storage manager bootstrap errors?",
        default=False,
        input_stream=input_stream,
        output_stream=output_stream,
    )
    storage_startup_on_add = _ask_yes_no(
        "Run store startup checks while adding stores?",
        default=False,
        input_stream=input_stream,
        output_stream=output_stream,
    )

    output_stream.write("\nCreation summary\n")
    output_stream.write("  database_path: {}\n".format(db_path))
    output_stream.write("  db_type: {}\n".format(db_type))
    output_stream.write("  backup_existing: {}\n".format(backup_existing))
    output_stream.write("  enable_storage_manager: {}\n".format(enable_storage_manager))
    output_stream.write("  strict_storage_manager_bootstrap: {}\n".format(strict_storage_manager_bootstrap))
    output_stream.write("  storage_startup_on_add: {}\n".format(storage_startup_on_add))
    output_stream.flush()

    proceed = _ask_yes_no(
        "Proceed with creation?",
        default=True,
        input_stream=input_stream,
        output_stream=output_stream,
    )
    if not proceed:
        output_stream.write("Wizard canceled.\n")
        output_stream.flush()
        return None

    return DatabaseCreationWizardConfig(
        database_path=db_path,
        db_type=db_type,
        backup_existing=bool(backup_existing),
        enable_storage_manager=bool(enable_storage_manager),
        strict_storage_manager_bootstrap=bool(strict_storage_manager_bootstrap),
        storage_startup_on_add=bool(storage_startup_on_add),
    )


def create_database_from_wizard(config: DatabaseCreationWizardConfig) -> Path:
    """Create a database using wizard-provided configuration."""
    db_path = config.database_path.expanduser()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=config.db_type,
        create=True,
        backup=bool(config.backup_existing),
        enable_storage_manager=bool(config.enable_storage_manager),
        strict_storage_manager_bootstrap=bool(config.strict_storage_manager_bootstrap),
        storage_startup_on_add=bool(config.storage_startup_on_add),
    ):
        pass
    return db_path


@dataclass
class _BrowseWindow:
    """Stores the current browsing table and paging cursor."""

    table: str
    limit: int
    offset: int = 0


class TextDatabaseBrowser:
    """Small read-only command shell for database browsing."""

    prompt = "liuxin-db> "

    def __init__(
        self,
        db: Database,
        *,
        page_size: int = 20,
        input: Optional[TextIO] = None,
        output: Optional[TextIO] = None,
        lifecycle_plugins: Optional[Sequence[TerminalLifecyclePluginAPI]] = None,
    ) -> None:
        self.db = db
        self.page_size = max(1, int(page_size))
        self.input = input or sys.stdin
        self.output = output or sys.stdout
        self.current_table: Optional[str] = None
        self.window: Optional[_BrowseWindow] = None
        self._commands: dict[str, TerminalCommandAPI] = {}
        self._group_alias_to_group: dict[str, str] = {}
        self._command_groups: dict[str, dict[str, TerminalCommandAPI]] = {}
        self._lifecycle_plugins: list[TerminalLifecyclePluginAPI] = []
        self._started = False
        self._closed = False
        self._shutdown_reason: Optional[str] = None
        self.register_command(QuitCommand())
        self.register_command(SummaryCommand())
        self.register_command(ShowTagsCommand())
        self.register_command(ShowNotesCommand())
        self.register_command(ShowGenresCommand())
        self.register_command(ShowSubjectsCommand())
        self.register_command(ShowLanguageCommand())
        self.register_command(ShowSeriesCommand())
        self.register_command(ShowAllCommand())
        self.register_command(LinkCommand())
        self.register_command(UnlinkCommand())
        self.register_command(LinksCommand())
        self.register_command(OnNoteCommand())
        self.register_command(OnTagCommand())
        self.register_command(OnGenreCommand())
        self.register_command(OnSubjectCommand())
        self.register_command(OnLanguageCommand())
        self.register_command(OnSeriesCommand())
        self.register_command(OffNoteCommand())
        self.register_command(OffTagCommand())
        self.register_command(OffGenreCommand())
        self.register_command(OffSubjectCommand())
        self.register_command(OffLanguageCommand())
        self.register_command(OffSeriesCommand())
        self.register_command(NoteOnCommand())
        self.register_command(NewStoreWizardCommand())
        self.register_command(NewCreatorWizardCommand())
        self.register_command(NewExpressionWizardCommand())
        self.register_command(NewGenreWizardCommand())
        self.register_command(NewItemWizardCommand())
        self.register_command(NewNoteWizardCommand())
        self.register_command(NewOrganisationWizardCommand())
        self.register_command(NewPublisherWizardCommand())
        self.register_command(NewSeriesWizardCommand())
        self.register_command(NewTagWizardCommand())
        self.register_command(NewSubjectWizardCommand())
        self.register_command(NewTitleWizardCommand())
        self.register_command(NewWorkWizardCommand())
        self.register_command(NewManifestationWizardCommand())
        self.register_command(IngestDiskCommand())
        self.register_command(SyncStoreCommand())
        self.register_command(StoreListCommand())
        self.register_command(StoreShowCommand())
        self.register_command(StoreFilesCommand())
        self.register_command(TopCommand())
        if lifecycle_plugins:
            for plugin in lifecycle_plugins:
                self.register_lifecycle_plugin(plugin)

    def run(self) -> int:
        """Run the interactive command loop until `quit`/`exit` is entered."""
        self.startup()
        self._write("LiuXin text browser. Type `help` for commands.")
        exit_code = 0
        try:
            while True:
                line = self._read_command_line()
                if line == "":
                    self._write("")
                    self.request_shutdown("eof")
                    return exit_code
                try:
                    keep_going = self.execute_line(line)
                except Exception as exc:
                    self._write("ERROR: {}".format(exc))
                    continue
                if not keep_going:
                    return exit_code
        finally:
            self.shutdown(reason=self._shutdown_reason or "run_complete")

    def _can_use_readline_prompt(self) -> bool:
        """Whether we can safely use readline-backed `input()` prompt editing."""
        if _readline is None:
            return False
        if self.input is not sys.stdin or self.output is not sys.stdout:
            return False
        input_is_tty = bool(getattr(self.input, "isatty", lambda: False)())
        output_is_tty = bool(getattr(self.output, "isatty", lambda: False)())
        return input_is_tty and output_is_tty

    def _read_command_line(self) -> str:
        """Read one command line, enabling arrow-key history on interactive TTY."""
        if self._can_use_readline_prompt():
            try:
                line = input(self.prompt)
            except EOFError:
                return ""
            if line.strip():
                try:
                    _readline.add_history(line)
                except Exception:
                    pass
            return line + "\n"

        self._write(self.prompt, end="")
        return self.input.readline()

    def run_commands(self, commands: Sequence[str]) -> int:
        """Run commands non-interactively with full lifecycle hooks."""
        self.startup()
        exit_code = 0
        try:
            for command in commands:
                keep_going = self.execute_line(command)
                if not keep_going:
                    break
            return exit_code
        finally:
            self.shutdown(reason=self._shutdown_reason or "commands_complete")

    def execute_line(self, line: str) -> bool:
        """Execute one command line; returns False when session should exit."""
        stripped = line.strip()
        if not stripped:
            return True

        tokens = shlex.split(stripped)
        if not tokens:
            return True

        command = tokens[0].lower()
        args = tokens[1:]

        if command in {"help", "h", "?"}:
            self._print_help()
            return True

        group_name = self._group_alias_to_group.get(command)
        if group_name is not None:
            return self._execute_group_command(group_name, args)

        command_impl = self._commands.get(command)
        if command_impl is not None:
            return self._execute_command(command, command_impl, args)
        if command == "tables":
            self._cmd_tables(args)
            return True
        if command == "use":
            self._cmd_use(args)
            return True
        if command in {"schema", "columns"}:
            self._cmd_schema(args)
            return True
        if command == "count":
            self._cmd_count(args)
            return True
        if command in {"browse", "ls"}:
            self._cmd_browse(args)
            return True
        if command == "next":
            self._cmd_next(args)
            return True
        if command == "prev":
            self._cmd_prev(args)
            return True
        if command == "row":
            self._cmd_row(args)
            return True
        if command == "search":
            self._cmd_search(args)
            return True
        if command == "pagesize":
            self._cmd_pagesize(args)
            return True

        self._write("Unknown command: {}. Type `help`.".format(command))
        return True

    def _execute_command(self, command_token: str, command_impl: TerminalCommandAPI, args: list[str]) -> bool:
        should_continue = bool(command_impl.execute(self, args))
        if not should_continue and self._shutdown_reason is None:
            self.request_shutdown("command:{}".format(command_token))
        return should_continue

    def _execute_group_command(self, group_name: str, args: list[str]) -> bool:
        if not args:
            group_map = self._command_groups.get(group_name, {})
            if not group_map:
                raise ValueError("Command group {!r} has no subcommands registered.".format(group_name))
            unique: dict[int, TerminalCommandAPI] = {}
            for command in group_map.values():
                unique[id(command)] = command
            self._write("Available `{}` subcommands:".format(group_name))
            for command in sorted(unique.values(), key=lambda c: c.name):
                usage = command.usage or "{} {}".format(group_name, command.name)
                self._write("  {:<34} {}".format(usage, command.summary))
            return True

        subcommand_token = args[0].strip().lower()
        if not subcommand_token:
            raise ValueError("Missing subcommand for group {!r}.".format(group_name))

        group_map = self._command_groups.get(group_name, {})
        command_impl = group_map.get(subcommand_token)
        if command_impl is None and group_name == "on":
            legacy_rewrite = self._rewrite_on_legacy_group_args(group_map, args)
            if legacy_rewrite is not None:
                command_impl, rewritten_args = legacy_rewrite
                return self._execute_command(
                    "{} {}".format(group_name, command_impl.name),
                    command_impl,
                    rewritten_args,
                )
        if command_impl is None and group_name == "off":
            legacy_rewrite = self._rewrite_off_legacy_group_args(group_map, args)
            if legacy_rewrite is not None:
                command_impl, rewritten_args = legacy_rewrite
                return self._execute_command(
                    "{} {}".format(group_name, command_impl.name),
                    command_impl,
                    rewritten_args,
                )
        if command_impl is None and group_name == "show":
            legacy_rewrite = self._rewrite_show_legacy_group_args(group_map, args)
            if legacy_rewrite is not None:
                command_impl, rewritten_args = legacy_rewrite
                return self._execute_command(
                    "{} {}".format(group_name, command_impl.name),
                    command_impl,
                    rewritten_args,
                )
        if command_impl is None:
            available = sorted(group_map.keys())
            raise ValueError(
                "Unknown subcommand {} {}. Available: {}".format(
                    group_name,
                    subcommand_token,
                    ", ".join(available) if available else "<none>",
                )
            )

        return self._execute_command("{} {}".format(group_name, subcommand_token), command_impl, args[1:])

    def _rewrite_on_legacy_group_args(
        self,
        group_map: dict[str, TerminalCommandAPI],
        args: list[str],
    ) -> Optional[tuple[TerminalCommandAPI, list[str]]]:
        """Compatibility for legacy `on <table> <id> <kind> <value...>` syntax."""
        if not args:
            return None

        # Legacy compact target form: on <table:id> <kind> <value...>
        if len(args) >= 2:
            legacy_kind = self._normalize_command_token(args[1])
            command_impl = group_map.get(legacy_kind)
            if command_impl is not None:
                return command_impl, [args[0]] + args[2:]
            compact_token = str(args[0]).strip()
            if ":" in compact_token:
                table_part, selector = compact_token.rsplit(":", 1)
                if _looks_like_id_selector(selector):
                    table_is_valid = True
                    try:
                        self._resolve_table(table_part)
                    except ValueError:
                        table_is_valid = False
                    if table_is_valid:
                        raise ValueError(
                            "Unsupported `on` kind: {!r}. Expected one of: note, tag, genre, subject, language, series.".format(
                                args[1]
                            )
                        )

        # Legacy split target form: on <table> <id> <kind> <value...>
        if len(args) >= 3:
            legacy_kind = self._normalize_command_token(args[2])
            command_impl = group_map.get(legacy_kind)
            if command_impl is not None:
                return command_impl, [args[0], args[1]] + args[3:]
            if _looks_like_id_selector(args[1]):
                table_is_valid = True
                try:
                    self._resolve_table(args[0])
                except ValueError:
                    table_is_valid = False
                if table_is_valid:
                    raise ValueError(
                        "Unsupported `on` kind: {!r}. Expected one of: note, tag, genre, subject, language, series.".format(args[2])
                    )

        return None

    def _rewrite_off_legacy_group_args(
        self,
        group_map: dict[str, TerminalCommandAPI],
        args: list[str],
    ) -> Optional[tuple[TerminalCommandAPI, list[str]]]:
        """Compatibility for legacy `off <table> <id> <kind> <value...>` syntax."""
        if not args:
            return None

        # Legacy compact target form: off <table:id> <kind> <value...>
        if len(args) >= 2:
            legacy_kind = self._normalize_command_token(args[1])
            command_impl = group_map.get(legacy_kind)
            if command_impl is not None:
                return command_impl, [args[0]] + args[2:]
            compact_token = str(args[0]).strip()
            if ":" in compact_token:
                table_part, selector = compact_token.rsplit(":", 1)
                if _looks_like_id_selector(selector):
                    table_is_valid = True
                    try:
                        self._resolve_table(table_part)
                    except ValueError:
                        table_is_valid = False
                    if table_is_valid:
                        raise ValueError(
                            "Unsupported `off` kind: {!r}. Expected one of: note, tag, genre, subject, language, series.".format(
                                args[1]
                            )
                        )

        # Legacy split target form: off <table> <id> <kind> <value...>
        if len(args) >= 3:
            legacy_kind = self._normalize_command_token(args[2])
            command_impl = group_map.get(legacy_kind)
            if command_impl is not None:
                return command_impl, [args[0], args[1]] + args[3:]
            if _looks_like_id_selector(args[1]):
                table_is_valid = True
                try:
                    self._resolve_table(args[0])
                except ValueError:
                    table_is_valid = False
                if table_is_valid:
                    raise ValueError(
                        "Unsupported `off` kind: {!r}. Expected one of: note, tag, genre, subject, language, series.".format(
                            args[2]
                        )
                    )

        return None

    def _rewrite_show_legacy_group_args(
        self,
        group_map: dict[str, TerminalCommandAPI],
        args: list[str],
    ) -> Optional[tuple[TerminalCommandAPI, list[str]]]:
        """Compatibility for legacy `show <table> <id> <kind>` syntax."""
        if not args:
            return None

        known_kinds = "tags, notes, genres, subjects, language, series, all"
        all_impl = group_map.get("all")

        # New default behavior: show <table:id> -> show all <table:id>
        if len(args) == 1 and all_impl is not None:
            compact_token = str(args[0]).strip()
            if ":" in compact_token:
                table_part, id_part = compact_token.rsplit(":", 1)
                if _safe_int(id_part) is not None:
                    try:
                        self._resolve_table(table_part)
                    except ValueError:
                        pass
                    else:
                        return all_impl, [args[0]]
                elif _looks_like_id_selector(id_part):
                    table_is_valid = True
                    try:
                        self._resolve_table(table_part)
                    except ValueError:
                        table_is_valid = False
                    if table_is_valid:
                        raise ValueError(
                            "`show` supports a single row id only. Selectors like `1,2,3` or `10-20` are not supported."
                        )

        # New default behavior: show <table> <id> -> show all <table> <id>
        if len(args) == 2 and all_impl is not None:
            if _safe_int(args[1]) is not None:
                try:
                    self._resolve_table(args[0])
                except ValueError:
                    pass
                else:
                    return all_impl, [args[0], args[1]]
            elif _looks_like_id_selector(args[1]):
                table_is_valid = True
                try:
                    self._resolve_table(args[0])
                except ValueError:
                    table_is_valid = False
                if table_is_valid:
                    raise ValueError(
                        "`show` supports a single row id only. Selectors like `1,2,3` or `10-20` are not supported."
                    )

        # Legacy compact target form: show <table:id> <kind>
        if len(args) >= 2:
            kind_token = self._normalize_command_token(args[1])
            command_impl = group_map.get(kind_token)
            if command_impl is not None:
                return command_impl, [args[0]] + args[2:]

            compact_token = str(args[0]).strip()
            if ":" in compact_token:
                table_part, selector = compact_token.rsplit(":", 1)
                if _safe_int(selector) is None and _looks_like_id_selector(selector):
                    table_is_valid = True
                    try:
                        self._resolve_table(table_part)
                    except ValueError:
                        table_is_valid = False
                    if table_is_valid:
                        raise ValueError(
                            "`show` supports a single row id only. Selectors like `1,2,3` or `10-20` are not supported."
                        )
                elif _safe_int(selector) is not None:
                    table_is_valid = True
                    try:
                        self._resolve_table(table_part)
                    except ValueError:
                        table_is_valid = False
                    if table_is_valid:
                        raise ValueError(
                            "Unknown linked kind/table {!r}. Try: {}.".format(args[1], known_kinds)
                        )

        # Legacy split target form: show <table> <id> <kind>
        if len(args) >= 3:
            kind_token = self._normalize_command_token(args[2])
            command_impl = group_map.get(kind_token)
            if command_impl is not None:
                return command_impl, [args[0], args[1]] + args[3:]
            if _safe_int(args[1]) is None and _looks_like_id_selector(args[1]):
                table_is_valid = True
                try:
                    self._resolve_table(args[0])
                except ValueError:
                    table_is_valid = False
                if table_is_valid:
                    raise ValueError(
                        "`show` supports a single row id only. Selectors like `1,2,3` or `10-20` are not supported."
                    )
            elif _safe_int(args[1]) is not None:
                table_is_valid = True
                try:
                    self._resolve_table(args[0])
                except ValueError:
                    table_is_valid = False
                if table_is_valid:
                    raise ValueError("Unknown linked kind/table {!r}. Try: {}.".format(args[2], known_kinds))

        return None

    def startup(self) -> None:
        """Run startup lifecycle hooks once."""
        if self._started:
            return
        self._started = True
        for plugin in self._lifecycle_plugins:
            plugin_name = getattr(plugin, "name", plugin.__class__.__name__)
            try:
                plugin.on_startup(self)
            except Exception as exc:
                raise RuntimeError(
                    "Lifecycle startup plugin {!r} failed: {}".format(plugin_name, exc)
                ) from exc

    def shutdown(self, *, reason: str) -> None:
        """Run shutdown lifecycle hooks once."""
        if self._closed:
            return
        self._closed = True
        self._shutdown_reason = str(reason)
        errors: list[str] = []
        for plugin in reversed(self._lifecycle_plugins):
            plugin_name = getattr(plugin, "name", plugin.__class__.__name__)
            try:
                plugin.on_shutdown(self, reason=self._shutdown_reason)
            except Exception as exc:
                errors.append("plugin {!r}: {}".format(plugin_name, exc))
        if errors:
            self._write("WARNING: lifecycle shutdown errors:")
            for err in errors:
                self._write("  {}".format(err))

    def request_shutdown(self, reason: str) -> None:
        """Mark preferred shutdown reason for this browser session."""
        self._shutdown_reason = str(reason)

    def register_command(self, command: TerminalCommandAPI) -> None:
        """Register a command implementation (name + aliases)."""
        names = [command.name] + list(command.aliases)
        if bool(getattr(command, "expose_direct", True)):
            for raw_name in names:
                name = self._normalize_command_token(raw_name)
                if not name:
                    continue
                existing = self._commands.get(name)
                if existing is not None and existing is not command:
                    raise ValueError("Command name already registered: {!r}".format(name))
                self._commands[name] = command

        raw_group = getattr(command, "group", None)
        group_name = self._normalize_command_token(raw_group)
        if not group_name:
            return

        self._register_group_alias(group_name, group_name)
        # Convenience alias: `new <thing>` behaves like `add <thing>`.
        if group_name == "add":
            self._register_group_alias("new", group_name)
        for raw_alias in getattr(command, "group_aliases", ()) or ():
            alias = self._normalize_command_token(raw_alias)
            if alias:
                self._register_group_alias(alias, group_name)

        group_map = self._command_groups.setdefault(group_name, {})
        for raw_name in names:
            name = self._normalize_command_token(raw_name)
            if not name:
                continue
            existing = group_map.get(name)
            if existing is not None and existing is not command:
                raise ValueError("Command {} subcommand already registered: {!r}".format(group_name, name))
            group_map[name] = command

    @staticmethod
    def _normalize_command_token(token: Optional[str]) -> str:
        if token is None:
            return ""
        return str(token).strip().lower()

    def _register_group_alias(self, alias: str, group_name: str) -> None:
        existing = self._group_alias_to_group.get(alias)
        if existing is not None and existing != group_name:
            raise ValueError(
                "Command group alias collision: {!r} maps to {!r} and {!r}".format(alias, existing, group_name)
            )
        self._group_alias_to_group[alias] = group_name

    def register_lifecycle_plugin(self, plugin: TerminalLifecyclePluginAPI) -> None:
        """Register a lifecycle plugin for startup/shutdown events."""
        plugin_name = str(getattr(plugin, "name", plugin.__class__.__name__) or "").strip()
        if not plugin_name:
            plugin_name = plugin.__class__.__name__
        for existing in self._lifecycle_plugins:
            existing_name = str(getattr(existing, "name", existing.__class__.__name__) or "").strip()
            if existing_name == plugin_name and existing is not plugin:
                raise ValueError("Lifecycle plugin already registered: {!r}".format(plugin_name))
        self._lifecycle_plugins.append(plugin)

    def iter_lifecycle_plugins(self) -> list[TerminalLifecyclePluginAPI]:
        """Return lifecycle plugins in registration order."""
        return list(self._lifecycle_plugins)

    def iter_registered_commands(self) -> list[TerminalCommandAPI]:
        """Return unique command instances sorted by primary command name."""
        by_id: dict[int, TerminalCommandAPI] = {}
        for command in self._commands.values():
            by_id[id(command)] = command
        return sorted(by_id.values(), key=lambda c: c.name)

    def iter_registered_command_groups(self) -> list[tuple[str, list[TerminalCommandAPI]]]:
        """Return command groups as (group_name, unique_commands) tuples."""
        groups: list[tuple[str, list[TerminalCommandAPI]]] = []
        for group_name in sorted(self._command_groups.keys()):
            by_id: dict[int, TerminalCommandAPI] = {}
            for command in self._command_groups[group_name].values():
                by_id[id(command)] = command
            commands = sorted(by_id.values(), key=lambda c: c.name)
            groups.append((group_name, commands))
        return groups

    @property
    def database_path(self) -> str:
        """Best-effort path string for the connected database."""
        metadata = getattr(self.db, "metadata", {}) or {}
        return str(metadata.get("database_path", ""))

    def emit(self, text: str, *, end: str = "\n") -> None:
        """Public output sink for command implementations."""
        self._write(text, end=end)

    def prompt_text(self, prompt: str, *, default: Optional[str] = None) -> str:
        """Prompt for one text input line using browser input/output streams."""
        return _ask_text(
            prompt,
            default=default,
            input_stream=self.input,
            output_stream=self.output,
        )

    def prompt_yes_no(self, prompt: str, *, default: bool) -> bool:
        """Prompt for a yes/no decision using browser input/output streams."""
        return _ask_yes_no(
            prompt,
            default=bool(default),
            input_stream=self.input,
            output_stream=self.output,
        )

    def list_tables(self) -> list[str]:
        """Return all table names sorted alphabetically."""
        return self._all_tables()

    def get_table_row_count(self, table: str) -> Optional[int]:
        """Return row count for a table, or None if unavailable."""
        value = self._row_count(table)
        return value if value >= 0 else None

    def get_table_columns(self, table: str) -> list[str]:
        """Return ordered column names for a table."""
        return list(self.db.get_column_headings(table))

    def get_terminal_width(self) -> int:
        """Return detected terminal width for table rendering."""
        try:
            columns = int(shutil.get_terminal_size(fallback=(120, 30)).columns)
        except Exception:
            columns = 120
        return max(40, columns)

    def resolve_table(self, raw: Optional[str]) -> str:
        """Resolve a table name or raise a clear user-facing error."""
        return self._resolve_table(raw)

    def table_slice(self, table: str, *, limit: int, offset: int = 0):
        """Return a limited ordered slice of rows from a table."""
        return self._table_slice(table, limit=max(1, int(limit)), offset=max(0, int(offset)))

    def format_row(self, table: str, row) -> str:
        """Format one row for terminal output."""
        return self._format_row(table, row)

    def format_rows_as_table(
        self,
        table: str,
        rows: Sequence[object],
        *,
        max_cell_width: int = 60,
    ) -> str:
        """Render rows as an ASCII table using schema column order."""
        columns = self.get_table_columns(table)
        display_columns = _shorten_column_headers(columns, table_name=table)
        table_rows: list[list[object]] = []
        for row in rows:
            table_rows.append([row[column] if column in row else None for column in columns])
        return _render_ascii_table(
            display_columns,
            table_rows,
            max_cell_width=max_cell_width,
            max_table_width=self.get_terminal_width(),
        )

    def render_table(
        self,
        headers: Sequence[object],
        rows: Sequence[Sequence[object]],
        *,
        max_cell_width: int = 60,
    ) -> str:
        """Render arbitrary headers/rows as an ASCII table."""
        display_headers = [str(header) for header in headers]
        return _render_ascii_table(
            display_headers,
            rows,
            max_cell_width=max_cell_width,
            max_table_width=self.get_terminal_width(),
        )

    def _write(self, text: str, *, end: str = "\n") -> None:
        self.output.write(text + end)
        self.output.flush()

    def _print_help(self) -> None:
        self._write("Commands:")
        self._write("  tables [pattern]                  List tables (+ row counts).")
        self._write("  use <table>                       Set current table.")
        self._write("  schema [table]                    Show columns for table/current table.")
        self._write("  count [table]                     Show row count for table/current table.")
        self._write("  browse [table] [limit] [offset]   Show rows for table/current table.")
        self._write("  next [limit]                      Next page for current browse table.")
        self._write("  prev [limit]                      Previous page for current browse table.")
        self._write("  row <table> <id>                  Show one row by id.")
        self._write("  search <table> <column> <value> [limit]")
        self._write("                                    Exact-match search.")
        self._write("  pagesize [n]                      Show or set default page size.")
        grouped_command_ids: set[int] = set()
        grouped = self.iter_registered_command_groups()
        if grouped:
            self._write("  -- grouped --")
            for group_name, commands in grouped:
                self._write("  {} <subcommand>".format(group_name))
                for command in commands:
                    grouped_command_ids.add(id(command))
                    usage = command.usage or "{} {}".format(group_name, command.name)
                    self._write("    {:<32} {}".format(usage, command.summary))

        self._write("  -- direct --")
        for command in self.iter_registered_commands():
            if id(command) in grouped_command_ids:
                continue
            usage = command.usage or command.name
            self._write("  {:<34} {}".format(usage, command.summary))
        self._write("  help                              Show this help.")

    def _all_tables(self) -> list[str]:
        return sorted(str(t) for t in self.db.get_tables())

    def _resolve_table_token(self, token: str) -> str:
        """Resolve tolerant user table tokens (singular/plural + common aliases)."""
        text = str(token).strip().lower()
        if not text:
            raise ValueError("Table token cannot be blank.")

        tables = set(self._all_tables())
        if text in tables:
            return text

        # Common logical aliases that appear in user-facing command usage.
        if text in {"tag", "tags", "label", "labels"}:
            if "labels" in tables:
                return "labels"
            if "tags" in tables:
                return "tags"

        common_aliases = {
            "note": "notes",
            "work": "works",
            "expression": "expressions",
            "manifestation": "manifestations",
            "item": "items",
            "genre": "genres",
            "subject": "subjects",
            "store": "stores",
            "file": "files",
            "folder": "folders",
            "language": "languages",
            "comment": "comments",
            "identifier": "identifiers",
            "synopsis": "synopses",
            "image": "images",
            "agent": "agents",
            "human_agent": "human_agents",
            "org_agent": "org_agents",
        }
        alias_target = common_aliases.get(text)
        if alias_target in tables:
            return alias_target

        candidates: list[str] = []

        def _add_candidate(candidate: str) -> None:
            if candidate and candidate not in candidates:
                candidates.append(candidate)

        # Singular -> plural
        _add_candidate(text + "s")
        _add_candidate(text + "es")
        if text.endswith("y") and len(text) > 1:
            _add_candidate(text[:-1] + "ies")

        # Plural -> singular
        if text.endswith("ies") and len(text) > 3:
            _add_candidate(text[:-3] + "y")
        if text.endswith("es") and len(text) > 2:
            _add_candidate(text[:-2])
        if text.endswith("s") and len(text) > 1:
            _add_candidate(text[:-1])

        for candidate in candidates:
            if candidate in tables:
                return candidate

        raise ValueError("Unknown table: {!r}".format(token))

    def _resolve_table(self, raw: Optional[str]) -> str:
        if raw is None:
            if self.current_table is None:
                raise ValueError("No table selected. Use `use <table>` or pass a table name.")
            table = self.current_table
            if table not in set(self._all_tables()):
                raise ValueError("Unknown table: {!r}".format(table))
            return table

        table = self._resolve_table_token(raw)
        return table

    def _row_count(self, table: str) -> int:
        try:
            return int(self.db.get_record_count(table))
        except Exception:
            return -1

    def _format_row(self, table: str, row) -> str:
        columns = self.db.get_column_headings(table)
        id_column = None
        try:
            id_column = self.db.driver_wrapper.get_id_column(table)
        except Exception:
            id_column = None

        pieces: list[str] = []
        if id_column is not None and id_column in row:
            pieces.append("{}={}".format(id_column, _truncate(row[id_column], width=24)))

        for column in columns:
            if id_column is not None and column == id_column:
                continue
            try:
                pieces.append("{}={}".format(column, _truncate(row[column])))
            except Exception:
                continue

        return " | ".join(pieces)

    def _table_slice(self, table: str, *, limit: int, offset: int):
        rows = []
        for idx, row in enumerate(self.db.get_all_rows(table, iterator_return=True)):
            if idx < offset:
                continue
            rows.append(row)
            if len(rows) >= limit:
                break
        return rows

    def _cmd_tables(self, args: list[str]) -> None:
        pattern = args[0].lower() if args else None
        tables = self._all_tables()
        if pattern:
            tables = [t for t in tables if pattern in t.lower()]

        if not tables:
            self._write("No tables found.")
            return

        for table in tables:
            count = self._row_count(table)
            count_text = str(count) if count >= 0 else "?"
            current_marker = " *" if table == self.current_table else ""
            self._write("{} [{}]{}".format(table, count_text, current_marker))

    def _cmd_use(self, args: list[str]) -> None:
        if not args:
            raise ValueError("Usage: use <table>")
        table = self._resolve_table(args[0])
        self.current_table = table
        self.window = None
        self._write("Current table: {}".format(table))

    def _cmd_schema(self, args: list[str]) -> None:
        table = self._resolve_table(args[0] if args else None)
        columns = self.db.get_column_headings(table)
        self._write("Schema for {} ({} columns):".format(table, len(columns)))
        for name in columns:
            self._write("  {}".format(name))

    def _cmd_count(self, args: list[str]) -> None:
        table = self._resolve_table(args[0] if args else None)
        count = self._row_count(table)
        if count >= 0:
            self._write("{} rows: {}".format(table, count))
        else:
            self._write("{} rows: ?".format(table))

    def _cmd_browse(self, args: list[str]) -> None:
        table_arg = None
        limit = self.page_size
        offset = 0

        if args:
            maybe_limit = _safe_int(args[0])
            if maybe_limit is None:
                table_arg = args[0]
                args = args[1:]

        table = self._resolve_table(table_arg)

        if args:
            maybe_limit = _safe_int(args[0])
            if maybe_limit is None:
                raise ValueError("limit must be an integer")
            limit = max(1, maybe_limit)
            args = args[1:]
        if args:
            maybe_offset = _safe_int(args[0])
            if maybe_offset is None:
                raise ValueError("offset must be an integer")
            offset = max(0, maybe_offset)
            args = args[1:]
        if args:
            raise ValueError("Usage: browse [table] [limit] [offset]")

        total = self._row_count(table)
        rows = self._table_slice(table, limit=limit, offset=offset)
        shown_to = offset + len(rows)
        total_text = str(total) if total >= 0 else "?"
        self._write(
            "Browsing {} rows {}..{} of {}".format(
                table,
                offset + 1 if rows else 0,
                shown_to,
                total_text,
            )
        )
        if not rows:
            self._write("(no rows)")
        else:
            for row in rows:
                self._write("  {}".format(self._format_row(table, row)))

        self.current_table = table
        self.window = _BrowseWindow(table=table, limit=limit, offset=offset)

    def _cmd_next(self, args: list[str]) -> None:
        if self.window is None:
            raise ValueError("No active browse window. Use `browse` first.")
        limit = self.window.limit
        if args:
            maybe_limit = _safe_int(args[0])
            if maybe_limit is None:
                raise ValueError("limit must be an integer")
            limit = max(1, maybe_limit)
        self._cmd_browse([self.window.table, str(limit), str(self.window.offset + limit)])

    def _cmd_prev(self, args: list[str]) -> None:
        if self.window is None:
            raise ValueError("No active browse window. Use `browse` first.")
        limit = self.window.limit
        if args:
            maybe_limit = _safe_int(args[0])
            if maybe_limit is None:
                raise ValueError("limit must be an integer")
            limit = max(1, maybe_limit)
        offset = max(0, self.window.offset - limit)
        self._cmd_browse([self.window.table, str(limit), str(offset)])

    def _cmd_row(self, args: list[str]) -> None:
        if not args:
            raise ValueError("Usage: row <table> <id> OR row <table>:<id>")

        if len(args) == 1:
            token = str(args[0]).strip()
            if ":" not in token:
                raise ValueError("Usage: row <table> <id> OR row <table>:<id>")
            table_token, id_token = token.rsplit(":", 1)
            table = self._resolve_table(table_token)
            row_id = _safe_int(id_token)
        elif len(args) == 2:
            table = self._resolve_table(args[0])
            row_id = _safe_int(args[1])
        else:
            raise ValueError("Usage: row <table> <id> OR row <table>:<id>")
        if row_id is None:
            raise ValueError("row id must be an integer")
        row = self.db.get_row_from_id(table, row_id)
        if row is None:
            self._write("No row found in {} for id {}.".format(table, row_id))
            return
        self._write(self._format_row(table, row))

    def _cmd_search(self, args: list[str]) -> None:
        if len(args) < 3:
            raise ValueError("Usage: search <table> <column> <value> [limit]")
        table = self._resolve_table(args[0])
        column = args[1]
        value = args[2]
        limit = self.page_size
        if len(args) >= 4:
            maybe_limit = _safe_int(args[3])
            if maybe_limit is None:
                raise ValueError("limit must be an integer")
            limit = max(1, maybe_limit)

        matches = self.db.search(table, column, value)
        self._write("Search {}.{} == {!r}: {} match(es)".format(table, column, value, len(matches)))
        for row in matches[:limit]:
            self._write("  {}".format(self._format_row(table, row)))
        if len(matches) > limit:
            self._write("  ... {} more".format(len(matches) - limit))

    def _cmd_pagesize(self, args: list[str]) -> None:
        if not args:
            self._write("Default page size: {}".format(self.page_size))
            return
        size = _safe_int(args[0])
        if size is None:
            raise ValueError("pagesize must be an integer")
        self.page_size = max(1, size)
        self._write("Default page size set to {}".format(self.page_size))


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser for the text browser entry point."""
    parser = argparse.ArgumentParser(description="LiuXin text browser")
    parser.add_argument("--database", required=True, help="Path to LiuXin database")
    parser.add_argument("--db-type", default="SQLite", help="Database backend type (default: SQLite)")
    parser.add_argument(
        "--create-new-db",
        action="store_true",
        help="Run an interactive wizard and create a new database before browsing.",
    )
    parser.add_argument("--page-size", type=int, default=20, help="Default browse page size")
    parser.add_argument(
        "--no-create-if-missing",
        action="store_true",
        help="Fail if the database path does not exist (default creates a new library database).",
    )
    parser.add_argument(
        "--command",
        action="append",
        default=[],
        help="Execute command(s) non-interactively; may be repeated.",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    """CLI entry point for the text browser."""
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        database_path = args.database
        db_type = args.db_type

        if args.create_new_db:
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

        with _open_database(
            database_path=database_path,
            db_type=db_type,
            create_if_missing=not bool(args.no_create_if_missing),
        ) as db:
            shell = TextDatabaseBrowser(db, page_size=args.page_size, output=sys.stdout)
            if args.command:
                return shell.run_commands(args.command)
            return shell.run()
    except Exception as exc:
        print("ERROR: {}".format(exc), file=sys.stderr)
        return 2


__all__ = [
    "DatabaseCreationWizardConfig",
    "TextDatabaseBrowser",
    "run_database_creation_wizard",
    "create_database_from_wizard",
    "build_parser",
    "main",
]
