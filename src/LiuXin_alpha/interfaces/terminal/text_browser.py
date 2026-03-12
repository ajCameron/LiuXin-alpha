"""Interactive text UI for browsing LiuXin database contents.

This module intentionally provides a read-only shell focused on schema and row
inspection while the broader interfaces layer is still being built out.
"""

from __future__ import annotations

import argparse
import os
import shlex
import shutil
import sys

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Optional, Sequence, TextIO

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.interfaces.terminal.commands import TerminalCommandAPI, build_default_commands
from LiuXin_alpha.interfaces.terminal.plugins import TerminalLifecyclePluginAPI
from LiuXin_alpha.utils.jobs import default_job_manager

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


def _default_history_file_path() -> Path:
    """
    Return the default on-disk command history path for the terminal browser.

    Preference order:
    - ``$XDG_STATE_HOME/liuxin_alpha/terminal_history``
    - ``~/.local/state/liuxin_alpha/terminal_history``
    """
    xdg_state_home = os.environ.get("XDG_STATE_HOME", "").strip()
    if xdg_state_home:
        base = Path(xdg_state_home).expanduser()
    else:
        base = Path.home() / ".local" / "state"
    return base / "liuxin_alpha" / "terminal_history"


def _build_default_core_runtime(db: Database, *, job_manager):
    """
    Build an in-process core runtime around the active browser database.

    This keeps write-path command routing local and low-latency while enforcing
    the "writes go through core" boundary for commands that opt in.
    """
    from LiuXin_alpha.core.runtime import CoreRuntime
    from LiuXin_alpha.library.library import Library

    library = Library(database=db, close_database_on_close=False)
    return CoreRuntime(library=library, job_manager=job_manager)


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


def _truncate_text(value: object, *, width: int = 80) -> str:
    text = str(value)
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


def _summarize_exception(exc: BaseException) -> str:
    text = str(exc).strip()
    name = exc.__class__.__name__
    if text:
        return "{}: {}".format(name, text)
    return name


def _stringify_table_cell(value: object, *, width: int = 60) -> str:
    if value is None:
        text = ""
    else:
        text = str(value)
    text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "\\n")
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


def _preview_row_text(value: object, *, max_len: int = 64) -> str:
    text = str(value or "").replace("\r\n", " ").replace("\r", " ").replace("\n", " ").strip()
    if len(text) <= max_len:
        return text
    return text[: max(0, max_len - 3)] + "..."


def _row_detail_group(column: str, *, id_column: Optional[str]) -> str:
    text = str(column).strip().lower()
    if not text:
        return "other"
    if id_column is not None and text == str(id_column).strip().lower():
        return "identity"
    if text.endswith("_id"):
        return "references"
    if text.startswith("supports_") or text.startswith("is_") or "read_only" in text or "eventually_consistent" in text:
        return "capabilities"
    if any(token in text for token in ("timestamp", "datestamp", "date", "year", "seen", "healthcheck")):
        return "dates"
    if any(token in text for token in ("uri", "path", "root", "protocol", "auth", "credential", "mask", "mount", "latency", "online", "location", "policy")):
        return "access"
    if any(
        token in text
        for token in (
            "name",
            "title",
            "canonical",
            "sort",
            "kind",
            "type",
            "medium",
            "status",
            "note",
            "flags",
            "scratch",
        )
    ):
        return "identity"
    return "other"


def _pretty_row_detail_group(group: str) -> str:
    mapping = {
        "identity": "Identity",
        "references": "References",
        "access": "Access",
        "capabilities": "Capabilities",
        "dates": "Dates",
        "other": "Other",
    }
    return mapping.get(str(group).strip().lower(), str(group).strip().title() or "Other")


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
    output_stream.write(
        _render_ascii_table(
            ["field", "value"],
            [
                ["database_path", db_path],
                ["db_type", db_type],
                ["backup_existing", backup_existing],
                ["enable_storage_manager", enable_storage_manager],
                ["strict_storage_manager_bootstrap", strict_storage_manager_bootstrap],
                ["storage_startup_on_add", storage_startup_on_add],
            ],
            max_cell_width=120,
        )
    )
    output_stream.write("\n")
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


@dataclass(frozen=True)
class _CommandCompletion:
    """One token-completion result for the current input buffer."""

    token_start: int
    token_end: int
    prefix: str
    candidates: tuple[str, ...]


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
        history_file: Optional[str | Path] = None,
        lifecycle_plugins: Optional[Sequence[TerminalLifecyclePluginAPI]] = None,
        job_manager=None,
        core_runtime=None,
    ) -> None:
        self.db = db
        self.page_size = max(1, int(page_size))
        self.input = input or sys.stdin
        self.output = output or sys.stdout
        self.job_manager = job_manager if job_manager is not None else default_job_manager()
        self._core_runtime = core_runtime
        self._owns_core_runtime = False
        self._core_runtime_init_error: Optional[str] = None
        if self._core_runtime is None:
            try:
                self._core_runtime = _build_default_core_runtime(db, job_manager=self.job_manager)
                self._owns_core_runtime = True
            except Exception as exc:
                # Core wiring is best-effort here; browser remains fully usable.
                self._core_runtime = None
                self._owns_core_runtime = False
                self._core_runtime_init_error = _summarize_exception(exc)
        self.current_table: Optional[str] = None
        self.window: Optional[_BrowseWindow] = None
        self._commands: dict[str, TerminalCommandAPI] = {}
        self._group_alias_to_group: dict[str, str] = {}
        self._command_groups: dict[str, dict[str, TerminalCommandAPI]] = {}
        self._lifecycle_plugins: list[TerminalLifecyclePluginAPI] = []
        self._started = False
        self._closed = False
        self._shutdown_reason: Optional[str] = None
        self._history_file = (
            Path(history_file).expanduser()
            if history_file is not None
            else _default_history_file_path()
        )
        self._history_loaded = False
        self._history_enabled_for_session = False
        self._readline_completion_configured = False
        self._readline_completion_matches: list[str] = []
        for command in build_default_commands():
            self.register_command(command)
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
            self._configure_readline_completion()
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

        group_name = self._group_alias_to_group.get(command)
        if group_name is not None:
            return self._execute_group_command(group_name, args)

        command_impl = self._commands.get(command)
        if command_impl is not None:
            return self._execute_command(command, command_impl, args)

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
        rewritten_group_args: Optional[list[str]] = None
        if command_impl is None and ":" in subcommand_token:
            # Convenience compact form for grouped commands:
            #   <group> <subcommand>:<arg0> [arg1...]
            # Example:
            #   sync store:1 --no-refresh
            compact_subcommand, compact_first_arg = subcommand_token.split(":", 1)
            compact_subcommand = self._normalize_command_token(compact_subcommand)
            compact_first_arg = str(compact_first_arg).strip()
            if compact_subcommand:
                compact_impl = group_map.get(compact_subcommand)
                if compact_impl is not None and compact_first_arg:
                    command_impl = compact_impl
                    rewritten_group_args = [compact_first_arg] + args[1:]
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

        if rewritten_group_args is not None:
            return self._execute_command(
                "{} {}".format(group_name, command_impl.name),
                command_impl,
                rewritten_group_args,
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
        self._load_command_history()
        core_warning = self.core_runtime_startup_warning()
        if core_warning:
            self._write("WARNING: {}".format(core_warning))
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
        self._save_command_history()
        if self._owns_core_runtime and self._core_runtime is not None:
            try:
                self._core_runtime.shutdown()
            except Exception:
                pass
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

    def supports_core_commands(self) -> bool:
        """Whether this browser can dispatch write commands through core runtime."""
        return self._core_runtime is not None

    def supports_core_queries(self) -> bool:
        """Whether this browser can dispatch read queries through core runtime."""
        return self._core_runtime is not None

    def core_runtime_status_summary(self) -> str:
        """Summarize core runtime availability for status surfaces."""
        if self._core_runtime is not None:
            return "core: enabled"
        if self._core_runtime_init_error:
            return "core: local-only | {}".format(_truncate_text(self._core_runtime_init_error, width=120))
        return "core: local-only"

    def core_runtime_startup_warning(self) -> Optional[str]:
        """User-facing startup warning for core runtime bootstrap failures."""
        if not self._core_runtime_init_error:
            return None
        return "Core runtime unavailable; using local-only mode. {}".format(
            _truncate_text(self._core_runtime_init_error, width=160)
        )

    def execute_core_command(self, name: str, *, payload: Optional[dict[str, object]] = None):
        """
        Execute one core write command and return command result payload.

        Raises a user-facing error when core runtime is unavailable.
        """
        if self._core_runtime is None:
            raise RuntimeError("Core runtime is not available for this browser session.")
        from LiuXin_alpha.core.commands import CoreCommand

        envelope = CoreCommand(
            name=str(name),
            payload=dict(payload or {}),
        )
        return self._core_runtime.execute_command(envelope).result

    def execute_core_query(self, name: str, *, payload: Optional[dict[str, object]] = None):
        """
        Execute one core read query and return query result payload.

        Raises a user-facing error when core runtime is unavailable.
        """
        if self._core_runtime is None:
            raise RuntimeError("Core runtime is not available for this browser session.")
        from LiuXin_alpha.core.queries import CoreQuery

        envelope = CoreQuery(
            name=str(name),
            payload=dict(payload or {}),
        )
        return self._core_runtime.execute_query(envelope).result

    def supports_job_output_panel(self) -> bool:
        """Whether this browser can route one job log stream to a dedicated panel."""
        return False

    def attach_job_output_panel(self, job_id: str) -> bool:
        """Attach the dedicated job output panel to a specific job id."""
        del job_id
        return False

    def detach_job_output_panel(self) -> bool:
        """Detach any active dedicated job output panel."""
        return False

    def clear_output(self) -> bool:
        """Clear terminal output buffer or screen when supported."""
        stream = self.output
        if hasattr(stream, "seek") and hasattr(stream, "truncate"):
            try:
                stream.seek(0)
                stream.truncate(0)
                stream.flush()
                return True
            except Exception:
                pass

        is_tty = bool(getattr(stream, "isatty", lambda: False)())
        if is_tty:
            try:
                stream.write("\x1b[2J\x1b[H")
                stream.flush()
                return True
            except Exception:
                pass
        return False

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

    def get_table_display_columns(self, table: str) -> list[str]:
        """Return shortened display column names for a table."""
        columns = self.get_table_columns(table)
        return _shorten_column_headers(columns, table_name=table)

    def get_table_id_column(self, table: str) -> Optional[str]:
        """Return the primary id column for a table when available."""
        return self._table_id_column(table)

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

    def resolve_table_column(self, table: str, raw: Optional[str]) -> str:
        """Resolve one table column from a schema or display token."""
        token = self._normalize_command_token(raw)
        if not token:
            raise ValueError("Column name cannot be blank.")

        columns = self.get_table_columns(table)
        display_columns = self.get_table_display_columns(table)
        exact_full = {self._normalize_command_token(column): column for column in columns}
        if token in exact_full:
            return exact_full[token]

        exact_display: dict[str, str | None] = {}
        for column, display in zip(columns, display_columns):
            alias = self._normalize_command_token(display)
            existing = exact_display.get(alias)
            if existing is not None and existing != column:
                exact_display[alias] = None
            elif alias not in exact_display:
                exact_display[alias] = column

        if token in exact_display:
            resolved = exact_display[token]
            if resolved is None:
                matches = [column for column, display in zip(columns, display_columns) if self._normalize_command_token(display) == token]
                raise ValueError(
                    "Ambiguous column {!r} for table {!r}. Matches: {}".format(
                        raw,
                        table,
                        ", ".join(matches),
                    )
                )
            return resolved

        prefix_matches: list[str] = []
        seen: set[str] = set()
        for column, display in zip(columns, display_columns):
            normalized_column = self._normalize_command_token(column)
            normalized_display = self._normalize_command_token(display)
            if normalized_column.startswith(token) or normalized_display.startswith(token):
                if column not in seen:
                    seen.add(column)
                    prefix_matches.append(column)

        if len(prefix_matches) == 1:
            return prefix_matches[0]
        if not prefix_matches:
            raise ValueError(
                "Unknown column {!r} for table {!r}. Try one of: {}".format(
                    raw,
                    table,
                    ", ".join(display_columns),
                )
            )
        raise ValueError(
            "Ambiguous column {!r} for table {!r}. Matches: {}".format(
                raw,
                table,
                ", ".join(prefix_matches),
            )
        )

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

    def build_row_detail_sections(self, table: str, row) -> list[tuple[str, list[tuple[object, object]]]]:
        """Build grouped vertical detail sections for one row."""
        columns = self.get_table_columns(table)
        display_columns = _shorten_column_headers(columns, table_name=table)
        id_column = self._table_id_column(table)
        group_rank = {
            "identity": 0,
            "references": 1,
            "access": 2,
            "capabilities": 3,
            "dates": 4,
            "other": 5,
        }
        grouped_rows: dict[str, list[tuple[object, object]]] = {}
        ordered = list(enumerate(columns))
        ordered.sort(
            key=lambda pair: (
                group_rank.get(_row_detail_group(pair[1], id_column=id_column), 99),
                0 if id_column is not None and pair[1] == id_column else 1,
                pair[0],
            )
        )
        for idx, column in ordered:
            raw_group_name = _row_detail_group(column, id_column=id_column)
            grouped_rows.setdefault(raw_group_name, []).append(
                (display_columns[idx], row[column] if column in row else None)
            )

        sections: list[tuple[str, list[tuple[object, object]]]] = []
        ordered_groups = sorted(grouped_rows.keys(), key=lambda name: group_rank.get(name, 99))
        for raw_group_name in ordered_groups:
            sections.append((_pretty_row_detail_group(raw_group_name), grouped_rows[raw_group_name]))
        return sections

    def render_row_details(
        self,
        table: str,
        row,
        *,
        max_cell_width: int = 120,
    ) -> str:
        """Render one row as grouped vertical detail tables."""
        return self.render_detail_sections(
            self.build_row_detail_sections(table, row),
            key_header="column",
            value_header="value",
            max_cell_width=max_cell_width,
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

    def render_detail_sections(
        self,
        sections: Sequence[tuple[str, Sequence[tuple[object, object]]]],
        *,
        key_header: str = "field",
        value_header: str = "value",
        max_cell_width: int = 120,
    ) -> str:
        """Render one or more titled two-column detail sections."""
        blocks: list[str] = []
        for section_title, rows in sections:
            normalized_rows = [(str(key), value) for key, value in rows]
            if not normalized_rows:
                continue
            if blocks:
                blocks.append("")
            title = str(section_title).strip()
            if title:
                blocks.append(title)
            blocks.append(
                self.render_table(
                    [key_header, value_header],
                    normalized_rows,
                    max_cell_width=max_cell_width,
                )
            )
        return "\n".join(blocks)

    def emit_detail_sections(
        self,
        sections: Sequence[tuple[str, Sequence[tuple[object, object]]]],
        *,
        title: Optional[str] = None,
        key_header: str = "field",
        value_header: str = "value",
        max_cell_width: int = 120,
    ) -> None:
        """Write titled detail sections to the current output stream."""
        rendered = self.render_detail_sections(
            sections,
            key_header=key_header,
            value_header=value_header,
            max_cell_width=max_cell_width,
        )
        if title:
            self.emit(title)
            if rendered:
                self.emit("")
        if rendered:
            self.emit(rendered)

    def _write(self, text: str, *, end: str = "\n") -> None:
        self.output.write(text + end)
        self.output.flush()

    def _load_command_history(self) -> None:
        """
        Load persistent readline history for interactive sessions.

        This is intentionally best-effort and should never block startup.
        """
        if self._history_loaded:
            return
        self._history_loaded = True
        if _readline is None:
            return
        if not self._can_use_readline_prompt():
            return
        try:
            if self._history_file.exists():
                _readline.read_history_file(str(self._history_file))
            self._history_enabled_for_session = True
        except Exception:
            self._history_enabled_for_session = False

    def _save_command_history(self) -> None:
        """
        Persist readline history for interactive sessions.

        This is intentionally best-effort and should never block shutdown.
        """
        if _readline is None:
            return
        if not self._history_enabled_for_session:
            return
        try:
            self._history_file.parent.mkdir(parents=True, exist_ok=True)
            _readline.write_history_file(str(self._history_file))
        except Exception:
            return

    def _format_command_aliases(self, aliases: Sequence[str]) -> str:
        normalized = [self._normalize_command_token(alias) for alias in aliases]
        filtered = [alias for alias in normalized if alias]
        if not filtered:
            return ""
        return " (aliases: {})".format(", ".join(filtered))

    def _root_completion_tokens(self) -> list[str]:
        tokens = set(self._commands.keys()) | set(self._group_alias_to_group.keys())
        return sorted(token for token in tokens if token)

    def _group_completion_tokens(self, group_name: str) -> list[str]:
        group_map = self._command_groups.get(group_name, {})
        return sorted(token for token in set(group_map.keys()) if token)

    def _table_completion_tokens(self) -> list[str]:
        try:
            return self._all_tables()
        except Exception:
            return []

    def _table_token_completion_candidates(self, token: str) -> list[str]:
        normalized = self._normalize_command_token(token)
        return [table for table in self._table_completion_tokens() if table.startswith(normalized)]

    def _resolve_completion_table_token(self, token: str) -> Optional[str]:
        try:
            return self._resolve_table_token(token)
        except Exception:
            return None

    def _table_id_column(self, table: str) -> Optional[str]:
        try:
            return str(self.db.driver_wrapper.get_id_column(table))
        except Exception:
            return None

    def _row_id_completion_candidates(self, table: str, token: str, *, max_candidates: int = 20) -> list[str]:
        prefix = str(token).strip()
        if prefix and not prefix.isdigit():
            return []

        id_column = self._table_id_column(table)
        if not id_column:
            return []

        limit = max(1, int(max_candidates))
        candidates: list[str] = []
        seen: set[str] = set()

        def _add_rows(rows) -> bool:
            for row in rows:
                try:
                    raw_value = row[id_column]
                except Exception:
                    continue
                value = str(raw_value).strip()
                if not value:
                    continue
                if prefix and not value.startswith(prefix):
                    continue
                if value in seen:
                    continue
                seen.add(value)
                candidates.append(value)
                if len(candidates) >= limit:
                    return True
            return False

        if self.window is not None and self.window.table == table:
            reached_limit = _add_rows(
                self._table_slice(
                    table,
                    limit=min(limit, max(1, int(self.window.limit))),
                    offset=max(0, int(self.window.offset)),
                )
            )
            if reached_limit or (not prefix):
                return candidates

        if prefix:
            try:
                if _add_rows(self.db.get_all_rows(table, iterator_return=True)):
                    return candidates
            except Exception:
                return candidates
            return candidates

        try:
            _add_rows(self._table_slice(table, limit=limit, offset=0))
        except Exception:
            return candidates
        return candidates

    def _row_ref_token_completion_candidates(self, token: str) -> list[str]:
        raw = str(token)
        if ":" not in raw:
            return self._table_token_completion_candidates(raw)

        table_token, selector_token = raw.split(":", 1)
        resolved_table = self._resolve_completion_table_token(table_token)
        if resolved_table is not None:
            return ["{}:{}".format(resolved_table, row_id) for row_id in self._row_id_completion_candidates(resolved_table, selector_token)]

        normalized_table = self._normalize_command_token(table_token)
        return [table + ":" + selector_token for table in self._table_completion_tokens() if table.startswith(normalized_table)]

    @staticmethod
    def _looks_like_compact_row_ref_prefix(token: str) -> bool:
        return ":" in str(token)

    def _table_scoped_id_completion_candidates(self, table_token: str, current_token: str) -> list[str]:
        resolved_table = self._resolve_completion_table_token(table_token)
        if resolved_table is None:
            return []
        return self._row_id_completion_candidates(resolved_table, current_token)

    def _table_column_completion_candidates(self, table_token: str, current_token: str) -> list[str]:
        resolved_table = self._resolve_completion_table_token(table_token)
        if resolved_table is None:
            return []

        normalized = self._normalize_command_token(current_token)
        columns = self.get_table_columns(resolved_table)
        display_columns = self.get_table_display_columns(resolved_table)

        candidates: list[str] = []
        seen: set[str] = set()
        prefer_full = "_" in normalized
        sources = (columns, display_columns) if prefer_full else (display_columns, columns)
        for source in sources:
            for candidate in source:
                normalized_candidate = self._normalize_command_token(candidate)
                if normalized and not normalized_candidate.startswith(normalized):
                    continue
                if candidate in seen:
                    continue
                seen.add(candidate)
                candidates.append(candidate)
        return candidates

    def _completion_candidates_for_help(self, help_tokens: Sequence[str]) -> list[str]:
        if not help_tokens:
            return self._root_completion_tokens()
        group_name = self._group_alias_to_group.get(self._normalize_command_token(help_tokens[0]))
        if group_name is None:
            return []
        if len(help_tokens) == 1:
            return self._group_completion_tokens(group_name)
        return []

    def _completion_candidates_for_direct_command(
        self,
        command: TerminalCommandAPI,
        args_before_current: Sequence[str],
        current_token: str,
    ) -> list[str]:
        command_name = self._normalize_command_token(command.name)
        if command_name == "help":
            return self._completion_candidates_for_help(args_before_current)

        if command_name in {"use", "schema", "count", "browse", "top", "search"}:
            if len(args_before_current) == 0:
                return self._table_token_completion_candidates(current_token)
            return []

        if command_name == "row":
            if len(args_before_current) == 0:
                return self._row_ref_token_completion_candidates(current_token)
            if len(args_before_current) == 1:
                return self._table_scoped_id_completion_candidates(args_before_current[0], current_token)
            return []

        if command_name == "set":
            if len(args_before_current) == 0:
                return self._row_ref_token_completion_candidates(current_token)
            first_token = args_before_current[0]
            if self._looks_like_compact_row_ref_prefix(first_token):
                if len(args_before_current) == 1:
                    return self._table_column_completion_candidates(first_token.split(":", 1)[0], current_token)
                return []
            if len(args_before_current) == 1:
                return self._table_scoped_id_completion_candidates(first_token, current_token)
            if len(args_before_current) == 2:
                return self._table_column_completion_candidates(first_token, current_token)
            return []

        if command_name == "edit":
            if len(args_before_current) == 0:
                return self._row_ref_token_completion_candidates(current_token)
            first_token = args_before_current[0]
            if self._looks_like_compact_row_ref_prefix(first_token):
                return self._table_column_completion_candidates(first_token.split(":", 1)[0], current_token)
            if len(args_before_current) == 1:
                return self._table_scoped_id_completion_candidates(first_token, current_token)
            return self._table_column_completion_candidates(first_token, current_token)

        if command_name == "delete":
            if len(args_before_current) == 0:
                return self._row_ref_token_completion_candidates(current_token)
            first_token = args_before_current[0]
            if self._looks_like_compact_row_ref_prefix(first_token):
                return []
            if len(args_before_current) == 1:
                return self._table_scoped_id_completion_candidates(first_token, current_token)
            return []

        if command_name == "links":
            if len(args_before_current) == 0:
                return self._row_ref_token_completion_candidates(current_token)
            if len(args_before_current) == 1:
                if self._looks_like_compact_row_ref_prefix(args_before_current[0]):
                    return self._table_token_completion_candidates(current_token)
                return self._table_scoped_id_completion_candidates(args_before_current[0], current_token)
            if len(args_before_current) == 2:
                return self._table_token_completion_candidates(current_token)
            return []

        if command_name in {"link", "unlink"}:
            remaining = list(args_before_current)
            if not remaining:
                return self._row_ref_token_completion_candidates(current_token)

            first_token = remaining.pop(0)
            if not self._looks_like_compact_row_ref_prefix(first_token):
                if not remaining:
                    return self._table_scoped_id_completion_candidates(first_token, current_token)
                next_token = remaining.pop(0)
                if self._normalize_command_token(next_token) == "to":
                    return []

            if remaining and self._normalize_command_token(remaining[0]) == "to":
                remaining.pop(0)

            if not remaining:
                return self._row_ref_token_completion_candidates(current_token)

            second_token = remaining.pop(0)
            if self._looks_like_compact_row_ref_prefix(second_token):
                return []
            if not remaining:
                return self._table_scoped_id_completion_candidates(second_token, current_token)
            return []

        return []

    def _completion_candidates_for_group_command(
        self,
        group_name: str,
        args_before_current: Sequence[str],
        current_token: str,
    ) -> list[str]:
        if not args_before_current:
            return self._group_completion_tokens(group_name)

        group_map = self._command_groups.get(group_name, {})
        subcommand_token = self._normalize_command_token(args_before_current[0])
        subcommand = group_map.get(subcommand_token)
        if subcommand is None:
            return []

        if group_name in {"show", "on", "off"}:
            if len(args_before_current) == 1:
                return self._row_ref_token_completion_candidates(current_token)
            if len(args_before_current) == 2:
                return self._table_scoped_id_completion_candidates(args_before_current[1], current_token)

        return []

    def command_completion_candidates(
        self,
        line: str,
        *,
        cursor: Optional[int] = None,
    ) -> _CommandCompletion:
        """
        Return token completion candidates for the current input line.

        Completion is intentionally command-focused:
        - root prompt: direct commands + command groups/aliases
        - grouped commands: subcommands + aliases
        - `help`: commands/groups, then grouped subcommands
        """
        text = str(line)
        cursor_pos = len(text) if cursor is None else max(0, min(len(text), int(cursor)))
        before_cursor = text[:cursor_pos]

        token_start = cursor_pos
        while token_start > 0 and not before_cursor[token_start - 1].isspace():
            token_start -= 1
        token_prefix = before_cursor[token_start:cursor_pos]
        previous_tokens = [token for token in before_cursor[:token_start].split() if token]
        normalized_prefix = self._normalize_command_token(token_prefix)

        candidates: list[str] = []
        if not previous_tokens:
            candidates = self._root_completion_tokens()
        else:
            root_token = self._normalize_command_token(previous_tokens[0])
            group_name = self._group_alias_to_group.get(root_token)
            if group_name is not None and len(previous_tokens) == 1:
                candidates = self._group_completion_tokens(group_name)
            elif group_name is not None:
                candidates = self._completion_candidates_for_group_command(
                    group_name,
                    previous_tokens[1:],
                    token_prefix,
                )
            else:
                command = self._commands.get(root_token)
                if command is not None:
                    candidates = self._completion_candidates_for_direct_command(
                        command,
                        previous_tokens[1:],
                        token_prefix,
                    )

        if normalized_prefix:
            if ":" in token_prefix:
                table_prefix = self._normalize_command_token(token_prefix.split(":", 1)[0])
                candidates = [
                    candidate
                    for candidate in candidates
                    if self._normalize_command_token(candidate.split(":", 1)[0]).startswith(table_prefix)
                ]
            else:
                candidates = [candidate for candidate in candidates if candidate.startswith(normalized_prefix)]

        return _CommandCompletion(
            token_start=token_start,
            token_end=cursor_pos,
            prefix=token_prefix,
            candidates=tuple(candidates),
        )

    def _configure_readline_completion(self) -> None:
        """Install one readline completer for this browser session."""
        if self._readline_completion_configured:
            return
        readline_mod = _readline
        if readline_mod is None:
            return
        try:
            parse_and_bind = getattr(readline_mod, "parse_and_bind", None)
            if callable(parse_and_bind):
                parse_and_bind("tab: complete")
        except Exception:
            pass
        try:
            set_completer_delims = getattr(readline_mod, "set_completer_delims", None)
            if callable(set_completer_delims):
                set_completer_delims(" \t\n")
        except Exception:
            pass
        try:
            set_completer = getattr(readline_mod, "set_completer", None)
            if callable(set_completer):
                set_completer(self._readline_completer)
                self._readline_completion_configured = True
        except Exception:
            self._readline_completion_configured = False

    def _readline_completer(self, text: str, state: int) -> Optional[str]:
        """Readline callback returning one completion match at a time."""
        if state == 0:
            readline_mod = _readline
            line_buffer = str(text)
            endidx = len(line_buffer)
            if readline_mod is not None:
                try:
                    line_buffer = str(readline_mod.get_line_buffer())
                except Exception:
                    line_buffer = str(text)
                try:
                    endidx = int(readline_mod.get_endidx())
                except Exception:
                    endidx = len(line_buffer)
            completion = self.command_completion_candidates(line_buffer, cursor=endidx)
            self._readline_completion_matches = list(completion.candidates)
        if state < 0 or state >= len(self._readline_completion_matches):
            return None
        return self._readline_completion_matches[state]

    def _normalized_command_tokens(self, tokens: Sequence[str]) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        for raw in tokens:
            token = self._normalize_command_token(raw)
            if not token or token in seen:
                continue
            seen.add(token)
            normalized.append(token)
        return normalized

    def _group_aliases(self, group_name: str) -> list[str]:
        aliases: list[str] = []
        for alias, target in sorted(self._group_alias_to_group.items()):
            if target == group_name and alias != group_name:
                aliases.append(alias)
        return aliases

    def _write_group_help(self, group_name: str) -> None:
        commands = dict(self._command_groups.get(group_name, {}))
        if not commands:
            raise ValueError("Unknown command group: {!r}.".format(group_name))

        self._write("Command group: {}".format(group_name))
        aliases = self._group_aliases(group_name)
        if aliases:
            self._write("Aliases: {}".format(", ".join(aliases)))
        self._write("Subcommands:")

        by_id: dict[int, TerminalCommandAPI] = {}
        for command in commands.values():
            by_id[id(command)] = command
        for command in sorted(by_id.values(), key=lambda c: c.name):
            usage = command.usage or "{} {}".format(group_name, command.name)
            alias_text = self._format_command_aliases(command.aliases)
            self._write("  {:<34} {}{}".format(usage, command.summary, alias_text))

        self._write("Use `help {} <subcommand>` for details.".format(group_name))

    def _write_command_help(self, command: TerminalCommandAPI, *, group_name: Optional[str] = None) -> None:
        canonical_name = command.name
        if group_name:
            canonical_name = "{} {}".format(group_name, command.name)

        self._write("Command: {}".format(canonical_name))
        summary = str(getattr(command, "summary", "") or "").strip()
        if summary:
            self._write("Summary: {}".format(summary))

        usage = str(getattr(command, "usage", "") or "").strip() or canonical_name
        self._write("Usage: {}".format(usage))

        if group_name:
            direct_names: list[str] = []
            if bool(getattr(command, "expose_direct", True)):
                direct_names = self._normalized_command_tokens([command.name] + list(command.aliases))
            if direct_names:
                self._write("Direct names: {}".format(", ".join(direct_names)))
            group_aliases = self._group_aliases(group_name)
            if group_aliases:
                self._write("Group aliases: {}".format(", ".join(group_aliases)))
        else:
            aliases = self._normalized_command_tokens(list(command.aliases))
            if aliases:
                self._write("Aliases: {}".format(", ".join(aliases)))

    def _print_help(self, args: Optional[Sequence[str]] = None) -> None:
        help_args = [str(arg) for arg in (args or []) if str(arg).strip()]
        if len(help_args) > 2:
            raise ValueError("Usage: help [command] [subcommand]")

        if len(help_args) == 1:
            target = self._normalize_command_token(help_args[0])
            group_name = self._group_alias_to_group.get(target)
            if group_name is not None:
                self._write_group_help(group_name)
                return

            command = self._commands.get(target)
            if command is None:
                raise ValueError("Unknown command or group: {!r}.".format(help_args[0]))
            self._write_command_help(command)
            return

        if len(help_args) == 2:
            group_token = self._normalize_command_token(help_args[0])
            group_name = self._group_alias_to_group.get(group_token)
            if group_name is None:
                raise ValueError("Unknown command group: {!r}.".format(help_args[0]))

            subcommand_token = self._normalize_command_token(help_args[1])
            command = self._command_groups.get(group_name, {}).get(subcommand_token)
            if command is None:
                raise ValueError("Unknown subcommand {} {}.".format(group_name, help_args[1]))
            self._write_command_help(command, group_name=group_name)
            return

        self._write("Commands:")
        self._write("  Use `help <command>` or `help <group> <subcommand>` for details.")
        grouped_command_ids: set[int] = set()
        grouped = self.iter_registered_command_groups()
        if grouped:
            self._write("  -- grouped --")
            for group_name, commands in grouped:
                self._write("  {} <subcommand>".format(group_name))
                for command in commands:
                    grouped_command_ids.add(id(command))
                    usage = command.usage or "{} {}".format(group_name, command.name)
                    alias_text = self._format_command_aliases(command.aliases)
                    self._write("    {:<32} {}{}".format(usage, command.summary, alias_text))

        self._write("  -- direct --")
        for command in self.iter_registered_commands():
            if id(command) in grouped_command_ids:
                continue
            usage = command.usage or command.name
            alias_text = self._format_command_aliases(command.aliases)
            self._write("  {:<34} {}{}".format(usage, command.summary, alias_text))

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
        id_column = None
        try:
            id_column = self.db.driver_wrapper.get_id_column(table)
        except Exception:
            id_column = None

        def _lookup_value(column: str):
            if isinstance(row, Mapping):
                if column in row:
                    return True, row.get(column)
                return False, None
            try:
                if column in row:
                    return True, row[column]
            except Exception:
                pass
            try:
                return True, row[column]
            except Exception:
                return False, None

        preferred_fields_by_table = {
            "works": ("work_title", "work_canonical_title", "work_sort_title"),
            "stores": ("store_name", "store_kind", "store_root_uri"),
            "labels": ("label_text", "label", "label_text_norm"),
            "tags": ("tag", "label_text", "label"),
            "notes": ("note", "note_text", "note_body"),
            "folders": ("folder_name", "folder_relpath"),
            "files": ("file_name", "file_original_name", "file_storage_key"),
            "creators": ("creator", "creator_name", "agent_canonical_name", "agent_name"),
            "agents": ("agent_canonical_name", "agent_name", "agent_sort_name"),
            "series": ("series_name", "series_title", "series_sort_title"),
            "genres": ("genre", "genre_text", "genre_name"),
            "subjects": ("subject", "subject_text", "subject_name"),
            "languages": ("language", "language_name", "language_code"),
            "expressions": ("expression_label", "expression_title_override", "expression_type"),
            "manifestations": ("manifestation_label", "manifestation_title", "manifestation_type"),
            "items": ("item_source_name", "item_inventory_code", "item_location"),
        }

        summary_parts: list[str] = []
        seen_summary_values: set[str] = set()

        def _add_summary_value(raw_value: object) -> None:
            text = _preview_row_text(raw_value)
            if not text:
                return
            if text in seen_summary_values:
                return
            seen_summary_values.add(text)
            summary_parts.append(text)

        raw_id = None
        if id_column is not None:
            has_id, raw_id = _lookup_value(id_column)
            if not has_id:
                raw_id = None

        for field in preferred_fields_by_table.get(table, ()):
            present, value = _lookup_value(field)
            if present:
                _add_summary_value(value)

        if not summary_parts:
            keyword_priority = (
                "name",
                "title",
                "label",
                "note",
                "text",
                "path",
                "uri",
                "kind",
                "type",
                "location",
                "code",
            )
            columns = list(self.db.get_column_headings(table))
            ordered_columns = sorted(
                columns,
                key=lambda key: (
                    min(
                        (idx for idx, token in enumerate(keyword_priority) if token in str(key).lower()),
                        default=len(keyword_priority),
                    ),
                    str(key),
                ),
            )
            for column in ordered_columns:
                if id_column is not None and column == id_column:
                    continue
                present, value = _lookup_value(column)
                if not present:
                    continue
                _add_summary_value(value)
                if len(summary_parts) >= 3:
                    break

        if summary_parts:
            parts: list[str] = []
            if raw_id not in {None, ""}:
                parts.append("#{}".format(raw_id))
            parts.extend(summary_parts[:3])
            return " | ".join(parts)

        columns = self.db.get_column_headings(table)
        pieces: list[str] = []
        if id_column is not None:
            present, value = _lookup_value(id_column)
            if present:
                pieces.append("{}={}".format(id_column, _truncate(value, width=24)))

        for column in columns:
            if id_column is not None and column == id_column:
                continue
            present, value = _lookup_value(column)
            if not present:
                continue
            pieces.append("{}={}".format(column, _truncate(value)))

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
        self.emit(self.render_row_details(table, row, max_cell_width=120))

    def _cmd_search(self, args: list[str]) -> None:
        if len(args) < 2:
            raise ValueError(
                "Usage: search <table> <term> [--limit n] OR search <table> <column> <value> [limit]"
            )
        table = self._resolve_table(args[0])
        columns = set(self.db.get_column_headings(table))

        # Legacy exact-match mode: search <table> <column> <value> [limit]
        if len(args) >= 3 and args[1] in columns:
            column = args[1]
            value = args[2]
            limit = self.page_size
            if len(args) >= 4:
                maybe_limit = _safe_int(args[3])
                if maybe_limit is None:
                    raise ValueError("limit must be an integer")
                limit = max(1, maybe_limit)

            matches = self.db.search(table, column, value)
            shown_rows = matches[:limit]
            self._write("Search {}.{} == {!r}".format(table, column, value))
            if not shown_rows:
                self._write("(no rows)")
            else:
                self._write(self.format_rows_as_table(table, shown_rows))
            self._write(
                "Summary: matches_total={} shown={} limit={}".format(
                    len(matches),
                    len(shown_rows),
                    limit,
                )
            )
            return

        # Table-wide contains mode: search <table> <term...> [--limit n]
        limit = self.page_size
        term_tokens = list(args[1:])
        if "--limit" in term_tokens:
            idx = term_tokens.index("--limit")
            if idx + 1 >= len(term_tokens):
                raise ValueError("--limit requires an integer value")
            maybe_limit = _safe_int(term_tokens[idx + 1])
            if maybe_limit is None:
                raise ValueError("--limit must be an integer")
            limit = max(1, maybe_limit)
            del term_tokens[idx : idx + 2]

        search_term = " ".join(token for token in term_tokens if str(token).strip())
        if not search_term:
            raise ValueError("search term cannot be blank")

        search_key = search_term.casefold()
        table_columns = list(self.db.get_column_headings(table))
        rows = self.db.get_all_rows(table, iterator_return=False)

        matches = []
        for row in rows:
            for column in table_columns:
                if column not in row:
                    continue
                value = row[column]
                if value is None:
                    continue
                if search_key in str(value).casefold():
                    matches.append(row)
                    break

        shown_rows = matches[:limit]
        self._write("Search {} contains {!r}".format(table, search_term))
        if not shown_rows:
            self._write("(no rows)")
        else:
            self._write(self.format_rows_as_table(table, shown_rows))
        self._write(
            "Summary: scanned_rows={} matches_total={} shown={} limit={}".format(
                len(rows),
                len(matches),
                len(shown_rows),
                limit,
            )
        )

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
        "--command",
        action="append",
        default=[],
        help="Execute command(s) non-interactively; may be repeated.",
    )
    return parser


def run_windowed_text_browser(
    db: Database,
    *,
    page_size: int = 20,
    history_file: Optional[str | Path] = None,
    status_refresh_s: float = 1.0,
    status_height: int = 9,
    job_panel_height: int = 10,
) -> int:
    """Run the split-pane curses UI wrapper around the text browser."""
    from LiuXin_alpha.interfaces.terminal.windowed_ui import WindowedUiConfig, run_windowed_browser

    config = WindowedUiConfig(
        status_refresh_s=float(status_refresh_s),
        status_height=max(5, int(status_height)),
        job_panel_height=max(4, int(job_panel_height)),
    )
    return run_windowed_browser(
        db,
        page_size=page_size,
        history_file=history_file,
        config=config,
    )


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
            if args.command:
                shell = TextDatabaseBrowser(
                    db,
                    page_size=args.page_size,
                    output=sys.stdout,
                    history_file=args.history_file,
                )
                return shell.run_commands(args.command)

            if args.ui_mode == "windowed":
                return run_windowed_text_browser(
                    db,
                    page_size=args.page_size,
                    history_file=args.history_file,
                    status_refresh_s=args.windowed_status_refresh_s,
                    status_height=args.windowed_status_height,
                    job_panel_height=args.windowed_job_panel_height,
                )

            shell = TextDatabaseBrowser(
                db,
                page_size=args.page_size,
                output=sys.stdout,
                history_file=args.history_file,
            )
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
    "run_windowed_text_browser",
    "main",
]
