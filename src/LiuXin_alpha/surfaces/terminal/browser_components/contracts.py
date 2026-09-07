"""Named shared state and cross-owner calls required by browser composition.

Abstract methods make missing owner mixins fail at construction; concrete methods
remain with the responsibility that implements them. No concrete browser import
is needed, including for typing. Dynamic Core payloads retain their API boundary.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from pathlib import Path
from typing import Any, TextIO

from LiuXin_alpha.core import CoreClientAPI
from LiuXin_alpha.surfaces.core import (
    CoreDatabaseView,
    CoreRow,
    CoreSurfaceModel,
    SurfaceCoreSession,
)
from LiuXin_alpha.surfaces.terminal.commands import (
    TerminalCommandAPI,
)
from LiuXin_alpha.surfaces.terminal.plugins import TerminalLifecyclePluginAPI
from LiuXin_alpha.utils.jobs.manager import JobManagerAPI

from .models import BrowseWindow as _BrowseWindow
from .models import CommandCompletion as _CommandCompletion
from .models import RowRecord


class BrowserState[HostT](ABC):
    """State initialized by the composition root and calls shared by its owners."""

    @property
    @abstractmethod
    def _extension_host(self) -> HostT:
        """The concrete host supplied to command and lifecycle extensions."""
        ...

    core: CoreClientAPI
    model: CoreSurfaceModel
    db: CoreDatabaseView
    _compatibility_core_session: SurfaceCoreSession | None
    metadata_read_source: Any
    job_manager: JobManagerAPI
    input: TextIO
    output: TextIO
    page_size: int
    current_table: str | None
    window: _BrowseWindow | None
    _commands: dict[str, TerminalCommandAPI[HostT]]
    _command_groups: dict[str, dict[str, TerminalCommandAPI[HostT]]]
    _group_alias_to_group: dict[str, str]
    _lifecycle_plugins: list[TerminalLifecyclePluginAPI[HostT]]
    _started: bool
    _closed: bool
    _shutdown_reason: str | None
    _history_file: Path
    _history_loaded: bool
    _history_enabled_for_session: bool
    _readline_completion_configured: bool
    _readline_completion_matches: list[str]
    prompt: str

    @abstractmethod
    def execute_line(self, line: str) -> bool: ...

    @abstractmethod
    def _rewrite_on_legacy_group_args(
        self, group_map: dict[str, TerminalCommandAPI[HostT]], args: list[str]
    ) -> tuple[TerminalCommandAPI[HostT], list[str]] | None: ...

    @abstractmethod
    def _rewrite_off_legacy_group_args(
        self, group_map: dict[str, TerminalCommandAPI[HostT]], args: list[str]
    ) -> tuple[TerminalCommandAPI[HostT], list[str]] | None: ...

    @abstractmethod
    def _rewrite_show_legacy_group_args(
        self, group_map: dict[str, TerminalCommandAPI[HostT]], args: list[str]
    ) -> tuple[TerminalCommandAPI[HostT], list[str]] | None: ...

    @abstractmethod
    def request_shutdown(self, reason: str) -> None: ...

    @abstractmethod
    def register_command(self, command: TerminalCommandAPI[HostT]) -> None: ...

    @staticmethod
    @abstractmethod
    def _normalize_command_token(token: str | None) -> str: ...

    @abstractmethod
    def register_lifecycle_plugin(
        self, plugin: TerminalLifecyclePluginAPI[HostT]
    ) -> None: ...

    @abstractmethod
    def iter_registered_commands(self) -> list[TerminalCommandAPI[HostT]]: ...

    @abstractmethod
    def iter_registered_command_groups(
        self,
    ) -> list[tuple[str, list[TerminalCommandAPI[HostT]]]]: ...

    @abstractmethod
    def emit(self, text: str, *, end: str = "\n") -> None: ...

    @abstractmethod
    def notify_write_completed(self) -> bool: ...

    @abstractmethod
    def core_runtime_startup_warning(self) -> str | None: ...

    @abstractmethod
    def get_table_columns(self, table: str) -> list[str]: ...

    @abstractmethod
    def get_table_display_columns(self, table: str) -> list[str]: ...

    @abstractmethod
    def get_terminal_width(self) -> int: ...

    @abstractmethod
    def format_rows_as_table(
        self, table: str, rows: Sequence[RowRecord], *, max_cell_width: int = 60
    ) -> str: ...

    @abstractmethod
    def render_row_details(
        self, table: str, row: RowRecord, *, max_cell_width: int = 120
    ) -> str: ...

    @abstractmethod
    def _write(self, text: str, *, end: str = "\n") -> None: ...

    @abstractmethod
    def _root_completion_tokens(self) -> list[str]: ...

    @abstractmethod
    def _group_completion_tokens(self, group_name: str) -> list[str]: ...

    @abstractmethod
    def _table_token_completion_candidates(self, token: str) -> list[str]: ...

    @abstractmethod
    def _table_id_column(self, table: str) -> str | None: ...

    @abstractmethod
    def _row_ref_token_completion_candidates(self, token: str) -> list[str]: ...

    @staticmethod
    @abstractmethod
    def _looks_like_compact_row_ref_prefix(token: str) -> bool: ...

    @abstractmethod
    def _table_scoped_id_completion_candidates(
        self, table_token: str, current_token: str
    ) -> list[str]: ...

    @abstractmethod
    def _table_column_completion_candidates(
        self, table_token: str, current_token: str
    ) -> list[str]: ...

    @abstractmethod
    def _completion_candidates_for_help(
        self, help_tokens: Sequence[str]
    ) -> list[str]: ...

    @abstractmethod
    def command_completion_candidates(
        self, line: str, *, cursor: int | None = None
    ) -> _CommandCompletion: ...

    @abstractmethod
    def _all_tables(self) -> list[str]: ...

    @abstractmethod
    def _resolve_table_token(self, token: str) -> str: ...

    @abstractmethod
    def _resolve_table(self, raw: str | None) -> str: ...

    @abstractmethod
    def _row_count(self, table: str) -> int: ...

    @abstractmethod
    def _format_row(self, table: str, row: RowRecord) -> str: ...

    @abstractmethod
    def _table_slice(self, table: str, *, limit: int, offset: int) -> list[CoreRow]: ...
