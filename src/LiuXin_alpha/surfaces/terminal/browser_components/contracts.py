"""
Declare the browser state and cross-owner calls required by terminal composition.

The composition root initializes these annotated attributes; annotations do not
allocate values or validate instances. Abstract methods require concrete owner
implementations before instantiation, without importing the concrete browser.
Contract summaries and fields refer to the maintained owner methods rather than
supplying fallback implementations or hiding dynamic Core payload boundaries.
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
    """
    Require a shared browser state layout and the cross-owner methods used by its command/session components.

    HostT identifies the concrete object passed to commands and lifecycle plugins.
    Core/model/database views, streams, job management, selected rows, registries,
    and history/lifecycle flags must be initialized by composition; this ABC does
    not initialize or validate those fields. Method contracts describe the maintained
    owner, while abstractness alone checks method presence rather than behavior.

    Example:
        >>> import inspect
        >>> inspect.isabstract(BrowserState)
        True
    """

    @property
    @abstractmethod
    def _extension_host(self) -> HostT:
        """
        Expose this exact browser instance to typed commands and lifecycle plugins without wrapping it.

        See :meth:`LiuXin_alpha.surfaces.terminal.browser.TextDatabaseBrowser._extension_host`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._extension_host  # doctest: +SKIP


        :return: Self, preserving the concrete browser's extension capabilities and identity.
        """
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
    def execute_line(self, line: str) -> bool:
        """
        Shell-split a command line and dispatch its lowercase root token, groups first.

        See :meth:`LiuXin_alpha.surfaces.terminal.browser_components.registry.RegistryMixin.execute_line`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host.execute_line(line)  # doctest: +SKIP


        :param line: Input line whose quoting is interpreted by ``shlex.split``.
        :return: Whether the session should continue after handling the line.
        """
        ...

    @abstractmethod
    def _rewrite_on_legacy_group_args(
        self, group_map: dict[str, TerminalCommandAPI[HostT]], args: list[str]
    ) -> tuple[TerminalCommandAPI[HostT], list[str]] | None:
        """
        Select an on-group command from a legacy target-first argument sequence.

        See :meth:`LiuXin_alpha.surfaces.terminal.browser_components.legacy.LegacyMixin._rewrite_on_legacy_group_args`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._rewrite_on_legacy_group_args(group_map, args)  # doctest: +SKIP


        :param group_map: On-group kind and alias tokens mapped to command implementations.
        :param args: Arguments after ``on``, with a compact or split target before the kind.
        :return: Selected command and arguments without its kind token, or ``None``.
        :raises ValueError: If a recognizable target is followed by an unsupported kind.
        """
        ...

    @abstractmethod
    def _rewrite_off_legacy_group_args(
        self, group_map: dict[str, TerminalCommandAPI[HostT]], args: list[str]
    ) -> tuple[TerminalCommandAPI[HostT], list[str]] | None:
        """
        Select an off-group command from a legacy target-first argument sequence.

        See :meth:`LiuXin_alpha.surfaces.terminal.browser_components.legacy.LegacyMixin._rewrite_off_legacy_group_args`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._rewrite_off_legacy_group_args(group_map, args)  # doctest: +SKIP


        :param group_map: Off-group kind and alias tokens mapped to command implementations.
        :param args: Arguments after ``off``, preserving the target and value tokens.
        :return: Selected command and arguments without its kind token, or ``None``.
        :raises ValueError: If a recognizable target is followed by an unsupported kind.
        """
        ...

    @abstractmethod
    def _rewrite_show_legacy_group_args(
        self, group_map: dict[str, TerminalCommandAPI[HostT]], args: list[str]
    ) -> tuple[TerminalCommandAPI[HostT], list[str]] | None:
        """
        Select a show command from default-all or explicit target-first legacy syntax.

        See :meth:`LiuXin_alpha.surfaces.terminal.browser_components.legacy.LegacyMixin._rewrite_show_legacy_group_args`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._rewrite_show_legacy_group_args(group_map, args)  # doctest: +SKIP


        :param group_map: Show-group kind/alias tokens mapped to implementations.
        :param args: Arguments after ``show`` in compact or split target form.
        :return: Selected command and rewritten arguments, or ``None`` if unrecognized.
        :raises ValueError: If a recognizable target has an invalid selector or kind.
        """
        ...

    @abstractmethod
    def request_shutdown(self, reason: str) -> None:
        """
        Record the preferred reason to use when the command loop later shuts down.

        See :meth:`LiuXin_alpha.surfaces.terminal.browser_components.session.SessionMixin.request_shutdown`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host.request_shutdown(reason)  # doctest: +SKIP


        :param reason: Explanation to retain for the eventual shutdown call.
        :return: ``None`` after storing the reason as text.
        """
        ...

    @abstractmethod
    def register_command(self, command: TerminalCommandAPI[HostT]) -> None:
        """
        Register normalized command names and, when specified, group names and aliases.

        See :meth:`LiuXin_alpha.surfaces.terminal.browser_components.registry.RegistryMixin.register_command`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host.register_command(command)  # doctest: +SKIP


        :param command: Extension supplying primary name, aliases, group, and exposure metadata.
        :return: ``None``; registry mappings are updated in place.
        :raises ValueError: If a name/group alias is already bound incompatibly.
        """
        ...

    @staticmethod
    @abstractmethod
    def _normalize_command_token(token: str | None) -> str:
        """
        Convert a token to stripped lowercase text, treating ``None`` as empty.

        See :meth:`LiuXin_alpha.surfaces.terminal.browser_components.registry.RegistryMixin._normalize_command_token`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._normalize_command_token(token)  # doctest: +SKIP


        :param token: Name or alias to normalize for registry lookup.
        :return: Lowercase string with surrounding whitespace removed, or an empty string.
        """
        ...

    @abstractmethod
    def register_lifecycle_plugin(
        self, plugin: TerminalLifecyclePluginAPI[HostT]
    ) -> None:
        """
        Append a lifecycle plugin after checking its name against existing registrations.

        See :meth:`LiuXin_alpha.surfaces.terminal.browser_components.registry.RegistryMixin.register_lifecycle_plugin`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host.register_lifecycle_plugin(plugin)  # doctest: +SKIP


        :param plugin: Extension supplying startup/shutdown hooks and optional name metadata.
        :return: ``None``; the plugin is appended in registration order.
        :raises ValueError: If an existing distinct plugin has the same compared name.
        """
        ...

    @abstractmethod
    def iter_registered_commands(self) -> list[TerminalCommandAPI[HostT]]:
        """
        List directly exposed command objects once each, sorted by their primary names.

        See :meth:`LiuXin_alpha.surfaces.terminal.browser_components.registry.RegistryMixin.iter_registered_commands`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host.iter_registered_commands()  # doctest: +SKIP


        :return: New list deduplicated by command identity rather than name equality.
        """
        ...

    @abstractmethod
    def iter_registered_command_groups(
        self,
    ) -> list[tuple[str, list[TerminalCommandAPI[HostT]]]]:
        """
        List canonical groups alphabetically, with unique command objects sorted by name.

        See :meth:`LiuXin_alpha.surfaces.terminal.browser_components.registry.RegistryMixin.iter_registered_command_groups`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host.iter_registered_command_groups()  # doctest: +SKIP


        :return: New group/command-list pairs containing the original command instances.
        """
        ...

    @abstractmethod
    def emit(self, text: str, *, end: str = "\n") -> None:
        """
        Send command output through the browser's overridable writing hook.

        See :meth:`LiuXin_alpha.surfaces.terminal.browser_components.host.HostMixin.emit`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host.emit(text, end=end)  # doctest: +SKIP


        :param text: Text to send without implicit string coercion.
        :param end: Suffix appended to the text; defaults to a newline.
        :return: ``None`` after the output hook completes.
        """
        ...

    @abstractmethod
    def notify_write_completed(self) -> bool:
        """
        Request a best-effort refresh of attached metadata views after a write.

        See :meth:`LiuXin_alpha.surfaces.terminal.browser_components.host.HostMixin.notify_write_completed`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host.notify_write_completed()  # doctest: +SKIP


        :return: Whether an attached read source reported a successful refresh.
        """
        ...

    @abstractmethod
    def core_runtime_startup_warning(self) -> str | None:
        """
        Provide the optional startup-warning hook for browser compositions.

        See :meth:`LiuXin_alpha.surfaces.terminal.browser_components.host.HostMixin.core_runtime_startup_warning`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host.core_runtime_startup_warning()  # doctest: +SKIP


        :return: ``None`` in this composition; overrides may supply user-facing warning text.
        """
        ...

    @abstractmethod
    def get_table_columns(self, table: str) -> list[str]:
        """
        Materialize database-view column headings in their advertised order without renaming or deduplication.

        See :meth:`LiuXin_alpha.surfaces.terminal.browser_components.catalog.CatalogMixin.get_table_columns`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host.get_table_columns(table)  # doctest: +SKIP


        :param table: Resolved table name whose headings are requested.
        :return: New list of headings; lookup and iteration failures propagate.
        """
        ...

    @abstractmethod
    def get_table_display_columns(self, table: str) -> list[str]:
        """
        Derive terminal display headings from schema headings using the shared prefix-shortening rules.

        See :meth:`LiuXin_alpha.surfaces.terminal.browser_components.catalog.CatalogMixin.get_table_display_columns`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host.get_table_display_columns(table)  # doctest: +SKIP


        :param table: Resolved table whose headings and table-name prefix guide display shortening.
        :return: Ordered display headings, without changing the underlying schema names.
        """
        ...

    @abstractmethod
    def get_terminal_width(self) -> int:
        """
        Choose a usable rendering width from terminal detection or a safe fallback.

        See :meth:`LiuXin_alpha.surfaces.terminal.browser_components.host.HostMixin.get_terminal_width`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host.get_terminal_width()  # doctest: +SKIP


        :return: At least 40 columns, using 120 when terminal-size detection fails.
        """
        ...

    @abstractmethod
    def format_rows_as_table(
        self, table: str, rows: Sequence[RowRecord], *, max_cell_width: int = 60
    ) -> str:
        """
        Render schema-ordered cells with shortened headings and the browser's terminal-width budget.

        See :meth:`LiuXin_alpha.surfaces.terminal.browser_components.rows.RowsMixin.format_rows_as_table`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host.format_rows_as_table(table, rows, max_cell_width=max_cell_width)  # doctest: +SKIP


        :param table: Resolved table whose schema heading order defines displayed columns.
        :param rows: Ordered row records inspected by membership and item access for each heading.
        :param max_cell_width: Initial data-cell character budget, including any truncation marker.
        :return: Multiline ASCII table or the shared renderer's no-columns/insufficient-width message.
        """
        ...

    @abstractmethod
    def render_row_details(
        self, table: str, row: RowRecord, *, max_cell_width: int = 120
    ) -> str:
        """
        Build and render a row's grouped schema fields as column/value tables without emitting them.

        See :meth:`LiuXin_alpha.surfaces.terminal.browser_components.rows.RowsMixin.render_row_details`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host.render_row_details(table, row, max_cell_width=max_cell_width)  # doctest: +SKIP


        :param table: Resolved table whose headings and ID hint determine detail sections.
        :param row: Row values to group and render, including None for missing schema keys.
        :param max_cell_width: Initial character budget applied to detail table cells before terminal-width fitting.
        :return: Newline-joined titled detail tables, or an empty string when there are no fields.
        """
        ...

    @abstractmethod
    def _write(self, text: str, *, end: str = "\n") -> None:
        """
        Write one text fragment and immediately flush the configured output stream.

        See :meth:`LiuXin_alpha.surfaces.terminal.browser_components.host.HostMixin._write`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._write(text, end=end)  # doctest: +SKIP


        :param text: Output fragment without implicit string coercion.
        :param end: Suffix appended before flushing; defaults to a newline.
        :return: ``None`` after both write and flush complete.
        :raises OSError: If the underlying stream cannot write or flush.
        """
        ...

    @abstractmethod
    def _root_completion_tokens(self) -> list[str]:
        """
        List direct-command names and command-group aliases available at the prompt.

        See :meth:`LiuXin_alpha.surfaces.terminal.browser_components.completion_sources.CompletionSourcesMixin._root_completion_tokens`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._root_completion_tokens()  # doctest: +SKIP


        :return: Sorted, distinct, nonempty command and group tokens.
        """
        ...

    @abstractmethod
    def _group_completion_tokens(self, group_name: str) -> list[str]:
        """
        List registered kind and alias tokens within one command group.

        See :meth:`LiuXin_alpha.surfaces.terminal.browser_components.completion_sources.CompletionSourcesMixin._group_completion_tokens`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._group_completion_tokens(group_name)  # doctest: +SKIP


        :param group_name: Canonical registry key of the command group.
        :return: Sorted nonempty group tokens, or an empty list for an unknown group.
        """
        ...

    @abstractmethod
    def _table_token_completion_candidates(self, token: str) -> list[str]:
        """
        Filter available table names by a normalized user-entered prefix.

        See :meth:`LiuXin_alpha.surfaces.terminal.browser_components.completion_sources.CompletionSourcesMixin._table_token_completion_candidates`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._table_token_completion_candidates(token)  # doctest: +SKIP


        :param token: Partial table token to normalize before matching.
        :return: Matching table names in the source table-list order.
        """
        ...

    @abstractmethod
    def _table_id_column(self, table: str) -> str | None:
        """
        Ask the database view for a table's ID-column name on a best-effort basis.

        See :meth:`LiuXin_alpha.surfaces.terminal.browser_components.completion_sources.CompletionSourcesMixin._table_id_column`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._table_id_column(table)  # doctest: +SKIP


        :param table: Resolved table name whose row IDs should be completed.
        :return: ID-column name converted to text, or ``None`` when lookup fails.
        """
        ...

    @abstractmethod
    def _row_ref_token_completion_candidates(self, token: str) -> list[str]:
        """
        Complete either a table prefix or a compact table-and-ID reference.

        See :meth:`LiuXin_alpha.surfaces.terminal.browser_components.completion_sources.CompletionSourcesMixin._row_ref_token_completion_candidates`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._row_ref_token_completion_candidates(token)  # doctest: +SKIP


        :param token: Partial table token or compact ``table:selector`` text.
        :return: Table-name candidates or full compact row-reference candidates.
        """
        ...

    @staticmethod
    @abstractmethod
    def _looks_like_compact_row_ref_prefix(token: str) -> bool:
        """
        Recognize the colon marker used to select compact-reference completion.

        See :meth:`LiuXin_alpha.surfaces.terminal.browser_components.completion_sources.CompletionSourcesMixin._looks_like_compact_row_ref_prefix`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._looks_like_compact_row_ref_prefix(token)  # doctest: +SKIP


        :param token: Current input token to inspect.
        :return: Whether the token's text contains any colon.
        """
        ...

    @abstractmethod
    def _table_scoped_id_completion_candidates(
        self, table_token: str, current_token: str
    ) -> list[str]:
        """
        Complete row IDs after resolving the separately supplied table token.

        See :meth:`LiuXin_alpha.surfaces.terminal.browser_components.completion_sources.CompletionSourcesMixin._table_scoped_id_completion_candidates`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._table_scoped_id_completion_candidates(table_token, current_token)  # doctest: +SKIP


        :param table_token: Table name or alias supplied before the current token.
        :param current_token: Partial row-ID text to match.
        :return: ID candidates, or an empty list if the table cannot be resolved.
        """
        ...

    @abstractmethod
    def _table_column_completion_candidates(
        self, table_token: str, current_token: str
    ) -> list[str]:
        """
        Complete schema or display column names using the spelling suggested by the prefix.

        See :meth:`LiuXin_alpha.surfaces.terminal.browser_components.completion_sources.CompletionSourcesMixin._table_column_completion_candidates`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._table_column_completion_candidates(table_token, current_token)  # doctest: +SKIP


        :param table_token: Table name or alias containing the desired columns.
        :param current_token: Column prefix normalized before matching.
        :return: Matching column spellings, or an empty list for an unresolved table.
        """
        ...

    @abstractmethod
    def _completion_candidates_for_help(self, help_tokens: Sequence[str]) -> list[str]:
        """
        Suggest help subjects at the root or immediately after a recognized command group.

        See :meth:`LiuXin_alpha.surfaces.terminal.browser_components.completion_sources.CompletionSourcesMixin._completion_candidates_for_help`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._completion_candidates_for_help(help_tokens)  # doctest: +SKIP


        :param help_tokens: Completed help arguments preceding the token being edited.
        :return: Root/group subjects, or no candidates beyond an unsupported help depth.
        """
        ...

    @abstractmethod
    def command_completion_candidates(
        self, line: str, *, cursor: int | None = None
    ) -> _CommandCompletion:
        """
        Describe candidates and the token-prefix span ending at a clamped input cursor.

        See :meth:`LiuXin_alpha.surfaces.terminal.browser_components.completion.CompletionMixin.command_completion_candidates`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host.command_completion_candidates(line, cursor=cursor)  # doctest: +SKIP


        :param line: Input text to inspect without executing or replacing it.
        :param cursor: Character offset clamped into the line; ``None`` selects its end.
        :return: Replacement start/end, original prefix, and ordered candidate tuple; suffix text is excluded.
        """
        ...

    @abstractmethod
    def _all_tables(self) -> list[str]:
        """
        Sort stringified database-view table names without suppressing enumeration or conversion errors.

        See :meth:`LiuXin_alpha.surfaces.terminal.browser_components.catalog.CatalogMixin._all_tables`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._all_tables()  # doctest: +SKIP


        :return: Lexically sorted table-name list; duplicate advertised names are retained.
        """
        ...

    @abstractmethod
    def _resolve_table_token(self, token: str) -> str:
        """
        Resolve a lowercase token by exact table name, tag/label alias, common alias, then ordered spelling guesses.

        See :meth:`LiuXin_alpha.surfaces.terminal.browser_components.catalog.CatalogMixin._resolve_table_token`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._resolve_table_token(token)  # doctest: +SKIP


        :param token: User table token stringified, stripped, and lowercased before lookup.
        :return: Existing exact or alias/spelling target, without modifying browser selection.
        :raises ValueError: If normalization is empty or no candidate names an advertised table.
        """
        ...

    @abstractmethod
    def _resolve_table(self, raw: str | None) -> str:
        """
        Validate the current table for None input or resolve an explicit token through tolerant aliases.

        See :meth:`LiuXin_alpha.surfaces.terminal.browser_components.catalog.CatalogMixin._resolve_table`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._resolve_table(raw)  # doctest: +SKIP


        :param raw: Explicit table token or None for the existing current_table value.
        :return: Validated/resolved table name without assigning current_table or resetting a window.
        :raises ValueError: If no current table exists or the selected/requested table is invalid.
        """
        ...

    @abstractmethod
    def _row_count(self, table: str) -> int:
        """
        Read and integer-convert a database count, returning -1 on any ordinary failure.

        See :meth:`LiuXin_alpha.surfaces.terminal.browser_components.catalog.CatalogMixin._row_count`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._row_count(table)  # doctest: +SKIP


        :param table: Resolved table name forwarded to get_record_count.
        :return: Converted count, or -1 when lookup/call/conversion raises Exception.
        """
        ...

    @abstractmethod
    def _format_row(self, table: str, row: RowRecord) -> str:
        """
        Prefer table-specific preview fields, then heuristic schema fields, and finally a full row representation.

        See :meth:`LiuXin_alpha.surfaces.terminal.browser_components.rows.RowsMixin._format_row`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._format_row(table, row)  # doctest: +SKIP


        :param table: Table selecting preferred columns and schema-driven fallback behavior.
        :param row: Record to inspect without mutating it or reading linked records.
        :return: ID/preview parts joined by vertical bars, or full present-column representations when no preview is usable.
        """
        ...

    @abstractmethod
    def _table_slice(self, table: str, *, limit: int, offset: int) -> list[CoreRow]:
        """
        Consume the database row iterator through an offset and collect a caller-bounded page in backend order.

        See :meth:`LiuXin_alpha.surfaces.terminal.browser_components.catalog.CatalogMixin._table_slice`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._table_slice(table, limit=limit, offset=offset)  # doctest: +SKIP


        :param table: Resolved table requested through the database view's all-rows iterator.
        :param limit: Raw maximum selected count; callers should supply a positive value.
        :param offset: Raw number of enumerated rows to skip before collecting results.
        :return: Collected row references in iteration order, possibly shorter than the requested page.
        """
        ...
