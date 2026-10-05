"""
Compose the public Core-backed terminal browser and retain historical model/helper aliases.

Command, session, completion, and browsing behavior live in browser_components.
Application startup selects the UI; this owner never imports the curses adapter.
Construction initializes the owners' shared state and registers default extensions;
session execution and cleanup remain with the session mixin. Legacy database inputs
are enclosed at the shared surface/Core boundary rather than retained as a raw backend.
"""

from __future__ import annotations

import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any, TextIO

from LiuXin_alpha.core import CoreClientAPI
from LiuXin_alpha.surfaces.core import (
    CoreDatabaseView,
    CoreSurfaceModel,
    coerce_surface_core,
)
from LiuXin_alpha.surfaces.terminal.commands import (
    TerminalCommandAPI,
    build_default_commands,
)
from LiuXin_alpha.surfaces.terminal.plugins import TerminalLifecyclePluginAPI
from LiuXin_alpha.utils.jobs import default_job_manager
from LiuXin_alpha.utils.jobs.manager import JobManagerAPI

from .browser_components.browsing import BrowsingMixin
from .browser_components.catalog import CatalogMixin
from .browser_components.completion import CompletionMixin
from .browser_components.completion_sources import CompletionSourcesMixin
from .browser_components.help import HelpMixin
from .browser_components.host import HostMixin
from .browser_components.legacy import LegacyMixin
from .browser_components.models import (
    BrowseWindow,
    CommandCompletion,
    default_history_file_path,
)
from .browser_components.registry import RegistryMixin
from .browser_components.rows import RowsMixin
from .browser_components.session import SessionMixin

_CommandCompletion = CommandCompletion
_BrowseWindow = BrowseWindow
_default_history_file_path = default_history_file_path


class TextDatabaseBrowser(
    SessionMixin["TextDatabaseBrowser"],
    RegistryMixin["TextDatabaseBrowser"],
    LegacyMixin["TextDatabaseBrowser"],
    HelpMixin["TextDatabaseBrowser"],
    CompletionSourcesMixin["TextDatabaseBrowser"],
    CompletionMixin["TextDatabaseBrowser"],
    CatalogMixin["TextDatabaseBrowser"],
    RowsMixin["TextDatabaseBrowser"],
    HostMixin["TextDatabaseBrowser"],
    BrowsingMixin["TextDatabaseBrowser"],
):
    """
    Combine command/session, completion, schema/row, output, and browsing owners into one Core-backed shell.

    Construction prepares the shell without starting its command loop or lifecycle
    plugins. Commands and plugins receive this concrete instance through _extension_host.
    Existing Core clients are borrowed; legacy database inputs may bootstrap an owned
    compatibility session whose eventual cleanup belongs to the session owner.

    Example:
        >>> from io import StringIO
        >>> from unittest.mock import Mock
        >>> shell = TextDatabaseBrowser(Mock(spec=CoreClientAPI), input=StringIO(), output=StringIO(), job_manager=Mock())
        >>> shell.page_size, shell.current_table
        (20, None)
    """

    prompt = "liuxin-db> "

    @property
    def _extension_host(self) -> TextDatabaseBrowser:
        """
        Expose this exact browser instance to typed commands and lifecycle plugins without wrapping it.

        Example:
            >>> shell = object.__new__(TextDatabaseBrowser)
            >>> shell._extension_host is shell
            True


        :return: Self, preserving the concrete browser's extension capabilities and identity.
        """
        return self

    def __init__(
        self,
        core: CoreClientAPI | Any,
        *,
        page_size: int = 20,
        input: TextIO | None = None,
        output: TextIO | None = None,
        history_file: str | Path | None = None,
        lifecycle_plugins: Sequence[TerminalLifecyclePluginAPI[TextDatabaseBrowser]]
        | None = None,
        job_manager: JobManagerAPI | None = None,
        metadata_read_source: Any = None,
    ) -> None:
        """
        Coerce the Core input, initialize browser state, and register default commands and optional lifecycle plugins.

        Legacy coercion precedes page/history validation and may bootstrap Core.
        Later initialization/registration errors propagate without constructor-level
        rollback of that session. No command loop, history loading, or plugin startup
        runs here. Metadata-read-source and stream fallbacks use truthiness, while
        a supplied job manager is selected by its being non-None.

        Example:
            >>> from io import StringIO
            >>> from unittest.mock import Mock
            >>> shell = TextDatabaseBrowser(Mock(spec=CoreClientAPI), page_size=0, input=StringIO(), output=StringIO(), job_manager=Mock())
            >>> shell.page_size, shell._started, shell._closed
            (1, False, False)


        :param core: Borrowed CoreClientAPI or legacy database object enclosed by coerce_surface_core.
        :param page_size: Initial page length integer-converted and clamped to at least one, without an upper limit.
        :param input: Input stream, with a missing/falsey value selecting current sys.stdin.
        :param output: Output stream, with a missing/falsey value selecting current sys.stdout.
        :param history_file: Path expanded for a leading user marker, or None for the default history path; an explicit empty string becomes Path('.').
        :param lifecycle_plugins: Optional sequence registered in input order after all default commands.
        :param job_manager: Optional manager passed through legacy coercion and retained locally; None selects default_job_manager afterward.
        :param metadata_read_source: Compatibility read source forwarded to coercion and retained when truthy, otherwise replaced locally by the CoreSurfaceModel.
        :return: None after fully initializing the shell; construction does not itself run or shut down a session.
        """
        core, compatibility_session = coerce_surface_core(
            core,
            job_manager=job_manager,
            read_source=metadata_read_source,
        )
        self._compatibility_core_session = compatibility_session
        self.core = core
        self.model = CoreSurfaceModel(core)
        self.db = CoreDatabaseView(core, model=self.model)
        self.metadata_read_source = metadata_read_source or self.model
        self.page_size = max(1, int(page_size))
        self.input = input or sys.stdin
        self.output = output or sys.stdout
        self.job_manager = (
            job_manager if job_manager is not None else default_job_manager()
        )
        self.current_table: str | None = None
        self.window: _BrowseWindow | None = None
        self._commands: dict[str, TerminalCommandAPI[TextDatabaseBrowser]] = {}
        self._group_alias_to_group: dict[str, str] = {}
        self._command_groups: dict[
            str, dict[str, TerminalCommandAPI[TextDatabaseBrowser]]
        ] = {}
        self._lifecycle_plugins: list[
            TerminalLifecyclePluginAPI[TextDatabaseBrowser]
        ] = []
        self._started = False
        self._closed = False
        self._shutdown_reason: str | None = None
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


__all__ = ["TextDatabaseBrowser"]
