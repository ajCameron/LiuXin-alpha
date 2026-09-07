"""Public Core-backed terminal browser composition and compatibility types.

Command, session, completion, and browsing behavior live in browser_components.
Application startup selects the UI; this owner never imports the curses adapter.
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
    """Core-backed command shell with browsing, mutation, and lifecycle extensions."""

    prompt = "liuxin-db> "

    @property
    def _extension_host(self) -> TextDatabaseBrowser:
        """Supply the concrete browser to typed command and lifecycle extensions."""
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
