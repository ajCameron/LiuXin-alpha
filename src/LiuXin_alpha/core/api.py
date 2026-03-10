"""Core runtime API contract."""

from __future__ import annotations

import abc

from typing import Any, Callable

from LiuXin_alpha.core.commands import CoreCommand, CoreCommandResult
from LiuXin_alpha.core.events import CoreEvent
from LiuXin_alpha.core.queries import CoreQuery, CoreQueryResult


class CoreAPI(abc.ABC):
    """Contract for core runtime implementations."""

    @property
    @abc.abstractmethod
    def core_uuid(self) -> str:
        """Unique identifier of the running core instance."""

    @property
    @abc.abstractmethod
    def core_version(self) -> str:
        """Version string advertised by the running core instance."""

    @abc.abstractmethod
    def execute_command(self, command: CoreCommand) -> CoreCommandResult:
        """Execute a write-path command envelope."""

    @abc.abstractmethod
    def execute_query(self, query: CoreQuery) -> CoreQueryResult:
        """Execute a read-path query envelope."""

    @abc.abstractmethod
    def subscribe(self, callback: Callable[[CoreEvent], None]) -> Callable[[], None]:
        """Register an event subscriber and return an unsubscribe function."""

    @abc.abstractmethod
    def shutdown(self) -> int:
        """Perform runtime shutdown and return process-style exit code."""

