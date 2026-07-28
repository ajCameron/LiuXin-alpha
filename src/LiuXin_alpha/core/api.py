"""Core runtime API contract."""

from __future__ import annotations

import abc

from typing import Any, Callable, Mapping, Protocol, runtime_checkable

from LiuXin_alpha.core.commands import CoreCommand, CoreCommandResult
from LiuXin_alpha.core.events import CoreEvent
from LiuXin_alpha.core.queries import CoreQuery, CoreQueryResult


@runtime_checkable
class CoreClientAPI(Protocol):
    """One client contract implemented by direct and RPC Core access."""

    @property
    def core_uuid(self) -> str: ...

    @property
    def core_version(self) -> str: ...

    @property
    def api_version(self) -> str: ...

    def execute_command(self, command: CoreCommand) -> CoreCommandResult: ...

    def execute_query(self, query: CoreQuery) -> CoreQueryResult: ...

    def command(
        self,
        name: str,
        payload: Mapping[str, Any] | None = None,
        *,
        command_id: str | None = None,
        correlation_id: str | None = None,
    ) -> Any: ...

    def query(
        self,
        name: str,
        payload: Mapping[str, Any] | None = None,
        *,
        query_id: str | None = None,
        correlation_id: str | None = None,
    ) -> Any: ...

    def health(self) -> Mapping[str, Any]: ...

    def describe_api(
        self,
        *,
        include_targets: bool = True,
        target: str | None = None,
    ) -> Mapping[str, Any]: ...

    def subscribe(
        self,
        callback: Callable[[CoreEvent], None],
    ) -> Callable[[], None]: ...

    def shutdown(self) -> int: ...


class CoreAPI(abc.ABC):
    """Contract shared by the in-process runtime and transport clients."""

    @property
    @abc.abstractmethod
    def core_uuid(self) -> str:
        """Unique identifier of the running core instance."""

    @property
    @abc.abstractmethod
    def core_version(self) -> str:
        """Version string advertised by the running core instance."""

    @property
    @abc.abstractmethod
    def api_version(self) -> str:
        """Version of the stable command/query contract."""

    @abc.abstractmethod
    def execute_command(self, command: CoreCommand) -> CoreCommandResult:
        """Execute a write-path command envelope."""

    @abc.abstractmethod
    def execute_query(self, query: CoreQuery) -> CoreQueryResult:
        """Execute a read-path query envelope."""

    @abc.abstractmethod
    def describe_api(
        self,
        *,
        include_targets: bool = True,
        target: str | None = None,
    ) -> dict[str, Any]:
        """Return an inspectable description of the core API surface."""

    @abc.abstractmethod
    def subscribe(self, callback: Callable[[CoreEvent], None]) -> Callable[[], None]:
        """Register an event subscriber and return an unsubscribe function."""

    @abc.abstractmethod
    def shutdown(self) -> int:
        """Perform runtime shutdown and return process-style exit code."""

    def command(
        self,
        name: str,
        payload: Mapping[str, Any] | None = None,
        *,
        command_id: str | None = None,
        correlation_id: str | None = None,
    ) -> Any:
        """Execute a named command and return its transport-safe result."""

        kwargs: dict[str, Any] = {
            "name": str(name),
            "payload": dict(payload or {}),
            "correlation_id": correlation_id,
        }
        if command_id is not None:
            kwargs["command_id"] = str(command_id)
        return self.execute_command(CoreCommand(**kwargs)).result

    def query(
        self,
        name: str,
        payload: Mapping[str, Any] | None = None,
        *,
        query_id: str | None = None,
        correlation_id: str | None = None,
    ) -> Any:
        """Execute a named query and return its transport-safe result."""

        kwargs: dict[str, Any] = {
            "name": str(name),
            "payload": dict(payload or {}),
            "correlation_id": correlation_id,
        }
        if query_id is not None:
            kwargs["query_id"] = str(query_id)
        return self.execute_query(CoreQuery(**kwargs)).result

    def health(self) -> Mapping[str, Any]:
        """Return the named Core health result."""

        result = self.query("health")
        if not isinstance(result, Mapping):
            raise TypeError("Core health result must be a mapping.")
        return result


__all__ = [
    "CoreAPI",
    "CoreClientAPI",
]
