"""Borrow Core through a reader that refuses commands and effectful queries."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol

from LiuXin_alpha.core.api import CoreAPI, CoreClientAPI
from LiuXin_alpha.core.authorization import CoreAuthorizationError, authorize_endpoint
from LiuXin_alpha.core.commands import CoreCommand, CoreCommandResult
from LiuXin_alpha.core.description import CoreEndpointDescription
from LiuXin_alpha.core.events import CoreEvent
from LiuXin_alpha.core.proxies.local import LocalCoreClient
from LiuXin_alpha.core.queries import CoreQuery, CoreQueryResult
from LiuXin_alpha.core.runtime import CoreRuntime


class CoreReadClientAPI(Protocol):
    """Expose only named queries to a read-only surface.

    Example:
        >>> reader: CoreReadClientAPI  # An annotation never opens a runtime.
    """

    def query(
        self,
        name: str,
        payload: dict[str, Any] | None = None,
        *,
        query_id: str | None = None,
        correlation_id: str | None = None,
    ) -> Any:
        """Read one declared endpoint; names do not themselves prove permission.

        Example:
            >>> reader.query("schema.tables")  # doctest: +SKIP
        """
        ...


class ReadOnlyCoreClient(CoreAPI):
    """Restrict a borrowed client without transferring its lifecycle.

    Local runtimes authorize the selected handler atomically. Remote calls check
    the server's declaration as a client guard and still require server policy;
    a read-only surface must connect to a read-only deployment role.

    Example:
        >>> from LiuXin_alpha.core import core_client
        >>> reader = ReadOnlyCoreClient(core_client(endpoint="http://127.0.0.1:8080"))
    """

    def __init__(self, client: CoreClientAPI) -> None:
        """Retain the borrowed client without I/O.

        Example:
            >>> reader = ReadOnlyCoreClient(client)  # doctest: +SKIP
        """
        self._client = client

    @property
    def core_uuid(self) -> str:
        """Read the borrowed instance identity.

        Example:
            >>> reader.core_uuid  # doctest: +SKIP
        """
        return self._client.core_uuid

    @property
    def core_version(self) -> str:
        """Read the borrowed implementation version.

        Example:
            >>> reader.core_version  # doctest: +SKIP
        """
        return self._client.core_version

    @property
    def api_version(self) -> str:
        """Read the borrowed API version.

        Example:
            >>> reader.api_version  # doctest: +SKIP
        """
        return self._client.api_version

    def execute_command(self, command: CoreCommand) -> CoreCommandResult:
        """Refuse every command before delegating.

        Example:
            >>> reader.execute_command(command)  # doctest: +SKIP
        """
        raise CoreAuthorizationError("Read-only clients cannot execute commands.")

    def execute_query(self, query: CoreQuery) -> CoreQueryResult:
        """Allow only declared read authority, including when using a local runtime.

        Example:
            >>> reader.execute_query(CoreQuery("health"))  # doctest: +SKIP
        """
        client = self._client
        runtime = client._runtime if isinstance(client, LocalCoreClient) else client
        if isinstance(runtime, CoreRuntime):
            return runtime.execute_query(query, capabilities=frozenset({"read"}))
        descriptions = client.describe_api(include_targets=False).get("queries", ())
        name = query.name.strip().lower()
        description = next(
            (item for item in descriptions if item["name"] == name), None
        )
        declaration = None
        if description is not None:
            capabilities = description.get("remote_capabilities")
            declaration = CoreEndpointDescription(
                name=name,
                kind="query",
                remote_capabilities=None
                if capabilities is None
                else tuple(capabilities),
            )
        authorize_endpoint(declaration, frozenset({"read"}), query.payload)
        return client.execute_query(query)

    def describe_api(
        self,
        *,
        include_targets: bool = False,
        target: str | None = None,
    ) -> dict[str, Any]:
        """Describe named endpoints without exposing generic invocation targets.

        Example:
            >>> reader.describe_api(include_targets=False)  # doctest: +SKIP
        """
        if include_targets or target is not None:
            raise CoreAuthorizationError(
                "Read-only clients cannot introspect invocation targets."
            )
        return dict(self.query("api.describe", {"include_targets": False}))

    def subscribe(self, callback: Callable[[CoreEvent], None]) -> Callable[[], None]:
        """Refuse global events, which can contain privileged operation details.

        Example:
            >>> reader.subscribe(print)  # doctest: +SKIP
        """
        raise CoreAuthorizationError(
            "Read-only clients cannot subscribe to global events."
        )

    def shutdown(self) -> int:
        """Refuse server lifecycle mutation; the owner closes its own session.

        Example:
            >>> reader.shutdown()  # doctest: +SKIP
        """
        raise CoreAuthorizationError("Read-only clients cannot shut down Core.")
