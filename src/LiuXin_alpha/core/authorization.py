"""Declare endpoint authority separately from serialization and caller payloads."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from LiuXin_alpha.core.description import CoreEndpointDescription
from LiuXin_alpha.core.errors import CoreError

KNOWN_CAPABILITIES = frozenset({"read", "write", "admin", "execute", "invoke"})


class CoreAuthorizationError(CoreError):
    """Refuse an operation before handler execution or lifecycle events.

    Example:
        >>> CoreAuthorizationError("Permission required").code
        'permission_denied'
    """

    code = "permission_denied"


@dataclass(frozen=True)
class CoreTransportPolicy:
    """Bind one deployment's authority to server configuration.

    An anonymous deployment grants only declared reads. Any broader grant needs
    a bearer token. The token never comes from a request's claimed permissions.
    Tokens authenticate this deployment role, not individual human identities.

    Example:
        >>> CoreTransportPolicy().capabilities
        frozenset({'read'})
    """

    capabilities: frozenset[str] = frozenset({"read"})
    bearer_token: str | None = field(default=None, repr=False, compare=False)
    allowed_hosts: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Freeze and validate grants without opening a connection.

        Example:
            >>> CoreTransportPolicy(capabilities=frozenset({"unknown"}))
            Traceback (most recent call last):
            ...
            ValueError: Unknown transport capability.
        """
        capabilities = frozenset(self.capabilities)
        if not capabilities <= KNOWN_CAPABILITIES:
            raise ValueError("Unknown transport capability.")
        if capabilities - {"read"} and not self.bearer_token:
            raise ValueError(
                "Writable or privileged transport requires a bearer token."
            )
        if self.bearer_token is not None and (
            not self.bearer_token
            or not self.bearer_token.isascii()
            or any(
                char.isspace() or not 33 <= ord(char) <= 126
                for char in self.bearer_token
            )
        ):
            raise ValueError(
                "Bearer token must be nonempty printable ASCII without whitespace."
            )
        object.__setattr__(self, "capabilities", capabilities)
        object.__setattr__(
            self, "allowed_hosts", tuple(host.lower() for host in self.allowed_hosts)
        )

    @classmethod
    def for_access(
        cls,
        access: str,
        bearer_token: str | None = None,
        *,
        allowed_hosts: tuple[str, ...] = (),
    ) -> CoreTransportPolicy:
        """Resolve a deployment role in Core without duplicating grants in callers.

        Example:
            >>> CoreTransportPolicy.for_access("read-only").capabilities
            frozenset({'read'})
        """
        roles = {
            "read-only": frozenset({"read"}),
            "read-write": frozenset({"read", "write"}),
            "trusted": KNOWN_CAPABILITIES,
        }
        try:
            capabilities = roles[access]
        except KeyError:
            raise ValueError("Unknown Core deployment role.") from None
        return cls(capabilities, bearer_token, allowed_hosts)

    @classmethod
    def read_write(cls, bearer_token: str) -> CoreTransportPolicy:
        """Grant declared catalogue reads and writes to one authenticated role.

        Example:
            >>> "admin" in CoreTransportPolicy.read_write("example-token").capabilities
            False
        """
        return cls.for_access("read-write", bearer_token)

    @classmethod
    def trusted(cls, bearer_token: str) -> CoreTransportPolicy:
        """Explicitly grant administration, execution and generic invocation.

        Example:
            >>> "invoke" in CoreTransportPolicy.trusted("example-token").capabilities
            True
        """
        return cls.for_access("trusted", bearer_token)


def authorize_endpoint(
    description: CoreEndpointDescription | None,
    capabilities: frozenset[str],
    payload: Mapping[str, Any],
) -> None:
    """Check the selected registration before calling its handler.

    Missing declarations fail closed. A query spelling never establishes
    read-only behaviour. Refresh switches and dynamic target introspection need
    additional authority even when the ordinary query is a permitted read.

    Example:
        >>> authorize_endpoint(None, frozenset({"read"}), {})
        Traceback (most recent call last):
        ...
        LiuXin_alpha.core.authorization.CoreAuthorizationError: Endpoint is not exposed remotely.
    """
    if description is None or description.remote_capabilities is None:
        raise CoreAuthorizationError("Endpoint is not exposed remotely.")
    if description.kind == "command" and not capabilities - {"read"}:
        raise CoreAuthorizationError("Read-only transport cannot execute commands.")
    required = frozenset(description.remote_capabilities)
    if not required <= capabilities:
        raise CoreAuthorizationError("Endpoint requires additional authority.")
    if description.kind == "query" and any(
        payload.get(key) for key in ("refresh", "force_refresh")
    ):
        if "admin" not in capabilities:
            raise CoreAuthorizationError(
                "Refreshing state requires administrative authority."
            )
    if description.name == "api.describe" and (
        payload.get("include_targets", False) or payload.get("target")
    ):
        if "invoke" not in capabilities:
            raise CoreAuthorizationError(
                "Dynamic target descriptions require invocation authority."
            )


def read_bearer_token_file(path: str | Path | None) -> str | None:
    """Read a caller-selected private token file without putting its contents in errors.

    Example:
        >>> read_bearer_token_file(None) is None
        True
    """
    if path is None:
        return None
    try:
        token = Path(path).read_text(encoding="ascii").strip()
    except UnicodeError:
        raise ValueError("Bearer token file must contain ASCII text.") from None
    CoreTransportPolicy(bearer_token=token)
    return token
