"""Shared contracts for declarative Core program endpoint registration."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Protocol

from LiuXin_alpha.core.commands import CoreCommand
from LiuXin_alpha.core.description import CorePayloadFieldDescription
from LiuXin_alpha.core.queries import CoreQuery

if TYPE_CHECKING:
    from LiuXin_alpha.core.runtime import CoreRuntime

type ProgramCommandHandler = Callable[[CoreRuntime, CoreCommand], object]
type ProgramQueryHandler = Callable[[CoreRuntime, CoreQuery], object]


class ProgramEndpointRegistrar(Protocol):
    """Minimal runtime registration surface used by endpoint providers."""

    def register_query_handler(
        self,
        name: str,
        handler: ProgramQueryHandler,
        *,
        summary: str | None = None,
        description: str = "",
        payload_fields: tuple[CorePayloadFieldDescription, ...]
        | list[CorePayloadFieldDescription]
        | None = None,
        tags: tuple[str, ...] | list[str] | None = None,
        transport_stable: bool = True,
    ) -> None:
        """Register a query handler and its transport description."""
        ...

    def register_command_handler(
        self,
        name: str,
        handler: ProgramCommandHandler,
        *,
        summary: str | None = None,
        description: str = "",
        payload_fields: tuple[CorePayloadFieldDescription, ...]
        | list[CorePayloadFieldDescription]
        | None = None,
        tags: tuple[str, ...] | list[str] | None = None,
        transport_stable: bool = True,
    ) -> None:
        """Register a command handler and its transport description."""
        ...


def field(
    name: str,
    *,
    required: bool = False,
    field_type: str | None = None,
    description: str = "",
) -> CorePayloadFieldDescription:
    """Build one transport-facing payload-field declaration."""

    return CorePayloadFieldDescription(
        name=name,
        required=required,
        field_type=field_type,
        description=description,
    )
