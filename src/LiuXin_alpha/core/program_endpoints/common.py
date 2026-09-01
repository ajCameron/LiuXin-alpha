"""Shared contracts for declarative Core program endpoint registration."""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol

from LiuXin_alpha.core.description import CorePayloadFieldDescription

type ProgramEndpointHandler = Callable[..., object]
type RegisterEndpoint = Callable[..., None]


class ProgramEndpointHandlers(Protocol):
    """Dynamic handler surface consumed only while registering endpoints."""

    def __getattr__(self, name: str) -> ProgramEndpointHandler: ...


class ProgramEndpointRegistrar(Protocol):
    """Minimal runtime registration surface used by endpoint providers."""

    register_query_handler: RegisterEndpoint
    register_command_handler: RegisterEndpoint


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
