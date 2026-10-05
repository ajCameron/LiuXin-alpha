"""
Define the minimal program-endpoint registrar and build declarative payload-field metadata.

Command/query handlers take a runtime plus their respective envelope and may
return any object. Metadata describes the public contract; this module does not
validate request values, invoke handlers, or enforce read-only query behavior.
"""

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
    """
    Describe the registration methods endpoint providers require from a Core runtime.

    This structural typing contract does not prescribe registry storage, locking,
    duplicate-name handling, or error wrapping. CoreRuntime supplies the maintained
    implementation; the protocol is not decorated for runtime isinstance checks.

    Example:
        >>> from LiuXin_alpha.core.program_endpoints import install_program_endpoints
        >>> install_program_endpoints(api, runtime)  # doctest: +SKIP
    """

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
        """
        Register a runtime-and-query callable with the metadata advertised to API clients.

        Payload fields are descriptive, not an executable validation schema. The
        protocol alone cannot prevent side effects in a registered query handler.

        Example:
            >>> runtime.register_query_handler("preferences.list", handler, summary="List preferences")  # doctest: +SKIP


        :param name: Query operation name to expose through the registrar.
        :param handler: Callable receiving the executing runtime and a CoreQuery envelope.
        :param summary: Optional short description; None requests the registrar's fallback policy.
        :param description: Additional operation contract text for introspection.
        :param payload_fields: Optional ordered payload-field declarations; not request-validation rules.
        :param tags: Optional operation grouping labels for introspection.
        :param transport_stable: Whether to advertise a stable transport-facing result contract.
        :return: None after the implementation has registered the query or raised an error.
        """
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
        """
        Register a runtime-and-command callable with the metadata advertised to API clients.

        Registration does not execute the command or guarantee transactional
        behavior. Metadata and request validation remain separate concerns.

        Example:
            >>> runtime.register_command_handler("preferences.set", handler, summary="Set a preference")  # doctest: +SKIP


        :param name: Command operation name to expose through the registrar.
        :param handler: Callable receiving the executing runtime and a CoreCommand envelope.
        :param summary: Optional short description; None requests the registrar's fallback policy.
        :param description: Additional operation contract text for introspection.
        :param payload_fields: Optional ordered payload-field declarations; not request-validation rules.
        :param tags: Optional operation grouping labels for introspection.
        :param transport_stable: Whether to advertise a stable transport-facing result contract.
        :return: None after the implementation has registered the command or raised an error.
        """
        ...


def field(
    name: str,
    *,
    required: bool = False,
    field_type: str | None = None,
    description: str = "",
) -> CorePayloadFieldDescription:
    """
    Package one payload field's name, required flag, type label, and prose without interpreting them.

    The constructor retains the supplied values; neither names nor type labels
    become request-validation rules at this helper boundary.

    Example:
        >>> declaration = field("key", required=True, field_type="string", description="Preference key.")
        >>> declaration.name, declaration.required, declaration.field_type
        ('key', True, 'string')


    :param name: Public payload key described by the declaration.
    :param required: Whether introspection should advertise the field as mandatory.
    :param field_type: Optional transport-facing type label, not a Python type or validator.
    :param description: Human-readable explanation of the field's meaning or constraints.
    :return: New CorePayloadFieldDescription with the supplied metadata.
    """

    return CorePayloadFieldDescription(
        name=name,
        required=required,
        field_type=field_type,
        description=description,
    )
