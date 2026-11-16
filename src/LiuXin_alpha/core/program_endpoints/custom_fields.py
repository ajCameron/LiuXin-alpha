"""Register the native structured custom-field API with explicit authority."""

from LiuXin_alpha.core.program_endpoints.common import ProgramEndpointRegistrar, field
from LiuXin_alpha.core.program_endpoints.handlers import NativeFieldHandlers

QUERY_FIELDS = {
    "definitions": ("attachment", "include_retired"),
    "value": ("owner", "field"),
    "values": ("owners", "fields"),
    "owner": ("owner",),
    "inventory": ("field", "limit", "after_id", "unused"),
    "usage": ("field", "value"),
    "find": ("attachment", "predicates", "limit", "after_id"),
    "next-series": ("field", "series"),
    "audit": (),
    "plan": (
        "kind",
        "field",
        "changes",
        "owner_mapping",
        "value_mapping",
        "reduce",
        "collapse",
        "actions",
    ),
}
COMMAND_FIELDS = {
    "create": (
        "attachment",
        "label",
        "name",
        "datatype",
        "mode",
        "editable",
        "display",
    ),
    "patch": ("patches",),
    "entries": ("owner", "field", "add", "update", "remove", "order"),
    "rename-value": ("field", "value_id", "value"),
    "delete-value": ("field", "value_id"),
    "retire": ("field",),
    "apply": (*QUERY_FIELDS["plan"], "fingerprint"),
}


def install_queries(
    api: NativeFieldHandlers, runtime: ProgramEndpointRegistrar
) -> None:
    for operation, keys in QUERY_FIELDS.items():
        runtime.register_query_handler(
            "custom-fields.native." + operation,
            api.native_fields_query,
            summary="Native custom fields: " + operation,
            payload_fields=tuple(field(k) for k in keys),
            tags=("custom-fields", "read"),
            remote_capabilities=("admin",)
            if operation in {"audit", "plan"}
            else ("read",),
        )


def install_commands(
    api: NativeFieldHandlers, runtime: ProgramEndpointRegistrar
) -> None:
    for operation, keys in COMMAND_FIELDS.items():
        runtime.register_command_handler(
            "custom-fields.native." + operation,
            api.native_fields_command,
            summary="Native custom fields: " + operation,
            payload_fields=tuple(field(k) for k in keys),
            tags=("custom-fields", "write"),
            remote_capabilities=("admin",)
            if operation in {"create", "retire", "apply"}
            else ("execute",),
        )
