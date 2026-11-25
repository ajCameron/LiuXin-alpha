"""Move potentially long application operations onto the existing durable executor."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from LiuXin_alpha.core.commands import CoreCommand


def handlers() -> dict[str, Any]:
    """Return explicit adapters; captured requests can never select arbitrary imports."""
    from LiuXin_alpha.core.program_services import (
        database,
        storage_evacuation,
        storage_integrity,
        storage_recovery,
        storage_recreation,
        storage_repair,
        stores,
    )

    return {
        "database.backup": database.database_backup,
        "database.vacuum": database.database_vacuum,
        "database.migrations.apply": database.database_migrations_apply,
        "storage.file.copy": storage_recreation.restore,
        "storage.asset.restore": storage_recreation.restore,
        "storage.asset.recreate": storage_recreation.recreate,
        "storage.source.register": stores.storage_source_register,
        "storage.replica.verify": storage_integrity.storage_replica_verify,
        "storage.asset.verify": storage_integrity.storage_asset_verify,
        "storage.reconcile.apply": storage_integrity.storage_reconcile_apply,
        "storage.repair.apply": storage_repair.storage_repair_apply,
        "storage.store.evacuate.apply": storage_evacuation.storage_store_evacuate_apply,
        "storage.recovery.recover-pending": storage_recovery.storage_recovery_recover_pending,
        "storage.recovery.retry-ingest": storage_recovery.storage_recovery_retry_ingest,
    }


def submit(runtime: Any, command: CoreCommand) -> dict[str, Any]:
    """Capture connection and operation arguments before accepting long-running work."""
    from LiuXin_alpha.core.program_services.payloads import (
        _database_connection,
        _job_submit,
    )

    payload = dict(command.payload)
    if command.name == "storage.asset.verify":
        from LiuXin_alpha.core.program_services.storage_integrity import (
            asset_verification_options,
        )

        asset_verification_options(payload)
    options = {
        key: value
        for key, value in payload.items()
        if key not in {"job_timeout_s", "job_backend", "job_no_output", "label"}
    }
    return _job_submit(
        runtime,
        payload,
        function_name="run_application_operation_job",
        kwargs={
            **_database_connection(runtime),
            "operation": command.name,
            "payload": options,
        },
        default_label=command.name,
    )


def install(runtime: Any) -> None:
    """Retain public route names while advertising durable submission receipts."""
    from LiuXin_alpha.core.description import CorePayloadFieldDescription as Field
    from LiuXin_alpha.core.program_services import storage_recreation

    runtime.register_query_handler(
        "storage.asset.recreation.plan",
        storage_recreation.plan,
        payload_fields=(Field("asset_id", required=True, field_type="integer"),),
        remote_capabilities=("read",),
    )
    for name, function in (
        ("restore", storage_recreation.restore),
        ("recreate", storage_recreation.recreate),
    ):
        fields: tuple[Field, ...] = (
            Field("asset_id", required=True, field_type="integer"),
            Field("store", field_type="string|integer|null"),
        )
        if name == "recreate":
            fields += (Field("expected_state", required=True, field_type="string"),)
        runtime.register_command_handler(
            "storage.asset." + name,
            function,
            payload_fields=fields,
            remote_capabilities=("admin", "execute")
            if name == "recreate"
            else ("admin",),
        )
    for name in handlers():
        registration = runtime._command_handlers[name]
        runtime._command_handlers[name] = replace(
            registration,
            handler=submit,
            description=replace(
                registration.description,
                recovery="submission",
                summary="Queue durable execution. " + registration.description.summary,
            ),
            capability_check=lambda owner: (
                None
                if getattr(owner, "_durable_jobs", None) is not None
                else "Requires a configured durable jobs ledger"
            ),
        )


def run(
    *,
    database_path: str,
    db_type: str,
    operation: str,
    payload: dict[str, Any],
    database_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Execute the selected domain adapter with one independently owned graph."""
    from LiuXin_alpha.core.runtime import CoreRuntime
    from LiuXin_alpha.core.workflow_context import checkpoint
    from LiuXin_alpha.core.workflow_jobs import _open_library_services

    selected = handlers().get(operation)
    if selected is None:
        raise ValueError("Unknown durable application operation")
    with _open_library_services(
        database_path=database_path,
        db_type=db_type,
        database_metadata=database_metadata,
    ) as services:
        with CoreRuntime(library=services.library) as runtime:
            checkpoint()
            return selected(runtime, CoreCommand(operation, payload))
