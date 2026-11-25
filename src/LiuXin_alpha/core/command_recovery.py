"""Recover native metadata commands without turning short edits into jobs.

The catalogue journal commits with the mutation. Cache state is a return-time
observation and is never reconstructed as a durable claim of cache readiness.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Callable, Iterator
from contextlib import contextmanager, nullcontext
from contextvars import ContextVar
from dataclasses import replace
from typing import Any

from LiuXin_alpha.catalog.api.metadata_results import MetadataReconciliationError
from LiuXin_alpha.core.commands import CoreCommand
from LiuXin_alpha.core.description import CorePayloadFieldDescription
from LiuXin_alpha.core.errors import CoreDispatchError, core_error_details
from LiuXin_alpha.core.program_services.native_metadata.common import copy_input
from LiuXin_alpha.core.wire import to_wire
from LiuXin_alpha.databases import command_journal as journal

_command: ContextVar[Any] = ContextVar("core_active_command", default=None)
_required: ContextVar[Any] = ContextVar("core_active_command_authority", default=None)

_authority: ContextVar[frozenset[str] | None] = ContextVar(
    "command_outcome_authority", default=None
)


@contextmanager
def authority_scope(
    capabilities: frozenset[str] | None,
    command: CoreCommand | None = None,
    required: tuple[str, ...] | None = None,
) -> Iterator[None]:
    """Bind server-established authority for outcome access, restoring nested calls."""
    token = _authority.set(capabilities)
    command_token = _command.set(command)
    required_token = _required.set(required)
    try:
        yield
    finally:
        _required.reset(required_token)
        _command.reset(command_token)
        _authority.reset(token)


def identity(value: Any) -> str:
    """Accept existing caller keys as well as UUIDs without changing their spelling."""
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value) > 200
        or "\x00" in value
    ):
        raise CoreDispatchError(
            "command_id must be a nonblank string of at most 200 characters",
            code="invalid_payload",
        )
    return value


def snapshot(command: CoreCommand) -> CoreCommand:
    """Detach bounded input before subscribers or queued callers can change intent."""
    return replace(
        command,
        command_id=identity(command.command_id),
        payload=copy_input(command.payload),
    )


def fingerprint(command: CoreCommand) -> str:
    """Hash strict canonical JSON without retaining credentials or request payloads."""
    encoded = json.dumps(
        [command.name.strip().lower(), command.payload],
        sort_keys=True,
        ensure_ascii=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return hashlib.sha256(encoded.encode("ascii")).hexdigest()


def _authorize(required: list[str] | None) -> None:
    capabilities = _authority.get()
    if capabilities is not None and (
        required is None or not set(required) <= capabilities
    ):
        raise CoreDispatchError(
            "Original command authority is required to inspect its outcome",
            code="permission_denied",
        )


def execute(
    runtime: Any,
    command: CoreCommand,
    handler: Callable[..., Any],
    required: tuple[str, ...] | None,
) -> Any:
    """Execute once in the metadata owner's transaction, or return its prior result."""
    api = runtime.services.metadata
    database = runtime.catalog.db
    macros = database.macros
    schema = getattr(database.driver, "schema", None)
    digest = fingerprint(command)
    replayed = False

    def apply(bound: Any) -> dict[str, Any]:
        nonlocal replayed
        connection = macros.current_transaction_connection()
        if connection is None:
            raise RuntimeError(
                "Recoverable commands require an active managed transaction"
            )
        journal.install(connection, schema)
        journal.reserve(connection, command.command_id, schema)
        saved = journal.read(connection, command.command_id, schema)
        if saved is not None:
            _authorize(saved["authority"])
            if saved["fingerprint"] != digest:
                raise CoreDispatchError(
                    "This command ID already names different input",
                    code="command_conflict",
                    details={"command_id": command.command_id},
                )
            replayed = True
            return saved["outcome"]
        try:
            # Failed mutations roll back to this savepoint; the outer transaction
            # can retain their deterministic rejection without retaining effects.
            def invoke(nested: Any) -> dict[str, Any]:
                result = to_wire(handler(runtime, command))
                if not isinstance(result, dict) or "persistence" not in result:
                    raise TypeError(
                        "Transactional metadata handlers must return a write receipt"
                    )
                outcome: dict[str, Any] = {
                    "state": "succeeded",
                    "effects": "committed",
                    "result": result,
                }
                # Only the containing SQL commit makes this stored receipt visible.
                outcome["result"] = {
                    **result,
                    "persistence": "committed",
                    "cache": "not_configured"
                    if result["cache"] == "not_configured"
                    else "deferred",
                    "generation": None,
                }
                json.dumps(outcome, allow_nan=False)
                return outcome

            # A failed callback must discard both SQL effects and its metadata
            # change flags before the outer journal stores the rejection.
            outcome = bound.batch(invoke).result
        except Exception as error:
            code, details = core_error_details(error)
            outcome = {
                "state": "failed",
                "effects": "none",
                "result": None,
                "error": {
                    "code": code,
                    "message": str(error),
                    "details": to_wire(details),
                },
            }
        journal.insert(
            connection,
            command.command_id,
            command.name.strip().lower(),
            digest,
            required,
            outcome,
            schema,
        )
        return outcome

    try:
        receipt = api.batch(apply)
    except MetadataReconciliationError as error:
        outcome = error.receipt.result
        if isinstance(outcome, dict) and outcome.get("state") == "succeeded":
            result = {
                **outcome["result"],
                "persistence": error.receipt.persistence,
                "cache": error.receipt.cache,
                "generation": error.receipt.generation,
            }
            raise CoreDispatchError(
                str(error),
                code="metadata_reconciliation_failed",
                details={
                    "receipt": result,
                    "command_id": command.command_id,
                    "canonical_write_committed": error.receipt.persistence
                    == "committed",
                },
            ) from error
        raise
    outcome = receipt.result
    if outcome["state"] == "failed":
        failure = outcome["error"]
        raise CoreDispatchError(
            failure["message"],
            code=failure["code"],
            details={
                **failure["details"],
                "command_id": command.command_id,
                "effects": "none",
                "replayed": replayed,
            },
        )
    result = outcome["result"]
    if result is None:
        raise CoreDispatchError(
            "Command result was pruned; this identity cannot execute again",
            code="command_result_pruned",
        )
    return {
        **result,
        "persistence": receipt.persistence,
        "cache": receipt.cache,
        "generation": receipt.generation,
    }


def observe(
    runtime: Any,
    command: CoreCommand,
    handler: Callable[..., Any],
    required: tuple[str, ...] | None,
) -> Any:
    """Reserve identity for a non-atomic command; never replay uncertain effects.

    This contract protects external/system adapters whose owners cannot join a
    catalogue transaction. A crash after acceptance requires domain inspection.
    Native metadata uses execute instead, coupling mutation and outcome atomically.
    """
    database = runtime.catalog.db
    schema = getattr(database.driver, "schema", None)
    digest = fingerprint(command)
    with database.macros.transaction(
        serialized=True, require_outermost=True
    ) as connection:
        journal.install(connection, schema)
        journal.reserve(connection, command.command_id, schema)
        saved = journal.read(connection, command.command_id, schema)
        if saved is not None:
            _authorize(saved["authority"])
            if saved["fingerprint"] != digest:
                raise CoreDispatchError(
                    "Command ID already names different input", code="command_conflict"
                )
            outcome = saved["outcome"]
            if outcome.get("state") == "succeeded" and not outcome.get("result_pruned"):
                return outcome["result"]
            failure = outcome.get("error", {})
            raise CoreDispatchError(
                failure.get(
                    "message",
                    "Command requires outcome inspection; it cannot execute again",
                ),
                code=failure.get(
                    "code",
                    "command_result_pruned"
                    if outcome.get("result_pruned")
                    else "command_unresolved",
                ),
                details={
                    **failure.get("details", {}),
                    "command_id": command.command_id,
                    "effects": outcome.get("effects", "unknown"),
                    "replayed": True,
                },
            )
        journal.insert(
            connection,
            command.command_id,
            command.name,
            digest,
            required,
            {"state": "in_progress", "effects": "unknown", "result": None},
            schema,
        )
    try:
        result = to_wire(handler(runtime, command))
    except Exception as error:
        code, details = core_error_details(error)
        outcome = {
            "state": "failed",
            "effects": "committed"
            if details.get("canonical_write_committed")
            else "unknown",
            "result": None,
            "error": {"code": code, "message": str(error), "details": to_wire(details)},
        }
        with database.macros.transaction(
            serialized=True, require_outermost=True
        ) as connection:
            journal.finish(connection, command.command_id, outcome, schema)
        raise
    with database.macros.transaction(
        serialized=True, require_outermost=True
    ) as connection:
        journal.finish(
            connection,
            command.command_id,
            {"state": "succeeded", "effects": "reported", "result": result},
            schema,
        )
    return result


def lookup(runtime: Any, query: Any) -> dict[str, Any]:
    """Read durable evidence without refreshing caches, executing work or installing schema."""
    command_id = identity(query.payload.get("command_id"))
    database = runtime.catalog.db
    schema = getattr(database.driver, "schema", None)
    active = database.macros.current_transaction_connection()
    pending = active is not None
    with (
        nullcontext(active)
        if pending
        else database.macros.read_transaction() as connection
    ):
        saved = journal.read(connection, command_id, schema)
    if saved is None:
        return {"command_id": command_id, "state": "not_found", "effects": "unknown"}
    _authorize(saved["authority"])
    from LiuXin_alpha.databases import command_outbox

    with (
        nullcontext(active)
        if pending
        else database.macros.read_transaction() as connection
    ):
        delivered_job = command_outbox.job_id(connection, command_id, schema)
    outcome = {
        "command_id": command_id,
        "operation": saved["operation"],
        **saved["outcome"],
        "cache_observation": "historical",
    }
    if saved["outcome"].get("state") == "accepted":
        outcome["job_id"] = delivered_job
        service = getattr(runtime, "_durable_jobs", None)
        if delivered_job is not None and service is not None:
            latest = service.repository.latest_attempt(service._run_id(delivered_job))
            outcome["current_job_id"] = service._job_id(latest.job_run_id)
            outcome["job_state"] = latest.state.value
            outcome["attempt_number"] = latest.attempt_number
            outcome["execution"] = to_wire(service._view(latest).execution)
    if pending:
        # This connection can see its own uncommitted receipt. Do not let lookup
        # turn a caller-pending result into apparent durable confirmation.
        outcome.update(state="pending", effects="pending")
    return outcome


def prune(runtime: Any, command: CoreCommand) -> dict[str, Any]:
    """Prune explicit successful results without removing their delivery identities."""
    values = command.payload.get("command_ids")
    if not isinstance(values, (list, tuple)) or not 1 <= len(values) <= 1000:
        raise CoreDispatchError(
            "command_ids must contain 1 to 1000 identities", code="invalid_payload"
        )
    identities = tuple(dict.fromkeys(identity(value) for value in values))
    database = runtime.catalog.db
    schema = getattr(database.driver, "schema", None)
    with database.macros.transaction(
        serialized=True, require_outermost=True
    ) as connection:
        pruned = [
            value for value in identities if journal.prune(connection, value, schema)
        ]
    return {"pruned": pruned, "identities_retained": True}


def install(runtime: Any) -> None:
    """Register outcome inspection without opening the catalogue journal."""
    from LiuXin_alpha.core.workflow_operations import install as install_operations

    install_operations(runtime)
    names = (
        "sync.store.start",
        "metadata.identify.start",
        "metadata.covers.start",
        "ingest.disk.start",
        "ingest.remote-html.start",
        "ingest.web.start",
        "conversion.start",
        "backup.workflow.start",
        "backup.squashfs.start",
        "backup.squashfs.publish-store.start",
        "backup.squashfs.publish-files.start",
    )
    for name in names:
        registration = runtime._command_handlers[name]
        runtime._command_handlers[name] = replace(
            registration,
            description=replace(registration.description, recovery="submission"),
            capability_check=lambda owner: (
                None
                if getattr(owner, "_durable_jobs", None) is not None
                else "Requires a configured durable jobs ledger"
            ),
        )
    from LiuXin_alpha.databases.database import Database

    # Registration must remain passive: job-only inspection must not construct
    # Catalog or load metadata defaults just to classify database-backed routes.
    if isinstance(runtime.database, Database):
        for name, registration in tuple(runtime._command_handlers.items()):
            if (
                registration.description.recovery == "none"
                and registration.description.transport_stable
                and registration.serialized
                and not name.startswith("jobs.")
                and name != "shutdown"
            ):
                runtime._command_handlers[name] = replace(
                    registration,
                    description=replace(registration.description, recovery="observed"),
                )
    runtime.register_query_handler(
        "commands.outcome",
        lookup,
        remote_capabilities=(),
        summary="Inspect a command's retained outcome after response loss.",
        payload_fields=(
            CorePayloadFieldDescription(
                "command_id", required=True, field_type="string"
            ),
        ),
        tags=("commands", "recovery"),
    )
    runtime.register_command_handler(
        "commands.prune",
        prune,
        remote_capabilities=("admin",),
        summary="Prune selected results while retaining command identities and effects.",
        payload_fields=(
            CorePayloadFieldDescription(
                "command_ids", required=True, field_type="array"
            ),
        ),
        tags=("commands", "recovery"),
    )


def submit_workflow(
    runtime: Any,
    *,
    function_name: str,
    kwargs: dict[str, Any],
    label: str,
    timeout: float | None,
    backend: Any,
    no_output: bool,
) -> dict[str, Any]:
    """Accept a workflow and its receipt atomically before delivery to the jobs ledger."""
    from LiuXin_alpha.configuration import preferences_snapshot
    from LiuXin_alpha.core.workflow_delivery import FUNCTIONS
    from LiuXin_alpha.databases import command_outbox

    command = _command.get()
    if command is None:
        raise CoreDispatchError(
            "Durable workflow submission requires a command envelope"
        )
    command = snapshot(command)
    if function_name not in FUNCTIONS:
        raise CoreDispatchError(
            "Unsupported durable workflow", code="capability_unavailable"
        )
    if backend not in (None, "durable"):
        raise CoreDispatchError(
            "Durable workflows require the durable executor",
            code="invalid_payload",
        )
    if timeout not in (-1, None) and (
        isinstance(timeout, bool) or not math.isfinite(timeout) or timeout <= 0
    ):
        raise CoreDispatchError(
            "job_timeout_s must be positive and finite, or -1", code="invalid_payload"
        )
    timeout = None if timeout == -1 else timeout
    database = runtime.catalog.db
    schema = getattr(database.driver, "schema", None)
    digest = fingerprint(command)
    with database.macros.transaction(
        serialized=True, require_outermost=True
    ) as connection:
        journal.install(connection, schema)
        journal.reserve(connection, command.command_id, schema)
        saved = journal.read(connection, command.command_id, schema)
        if saved is not None:
            _authorize(saved["authority"])
            if saved["fingerprint"] != digest:
                raise CoreDispatchError(
                    "This command ID already names different input",
                    code="command_conflict",
                )
            result = saved["outcome"]["result"]
            if (
                not isinstance(result, dict)
                or saved["outcome"].get("state") != "accepted"
            ):
                raise CoreDispatchError(
                    "Command identity has no retained submission receipt",
                    code="command_result_pruned",
                )
        else:
            intent = {
                "version": 1,
                "library_uuid": str(database.uuid),
                "function": function_name,
                "kwargs": to_wire(kwargs),
                "label": label,
                "timeout_s": timeout,
                "preferences": preferences_snapshot(runtime.services.preferences),
            }
            command_outbox.add(connection, command.command_id, intent, schema)
            result = {
                "command_id": command.command_id,
                "job_id": None,
                "state": "accepted",
                "label": label,
                "backend": "durable",
                "timeout_s": timeout,
                "no_output": no_output,
            }
            journal.insert(
                connection,
                command.command_id,
                command.name.strip().lower(),
                digest,
                _required.get(),
                {"state": "accepted", "effects": "pending", "result": result},
                schema,
            )
        job_id = command_outbox.job_id(connection, command.command_id, schema)
    return {**result, "job_id": job_id}
