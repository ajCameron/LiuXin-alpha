"""Declare maintenance consistency modes independently of engine scheduling.

MaintenanceRuntime implements these contracts for explicitly installed table
subscriptions. The old queue engine remains a transient compatibility adapter.
"""

from enum import StrEnum
from typing import Literal, NotRequired, TypedDict


class MaintenanceMode(StrEnum):
    """Describe when a plugin's effects must become consistent with a write.

    LAZY permits temporarily stale derived state; eventual completion requires a
    durable obligation and retry/reconciliation support. Installed subscriptions
    capture obligations atomically; Core delivers them through the jobs ledger.

    PROMPT requires completion on the originating write's transaction before the
    operation reports success. Failure aborts that write unit. Dependent reads
    and transaction/savepoint completion flush required effects.

    Priority orders work within a supported execution path; it cannot strengthen
    a plugin's consistency mode.

    Example:
        >>> MaintenanceMode.LAZY.value
        'lazy'
    """

    LAZY = "lazy"
    PROMPT = "prompt"


class InstalledPlugin(TypedDict):
    """One row of the versioned maintenance_plugins schema."""

    plugin_id: str
    version: str
    mode: Literal["lazy", "prompt"]
    enabled: int
    priority: int
    dependencies_json: str
    definition_json: str
    retry_limit: int
    retry_backoff_s: float


class MaintenanceObligation(TypedDict):
    """An obligation joined with its immutable source-change identity.

    Nullable delivery and completion columns describe work that has not yet been
    published or acknowledged. The journal's SELECT owns this row shape.
    """

    obligation_id: int
    change_id: int
    plugin_id: str
    plugin_version: str
    mode: Literal["lazy", "prompt"]
    completed_ms: int | None
    attempts: int
    available_ms: int
    retry_generation: int
    last_error: str | None
    last_failure_run_id: int | None
    job_ledger_uuid: str | None
    job_run_id: int | None
    table_name: str
    row_id: int
    operation: Literal["insert", "update", "delete", "manual"]
    before_json: str | None
    after_json: str | None
    event_json: str | None
    ancestry_json: str
    created_ms: int


class MaintenanceObligationStatus(MaintenanceObligation):
    """Add transient operator diagnostics without changing the journal record."""

    age_ms: int
    implementation_available: bool


class RebuildCheckpoint(TypedDict):
    """JSON checkpoint written and resumed by the versioned rebuild contract."""

    name: str
    run_uuid: str
    signature: str
    plugins: list[str]
    source_table: str
    source_bound: int | None
    projection_bound: str | None
    source_cursor: int | None
    projection_cursor: str | None
    phase: Literal["source", "projections", "waiting"]
    scanned: int
    scheduled: int
    started_ms: int
    last_change_id: int
    updated_ms: NotRequired[int]
