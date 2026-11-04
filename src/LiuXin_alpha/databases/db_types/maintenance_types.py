"""Declare maintenance consistency modes independently of engine scheduling.

MaintenanceRuntime implements these contracts for explicitly installed table
subscriptions. The old queue engine remains a transient compatibility adapter.
"""

from enum import StrEnum


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
