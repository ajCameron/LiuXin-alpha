"""Persistence and cache reconciliation outcomes shared by metadata services."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, Literal, TypeVar

# Keep main's explicit covariance after moving the receipt to this module.
# Synthesized dataclass __replace__ methods make inferred variance invariant on
# Python 3.13+, but replacing a frozen receipt creates a new value.
_ResultT_co = TypeVar("_ResultT_co", covariant=True)


@dataclass(frozen=True, slots=True)
class MetadataWriteReceipt(Generic[_ResultT_co]):  # noqa: UP046 - explicit variance
    """Keep persistence and cache visibility explicit after a semantic write.

    ``pending`` means an outer transaction still controls commit or rollback; it
    must never be reported as durable success. ``deferred`` requires the owner to
    refresh after that transaction closes, including after rollback. ``current``
    means the bound cache can safely serve the completed change. A configured
    cache cannot be current for a pending write. ``dirty`` records failed cache
    reconciliation and is carried by an error, never a successful write return.
    ``not_configured`` and a None generation describe operation without a cache.

    ``changed`` describes an applied mutation request within the transaction, not
    a durable commit or a whole-metadata equality comparison. Supplied replacements
    may report true when values already match; known no-ops report false.
    A receipt captures return-time state; a pending receipt does not later become
    committed in place. Its generation is not evidence of transaction durability.
    This passive value does not validate state combinations or freeze result
    payloads. A reconciliation failure must preserve this receipt in the error;
    it must not turn a committed write into an apparent persistence failure.
    """

    result: _ResultT_co
    changed: bool
    persistence: Literal["committed", "pending"]
    cache: Literal["not_configured", "current", "deferred", "dirty"]
    generation: int | None


class MetadataReconciliationError(RuntimeError):
    """Preserve a completed Catalog operation whose cache reconciliation failed.

    Inspect receipt.persistence before deciding how an enclosing transaction must
    finish. A committed receipt must not trigger an automatic repeat of the write.
    The implementing owner must leave the cache dirty and recoverable.
    """

    def __init__(self, message: str, *, receipt: MetadataWriteReceipt[object]) -> None:
        """Retain the authoritative receipt without changing persistence state."""
        super().__init__(message)
        self.receipt = receipt
