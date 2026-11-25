"""Limit queued application calls before they wait on the shared database graph."""

from collections.abc import Callable, Iterable
from functools import wraps
from typing import Any

from LiuXin_alpha.core.errors import CoreDispatchError

SCAN_ROWS = 10_000
PAGE_ROWS = 1000


def bounded_values(values: Iterable[Any], *, maximum: int = SCAN_ROWS) -> list[Any]:
    """Buffer at most the scan budget, failing before projecting a partial result.

    Read one lookahead value to distinguish an exact-size result from overflow.
    The iterable owner retains responsibility for its own upstream buffering.
    """
    result: list[Any] = []
    for value in values:
        if len(result) == maximum:
            raise CoreDispatchError(
                "Legacy scan exceeds its row budget; narrow the selection or use native metadata search",
                code="scan_limit_exceeded",
                details={"max_rows": maximum},
            )
        result.append(value)
    return result


def bounded_table_rows(
    source: Any, table: str, *, maximum: int = SCAN_ROWS
) -> list[Any]:
    """Bound legacy table hydration, preferring the owner's lazy row iterator.

    An available count rejects oversized tables before materialization. Minimal
    injected read sources can omit the count and are still bounded on consumption.
    """
    count = getattr(source, "get_record_count", None)
    if callable(count):
        total = count(table)
        if type(total) is not int or total < 0:
            raise TypeError("Table reader returned an invalid row count")
        if total > maximum:
            raise CoreDispatchError(
                "Legacy table scan exceeds its row budget; use native metadata search",
                code="scan_limit_exceeded",
                details={"table": table, "max_rows": maximum},
            )
    try:
        values = source.get_all_rows(table, iterator_return=True)
    except TypeError:
        values = source.get_all_rows(table)
    return bounded_values(values, maximum=maximum)


def bounded_dispatch(function: Callable[..., Any]) -> Callable[..., Any]:
    """Admit at most 32 ordinary calls; independent liveness/cancellation bypass it."""

    @wraps(function)
    def call(runtime: Any, envelope: Any, **kwargs: Any) -> Any:
        if envelope.name in ("health.live", "jobs.cancel"):
            return function(runtime, envelope, **kwargs)
        if not runtime._dispatch_slots.acquire(blocking=False):
            raise CoreDispatchError(
                "Core application queue is full; retry with the same command ID",
                code="busy",
            )
        try:
            return function(runtime, envelope, **kwargs)
        finally:
            runtime._dispatch_slots.release()

    return call


def validate_input(value: Any) -> None:
    """Apply the wire budgets before effects and expose a structured rejection."""
    from LiuXin_alpha.core.wire import CoreWireError, to_wire

    try:
        to_wire(value, max_bytes=4 * 1024 * 1024)
    except CoreWireError as error:
        raise CoreDispatchError(str(error), code="invalid_payload") from error
