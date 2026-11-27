"""Resolve accepted command receipts to durable jobs without repeating submission."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from typing import Any


def wait_for_submission(
    query: Callable[..., Any], submission: Mapping[str, Any], *, timeout: float = 30.0
) -> str:
    """Wait only for outbox delivery; timeout preserves the logical command ID.

    Callers may detach with the original receipt and resolve it later. This never
    retries an operation or treats missing outcome evidence as rollback.
    """
    job_id = submission.get("job_id")
    if job_id:
        return str(job_id)
    command_id = submission.get("command_id")
    if not command_id:
        raise ValueError("Submission contains neither a job ID nor a command ID")
    deadline = time.monotonic() + timeout
    while True:
        outcome = query("commands.outcome", {"command_id": command_id})
        if outcome.get("job_id"):
            return str(outcome["job_id"])
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"Command {command_id} remains accepted; resolve commands.outcome before retrying"
            )
        time.sleep(min(0.1, max(0, deadline - time.monotonic())))
