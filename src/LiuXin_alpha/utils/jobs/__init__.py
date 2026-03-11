"""Job execution API and high-level manager interface."""

from __future__ import annotations

import os

from .api import (
    JobBackend,
    JobExecution,
    JobRequest,
    available_backends,
    execute_job,
    get_backend,
)
from .manager import InMemoryJobManager, JobManagerAPI, JobState, ManagedJob

_DEFAULT_MAX_WORKERS = int(os.environ.get("LIUXIN_JOB_MANAGER_WORKERS", "4") or "4")
_default_job_manager = InMemoryJobManager(max_workers=max(1, _DEFAULT_MAX_WORKERS))


def default_job_manager() -> InMemoryJobManager:
    """Return the process-global job manager instance."""
    return _default_job_manager


def submit_job(
    request: JobRequest,
    *,
    timeout: float = 300.0,
    no_output: bool = False,
    heartbeat=None,
    backend: str | JobBackend | None = None,
    label: str | None = None,
) -> str:
    """Submit one job through the process-global manager and return its job id."""
    return _default_job_manager.submit(
        request,
        timeout=timeout,
        no_output=no_output,
        heartbeat=heartbeat,
        backend=backend,
        label=label,
    )


__all__ = [
    "JobBackend",
    "JobExecution",
    "JobRequest",
    "available_backends",
    "execute_job",
    "get_backend",
    "JobState",
    "ManagedJob",
    "JobManagerAPI",
    "InMemoryJobManager",
    "default_job_manager",
    "submit_job",
]

