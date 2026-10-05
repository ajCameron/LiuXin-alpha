"""
Expose managed-job creation, scheduling and inspection operations.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise api through a consuming regression::

        python -m pytest -q tests/jobs/test_jobs_repository.py
"""

from LiuXin_alpha.jobs.handler_api import JobHandlerAPI, JobHandlerRegistry, JobRunContext
from LiuXin_alpha.jobs.models import (
    JobConcurrencyPolicy,
    JobDefinition,
    JobDefinitionState,
    JobEventKind,
    JobProgressUpdate,
    JobResult,
    JobResultPolicy,
    JobRun,
    JobRunEvent,
    JobRunState,
    JobTriggerKind,
    now_ep_k,
)
from LiuXin_alpha.jobs.repository import JobRepository
from LiuXin_alpha.jobs.scheduler import JobScheduler
from LiuXin_alpha.jobs.worker import JobWorker

__all__ = [
    "JobHandlerAPI",
    "JobHandlerRegistry",
    "JobRunContext",
    "JobConcurrencyPolicy",
    "JobDefinition",
    "JobDefinitionState",
    "JobEventKind",
    "JobProgressUpdate",
    "JobResult",
    "JobResultPolicy",
    "JobRun",
    "JobRunEvent",
    "JobRunState",
    "JobTriggerKind",
    "JobRepository",
    "JobScheduler",
    "JobWorker",
    "now_ep_k",
]
