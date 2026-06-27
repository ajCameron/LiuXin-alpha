"""Public application-level jobs API.

Jobs are intentionally a layer above the low-level `utils.jobs` helpers.
The durable model is:
- job definitions describe what should run
- job runs describe one concrete execution attempt
- handlers implement one job kind
- the repository persists definitions, runs, progress, and events
- the scheduler materialises due runs
- workers lease and execute queued runs in the background
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
