"""Core jobs value objects.

This module defines durable job definitions and individual job runs. A job
*definition* describes what should run; a job *run* is one concrete execution
attempt of that definition.
"""

from __future__ import annotations

import dataclasses
import time

from enum import StrEnum
from typing import Any


class JobDefinitionState(StrEnum):
    ENABLED = "enabled"
    PAUSED = "paused"
    DISABLED = "disabled"


class JobRunState(StrEnum):
    QUEUED = "queued"
    LEASED = "leased"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    CANCELLED = "cancelled"
    ABORTED = "aborted"


class JobTriggerKind(StrEnum):
    MANUAL = "manual"
    SCHEDULED = "scheduled"
    DEPENDENCY = "dependency"
    REPAIR = "repair"


class JobConcurrencyPolicy(StrEnum):
    ALLOW_PARALLEL = "allow_parallel"
    SKIP_IF_RUNNING = "skip_if_running"
    REPLACE_RUNNING = "replace_running"
    QUEUE_ONE = "queue_one"


class JobResultPolicy(StrEnum):
    KEEP_ALL = "keep_all"
    KEEP_FAILURES = "keep_failures"
    KEEP_LATEST_ONLY = "keep_latest_only"


class JobEventKind(StrEnum):
    CREATED = "created"
    QUEUED = "queued"
    LEASED = "leased"
    STARTED = "started"
    PROGRESS = "progress"
    HEARTBEAT = "heartbeat"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCEL_REQUESTED = "cancel_requested"
    CANCELLED = "cancelled"
    LOG = "log"


@dataclasses.dataclass(slots=True, frozen=True)
class JobDefinition:
    job_kind: str
    job_name: str
    payload_json: str
    job_definition_id: int | None = None
    state: JobDefinitionState = JobDefinitionState.ENABLED
    schedule_json: str | None = None
    priority: int = 100
    concurrency_policy: JobConcurrencyPolicy = JobConcurrencyPolicy.SKIP_IF_RUNNING
    result_policy: JobResultPolicy = JobResultPolicy.KEEP_FAILURES
    timeout_s: float | None = None
    max_retries: int = 0
    retry_backoff_s: float = 0.0
    heartbeat_timeout_s: float | None = None
    created_timestamp_ep_k: int | None = None
    modified_timestamp_ep_k: int | None = None
    last_queued_timestamp_ep_k: int | None = None
    last_started_timestamp_ep_k: int | None = None
    last_finished_timestamp_ep_k: int | None = None


@dataclasses.dataclass(slots=True, frozen=True)
class JobRun:
    job_kind: str
    job_definition_id: int
    trigger_kind: JobTriggerKind = JobTriggerKind.MANUAL
    state: JobRunState = JobRunState.QUEUED
    attempt_number: int = 1
    job_run_id: int | None = None
    worker_id: str | None = None
    lease_expires_timestamp_ep_k: int | None = None
    cancel_requested: bool = False
    not_before_timestamp_ep_k: int | None = None
    progress_current: int | None = None
    progress_total: int | None = None
    progress_unit: str | None = None
    progress_message: str | None = None
    result_json: str | None = None
    error_text: str | None = None
    log_path: str | None = None
    queued_timestamp_ep_k: int | None = None
    started_timestamp_ep_k: int | None = None
    heartbeat_timestamp_ep_k: int | None = None
    finished_timestamp_ep_k: int | None = None


@dataclasses.dataclass(slots=True, frozen=True)
class JobProgressUpdate:
    progress_current: int | None = None
    progress_total: int | None = None
    progress_unit: str | None = None
    progress_message: str | None = None


@dataclasses.dataclass(slots=True, frozen=True)
class JobRunEvent:
    job_run_id: int
    event_kind: JobEventKind
    event_message: str | None = None
    event_json: str | None = None
    event_id: int | None = None
    created_timestamp_ep_k: int | None = None


@dataclasses.dataclass(slots=True, frozen=True)
class JobResult:
    ok: bool
    result: dict[str, Any] | None = None
    error_text: str | None = None



def now_ep_k() -> int:
    return int(time.time() * 1000)


__all__ = [
    "JobDefinition",
    "JobDefinitionState",
    "JobRun",
    "JobRunEvent",
    "JobRunState",
    "JobTriggerKind",
    "JobConcurrencyPolicy",
    "JobResultPolicy",
    "JobProgressUpdate",
    "JobEventKind",
    "JobResult",
    "now_ep_k",
]
