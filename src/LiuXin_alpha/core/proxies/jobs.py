"""Shared typing/contracts for explicit jobs proxies."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Collection
from typing import Any, Mapping, Protocol, runtime_checkable


JobStatesArg = str | Collection[str]


def normalize_job_states_arg(states: JobStatesArg | None) -> str | list[str] | None:
    """Normalize optional job-state filters for proxy payloads."""
    if states is None:
        return None
    if isinstance(states, str):
        return str(states)

    values: set[str] = set()
    for raw in states:
        token = str(raw).strip().lower()
        if token:
            values.add(token)
    return sorted(values)


@runtime_checkable
class JobsProxyAPI(Protocol):
    """Runtime-checkable protocol for jobs proxy implementations."""

    def list(
        self,
        *,
        states: JobStatesArg | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> Mapping[str, Any]:
        """Return serialized job listing payload."""

    def get(self, job_id: str) -> Mapping[str, Any]:
        """Return one serialized job payload."""

    def wait(self, job_id: str, *, timeout_s: float | None = None) -> Mapping[str, Any]:
        """Block for one job and return serialized payload."""

    def cancel(self, job_id: str) -> Mapping[str, Any]:
        """Request cancellation for one job and return result payload."""


class JobsProxyABC(ABC):
    """Abstract base class for explicit jobs proxies."""

    @staticmethod
    def normalize_job_id(job_id: str) -> str:
        token = str(job_id).strip()
        if not token:
            raise ValueError("job_id cannot be blank.")
        return token

    @abstractmethod
    def list(
        self,
        *,
        states: JobStatesArg | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> Mapping[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def get(self, job_id: str) -> Mapping[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def wait(self, job_id: str, *, timeout_s: float | None = None) -> Mapping[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def cancel(self, job_id: str) -> Mapping[str, Any]:
        raise NotImplementedError


__all__ = [
    "JobStatesArg",
    "normalize_job_states_arg",
    "JobsProxyAPI",
    "JobsProxyABC",
]
