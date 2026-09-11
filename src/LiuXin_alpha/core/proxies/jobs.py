"""
Define the shared list/get/wait/cancel proxy surface and its payload-normalization helpers.

These contracts describe named Core job operations, not dynamic target method
dispatch. Proxies coerce payload values; the runtime/job manager owns state
validation, pagination, waiting, and cancellation semantics.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Collection
from typing import Any, Mapping, Protocol, runtime_checkable


JobStatesArg = str | Collection[str]


def normalize_job_states_arg(states: JobStatesArg | None) -> str | list[str] | None:
    """
    Preserve a scalar state string or normalize collection entries into sorted unique lowercase tokens.

    Scalar strings are not stripped, split, lowercased, or validated here. Collection
    entries are stringified and stripped; blanks are removed. Neither branch
    validates tokens against the job-state vocabulary.

    Example:
        >>> normalize_job_states_arg([" Running ", "running", "", "FAILED"])
        ['failed', 'running']
        >>> normalize_job_states_arg(" Running,FAILED ")
        ' Running,FAILED '


    :param states: Optional scalar state/filter string or collection of state tokens.
    :return: Original scalar text, sorted normalized token list, or ``None`` when no filter was supplied.
    """
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
    """
    Describe explicit job operations shared by local and remote Core proxies.

    Mapping results are endpoint payloads rather than JobInfo instances. Runtime
    protocol checks inspect member presence, not complete signatures or behavior.

    Example:
        >>> from LiuXin_alpha.core.proxies.local import LocalJobsProxy
        >>> from unittest.mock import Mock
        >>> isinstance(LocalJobsProxy(Mock()), JobsProxyAPI)
        True
    """

    def list(
        self,
        *,
        states: JobStatesArg | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> Mapping[str, Any]:
        """
        Query named job listings with optional state filtering and a display window.

        Example:
            >>> payload = jobs.list(states=["running"], limit=20)  # doctest: +SKIP


        :param states: Optional state string or token collection interpreted by the concrete proxy/runtime.
        :param limit: Optional result cap; ``None`` omits a proxy-level override.
        :param offset: Requested listing offset, with range policy owned by the endpoint.
        :return: Mapping containing the runtime's job listing and associated summary fields.
        """

    def get(self, job_id: str) -> Mapping[str, Any]:
        """
        Query the runtime's current record for a single job identifier.

        Example:
            >>> payload = jobs.get("job-1")  # doctest: +SKIP


        :param job_id: Job identifier subject to the concrete proxy's text validation.
        :return: Job endpoint mapping, including the endpoint's missing-job representation when applicable.
        """

    def wait(self, job_id: str, *, timeout_s: float | None = None) -> Mapping[str, Any]:
        """
        Wait through the runtime job endpoint and return its resulting job/status payload.

        A timeout-limited return does not by itself establish terminal success;
        remote transport timeouts can also bound the request independently.

        Example:
            >>> payload = jobs.wait("job-1", timeout_s=2.0)  # doctest: +SKIP


        :param job_id: Identifier of the job to observe.
        :param timeout_s: Optional requested wait timeout in seconds, distinct from HTTP request timeout.
        :return: Mapping describing the endpoint's observed job state or missing-job outcome.
        """

    def cancel(self, job_id: str) -> Mapping[str, Any]:
        """
        Request job cancellation without promising that the job has stopped when the call returns.

        Example:
            >>> payload = jobs.cancel("job-1")  # doctest: +SKIP


        :param job_id: Identifier of the job for which cancellation is requested.
        :return: Runtime cancellation outcome mapping, not an implicit wait-for-termination result.
        """


class JobsProxyABC(ABC):
    """
    Require explicit job-operation implementations and share nonblank identifier normalization.

    The abstract methods deliberately raise if their base implementation is called;
    subclasses must provide payload construction, dispatch, and result handling.

    Example:
        >>> import inspect
        >>> inspect.isabstract(JobsProxyABC)
        True
    """

    @staticmethod
    def normalize_job_id(job_id: str) -> str:
        """
        Stringify and strip a job identifier, rejecting only a blank result.

        This does not validate UUID syntax or check that the job exists.

        Example:
            >>> JobsProxyABC.normalize_job_id(" job-1 ")
            'job-1'


        :param job_id: Job identifier-like value converted to nonblank text.
        :return: Stripped identifier string without case normalization.
        :raises ValueError: If the converted identifier contains no non-whitespace characters.
        """
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
        """
        Require subclasses to return a named-job listing with optional filtering and pagination.

        Example:
            >>> payload = jobs.list(states="running", offset=0)  # doctest: +SKIP


        :param states: Optional scalar or collection-valued state filter.
        :param limit: Optional listing size override, with validation delegated to the implementation.
        :param offset: Requested starting offset for the job listing.
        :return: Job listing mapping when implemented by a concrete proxy.
        :raises NotImplementedError: When this abstract base implementation is invoked directly.
        """
        raise NotImplementedError

    @abstractmethod
    def get(self, job_id: str) -> Mapping[str, Any]:
        """
        Require subclasses to retrieve the named endpoint's payload for one job.

        Example:
            >>> payload = jobs.get("job-1")  # doctest: +SKIP


        :param job_id: Identifier to normalize and query in the concrete implementation.
        :return: Job detail mapping when implemented by a concrete proxy.
        :raises NotImplementedError: When this base implementation is called directly.
        """
        raise NotImplementedError

    @abstractmethod
    def wait(self, job_id: str, *, timeout_s: float | None = None) -> Mapping[str, Any]:
        """
        Require subclasses to wait through the job endpoint without assuming that return means success.

        Example:
            >>> payload = jobs.wait("job-1", timeout_s=1.0)  # doctest: +SKIP


        :param job_id: Identifier of the observed job.
        :param timeout_s: Optional endpoint wait limit in seconds, independent of transport timeout.
        :return: Observed job/status mapping when implemented by a concrete proxy.
        :raises NotImplementedError: When this base implementation is called directly.
        """
        raise NotImplementedError

    @abstractmethod
    def cancel(self, job_id: str) -> Mapping[str, Any]:
        """
        Require subclasses to request cancellation and expose the endpoint's acknowledgment.

        Cancellation acknowledgment need not mean that execution has terminated.

        Example:
            >>> payload = jobs.cancel("job-1")  # doctest: +SKIP


        :param job_id: Identifier of the job to cancel.
        :return: Cancellation result mapping when implemented by a concrete proxy.
        :raises NotImplementedError: When this base implementation is called directly.
        """
        raise NotImplementedError


__all__ = [
    "JobStatesArg",
    "normalize_job_states_arg",
    "JobsProxyAPI",
    "JobsProxyABC",
]
