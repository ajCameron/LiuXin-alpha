"""Local in-process proxies for core targets."""

from __future__ import annotations

from typing import Any, Mapping

from LiuXin_alpha.core.commands import CoreCommand
from LiuXin_alpha.core.proxies.jobs import JobStatesArg, JobsProxyABC, normalize_job_states_arg
from LiuXin_alpha.core.queries import CoreQuery
from LiuXin_alpha.core.runtime import CoreRuntime


WRITE_PREFIXES = (
    "add",
    "create",
    "delete",
    "dirty",
    "dupe",
    "ensure",
    "interlink",
    "link",
    "lock",
    "persist",
    "publish",
    "refresh",
    "register",
    "remove",
    "set",
    "shutdown",
    "sync",
    "unlink",
    "update",
)

WRITE_EXACT = {
    "backup",
    "bootstrap_storage_manager",
    "close",
}


def looks_like_write_method(method_name: str) -> bool:
    """Best-effort write-path classifier for proxy auto-dispatch."""
    token = str(method_name).strip().lower()
    if token in WRITE_EXACT:
        return True
    return token.startswith(WRITE_PREFIXES)


class _LocalTargetProxy:
    """Generic local proxy for one runtime target."""

    def __init__(self, runtime: CoreRuntime, target: str) -> None:
        self._runtime = runtime
        self._target = str(target)

    @property
    def target(self) -> str:
        return self._target

    def call(self, method: str, *args: Any, write: bool | None = None, **kwargs: Any) -> Any:
        method_token = str(method).strip()
        if not method_token:
            raise ValueError("Proxy method cannot be blank.")

        dispatch_write = looks_like_write_method(method_token) if write is None else bool(write)
        if dispatch_write:
            return self._runtime.invoke_command(
                target=self._target,
                method=method_token,
                args=tuple(args),
                kwargs=kwargs,
            )
        return self._runtime.invoke_query(
            target=self._target,
            method=method_token,
            args=tuple(args),
            kwargs=kwargs,
        )

    def query(self, method: str, *args: Any, **kwargs: Any) -> Any:
        return self._runtime.invoke_query(
            target=self._target,
            method=method,
            args=tuple(args),
            kwargs=kwargs,
        )

    def command(self, method: str, *args: Any, **kwargs: Any) -> Any:
        return self._runtime.invoke_command(
            target=self._target,
            method=method,
            args=tuple(args),
            kwargs=kwargs,
        )

    def __getattr__(self, method_name: str):
        def _caller(*args: Any, **kwargs: Any) -> Any:
            return self.call(method_name, *args, **kwargs)

        _caller.__name__ = "proxy_{}_{}".format(self._target, method_name)
        _caller.__doc__ = "Local proxy dispatcher for {}.{}(...)".format(self._target, method_name)
        return _caller


class LocalDatabaseProxy(_LocalTargetProxy):
    """Local proxy bound to the `database` target."""

    def __init__(self, runtime: CoreRuntime) -> None:
        super().__init__(runtime=runtime, target="database")


class LocalStorageProxy(_LocalTargetProxy):
    """Local proxy bound to the `storage` target."""

    def __init__(self, runtime: CoreRuntime) -> None:
        super().__init__(runtime=runtime, target="storage")


class LocalJobsProxy(JobsProxyABC):
    """Local proxy for explicit core jobs APIs."""

    def __init__(self, runtime: CoreRuntime) -> None:
        self._runtime = runtime

    def list(
        self,
        *,
        states: JobStatesArg | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> Mapping[str, Any]:
        payload: dict[str, Any] = {"offset": int(offset)}
        normalized_states = normalize_job_states_arg(states)
        if normalized_states is not None:
            payload["states"] = normalized_states
        if limit is not None:
            payload["limit"] = int(limit)
        envelope = CoreQuery(name="jobs.list", payload=payload)
        return self._runtime.execute_query(envelope).result

    def get(self, job_id: str) -> Mapping[str, Any]:
        envelope = CoreQuery(name="jobs.get", payload={"job_id": self.normalize_job_id(job_id)})
        return self._runtime.execute_query(envelope).result

    def wait(self, job_id: str, *, timeout_s: float | None = None) -> Mapping[str, Any]:
        payload: dict[str, Any] = {"job_id": self.normalize_job_id(job_id)}
        if timeout_s is not None:
            payload["timeout_s"] = float(timeout_s)
        envelope = CoreQuery(name="jobs.wait", payload=payload)
        return self._runtime.execute_query(envelope).result

    def cancel(self, job_id: str) -> Mapping[str, Any]:
        envelope = CoreCommand(name="jobs.cancel", payload={"job_id": self.normalize_job_id(job_id)})
        return self._runtime.execute_command(envelope).result


class LocalLibraryProxy(_LocalTargetProxy):
    """Top-level local proxy for library + child targets."""

    def __init__(self, runtime: CoreRuntime) -> None:
        super().__init__(runtime=runtime, target="library")
        self.database = LocalDatabaseProxy(runtime)
        self.storage = LocalStorageProxy(runtime)
        self.jobs = LocalJobsProxy(runtime)

    @property
    def core_uuid(self) -> str:
        return self._runtime.core_uuid

    @property
    def core_version(self) -> str:
        return self._runtime.core_version

    def health(self) -> Mapping[str, Any]:
        envelope = CoreQuery(name="health")
        return self._runtime.execute_query(envelope).result
