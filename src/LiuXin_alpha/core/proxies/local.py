"""
Adapt a borrowed in-process Core runtime to envelope, dynamic-target, and explicit-job client interfaces.

These proxies call the runtime synchronously without a transport hop. Dynamic
target access uses the legacy invoke routes, while explicit jobs/health methods
use named endpoint envelopes. They do not create or independently own a runtime.
"""

from __future__ import annotations

from typing import Any, Callable, Mapping

from LiuXin_alpha.core.api import CoreAPI
from LiuXin_alpha.core.commands import CoreCommand, CoreCommandResult
from LiuXin_alpha.core.dispatch import looks_like_write_method
from LiuXin_alpha.core.events import CoreEvent
from LiuXin_alpha.core.proxies.jobs import (
    JobStatesArg,
    JobsProxyABC,
    normalize_job_states_arg,
)
from LiuXin_alpha.core.queries import CoreQuery, CoreQueryResult
from LiuXin_alpha.core.runtime import CoreRuntime


def _require_mapping(value: Any, *, operation: str) -> Mapping[str, Any]:
    """
    Require a mapping-shaped endpoint result without copying or validating its fields.

    Example:
        >>> result = {"job": None}
        >>> _require_mapping(result, operation="jobs.get") is result
        True


    :param value: Unwrapped endpoint result to check.
    :param operation: Operation label used only in the nonmapping error message.
    :return: Original mapping object unchanged.
    :raises TypeError: If the result does not implement Mapping.
    """
    if not isinstance(value, Mapping):
        raise TypeError("{} result must be a mapping.".format(operation))
    return value


class LocalCoreClient(CoreAPI):
    """
    Forward the envelope-level client contract to an existing runtime without extra normalization.

    Inherited named-call helpers construct envelopes and unwrap results. Identity,
    subscription, and shutdown use the same borrowed runtime; shutdown affects it,
    not just this wrapper.

    Example:
        >>> from types import SimpleNamespace
        >>> LocalCoreClient(SimpleNamespace(core_uuid="core-1")).core_uuid
        'core-1'
    """

    def __init__(self, runtime: CoreRuntime) -> None:
        """
        Retain the runtime reference without validating, copying, or starting it.

        Example:
            >>> from unittest.mock import Mock
            >>> runtime = Mock()
            >>> LocalCoreClient(runtime)._runtime is runtime
            True


        :param runtime: Existing runtime implementing the Core client operations.
        :return: ``None`` after borrowing the runtime reference.
        """
        self._runtime = runtime

    @property
    def core_uuid(self) -> str:
        """
        Read the borrowed runtime's current instance identifier without caching it.

        Example:
            >>> from types import SimpleNamespace
            >>> LocalCoreClient(SimpleNamespace(core_uuid="core-1")).core_uuid
            'core-1'


        :return: Runtime core_uuid property value unchanged.
        """
        return self._runtime.core_uuid

    @property
    def core_version(self) -> str:
        """
        Read the implementation version advertised by the borrowed runtime.

        Example:
            >>> from types import SimpleNamespace
            >>> LocalCoreClient(SimpleNamespace(core_version="2.0.0")).core_version
            '2.0.0'


        :return: Runtime core_version property value without local validation.
        """
        return self._runtime.core_version

    @property
    def api_version(self) -> str:
        """
        Read the API contract version advertised by the borrowed runtime.

        Example:
            >>> from types import SimpleNamespace
            >>> LocalCoreClient(SimpleNamespace(api_version="2.0")).api_version
            '2.0'


        :return: Runtime api_version property value without negotiation.
        """
        return self._runtime.api_version

    def execute_command(
        self,
        command: CoreCommand,
    ) -> CoreCommandResult:
        """
        Forward the command envelope unchanged and return the runtime's response.

        Example:
            >>> from unittest.mock import Mock
            >>> runtime = Mock()
            >>> runtime.execute_command.return_value = CoreCommandResult(True, "request-1", result=7)
            >>> LocalCoreClient(runtime).execute_command(CoreCommand("example")).result
            7


        :param command: Command envelope passed by reference to the runtime.
        :return: Runtime response envelope unchanged; runtime execution errors propagate.
        """
        return self._runtime.execute_command(command)

    def execute_query(
        self,
        query: CoreQuery,
    ) -> CoreQueryResult:
        """
        Forward the query envelope unchanged and return the runtime's response.

        Example:
            >>> from unittest.mock import Mock
            >>> runtime = Mock()
            >>> runtime.execute_query.return_value = CoreQueryResult(True, "request-1", result=7)
            >>> LocalCoreClient(runtime).execute_query(CoreQuery("example")).result
            7


        :param query: Query envelope passed by reference to the runtime.
        :return: Runtime response envelope unchanged, without wrapper-level success checks.
        """
        return self._runtime.execute_query(query)

    def describe_api(
        self,
        *,
        include_targets: bool = True,
        target: str | None = None,
    ) -> dict[str, Any]:
        """
        Delegate API description selection directly to the runtime without copying its result.

        Example:
            >>> from unittest.mock import Mock
            >>> runtime = Mock()
            >>> runtime.describe_api.return_value = {"targets": []}
            >>> LocalCoreClient(runtime).describe_api(include_targets=False)
            {'targets': []}


        :param include_targets: Target-description selection forwarded without coercion.
        :param target: Optional target filter forwarded without wrapper-level validation.
        :return: Runtime description dictionary unchanged.
        """
        return self._runtime.describe_api(
            include_targets=include_targets,
            target=target,
        )

    def subscribe(
        self,
        callback: Callable[[CoreEvent], None],
    ) -> Callable[[], None]:
        """
        Register the callback on the borrowed runtime and return its unsubscribe function.

        No additional delivery queue or callback thread is introduced here.

        Example:
            >>> unsubscribe = client.subscribe(print)  # doctest: +SKIP


        :param callback: Event subscriber passed unchanged to runtime registration.
        :return: Runtime-provided callable that removes this subscription.
        """
        return self._runtime.subscribe(callback)

    def shutdown(self) -> int:
        """
        Shut down the borrowed runtime using its ownership and job-manager policy.

        This affects other clients sharing that runtime; it is not a wrapper-only close.

        Example:
            >>> from unittest.mock import Mock
            >>> runtime = Mock()
            >>> runtime.shutdown.return_value = 0
            >>> LocalCoreClient(runtime).shutdown()
            0


        :return: Exit code returned by runtime shutdown.
        """
        return self._runtime.shutdown()


LocalCoreProxy = LocalCoreClient


class _LocalTargetProxy:
    """
    Bind legacy invoke calls to a target name with explicit or name-inferred write routing.

    Unknown attributes manufacture callables without checking method existence.
    The runtime resolves the actual target/method when invoked. This is a generic
    compatibility path, not a promise of named-endpoint wire stability.

    Example:
        >>> from unittest.mock import Mock
        >>> _LocalTargetProxy(Mock(), "database").target
        'database'
    """

    def __init__(self, runtime: CoreRuntime, target: str) -> None:
        """
        Borrow a runtime and retain a stringified target label without resolving it.

        Example:
            >>> from unittest.mock import Mock
            >>> _LocalTargetProxy(Mock(), "library").target
            'library'


        :param runtime: Existing runtime providing invoke_command and invoke_query.
        :param target: Target label stringified without stripping or validating it.
        :return: ``None`` after storing runtime and target references.
        """
        self._runtime = runtime
        self._target = str(target)

    @property
    def target(self) -> str:
        """
        Expose the stored target label without probing the runtime registry.

        Example:
            >>> from unittest.mock import Mock
            >>> _LocalTargetProxy(Mock(), "storage").target
            'storage'


        :return: String target label captured during construction.
        """
        return self._target

    def call(
        self, method: str, *args: Any, write: bool | None = None, **kwargs: Any
    ) -> Any:
        """
        Strip a method name and invoke it through explicit or name-inferred command/query routing.

        ``write=None`` uses the shared naming heuristic; other values are truth-
        converted. The ``write`` keyword controls dispatch and is not passed to
        the target. Use explicit ``command``/``query`` to forward a target keyword
        also named write. Method existence and target argument validation are deferred.

        Example:
            >>> from unittest.mock import Mock
            >>> runtime = Mock()
            >>> runtime.invoke_query.return_value = 7
            >>> _LocalTargetProxy(runtime, "database").call(" get_row ", 1)
            7


        :param method: Target method label, stringified and stripped; blank labels are rejected.
        :param args: Positional target arguments forwarded as a tuple.
        :param write: Optional route override; ``None`` classifies the normalized method name.
        :param kwargs: Target keyword arguments excluding this wrapper's write control.
        :return: Runtime invoke result unchanged, without a wrapper-level mapping or wire check.
        :raises ValueError: If the normalized method name is blank.
        """
        method_token = str(method).strip()
        if not method_token:
            raise ValueError("Proxy method cannot be blank.")

        dispatch_write = (
            looks_like_write_method(method_token) if write is None else bool(write)
        )
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
        """
        Invoke the target explicitly through the query route without local method normalization.

        This selects routing, not an enforced read-only transaction or effect check.

        Example:
            >>> result = proxy.query("get_row", "works", 1)  # doctest: +SKIP


        :param method: Method label passed directly to runtime invoke_query.
        :param args: Positional arguments forwarded as a tuple.
        :param kwargs: Target keyword arguments, including write if the target accepts it.
        :return: Runtime query-invoke result unchanged.
        """
        return self._runtime.invoke_query(
            target=self._target,
            method=method,
            args=tuple(args),
            kwargs=kwargs,
        )

    def command(self, method: str, *args: Any, **kwargs: Any) -> Any:
        """
        Invoke the target explicitly through the command route without the naming heuristic.

        Example:
            >>> result = proxy.command("set_pref", "view", "list")  # doctest: +SKIP


        :param method: Method label passed directly to runtime invoke_command.
        :param args: Positional target arguments forwarded as a tuple.
        :param kwargs: Target keyword arguments without reserving a write-control keyword.
        :return: Runtime command-invoke result unchanged.
        """
        return self._runtime.invoke_command(
            target=self._target,
            method=method,
            args=tuple(args),
            kwargs=kwargs,
        )

    def __getattr__(
        self,
        method_name: str,
    ) -> Callable[..., Any]:
        """
        Manufacture a named/documented dispatcher for an otherwise missing proxy attribute.

        No method-existence check occurs, and the closure is not cached on the
        proxy. Each invocation reuses ``call`` and its optional write override.
        Generated runtime docstrings use reST parameter/return fields as well.

        Example:
            >>> from unittest.mock import Mock
            >>> dispatcher = _LocalTargetProxy(Mock(), "database").get_row
            >>> dispatcher.__name__
            'proxy_database_get_row'


        :param method_name: Missing attribute spelling retained as the target method label.
        :return: Fresh callable closing over this proxy and method name.
        """

        def _caller(*args: Any, **kwargs: Any) -> Any:
            """
            Forward a dynamically selected method through this proxy's generic routing helper.

            Its descriptive name and docstring are specialized by the enclosing factory.

            Example:
                >>> result = dispatcher(*args, **kwargs)  # doctest: +SKIP


            :param args: Positional arguments to forward to the selected target method.
            :param kwargs: Target keywords plus an optional write routing override consumed by call.
            :return: Runtime invoke result unchanged.
            """
            return self.call(method_name, *args, **kwargs)

        _caller.__name__ = "proxy_{}_{}".format(self._target, method_name)
        _caller.__doc__ = """
Dispatch the local target method {}.{} through the Core invoke routes.

Method existence is checked by the runtime when called. Routing uses the method
name unless a write keyword override is supplied; that override is consumed by
the proxy rather than forwarded to the target.

Example:
    >>> result = dispatcher(*args, **kwargs)  # doctest: +SKIP


:param args: Positional arguments forwarded to the selected target method.
:param kwargs: Target keyword arguments plus an optional write routing override.
:return: Runtime invoke result unchanged; execution failures propagate.
""".format(self._target, method_name)
        return _caller


class LocalDatabaseProxy(_LocalTargetProxy):
    """
    Bind dynamic compatibility method calls to the runtime's database target.

    Example:
        >>> from unittest.mock import Mock
        >>> LocalDatabaseProxy(Mock()).target
        'database'
    """

    def __init__(self, runtime: CoreRuntime) -> None:
        """
        Borrow a runtime and select its database target without opening another connection.

        Example:
            >>> from unittest.mock import Mock
            >>> LocalDatabaseProxy(Mock()).target
            'database'


        :param runtime: Existing runtime through which database methods will be invoked.
        :return: ``None`` after initializing the generic database-target proxy.
        """
        super().__init__(runtime=runtime, target="database")


class LocalStorageProxy(_LocalTargetProxy):
    """
    Bind dynamic compatibility calls to the storage target without probing storage availability.

    Example:
        >>> from unittest.mock import Mock
        >>> LocalStorageProxy(Mock()).target
        'storage'
    """

    def __init__(self, runtime: CoreRuntime) -> None:
        """
        Borrow a runtime and select storage without constructing or starting a storage manager.

        Example:
            >>> from unittest.mock import Mock
            >>> LocalStorageProxy(Mock()).target
            'storage'


        :param runtime: Existing runtime through which storage methods will be invoked.
        :return: ``None`` after binding the generic storage-target proxy.
        """
        super().__init__(runtime=runtime, target="storage")


class LocalJobsProxy(JobsProxyABC):
    """
    Build named job envelopes and require mapping results from the borrowed runtime.

    Each method unwraps the response's result without independently checking its
    success flag; runtime failures propagate. This proxy does not own job execution.

    Example:
        >>> from unittest.mock import Mock
        >>> isinstance(LocalJobsProxy(Mock()), JobsProxyABC)
        True
    """

    def __init__(self, runtime: CoreRuntime) -> None:
        """
        Retain the runtime used for named job operations without constructing a manager.

        Example:
            >>> from unittest.mock import Mock
            >>> runtime = Mock()
            >>> LocalJobsProxy(runtime)._runtime is runtime
            True


        :param runtime: Existing runtime accepting command/query envelopes.
        :return: ``None`` after borrowing the runtime reference.
        """
        self._runtime = runtime

    def list(
        self,
        *,
        states: JobStatesArg | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> Mapping[str, Any]:
        """
        Query jobs.list with normalized state filters and integer-coerced pagination values.

        Negative values are not rejected or clamped here. None state/limit values
        omit their fields, while offset is always supplied. Collection states are
        normalized by the shared helper; scalar state strings remain unchanged.

        Example:
            >>> from unittest.mock import Mock
            >>> runtime = Mock()
            >>> runtime.execute_query.return_value = CoreQueryResult(True, "request-1", result={"jobs": []})
            >>> LocalJobsProxy(runtime).list(states=["RUNNING"], limit=10)
            {'jobs': []}


        :param states: Optional scalar filter text or collection of state tokens.
        :param limit: Optional listing cap converted to an integer without local range checks.
        :param offset: Listing offset converted to an integer and always included.
        :return: Original mapping result from the jobs.list query.
        :raises TypeError: If the endpoint result is not a mapping.
        """
        payload: dict[str, Any] = {"offset": int(offset)}
        normalized_states = normalize_job_states_arg(states)
        if normalized_states is not None:
            payload["states"] = normalized_states
        if limit is not None:
            payload["limit"] = int(limit)
        envelope = CoreQuery(name="jobs.list", payload=payload)
        return _require_mapping(
            self._runtime.execute_query(envelope).result,
            operation="jobs.list",
        )

    def get(self, job_id: str) -> Mapping[str, Any]:
        """
        Query jobs.get using a stripped nonblank identifier and require a mapping response.

        A mapping can represent a missing job; its fields are not validated here.

        Example:
            >>> payload = jobs.get("job-1")  # doctest: +SKIP


        :param job_id: Identifier stringified and stripped by the shared validator.
        :return: Original jobs.get result mapping.
        :raises ValueError: If the normalized identifier is blank.
        :raises TypeError: If the endpoint result is not a mapping.
        """
        envelope = CoreQuery(
            name="jobs.get", payload={"job_id": self.normalize_job_id(job_id)}
        )
        return _require_mapping(
            self._runtime.execute_query(envelope).result,
            operation="jobs.get",
        )

    def wait(self, job_id: str, *, timeout_s: float | None = None) -> Mapping[str, Any]:
        """
        Call jobs.wait with an optional float timeout and return its mapping result.

        None omits the timeout field. No local range/finite check or additional
        deadline is imposed, and a returned record need not describe job success.

        Example:
            >>> payload = jobs.wait("job-1", timeout_s=1.0)  # doctest: +SKIP


        :param job_id: Identifier normalized to nonblank stripped text.
        :param timeout_s: Optional endpoint wait timeout in seconds, converted to float when present.
        :return: Original jobs.wait result mapping, with state interpretation left to the caller.
        :raises ValueError: For a blank normalized job identifier.
        :raises TypeError: If the endpoint result is not a mapping.
        """
        payload: dict[str, Any] = {"job_id": self.normalize_job_id(job_id)}
        if timeout_s is not None:
            payload["timeout_s"] = float(timeout_s)
        envelope = CoreQuery(name="jobs.wait", payload=payload)
        return _require_mapping(
            self._runtime.execute_query(envelope).result,
            operation="jobs.wait",
        )

    def cancel(self, job_id: str) -> Mapping[str, Any]:
        """
        Submit a jobs.cancel command and return its acknowledgment without waiting for termination.

        Example:
            >>> payload = jobs.cancel("job-1")  # doctest: +SKIP


        :param job_id: Identifier normalized to nonblank stripped text.
        :return: Original cancellation result mapping, not proof that execution has stopped.
        :raises ValueError: If the normalized identifier is blank.
        :raises TypeError: If the command result is not a mapping.
        """
        envelope = CoreCommand(
            name="jobs.cancel", payload={"job_id": self.normalize_job_id(job_id)}
        )
        return _require_mapping(
            self._runtime.execute_command(envelope).result,
            operation="jobs.cancel",
        )


class LocalLibraryProxy(_LocalTargetProxy):
    """
    Provide library-target dynamic calls plus shared-runtime Core, database, storage, and job child proxies.

    All children borrow the same runtime. Convenience health/introspection use
    named query envelopes rather than generic library method invocation.

    Example:
        >>> from unittest.mock import Mock
        >>> proxy = LocalLibraryProxy(Mock())
        >>> proxy.target, proxy.database.target, proxy.storage.target
        ('library', 'database', 'storage')
    """

    def __init__(self, runtime: CoreRuntime) -> None:
        """
        Bind library compatibility calls and construct child wrappers around the same runtime.

        Example:
            >>> from unittest.mock import Mock
            >>> proxy = LocalLibraryProxy(Mock())
            >>> proxy.core._runtime is proxy._runtime
            True


        :param runtime: Existing runtime borrowed by this proxy and every child wrapper.
        :return: ``None`` after creating library, Core, database, storage, and jobs entry points.
        """
        super().__init__(runtime=runtime, target="library")
        self.core = LocalCoreClient(runtime)
        self.database = LocalDatabaseProxy(runtime)
        self.storage = LocalStorageProxy(runtime)
        self.jobs = LocalJobsProxy(runtime)

    @property
    def core_uuid(self) -> str:
        """
        Read the shared runtime instance identifier without local caching.

        Example:
            >>> from types import SimpleNamespace
            >>> LocalLibraryProxy(SimpleNamespace(core_uuid="core-1")).core_uuid
            'core-1'


        :return: Runtime core_uuid value unchanged.
        """
        return self._runtime.core_uuid

    @property
    def core_version(self) -> str:
        """
        Read the shared runtime implementation version without normalization.

        Example:
            >>> from types import SimpleNamespace
            >>> LocalLibraryProxy(SimpleNamespace(core_version="2.0.0")).core_version
            '2.0.0'


        :return: Runtime core_version value unchanged.
        """
        return self._runtime.core_version

    def health(self) -> Mapping[str, Any]:
        """
        Execute the named health query and require a mapping without validating health fields.

        Example:
            >>> status = proxy.health()  # doctest: +SKIP


        :return: Original health result mapping without copying or inspecting the envelope success flag.
        :raises TypeError: If the unwrapped result is not a mapping.
        """
        envelope = CoreQuery(name="health")
        return _require_mapping(
            self._runtime.execute_query(envelope).result,
            operation="health",
        )

    def describe_api(
        self, *, include_targets: bool = True, target: str | None = None
    ) -> Mapping[str, Any]:
        """
        Query api.describe with a boolean target-inclusion flag and optional stringified target filter.

        Example:
            >>> description = proxy.describe_api(include_targets=False)  # doctest: +SKIP


        :param include_targets: Truth-converted flag included in the named query payload.
        :param target: Optional filter converted to text; ``None`` omits the field.
        :return: Original introspection result mapping; target validation remains in the runtime.
        :raises TypeError: If the unwrapped endpoint result is not a mapping.
        """
        payload: dict[str, Any] = {"include_targets": bool(include_targets)}
        if target is not None:
            payload["target"] = str(target)
        envelope = CoreQuery(name="api.describe", payload=payload)
        return _require_mapping(
            self._runtime.execute_query(envelope).result,
            operation="api.describe",
        )


__all__ = [
    "LocalCoreClient",
    "LocalCoreProxy",
    "LocalLibraryProxy",
    "LocalDatabaseProxy",
    "LocalStorageProxy",
    "LocalJobsProxy",
]
