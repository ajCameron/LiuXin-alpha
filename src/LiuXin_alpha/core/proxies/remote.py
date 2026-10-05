"""
Call daemon-hosted Core runtimes through synchronous JSON HTTP requests and optional event-polling threads.

Envelope clients use named RPC endpoints; target-specific proxies retain generic
invoke compatibility. Construction validates the endpoint prefix but does not
connect. Responses remain decoded wire values, not reconstructed database rows or
binary objects. No request retry is added except in the explicit event poller.
"""

from __future__ import annotations

import json
import threading
import urllib.parse
import urllib.error
import urllib.request

from typing import Any, Callable, Mapping

from LiuXin_alpha.core.api import CoreAPI
from LiuXin_alpha.core.commands import CoreCommand, CoreCommandResult
from LiuXin_alpha.core.events import CoreEvent
from LiuXin_alpha.core.proxies.jobs import (
    JobStatesArg,
    JobsProxyABC,
    normalize_job_states_arg,
)
from LiuXin_alpha.core.dispatch import looks_like_write_method
from LiuXin_alpha.core.queries import CoreQuery, CoreQueryResult


class RemoteProxyError(RuntimeError):
    """
    Carry remote request/response failure text and optional HTTP status, Core code, and details.

    Structured details are available for parsed HTTP error bodies but are not
    guaranteed for connection, JSON, or unsuccessful-200-envelope failures. A
    request failure does not establish that the server performed no write.

    Example:
        >>> error = RemoteProxyError("Conflict", status_code=409, code="conflict")
        >>> error.status_code, error.code, error.details
        (409, 'conflict', {})
    """

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        code: str | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        """
        Store failure metadata and a shallow copy of details without validating their content.

        Example:
            >>> RemoteProxyError("Failed", details={"committed": True}).details
            {'committed': True}


        :param message: Human-readable failure text passed to RuntimeError.
        :param status_code: Optional HTTP response status retained unchanged.
        :param code: Optional structured Core error code retained unchanged.
        :param details: Optional detail mapping copied at the top level.
        :return: ``None`` after initializing the exception and its metadata.
        """
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.details = dict(details or {})


class _RemoteProxyBase:
    """
    Configure an HTTP endpoint and supply JSON RPC helpers plus dynamic target invocation.

    Request timeout is passed to urllib, not enforced as a total operation budget.
    Endpoint/target attributes are configuration, not proof of server identity or
    target availability. Returned values are decoded JSON without tag decoding.

    Example:
        >>> _RemoteProxyBase(endpoint="http://127.0.0.1:8080/", target="database").endpoint
        'http://127.0.0.1:8080'
    """

    def __init__(
        self, *, endpoint: str, target: str, timeout_seconds: float = 10.0
    ) -> None:
        """
        Normalize endpoint text, require an HTTP(S) prefix, and store target and timeout without connecting.

        Stripping removes surrounding whitespace and all trailing slashes. Prefix
        checking is case-sensitive and is not full URL/hostname validation; timeout
        conversion imposes no range or finiteness check.

        Example:
            >>> proxy = _RemoteProxyBase(endpoint=" https://example.invalid/ ", target="library", timeout_seconds=3)
            >>> proxy.endpoint, proxy.target, proxy.timeout_seconds
            ('https://example.invalid', 'library', 3.0)


        :param endpoint: Base URL text normalized before its HTTP(S) prefix is checked.
        :param target: Logical target label stringified without registry validation.
        :param timeout_seconds: urllib request timeout converted to float.
        :return: ``None`` after storing client configuration.
        :raises ValueError: If the normalized endpoint lacks a lowercase HTTP(S) prefix or timeout conversion fails.
        """
        base = str(endpoint).strip().rstrip("/")
        if not base.startswith("http://") and not base.startswith("https://"):
            raise ValueError(
                "Remote endpoint must be an HTTP URL, got {!r}".format(endpoint)
            )
        self.endpoint = base
        self.target = str(target)
        self.timeout_seconds = float(timeout_seconds)

    def _url(self, suffix: str) -> str:
        """
        Concatenate an endpoint suffix literally, without URL joining or escaping.

        Example:
            >>> _RemoteProxyBase(endpoint="http://127.0.0.1:8080", target="core")._url("/health")
            'http://127.0.0.1:8080/health'


        :param suffix: Route suffix including its required leading slash and any encoded query string.
        :return: Stored endpoint text followed directly by the suffix.
        """
        return self.endpoint + suffix

    def _http_json(
        self, *, method: str, url: str, payload: Mapping[str, Any] | None = None
    ) -> dict[str, Any]:
        """
        Send one JSON request, read the complete response, and require a decoded top-level object.

        Payload encoding uses standard json defaults, not Core's strict wire
        converter. Encoding/request-construction errors precede the HTTP exception
        wrapper and propagate directly. urllib supplies its normal connection,
        redirect, proxy, and TLS policies; this method adds no response-size limit.

        HTTP errors preserve parsed error/code/mapping details when available;
        other request/read errors become RemoteProxyError without a status code.
        Successful HTTP responses are UTF-8 decoded and JSON parsed, but the ``ok``
        field is not interpreted here. Requests are not retried after failure.

        Example:
            >>> response = proxy._http_json(method="GET", url=proxy._url("/health"))  # doctest: +SKIP


        :param method: HTTP method string passed to urllib Request.
        :param url: Complete request URL; this helper does not confine it to the configured endpoint.
        :param payload: Optional JSON-encodable body; ``None`` sends no body or JSON Content-Type header.
        :return: Parsed dictionary response without validating endpoint-specific fields.
        :raises RemoteProxyError: For request/read failure, HTTP error status, invalid UTF-8/JSON, or nonobject JSON.
        """
        body: bytes | None = None
        headers = {"Accept": "application/json"}
        if payload is not None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json; charset=utf-8"
        request = urllib.request.Request(
            url=url, data=body, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(
                request, timeout=self.timeout_seconds
            ) as response:
                raw = response.read()
        except urllib.error.HTTPError as exc:
            try:
                err_body = exc.read().decode("utf-8", errors="replace")
            except Exception:
                err_body = str(exc)
            try:
                error_payload = json.loads(err_body)
            except Exception:
                error_payload = {}
            if not isinstance(error_payload, Mapping):
                error_payload = {}
            error_message = str(error_payload.get("error", err_body))
            details = error_payload.get("error_details", {})
            raise RemoteProxyError(
                "HTTP {} for {} {}: {}".format(
                    exc.code,
                    method,
                    url,
                    error_message,
                ),
                status_code=int(exc.code),
                code=(
                    None
                    if error_payload.get("error_code") is None
                    else str(error_payload.get("error_code"))
                ),
                details=(dict(details) if isinstance(details, Mapping) else {}),
            ) from exc
        except Exception as exc:
            raise RemoteProxyError(
                "HTTP request failed for {} {}: {}".format(method, url, exc)
            ) from exc

        try:
            data = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise RemoteProxyError(
                "Non-JSON response from {} {}: {}".format(method, url, exc)
            ) from exc
        if not isinstance(data, dict):
            raise RemoteProxyError(
                "JSON response must be an object for {} {}.".format(method, url)
            )
        return data

    def _rpc_query(self, name: str, *, payload: Mapping[str, Any] | None = None) -> Any:
        """
        Post a named query with copied payload, require a truthy ok field, and unwrap its result.

        A missing result becomes None. An unsuccessful HTTP-success envelope loses
        structured code/details here; parsed HTTP-error responses retain them in
        the lower-level request helper. No explicit request/correlation IDs are sent.

        Example:
            >>> result = proxy._rpc_query("jobs.list")  # doctest: +SKIP


        :param name: Query route converted to text without local registration checks.
        :param payload: Optional arguments shallow-copied into the request envelope.
        :return: Decoded response result field, or ``None`` when absent.
        :raises RemoteProxyError: If transport fails or the response ok field is missing/falsey.
        """
        body = {
            "name": str(name),
            "payload": dict(payload or {}),
        }
        response = self._http_json(
            method="POST", url=self._url("/rpc/query"), payload=body
        )
        if not bool(response.get("ok", False)):
            raise RemoteProxyError(
                "Remote query failed: {}".format(response.get("error"))
            )
        return response.get("result")

    def _rpc_command(
        self, name: str, *, payload: Mapping[str, Any] | None = None
    ) -> Any:
        """
        Post a named command with copied payload and unwrap its result after a truthiness success check.

        No request/correlation identity is supplied, and no retry or rollback is
        attempted after a response failure. A truthy nonboolean ok value is accepted.

        Example:
            >>> result = proxy._rpc_command("jobs.cancel", payload={"job_id": "job-1"})  # doctest: +SKIP


        :param name: Command route converted to text.
        :param payload: Optional command arguments shallow-copied into the request body.
        :return: Decoded result field, including ``None`` for a missing result.
        :raises RemoteProxyError: For HTTP/request failure or a missing/falsey response ok field.
        """
        body = {
            "name": str(name),
            "payload": dict(payload or {}),
        }
        response = self._http_json(
            method="POST", url=self._url("/rpc/command"), payload=body
        )
        if not bool(response.get("ok", False)):
            raise RemoteProxyError(
                "Remote command failed: {}".format(response.get("error"))
            )
        return response.get("result")

    def _invoke(
        self,
        *,
        method: str,
        call_args: tuple[Any, ...],
        call_kwargs: Mapping[str, Any],
        write: bool,
    ) -> Any:
        """
        Package the configured target and call arguments for the generic invoke command or query.

        This copies outer containers but does not wire-encode arbitrary values;
        unsupported JSON arguments can fail before a request is sent.

        Example:
            >>> result = proxy._invoke(method="get_row", call_args=(1,), call_kwargs={}, write=False)  # doctest: +SKIP


        :param method: Target method name stringified without stripping or existence checks.
        :param call_args: Positional target arguments converted to a list for the JSON payload.
        :param call_kwargs: Target keyword arguments shallow-copied into a dictionary.
        :param write: Truth-tested route selection: command if true, query otherwise.
        :return: Decoded result of the chosen invoke RPC route.
        """
        payload = {
            "target": self.target,
            "method": str(method),
            "args": list(call_args),
            "kwargs": dict(call_kwargs),
        }
        if write:
            return self._rpc_command("invoke", payload=payload)
        return self._rpc_query("invoke", payload=payload)

    def query(self, method: str, *args: Any, **kwargs: Any) -> Any:
        """
        Invoke a configured target method through the query route with all target arguments preserved.

        Route selection does not verify that the target method is side-effect-free.

        Example:
            >>> result = proxy.query("get_row", 1)  # doctest: +SKIP


        :param method: Method label passed to generic invoke without local normalization.
        :param args: Positional target arguments packed into the invoke payload.
        :param kwargs: Target keyword arguments, including any keyword named write.
        :return: Decoded invoke-query result.
        """
        return self._invoke(
            method=method, call_args=tuple(args), call_kwargs=kwargs, write=False
        )

    def command(self, method: str, *args: Any, **kwargs: Any) -> Any:
        """
        Invoke a configured target method explicitly through the command route.

        Example:
            >>> result = proxy.command("set_pref", "view", "list")  # doctest: +SKIP


        :param method: Target method label stringified by the generic invoke helper.
        :param args: Positional target arguments packed into the request.
        :param kwargs: Target keyword arguments forwarded without a special write-control parameter.
        :return: Decoded invoke-command result, without automatic retries after failures.
        """
        return self._invoke(
            method=method, call_args=tuple(args), call_kwargs=kwargs, write=True
        )

    def __getattr__(
        self,
        method_name: str,
    ) -> Callable[..., Any]:
        """
        Build a fresh callable for a missing attribute, choosing command/query by the shared name heuristic.

        No existence probe or caching occurs. The closure calls this object's
        command/query methods, so subclass overrides can select named RPCs instead
        of generic target invocation. Unlike the local target helper, no write
        override keyword is consumed by this closure.

        Example:
            >>> proxy = _RemoteProxyBase(endpoint="http://127.0.0.1:8080", target="database")
            >>> proxy.get_row.__name__
            'remote_proxy_get_row'


        :param method_name: Missing attribute spelling retained for routing and invocation.
        :return: Fresh named callable using the proxy's current command/query implementations.
        """

        def _caller(*args: Any, **kwargs: Any) -> Any:
            """
            Dispatch the captured remote method name through this proxy's command/query methods.

            The name heuristic selects the route on each call. Arguments are not
            checked here, and subclass command/query overrides remain in effect.

            Example:
                >>> result = dispatcher(*args, **kwargs)  # doctest: +SKIP


            :param args: Positional arguments forwarded after the captured method name.
            :param kwargs: Keyword arguments forwarded unchanged, without consuming a write override.
            :return: Selected command/query call result; transport or execution failures propagate.
            """
            if looks_like_write_method(method_name):
                return self.command(method_name, *args, **kwargs)
            return self.query(method_name, *args, **kwargs)

        _caller.__name__ = "remote_proxy_{}".format(method_name)
        return _caller


class RemoteCoreClient(_RemoteProxyBase, CoreAPI):
    """
    Execute named Core envelopes over HTTP and cache advertised identity after health lookup.

    This client uses named endpoints rather than target-level invoke for its
    command/query methods. Identity can become stale if the server behind the
    endpoint changes. Event subscriptions each own an independent polling thread.

    Example:
        >>> RemoteCoreClient(endpoint="http://127.0.0.1:8080").target
        'core'
    """

    def __init__(
        self,
        *,
        endpoint: str,
        timeout_seconds: float = 10.0,
    ) -> None:
        """
        Configure the Core endpoint and an empty identity cache without contacting the daemon.

        Example:
            >>> RemoteCoreClient(endpoint="http://127.0.0.1:8080/", timeout_seconds=2).timeout_seconds
            2.0


        :param endpoint: HTTP(S) base URL normalized and prefix-checked by the remote base class.
        :param timeout_seconds: Request timeout converted to float without a range check.
        :return: ``None`` after configuration and identity-cache initialization.
        """
        super().__init__(
            endpoint=endpoint,
            target="core",
            timeout_seconds=timeout_seconds,
        )
        self._identity: dict[str, str] = {}

    def _identity_value(self, name: str) -> str:
        """
        Return a cached identity field or query health and cache every advertised identity field.

        Only core_uuid/core_version/api_version fields are cached, with values
        stringified even when None. A missing requested field returns empty text
        and is not cached, so later access can repeat the request. Existing cached
        values are never refreshed automatically.

        Example:
            >>> from unittest.mock import Mock
            >>> client = RemoteCoreClient(endpoint="http://127.0.0.1:8080")
            >>> client.health = Mock(return_value={"core_uuid": "core-1"})
            >>> client._identity_value("core_uuid")
            'core-1'


        :param name: Identity-cache key to read, normally one of the three advertised fields.
        :return: Cached/fetched text, or an empty string when the health payload omits the field.
        """
        cached = self._identity.get(name)
        if cached is not None:
            return cached
        payload = self.health()
        for key in ("core_uuid", "core_version", "api_version"):
            if key in payload:
                self._identity[key] = str(payload[key])
        return str(self._identity.get(name, ""))

    @property
    def core_uuid(self) -> str:
        """
        Obtain the advertised instance ID through the lazy identity cache.

        Example:
            >>> identity = client.core_uuid  # doctest: +SKIP


        :return: Cached/fetched instance identifier or empty text when absent, without UUID validation.
        """
        return self._identity_value("core_uuid")

    @property
    def core_version(self) -> str:
        """
        Obtain the runtime version through cached identity or a health request.

        Example:
            >>> version = client.core_version  # doctest: +SKIP


        :return: Advertised version text, or empty text if missing from health.
        """
        return self._identity_value("core_version")

    @property
    def api_version(self) -> str:
        """
        Obtain the advertised API version without performing compatibility negotiation.

        Example:
            >>> version = client.api_version  # doctest: +SKIP


        :return: Cached/fetched API version text or an empty string for a missing field.
        """
        return self._identity_value("api_version")

    def execute_query(self, query: CoreQuery) -> CoreQueryResult:
        """
        Send a query envelope and reconstruct a successful response envelope from decoded JSON.

        Name/ID are stringified and payload copied before sending. Missing response
        ID falls back to the request; correlation does not fall back and is None
        when missing. A truthy ok field is required but need not be a boolean.
        Result tags remain decoded dictionaries rather than native binary/date objects.

        Example:
            >>> response = client.execute_query(CoreQuery("health"))  # doctest: +SKIP


        :param query: Query envelope supplying name, payload, request ID, and correlation token.
        :return: New CoreQueryResult with ok true, decoded result, and normalized response metadata.
        :raises RemoteProxyError: For transport/JSON failure or a falsey/missing response ok field.
        """
        response = self._http_json(
            method="POST",
            url=self._url("/rpc/query"),
            payload={
                "name": str(query.name),
                "payload": dict(query.payload or {}),
                "query_id": str(query.query_id),
                "correlation_id": query.correlation_id,
            },
        )
        if not bool(response.get("ok", False)):
            raise RemoteProxyError(
                "Remote query failed: {}".format(response.get("error"))
            )
        return CoreQueryResult(
            ok=True,
            query_id=str(response.get("query_id", query.query_id)),
            result=response.get("result"),
            error=(
                None if response.get("error") is None else str(response.get("error"))
            ),
            correlation_id=(
                None
                if response.get("correlation_id") is None
                else str(response.get("correlation_id"))
            ),
        )

    def execute_command(self, command: CoreCommand) -> CoreCommandResult:
        """
        Send a command envelope and reconstruct its successful response without retrying the write.

        A missing response ID reuses the request ID; a missing correlation becomes
        None. Response success is truth-tested, not schema-validated. Failure to
        receive a response does not show whether the server committed an effect.

        Example:
            >>> response = client.execute_command(CoreCommand("jobs.cancel", {"job_id": "job-1"}))  # doctest: +SKIP


        :param command: Command envelope with arguments, request identity, and optional correlation token.
        :return: New CoreCommandResult carrying decoded wire values and response metadata.
        :raises RemoteProxyError: For request/response failure or a missing/falsey response ok field.
        """
        response = self._http_json(
            method="POST",
            url=self._url("/rpc/command"),
            payload={
                "name": str(command.name),
                "payload": dict(command.payload or {}),
                "command_id": str(command.command_id),
                "correlation_id": command.correlation_id,
            },
        )
        if not bool(response.get("ok", False)):
            raise RemoteProxyError(
                "Remote command failed: {}".format(response.get("error"))
            )
        return CoreCommandResult(
            ok=True,
            command_id=str(response.get("command_id", command.command_id)),
            result=response.get("result"),
            error=(
                None if response.get("error") is None else str(response.get("error"))
            ),
            correlation_id=(
                None
                if response.get("correlation_id") is None
                else str(response.get("correlation_id"))
            ),
        )

    def query(
        self,
        name: str,
        payload: Mapping[str, Any] | None = None,
        *,
        query_id: str | None = None,
        correlation_id: str | None = None,
    ) -> Any:
        """
        Use the shared envelope adapter to execute a named remote query and unwrap its result.

        Example:
            >>> result = client.query("jobs.list", {"limit": 10})  # doctest: +SKIP


        :param name: Named query route, not a dynamic target method.
        :param payload: Optional query arguments shallow-copied by CoreAPI.query.
        :param query_id: Optional request identity override, otherwise generated by the envelope.
        :param correlation_id: Optional caller correlation token forwarded in the request.
        :return: Decoded response result after execute_query's success check.
        """
        return CoreAPI.query(
            self,
            name,
            payload,
            query_id=query_id,
            correlation_id=correlation_id,
        )

    def command(
        self,
        name: str,
        payload: Mapping[str, Any] | None = None,
        *,
        command_id: str | None = None,
        correlation_id: str | None = None,
    ) -> Any:
        """
        Use the shared envelope adapter to execute a named remote command and unwrap its result.

        Example:
            >>> result = client.command("jobs.cancel", {"job_id": "job-1"})  # doctest: +SKIP


        :param name: Registered command route, not a generic target method name.
        :param payload: Optional arguments shallow-copied by the shared command adapter.
        :param command_id: Optional request identity override, otherwise generated by the envelope.
        :param correlation_id: Optional caller correlation token forwarded to the server.
        :return: Decoded response result; execution/transport failures propagate without retry.
        """
        return CoreAPI.command(
            self,
            name,
            payload,
            command_id=command_id,
            correlation_id=correlation_id,
        )

    def describe_api(
        self,
        *,
        include_targets: bool = True,
        target: str | None = None,
    ) -> dict[str, Any]:
        """
        Request api.describe and require a mapping-shaped result before returning a shallow copy.

        Example:
            >>> description = client.describe_api(include_targets=False)  # doctest: +SKIP


        :param include_targets: Truth-converted target-description inclusion flag.
        :param target: Optional target filter stringified when supplied.
        :return: New top-level dictionary containing the server's introspection payload.
        :raises RemoteProxyError: If the named query fails or its result is not a mapping.
        """
        payload: dict[str, Any] = {
            "include_targets": bool(include_targets),
        }
        if target is not None:
            payload["target"] = str(target)
        result = self.query("api.describe", payload)
        if not isinstance(result, Mapping):
            raise RemoteProxyError("Remote API description must be an object.")
        return dict(result)

    def subscribe(
        self,
        callback: Callable[[CoreEvent], None],
    ) -> Callable[[], None]:
        """
        Start a daemon thread polling events from sequence zero and deliver accepted records to a callback.

        Each subscription has its own cursor and stop event. HTTP failures retry
        after 50 ms; malformed records and callback exceptions are silently dropped.
        Cursor advancement can precede validation/delivery, so this is not lossless
        consumption. Callbacks run on the polling thread. No thread registry ties
        subscriptions to shutdown; callers must retain and invoke the unsubscribe
        closure. Unsubscription waits only for a bounded join, not guaranteed exit.

        Example:
            >>> unsubscribe = client.subscribe(print)  # doctest: +SKIP
            >>> unsubscribe()  # doctest: +SKIP


        :param callback: Callable receiving reconstructed CoreEvent records on the poller thread.
        :return: Stop-and-bounded-join closure for this subscription.
        :raises TypeError: If callback is not callable.
        """

        if not callable(callback):
            raise TypeError("Core event subscriber must be callable.")
        stopped = threading.Event()

        def _poll() -> None:
            """
            Poll from cursor zero until stopped, skipping bad responses/events and swallowing callback failures.

            Server long-poll timeout is clamped between 0.05 and 1 second, while
            HTTP requests still use the client's configured timeout. Cursor parse
            failures retain the previous cursor; valid cursor updates happen before
            event validation. Malformed successful responses have no added backoff.

            Example:
                >>> _poll()  # doctest: +SKIP


            :return: ``None`` once the stop flag is observed, except for an unhandled loop-setup error.
            """
            sequence = 0
            while not stopped.is_set():
                params = urllib.parse.urlencode(
                    {
                        "after": sequence,
                        "timeout": min(
                            1.0,
                            max(0.05, self.timeout_seconds),
                        ),
                    }
                )
                try:
                    response = self._http_json(
                        method="GET",
                        url=self._url("/events/next?{}".format(params)),
                    )
                except Exception:
                    if stopped.is_set():
                        return
                    stopped.wait(0.05)
                    continue
                result = response.get("result", {})
                if not isinstance(result, Mapping):
                    continue
                try:
                    sequence = int(str(result.get("next_sequence", sequence)))
                except Exception:
                    pass
                raw_event = result.get("event")
                if not isinstance(raw_event, Mapping):
                    continue
                raw_payload = raw_event.get("payload", {})
                if not isinstance(raw_payload, Mapping):
                    continue
                try:
                    event = CoreEvent(
                        event_id=str(raw_event["event_id"]),
                        core_uuid=str(raw_event["core_uuid"]),
                        event_type=str(raw_event["event_type"]),
                        timestamp_utc=str(raw_event["timestamp_utc"]),
                        payload={str(key): value for key, value in raw_payload.items()},
                    )
                    callback(event)
                except Exception:
                    continue

        thread = threading.Thread(
            target=_poll,
            name="liuxin-core-event-client",
            daemon=True,
        )
        thread.start()

        def _unsubscribe() -> None:
            """
            Set the subscription stop event and wait at most the configured bounded join interval.

            Does not interrupt an in-flight request or callback, and the thread
            can outlive return. Calling from that thread attempts a self-join and
            can raise after already setting the stop flag.

            Example:
                >>> unsubscribe()  # doctest: +SKIP


            :return: ``None`` after signalling stop and attempting the bounded join.
            """
            stopped.set()
            thread.join(timeout=min(2.0, self.timeout_seconds + 0.2))

        return _unsubscribe

    def shutdown(self) -> int:
        """
        Send the named shutdown command to the hosted runtime and integer-convert its result.

        This is a server-runtime operation, not a local client disconnect; it does
        not automatically stop this client's independent subscription threads.

        Example:
            >>> exit_code = client.shutdown()  # doctest: +SKIP


        :return: Server-reported shutdown status converted to an integer.
        """
        result = self.command("shutdown")
        return int(result)


RemoteCoreProxy = RemoteCoreClient


class RemoteDatabaseProxy(_RemoteProxyBase):
    """
    Route generic remote method invocations to the database target.

    Example:
        >>> RemoteDatabaseProxy(endpoint="http://127.0.0.1:8080").target
        'database'
    """

    def __init__(self, *, endpoint: str, timeout_seconds: float = 10.0) -> None:
        """
        Configure the database-target proxy without opening a connection or probing methods.

        Example:
            >>> RemoteDatabaseProxy(endpoint="http://127.0.0.1:8080").timeout_seconds
            10.0


        :param endpoint: HTTP(S) daemon base URL normalized by the base constructor.
        :param timeout_seconds: Request timeout in seconds, converted to float.
        :return: ``None`` after storing database-target configuration.
        """
        super().__init__(
            endpoint=endpoint, target="database", timeout_seconds=timeout_seconds
        )


class RemoteStorageProxy(_RemoteProxyBase):
    """
    Route generic remote method invocations to storage without checking manager availability at construction.

    Example:
        >>> RemoteStorageProxy(endpoint="http://127.0.0.1:8080").target
        'storage'
    """

    def __init__(self, *, endpoint: str, timeout_seconds: float = 10.0) -> None:
        """
        Configure storage-target HTTP invocation without starting a remote storage manager.

        Example:
            >>> RemoteStorageProxy(endpoint="http://127.0.0.1:8080").target
            'storage'


        :param endpoint: HTTP(S) daemon base URL normalized without a reachability check.
        :param timeout_seconds: Float-coerced request timeout in seconds.
        :return: ``None`` after binding storage-target configuration.
        """
        super().__init__(
            endpoint=endpoint, target="storage", timeout_seconds=timeout_seconds
        )


class RemoteJobsProxy(_RemoteProxyBase, JobsProxyABC):
    """
    Call named remote job endpoints with shared filter/identifier normalization.

    Dictionary results are copied. Unlike LocalJobsProxy, a successful RPC with
    a non-dictionary result becomes an empty dictionary instead of raising, so
    an empty payload is not proof of absent jobs or a valid server response.

    Example:
        >>> isinstance(RemoteJobsProxy(endpoint="http://127.0.0.1:8080"), JobsProxyABC)
        True
    """

    def __init__(self, *, endpoint: str, timeout_seconds: float = 10.0) -> None:
        """
        Configure named-job requests while retaining library as the unused generic-target label.

        Example:
            >>> RemoteJobsProxy(endpoint="http://127.0.0.1:8080").target
            'library'


        :param endpoint: HTTP(S) daemon base URL normalized without connecting.
        :param timeout_seconds: HTTP request timeout, independent of a later job-wait timeout.
        :return: ``None`` after remote-base initialization.
        """
        # `target` is unused for named jobs.* RPC calls but retained for consistency.
        super().__init__(
            endpoint=endpoint, target="library", timeout_seconds=timeout_seconds
        )

    def list(
        self,
        *,
        states: JobStatesArg | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> Mapping[str, Any]:
        """
        Query jobs.list with normalized filters and integer pagination, copying dictionaries or returning empty for other results.

        None fields are omitted except offset, which is always sent. No local
        nonnegative check is applied to limit/offset, and scalar state strings
        remain unnormalized under the shared state helper.

        Example:
            >>> payload = jobs.list(states=["running"], limit=10)  # doctest: +SKIP


        :param states: Optional scalar state text or collection normalized to sorted unique tokens.
        :param limit: Optional cap converted to an integer without local range validation.
        :param offset: Offset converted to an integer and always included.
        :return: Shallow dictionary copy of the listing result, or an empty dictionary for a non-dictionary result.
        """
        payload: dict[str, Any] = {"offset": int(offset)}
        normalized_states = normalize_job_states_arg(states)
        if normalized_states is not None:
            payload["states"] = normalized_states
        if limit is not None:
            payload["limit"] = int(limit)
        result = self._rpc_query("jobs.list", payload=payload)
        return dict(result if isinstance(result, dict) else {})

    def get(self, job_id: str) -> Mapping[str, Any]:
        """
        Query jobs.get for a normalized identifier, flattening non-dictionary successful results to empty dictionaries.

        Example:
            >>> payload = jobs.get("job-1")  # doctest: +SKIP


        :param job_id: Identifier stringified, stripped, and required to be nonblank.
        :return: Copied job detail dictionary or empty dictionary for an unexpected result shape.
        :raises ValueError: If identifier normalization produces blank text.
        """
        result = self._rpc_query(
            "jobs.get", payload={"job_id": self.normalize_job_id(job_id)}
        )
        return dict(result if isinstance(result, dict) else {})

    def wait(self, job_id: str, *, timeout_s: float | None = None) -> Mapping[str, Any]:
        """
        Request jobs.wait with an optional endpoint timeout, still subject to the separate HTTP request timeout.

        None omits the endpoint timeout. A returned job need not be terminal or
        successful; an unexpected non-dictionary result becomes an empty dictionary.

        Example:
            >>> payload = jobs.wait("job-1", timeout_s=1.0)  # doctest: +SKIP


        :param job_id: Identifier normalized to nonblank stripped text.
        :param timeout_s: Optional float-coerced server wait timeout in seconds, without local range checks.
        :return: Copied result dictionary or empty dictionary for a non-dictionary RPC result.
        :raises ValueError: If the normalized job identifier is blank.
        """
        payload: dict[str, Any] = {"job_id": self.normalize_job_id(job_id)}
        if timeout_s is not None:
            payload["timeout_s"] = float(timeout_s)
        result = self._rpc_query("jobs.wait", payload=payload)
        return dict(result if isinstance(result, dict) else {})

    def cancel(self, job_id: str) -> Mapping[str, Any]:
        """
        Submit jobs.cancel and return a copied acknowledgment without awaiting job termination.

        Example:
            >>> payload = jobs.cancel("job-1")  # doctest: +SKIP


        :param job_id: Identifier normalized to nonblank stripped text.
        :return: Copied cancellation dictionary, or an empty dictionary for an unexpected successful result shape.
        :raises ValueError: If identifier normalization yields blank text.
        """
        result = self._rpc_command(
            "jobs.cancel", payload={"job_id": self.normalize_job_id(job_id)}
        )
        return dict(result if isinstance(result, dict) else {})


class RemoteLibraryProxy(_RemoteProxyBase):
    """
    Group library-target compatibility calls with independently configured Core/database/storage/job child proxies.

    Each child receives the same initial endpoint/timeout values, not a shared
    mutable configuration object. Changing a parent attribute later does not
    update its children. This object starts no daemon or subscription by itself.

    Example:
        >>> proxy = RemoteLibraryProxy(endpoint="http://127.0.0.1:8080")
        >>> proxy.target, proxy.database.target, proxy.storage.target
        ('library', 'database', 'storage')
    """

    def __init__(self, *, endpoint: str, timeout_seconds: float = 10.0) -> None:
        """
        Configure library invocation and construct Core, database, storage, and jobs wrappers for the same endpoint.

        Example:
            >>> proxy = RemoteLibraryProxy(endpoint="http://127.0.0.1:8080/")
            >>> proxy.core.endpoint == proxy.endpoint
            True


        :param endpoint: Initial HTTP(S) URL supplied to this proxy and every child constructor.
        :param timeout_seconds: Initial float-coerced request timeout for each independent wrapper.
        :return: ``None`` after constructing child proxy objects without network requests.
        """
        super().__init__(
            endpoint=endpoint, target="library", timeout_seconds=timeout_seconds
        )
        self.core = RemoteCoreClient(
            endpoint=endpoint,
            timeout_seconds=timeout_seconds,
        )
        self.database = RemoteDatabaseProxy(
            endpoint=endpoint, timeout_seconds=timeout_seconds
        )
        self.storage = RemoteStorageProxy(
            endpoint=endpoint, timeout_seconds=timeout_seconds
        )
        self.jobs = RemoteJobsProxy(endpoint=endpoint, timeout_seconds=timeout_seconds)

    def health(self) -> Mapping[str, Any]:
        """
        GET the daemon's health route, require truthy envelope success, and dictionary-convert its result.

        Missing result defaults to empty. This path uses dict directly, so an
        iterable of pairs may work while None/scalar results can raise raw conversion
        errors; it is not the envelope client's mapping-checking health adapter.

        Example:
            >>> status = proxy.health()  # doctest: +SKIP


        :return: New dictionary converted from the health response's result field.
        :raises RemoteProxyError: For request failure or a missing/falsey response ok field.
        """
        response = self._http_json(method="GET", url=self._url("/health"))
        if not bool(response.get("ok", False)):
            raise RemoteProxyError(
                "Health request failed: {}".format(response.get("error"))
            )
        return dict(response.get("result", {}))

    def describe_api(
        self, *, include_targets: bool = True, target: str | None = None
    ) -> Mapping[str, Any]:
        """
        Query api.describe, copying a dictionary result or returning empty for any other successful result shape.

        Example:
            >>> description = proxy.describe_api(include_targets=False)  # doctest: +SKIP


        :param include_targets: Truth-converted flag for including dynamic target descriptions.
        :param target: Optional target filter converted to text when present.
        :return: Shallow description dictionary, or an empty dictionary if the result was not a dictionary.
        """
        payload: dict[str, Any] = {"include_targets": bool(include_targets)}
        if target is not None:
            payload["target"] = str(target)
        result = self._rpc_query("api.describe", payload=payload)
        return dict(result if isinstance(result, dict) else {})


__all__ = [
    "RemoteProxyError",
    "RemoteCoreClient",
    "RemoteCoreProxy",
    "RemoteLibraryProxy",
    "RemoteDatabaseProxy",
    "RemoteStorageProxy",
    "RemoteJobsProxy",
]
