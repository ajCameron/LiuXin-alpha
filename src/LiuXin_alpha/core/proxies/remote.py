"""HTTP-backed remote proxies for daemon-hosted core runtimes."""

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
from LiuXin_alpha.core.proxies.jobs import JobStatesArg, JobsProxyABC, normalize_job_states_arg
from LiuXin_alpha.core.dispatch import looks_like_write_method
from LiuXin_alpha.core.queries import CoreQuery, CoreQueryResult


class RemoteProxyError(RuntimeError):
    """Raised when remote proxy requests fail."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        code: str | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.details = dict(details or {})


class _RemoteProxyBase:
    """Base remote proxy for one logical target."""

    def __init__(self, *, endpoint: str, target: str, timeout_seconds: float = 10.0) -> None:
        base = str(endpoint).strip().rstrip("/")
        if not base.startswith("http://") and not base.startswith("https://"):
            raise ValueError("Remote endpoint must be an HTTP URL, got {!r}".format(endpoint))
        self.endpoint = base
        self.target = str(target)
        self.timeout_seconds = float(timeout_seconds)

    def _url(self, suffix: str) -> str:
        return self.endpoint + suffix

    def _http_json(self, *, method: str, url: str, payload: Mapping[str, Any] | None = None) -> dict[str, Any]:
        body: bytes | None = None
        headers = {"Accept": "application/json"}
        if payload is not None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json; charset=utf-8"
        request = urllib.request.Request(url=url, data=body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
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
            error_message = str(
                error_payload.get("error", err_body)
            )
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
                details=(
                    dict(details)
                    if isinstance(details, Mapping)
                    else {}
                ),
            ) from exc
        except Exception as exc:
            raise RemoteProxyError("HTTP request failed for {} {}: {}".format(method, url, exc)) from exc

        try:
            data = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise RemoteProxyError("Non-JSON response from {} {}: {}".format(method, url, exc)) from exc
        if not isinstance(data, dict):
            raise RemoteProxyError("JSON response must be an object for {} {}.".format(method, url))
        return data

    def _rpc_query(self, name: str, *, payload: Mapping[str, Any] | None = None) -> Any:
        body = {
            "name": str(name),
            "payload": dict(payload or {}),
        }
        response = self._http_json(method="POST", url=self._url("/rpc/query"), payload=body)
        if not bool(response.get("ok", False)):
            raise RemoteProxyError("Remote query failed: {}".format(response.get("error")))
        return response.get("result")

    def _rpc_command(self, name: str, *, payload: Mapping[str, Any] | None = None) -> Any:
        body = {
            "name": str(name),
            "payload": dict(payload or {}),
        }
        response = self._http_json(method="POST", url=self._url("/rpc/command"), payload=body)
        if not bool(response.get("ok", False)):
            raise RemoteProxyError("Remote command failed: {}".format(response.get("error")))
        return response.get("result")

    def _invoke(self, *, method: str, call_args: tuple[Any, ...], call_kwargs: Mapping[str, Any], write: bool) -> Any:
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
        return self._invoke(method=method, call_args=tuple(args), call_kwargs=kwargs, write=False)

    def command(self, method: str, *args: Any, **kwargs: Any) -> Any:
        return self._invoke(method=method, call_args=tuple(args), call_kwargs=kwargs, write=True)

    def __getattr__(
        self,
        method_name: str,
    ) -> Callable[..., Any]:
        def _caller(*args: Any, **kwargs: Any) -> Any:
            if looks_like_write_method(method_name):
                return self.command(method_name, *args, **kwargs)
            return self.query(method_name, *args, **kwargs)

        _caller.__name__ = "remote_proxy_{}".format(method_name)
        return _caller


class RemoteCoreClient(_RemoteProxyBase, CoreAPI):
    """Envelope-level Core client with the same contract as ``CoreRuntime``."""

    def __init__(
        self,
        *,
        endpoint: str,
        timeout_seconds: float = 10.0,
    ) -> None:
        super().__init__(
            endpoint=endpoint,
            target="core",
            timeout_seconds=timeout_seconds,
        )
        self._identity: dict[str, str] = {}

    def _identity_value(self, name: str) -> str:
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
        return self._identity_value("core_uuid")

    @property
    def core_version(self) -> str:
        return self._identity_value("core_version")

    @property
    def api_version(self) -> str:
        return self._identity_value("api_version")

    def execute_query(self, query: CoreQuery) -> CoreQueryResult:
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
                None
                if response.get("error") is None
                else str(response.get("error"))
            ),
            correlation_id=(
                None
                if response.get("correlation_id") is None
                else str(response.get("correlation_id"))
            ),
        )

    def execute_command(self, command: CoreCommand) -> CoreCommandResult:
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
            command_id=str(
                response.get("command_id", command.command_id)
            ),
            result=response.get("result"),
            error=(
                None
                if response.get("error") is None
                else str(response.get("error"))
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
        payload: dict[str, Any] = {
            "include_targets": bool(include_targets),
        }
        if target is not None:
            payload["target"] = str(target)
        result = self.query("api.describe", payload)
        if not isinstance(result, Mapping):
            raise RemoteProxyError(
                "Remote API description must be an object."
            )
        return dict(result)

    def subscribe(
        self,
        callback: Callable[[CoreEvent], None],
    ) -> Callable[[], None]:
        """Poll the daemon event stream until the returned function is called."""

        if not callable(callback):
            raise TypeError("Core event subscriber must be callable.")
        stopped = threading.Event()

        def _poll() -> None:
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
                    sequence = int(
                        str(result.get("next_sequence", sequence))
                    )
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
                        payload={
                            str(key): value
                            for key, value in raw_payload.items()
                        },
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
            stopped.set()
            thread.join(timeout=min(2.0, self.timeout_seconds + 0.2))

        return _unsubscribe

    def shutdown(self) -> int:
        result = self.command("shutdown")
        return int(result)


RemoteCoreProxy = RemoteCoreClient


class RemoteDatabaseProxy(_RemoteProxyBase):
    """Remote proxy for `database` target."""

    def __init__(self, *, endpoint: str, timeout_seconds: float = 10.0) -> None:
        super().__init__(endpoint=endpoint, target="database", timeout_seconds=timeout_seconds)


class RemoteStorageProxy(_RemoteProxyBase):
    """Remote proxy for `storage` target."""

    def __init__(self, *, endpoint: str, timeout_seconds: float = 10.0) -> None:
        super().__init__(endpoint=endpoint, target="storage", timeout_seconds=timeout_seconds)


class RemoteJobsProxy(_RemoteProxyBase, JobsProxyABC):
    """Remote proxy for explicit core jobs APIs."""

    def __init__(self, *, endpoint: str, timeout_seconds: float = 10.0) -> None:
        # `target` is unused for named jobs.* RPC calls but retained for consistency.
        super().__init__(endpoint=endpoint, target="library", timeout_seconds=timeout_seconds)

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
        result = self._rpc_query("jobs.list", payload=payload)
        return dict(result if isinstance(result, dict) else {})

    def get(self, job_id: str) -> Mapping[str, Any]:
        result = self._rpc_query("jobs.get", payload={"job_id": self.normalize_job_id(job_id)})
        return dict(result if isinstance(result, dict) else {})

    def wait(self, job_id: str, *, timeout_s: float | None = None) -> Mapping[str, Any]:
        payload: dict[str, Any] = {"job_id": self.normalize_job_id(job_id)}
        if timeout_s is not None:
            payload["timeout_s"] = float(timeout_s)
        result = self._rpc_query("jobs.wait", payload=payload)
        return dict(result if isinstance(result, dict) else {})

    def cancel(self, job_id: str) -> Mapping[str, Any]:
        result = self._rpc_command("jobs.cancel", payload={"job_id": self.normalize_job_id(job_id)})
        return dict(result if isinstance(result, dict) else {})


class RemoteLibraryProxy(_RemoteProxyBase):
    """Top-level remote proxy for `library` target."""

    def __init__(self, *, endpoint: str, timeout_seconds: float = 10.0) -> None:
        super().__init__(endpoint=endpoint, target="library", timeout_seconds=timeout_seconds)
        self.core = RemoteCoreClient(
            endpoint=endpoint,
            timeout_seconds=timeout_seconds,
        )
        self.database = RemoteDatabaseProxy(endpoint=endpoint, timeout_seconds=timeout_seconds)
        self.storage = RemoteStorageProxy(endpoint=endpoint, timeout_seconds=timeout_seconds)
        self.jobs = RemoteJobsProxy(endpoint=endpoint, timeout_seconds=timeout_seconds)

    def health(self) -> Mapping[str, Any]:
        response = self._http_json(method="GET", url=self._url("/health"))
        if not bool(response.get("ok", False)):
            raise RemoteProxyError("Health request failed: {}".format(response.get("error")))
        return dict(response.get("result", {}))

    def describe_api(self, *, include_targets: bool = True, target: str | None = None) -> Mapping[str, Any]:
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
