"""Phase-1 core runtime scaffold.

The runtime is transport-agnostic and can be hosted directly in-process or by a
daemon wrapper.
"""

from __future__ import annotations

import inspect
import threading
import uuid

from typing import Any, Callable, Mapping, Optional

from LiuXin_alpha.core.api import CoreAPI
from LiuXin_alpha.core.commands import CoreCommand, CoreCommandResult
from LiuXin_alpha.core.description import (
    CoreEndpointDescription,
    CoreMethodDescription,
    CoreParameterDescription,
    CorePayloadFieldDescription,
    CoreTargetDescription,
)
from LiuXin_alpha.core.dispatch import looks_like_write_method
from LiuXin_alpha.core.errors import CoreDispatchError, CoreHandlerError, CoreShutdownError
from LiuXin_alpha.core.events import CoreEvent, make_core_event
from LiuXin_alpha.core.queries import CoreQuery, CoreQueryResult
from LiuXin_alpha.utils.jobs import JobRequest, default_job_manager
from LiuXin_alpha.utils.jobs.manager import JobManagerAPI


CommandHandler = Callable[["CoreRuntime", CoreCommand], Any]
QueryHandler = Callable[["CoreRuntime", CoreQuery], Any]
EventSubscriber = Callable[[CoreEvent], None]


class CoreRuntime(CoreAPI):
    """In-process runtime orchestrating command/query handlers and events."""

    def __init__(
        self,
        *,
        library: Any,
        core_uuid: Optional[str] = None,
        core_version: str = "0.1.0-phase1",
        job_manager: JobManagerAPI | None = None,
    ) -> None:
        self._library = library
        self._core_uuid = str(core_uuid or uuid.uuid4())
        self._core_version = str(core_version)
        self._shutdown = False
        self._job_manager: JobManagerAPI = job_manager if job_manager is not None else default_job_manager()

        self._command_handlers: dict[str, CommandHandler] = {}
        self._query_handlers: dict[str, QueryHandler] = {}
        self._command_descriptions: dict[str, CoreEndpointDescription] = {}
        self._query_descriptions: dict[str, CoreEndpointDescription] = {}
        self._event_subscribers: list[EventSubscriber] = []

        # Write-path serialization.
        self._command_lock = threading.RLock()
        # Protects handler and subscriber maps.
        self._state_lock = threading.RLock()

        self.register_command_handler(
            "invoke",
            self._handle_invoke_command,
            summary="Invoke a write-path method on a hosted target.",
            description="Generic escape hatch for library/database/storage write calls.",
            payload_fields=(
                CorePayloadFieldDescription(name="target", required=True, field_type="string"),
                CorePayloadFieldDescription(name="method", required=True, field_type="string"),
                CorePayloadFieldDescription(name="args", field_type="array"),
                CorePayloadFieldDescription(name="kwargs", field_type="object"),
            ),
            tags=("generic", "invoke"),
            transport_stable=False,
        )
        self.register_command_handler(
            "shutdown",
            self._handle_shutdown_command,
            summary="Shut the runtime down.",
            tags=("lifecycle",),
        )
        self.register_command_handler(
            "sync.store.start",
            self._handle_sync_store_start_command,
            summary="Submit a store sync as a managed background job.",
            payload_fields=(
                CorePayloadFieldDescription(name="sync_kwargs", required=True, field_type="object"),
                CorePayloadFieldDescription(name="job_timeout_s", field_type="number"),
                CorePayloadFieldDescription(name="job_no_output", field_type="boolean"),
                CorePayloadFieldDescription(name="job_backend", field_type="string"),
                CorePayloadFieldDescription(name="label", field_type="string"),
            ),
            tags=("sync", "jobs"),
        )
        self.register_command_handler(
            "sync.store.cancel",
            self._handle_sync_store_cancel_command,
            summary="Cancel a previously submitted sync job.",
            payload_fields=(CorePayloadFieldDescription(name="job_id", required=True, field_type="string"),),
            tags=("sync", "jobs"),
        )
        self.register_command_handler(
            "jobs.cancel",
            self._handle_jobs_cancel_command,
            summary="Cancel an existing managed job.",
            payload_fields=(CorePayloadFieldDescription(name="job_id", required=True, field_type="string"),),
            tags=("jobs",),
        )
        self.register_query_handler(
            "invoke",
            self._handle_invoke_query,
            summary="Invoke a read-path method on a hosted target.",
            description="Generic escape hatch for library/database/storage query calls.",
            payload_fields=(
                CorePayloadFieldDescription(name="target", required=True, field_type="string"),
                CorePayloadFieldDescription(name="method", required=True, field_type="string"),
                CorePayloadFieldDescription(name="args", field_type="array"),
                CorePayloadFieldDescription(name="kwargs", field_type="object"),
            ),
            tags=("generic", "invoke"),
            transport_stable=False,
        )
        self.register_query_handler(
            "health",
            self._handle_health_query,
            summary="Return runtime health and registration state.",
            tags=("lifecycle", "health"),
        )
        self.register_query_handler(
            "api.describe",
            self._handle_api_describe_query,
            summary="Describe the available core API surface.",
            payload_fields=(
                CorePayloadFieldDescription(name="include_targets", field_type="boolean"),
                CorePayloadFieldDescription(name="target", field_type="string"),
            ),
            tags=("api", "introspection"),
        )
        self.register_query_handler(
            "jobs.list",
            self._handle_jobs_list_query,
            summary="List managed jobs with optional filtering and pagination.",
            payload_fields=(
                CorePayloadFieldDescription(name="states", field_type="array"),
                CorePayloadFieldDescription(name="limit", field_type="integer"),
                CorePayloadFieldDescription(name="offset", field_type="integer"),
            ),
            tags=("jobs",),
        )
        self.register_query_handler(
            "jobs.get",
            self._handle_jobs_get_query,
            summary="Fetch one managed job by id.",
            payload_fields=(CorePayloadFieldDescription(name="job_id", required=True, field_type="string"),),
            tags=("jobs",),
        )
        self.register_query_handler(
            "jobs.wait",
            self._handle_jobs_wait_query,
            summary="Wait for a managed job to finish.",
            payload_fields=(
                CorePayloadFieldDescription(name="job_id", required=True, field_type="string"),
                CorePayloadFieldDescription(name="timeout_s", field_type="number"),
            ),
            tags=("jobs",),
        )

    @property
    def core_uuid(self) -> str:
        return self._core_uuid

    @property
    def core_version(self) -> str:
        return self._core_version

    @property
    def library(self) -> Any:
        """Expose hosted library object for local call paths."""
        return self._library

    @property
    def job_manager(self) -> JobManagerAPI:
        """Job manager used by command handlers for long-running work."""
        return self._job_manager

    @property
    def is_shutdown(self) -> bool:
        return bool(self._shutdown)

    def shutdown(self) -> int:
        if self._shutdown:
            return 0
        self._shutdown = True
        self.emit_event("core.shutdown", {"reason": "explicit"})
        return 0

    @staticmethod
    def _doc_summary(obj: Any) -> str:
        doc = inspect.getdoc(obj) or ""
        text = doc.strip()
        if not text:
            return ""
        return text.splitlines()[0].strip()

    @staticmethod
    def _normalize_payload_fields(
        payload_fields: tuple[CorePayloadFieldDescription, ...] | list[CorePayloadFieldDescription] | None,
    ) -> tuple[CorePayloadFieldDescription, ...]:
        if not payload_fields:
            return ()
        return tuple(payload_fields)

    def register_command_handler(
        self,
        name: str,
        handler: CommandHandler,
        *,
        summary: str | None = None,
        description: str = "",
        payload_fields: tuple[CorePayloadFieldDescription, ...] | list[CorePayloadFieldDescription] | None = None,
        tags: tuple[str, ...] | list[str] | None = None,
        transport_stable: bool = True,
    ) -> None:
        token = str(name).strip().lower()
        if not token:
            raise ValueError("Command name cannot be blank.")
        with self._state_lock:
            self._command_handlers[token] = handler
            self._command_descriptions[token] = CoreEndpointDescription(
                name=token,
                kind="command",
                summary=str(summary if summary is not None else self._doc_summary(handler)),
                description=str(description or ""),
                payload_fields=self._normalize_payload_fields(payload_fields),
                tags=tuple(str(tag) for tag in (tags or ())),
                transport_stable=bool(transport_stable),
            )

    def register_query_handler(
        self,
        name: str,
        handler: QueryHandler,
        *,
        summary: str | None = None,
        description: str = "",
        payload_fields: tuple[CorePayloadFieldDescription, ...] | list[CorePayloadFieldDescription] | None = None,
        tags: tuple[str, ...] | list[str] | None = None,
        transport_stable: bool = True,
    ) -> None:
        token = str(name).strip().lower()
        if not token:
            raise ValueError("Query name cannot be blank.")
        with self._state_lock:
            self._query_handlers[token] = handler
            self._query_descriptions[token] = CoreEndpointDescription(
                name=token,
                kind="query",
                summary=str(summary if summary is not None else self._doc_summary(handler)),
                description=str(description or ""),
                payload_fields=self._normalize_payload_fields(payload_fields),
                tags=tuple(str(tag) for tag in (tags or ())),
                transport_stable=bool(transport_stable),
            )

    def subscribe(self, callback: EventSubscriber) -> Callable[[], None]:
        with self._state_lock:
            self._event_subscribers.append(callback)

        def _unsubscribe() -> None:
            self.unsubscribe(callback)

        return _unsubscribe

    def unsubscribe(self, callback: EventSubscriber) -> None:
        with self._state_lock:
            try:
                self._event_subscribers.remove(callback)
            except ValueError:
                pass

    def emit_event(self, event_type: str, payload: Mapping[str, Any] | None = None) -> CoreEvent:
        event = make_core_event(core_uuid=self.core_uuid, event_type=event_type, payload=payload)
        with self._state_lock:
            subscribers = list(self._event_subscribers)
        for subscriber in subscribers:
            try:
                subscriber(event)
            except Exception:
                # Event handlers are best-effort and must not break runtime flow.
                continue
        return event

    def execute_command(self, command: CoreCommand) -> CoreCommandResult:
        if self._shutdown:
            raise CoreShutdownError("Core is shut down.")

        token = str(command.name).strip().lower()
        if not token:
            raise CoreDispatchError("Command name cannot be blank.")

        with self._state_lock:
            handler = self._command_handlers.get(token)
        if handler is None:
            raise CoreDispatchError("Unknown command handler: {!r}".format(command.name))

        self.emit_event(
            "command.started",
            {"command_id": command.command_id, "name": token, "correlation_id": command.correlation_id},
        )

        with self._command_lock:
            try:
                result = handler(self, command)
            except Exception as exc:
                self.emit_event(
                    "command.failed",
                    {
                        "command_id": command.command_id,
                        "name": token,
                        "error": str(exc),
                        "correlation_id": command.correlation_id,
                    },
                )
                raise CoreHandlerError("Command handler failed for {!r}: {}".format(token, exc)) from exc

        self.emit_event(
            "command.finished",
            {"command_id": command.command_id, "name": token, "correlation_id": command.correlation_id},
        )
        return CoreCommandResult(
            ok=True,
            command_id=command.command_id,
            result=result,
            correlation_id=command.correlation_id,
        )

    def execute_query(self, query: CoreQuery) -> CoreQueryResult:
        if self._shutdown:
            raise CoreShutdownError("Core is shut down.")

        token = str(query.name).strip().lower()
        if not token:
            raise CoreDispatchError("Query name cannot be blank.")

        with self._state_lock:
            handler = self._query_handlers.get(token)
        if handler is None:
            raise CoreDispatchError("Unknown query handler: {!r}".format(query.name))

        try:
            result = handler(self, query)
        except Exception as exc:
            raise CoreHandlerError("Query handler failed for {!r}: {}".format(token, exc)) from exc

        return CoreQueryResult(
            ok=True,
            query_id=query.query_id,
            result=result,
            correlation_id=query.correlation_id,
        )

    def invoke_command(
        self,
        *,
        target: str,
        method: str,
        args: tuple[Any, ...] = (),
        kwargs: Mapping[str, Any] | None = None,
        correlation_id: str | None = None,
    ) -> Any:
        envelope = CoreCommand(
            name="invoke",
            payload={
                "target": target,
                "method": method,
                "args": tuple(args),
                "kwargs": dict(kwargs or {}),
            },
            correlation_id=correlation_id,
        )
        return self.execute_command(envelope).result

    def invoke_query(
        self,
        *,
        target: str,
        method: str,
        args: tuple[Any, ...] = (),
        kwargs: Mapping[str, Any] | None = None,
        correlation_id: str | None = None,
    ) -> Any:
        envelope = CoreQuery(
            name="invoke",
            payload={
                "target": target,
                "method": method,
                "args": tuple(args),
                "kwargs": dict(kwargs or {}),
            },
            correlation_id=correlation_id,
        )
        return self.execute_query(envelope).result

    def _target_bindings(self) -> list[tuple[str, tuple[str, ...], str, Any]]:
        bindings: list[tuple[str, tuple[str, ...], str, Any]] = [
            ("library", ("library", "lib"), "Hosted library facade.", self.library),
        ]
        db = getattr(self.library, "database", None)
        if db is not None:
            bindings.append(("database", ("database", "db"), "Hosted database facade.", db))
        storage = getattr(self.library, "storage", None)
        if storage is not None:
            bindings.append(("storage", ("storage", "stores", "store_manager"), "Hosted storage facade.", storage))
        return bindings

    def _normalize_target_filter(self, target: str | None) -> str | None:
        if target is None:
            return None
        token = str(target).strip().lower()
        if not token:
            return None
        for canonical_name, aliases, _summary, _obj in self._target_bindings():
            if token == canonical_name or token in aliases:
                return canonical_name
        raise CoreDispatchError("Unknown target filter: {!r}".format(target))

    def _resolve_target(self, token: str) -> Any:
        key = str(token).strip().lower()
        for canonical_name, aliases, _summary, obj in self._target_bindings():
            if key == canonical_name or key in aliases:
                return obj
        raise CoreDispatchError("Unknown invoke target: {!r}".format(token))

    @staticmethod
    def _describe_parameter(parameter: inspect.Parameter) -> CoreParameterDescription:
        default = None if parameter.default is inspect.Signature.empty else parameter.default
        annotation = None if parameter.annotation is inspect.Signature.empty else str(parameter.annotation)
        return CoreParameterDescription(
            name=str(parameter.name),
            kind=str(parameter.kind.name).lower(),
            required=parameter.default is inspect.Signature.empty,
            default=default,
            annotation=annotation,
        )

    def _describe_target_method(self, method_name: str, method: Any) -> CoreMethodDescription:
        summary = self._doc_summary(method)
        description = inspect.getdoc(method) or ""
        parameters: tuple[CoreParameterDescription, ...] = ()
        return_annotation: str | None = None
        try:
            signature = inspect.signature(method)
        except Exception:
            signature = None
        if signature is not None:
            params = []
            for parameter in signature.parameters.values():
                if parameter.name == "self":
                    continue
                params.append(self._describe_parameter(parameter))
            parameters = tuple(params)
            if signature.return_annotation is not inspect.Signature.empty:
                return_annotation = str(signature.return_annotation)
        return CoreMethodDescription(
            name=str(method_name),
            write=looks_like_write_method(method_name),
            summary=summary,
            description=description,
            parameters=parameters,
            return_annotation=return_annotation,
        )

    def _describe_target(self, *, target_name: str, aliases: tuple[str, ...], summary: str, target_obj: Any) -> CoreTargetDescription:
        methods: list[CoreMethodDescription] = []
        for attribute_name in sorted(dir(target_obj)):
            if attribute_name.startswith("_"):
                continue
            try:
                attribute = getattr(target_obj, attribute_name)
            except Exception:
                continue
            if not callable(attribute):
                continue
            methods.append(self._describe_target_method(attribute_name, attribute))
        return CoreTargetDescription(
            name=target_name,
            aliases=tuple(aliases),
            summary=summary,
            description=inspect.getdoc(target_obj) or "",
            methods=tuple(methods),
        )

    def describe_api(self, *, include_targets: bool = True, target: str | None = None) -> dict[str, Any]:
        normalized_target = self._normalize_target_filter(target)
        with self._state_lock:
            commands = [self._command_descriptions[name].to_dict() for name in sorted(self._command_descriptions)]
            queries = [self._query_descriptions[name].to_dict() for name in sorted(self._query_descriptions)]

        payload: dict[str, Any] = {
            "core_uuid": self.core_uuid,
            "core_version": self.core_version,
            "commands": commands,
            "queries": queries,
        }
        if include_targets:
            targets = []
            for target_name, aliases, summary, obj in self._target_bindings():
                if normalized_target is not None and target_name != normalized_target:
                    continue
                targets.append(
                    self._describe_target(
                        target_name=target_name,
                        aliases=aliases,
                        summary=summary,
                        target_obj=obj,
                    ).to_dict()
                )
            payload["targets"] = targets
        return payload

    @staticmethod
    def _extract_invoke_payload(payload: Mapping[str, Any]) -> tuple[str, str, tuple[Any, ...], dict[str, Any]]:
        target = str(payload.get("target", "")).strip()
        method = str(payload.get("method", "")).strip()
        args = tuple(payload.get("args", ()))
        kwargs = dict(payload.get("kwargs", {}) or {})
        if not target:
            raise CoreDispatchError("Invoke payload missing `target`.")
        if not method:
            raise CoreDispatchError("Invoke payload missing `method`.")
        return target, method, args, kwargs

    def _invoke(self, payload: Mapping[str, Any]) -> Any:
        target_name, method_name, args, kwargs = self._extract_invoke_payload(payload)
        target = self._resolve_target(target_name)
        method = getattr(target, method_name, None)
        if method is None or not callable(method):
            raise CoreDispatchError(
                "Target {!r} has no callable method {!r}.".format(target_name, method_name)
            )
        return method(*args, **kwargs)

    def _handle_invoke_command(self, runtime: "CoreRuntime", command: CoreCommand) -> Any:
        del runtime
        return self._invoke(command.payload)

    def _handle_shutdown_command(self, runtime: "CoreRuntime", command: CoreCommand) -> int:
        del runtime, command
        return self.shutdown()

    def _handle_invoke_query(self, runtime: "CoreRuntime", query: CoreQuery) -> Any:
        del runtime
        return self._invoke(query.payload)

    def _handle_health_query(self, runtime: "CoreRuntime", query: CoreQuery) -> dict[str, Any]:
        del runtime, query
        return {
            "core_uuid": self.core_uuid,
            "core_version": self.core_version,
            "shutdown": self.is_shutdown,
            "registered_command_handlers": sorted(self._command_handlers.keys()),
            "registered_query_handlers": sorted(self._query_handlers.keys()),
        }

    def _handle_api_describe_query(self, runtime: "CoreRuntime", query: CoreQuery) -> dict[str, Any]:
        del runtime
        payload = dict(query.payload or {})
        include_targets = bool(payload.get("include_targets", True))
        target = payload.get("target", None)
        return self.describe_api(include_targets=include_targets, target=None if target is None else str(target))

    @staticmethod
    def _preview_value(value: Any, *, max_len: int = 400) -> str:
        text = repr(value)
        if len(text) <= max_len:
            return text
        return text[: max(0, max_len - 3)] + "..."

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except Exception:
            return None

    @classmethod
    def _serialize_job_execution(cls, execution: Any) -> dict[str, Any] | None:
        if execution is None:
            return None
        return {
            "ok": bool(getattr(execution, "ok", False)),
            "timed_out": bool(getattr(execution, "timed_out", False)),
            "aborted": bool(getattr(execution, "aborted", False)),
            "traceback": str(getattr(execution, "traceback", "") or ""),
            "log_path": str(getattr(execution, "log_path", "") or ""),
            # Intentionally a preview for JSON safety over remote transports.
            "result_preview": cls._preview_value(getattr(execution, "result", None)),
        }

    @classmethod
    def _serialize_managed_job(cls, info: Any) -> dict[str, Any]:
        request = getattr(info, "request", None)
        request_payload: dict[str, Any] = {}
        if request is not None:
            request_kwargs = getattr(request, "kwargs", {}) or {}
            request_payload = {
                "module_name": str(getattr(request, "module_name", "") or ""),
                "function_name": str(getattr(request, "function_name", "") or ""),
                "args_count": len(tuple(getattr(request, "args", ()) or ())),
                "kwargs_keys": sorted(str(key) for key in request_kwargs.keys()),
                "module_is_source_code": bool(getattr(request, "module_is_source_code", False)),
                "cwd": str(getattr(request, "cwd", "") or ""),
                "has_env": bool(getattr(request, "env", None)),
            }
        return {
            "job_id": str(getattr(info, "job_id", "") or ""),
            "label": str(getattr(info, "label", "") or ""),
            "state": str(getattr(info, "state", "") or ""),
            "backend_name": str(getattr(info, "backend_name", "") or ""),
            "submitted_at": cls._safe_float(getattr(info, "submitted_at", None)),
            "started_at": cls._safe_float(getattr(info, "started_at", None)),
            "finished_at": cls._safe_float(getattr(info, "finished_at", None)),
            "duration_s": cls._safe_float(getattr(info, "duration_s", None)),
            "timeout_s": cls._safe_float(getattr(info, "timeout_s", None)),
            "no_output": bool(getattr(info, "no_output", False)),
            "log_path": str(getattr(info, "log_path", "") or ""),
            "request": request_payload,
            "execution": cls._serialize_job_execution(getattr(info, "execution", None)),
        }

    @staticmethod
    def _normalize_job_id(payload: Mapping[str, Any], *, field: str = "job_id") -> str:
        job_id = str(payload.get(field, "")).strip()
        if not job_id:
            raise CoreDispatchError("`{}` is required.".format(field))
        return job_id

    @staticmethod
    def _normalize_job_states(payload: Mapping[str, Any]) -> set[str] | None:
        raw_states = payload.get("states", payload.get("state", None))
        if raw_states is None:
            return None
        values: set[str] = set()
        if isinstance(raw_states, str):
            for part in raw_states.replace(";", ",").split(","):
                token = part.strip().lower()
                if token:
                    values.add(token)
            return values
        if isinstance(raw_states, (list, tuple, set)):
            for part in raw_states:
                token = str(part).strip().lower()
                if token:
                    values.add(token)
            return values
        raise CoreDispatchError("`states` must be a string or sequence of strings.")

    @staticmethod
    def _normalize_optional_limit(payload: Mapping[str, Any], *, key: str = "limit") -> int | None:
        if key not in payload:
            return None
        raw = payload.get(key)
        if raw is None:
            return None
        try:
            value = int(raw)
        except Exception as exc:
            raise CoreDispatchError("`{}` must be an integer.".format(key)) from exc
        if value < 1:
            raise CoreDispatchError("`{}` must be >= 1.".format(key))
        return value

    @staticmethod
    def _normalize_offset(payload: Mapping[str, Any], *, key: str = "offset") -> int:
        if key not in payload:
            return 0
        raw = payload.get(key)
        if raw is None:
            return 0
        try:
            value = int(raw)
        except Exception as exc:
            raise CoreDispatchError("`{}` must be an integer.".format(key)) from exc
        if value < 0:
            raise CoreDispatchError("`{}` must be >= 0.".format(key))
        return value

    @staticmethod
    def _normalize_wait_timeout(payload: Mapping[str, Any]) -> float | None:
        if "timeout_s" in payload:
            raw = payload.get("timeout_s")
        elif "timeout" in payload:
            raw = payload.get("timeout")
        else:
            return None
        if raw is None:
            return None
        token = str(raw).strip().lower()
        if token in {"none", "off", "disable", "disabled"}:
            return None
        try:
            return float(raw)
        except Exception as exc:
            raise CoreDispatchError("`timeout_s` must be a float, int, or none-like token.") from exc

    def _handle_jobs_list_query(self, runtime: "CoreRuntime", query: CoreQuery) -> dict[str, Any]:
        del runtime
        payload = dict(query.payload or {})
        states = self._normalize_job_states(payload)
        limit = self._normalize_optional_limit(payload, key="limit")
        offset = self._normalize_offset(payload, key="offset")

        jobs = self.job_manager.list(states=states)
        total = len(jobs)
        if limit is None:
            window = jobs[offset:]
        else:
            window = jobs[offset : offset + limit]
        return {
            "jobs": [self._serialize_managed_job(info) for info in window],
            "total": total,
            "offset": offset,
            "limit": limit,
            "states": sorted(states) if states else [],
        }

    def _handle_jobs_get_query(self, runtime: "CoreRuntime", query: CoreQuery) -> dict[str, Any]:
        del runtime
        payload = dict(query.payload or {})
        job_id = self._normalize_job_id(payload)
        try:
            info = self.job_manager.get(job_id)
        except KeyError as exc:
            raise CoreDispatchError("Unknown job id: {!r}".format(job_id)) from exc
        return {
            "job": self._serialize_managed_job(info),
        }

    def _handle_jobs_wait_query(self, runtime: "CoreRuntime", query: CoreQuery) -> dict[str, Any]:
        del runtime
        payload = dict(query.payload or {})
        job_id = self._normalize_job_id(payload)
        timeout_s = self._normalize_wait_timeout(payload)
        try:
            info = self.job_manager.wait(job_id, timeout=timeout_s)
        except KeyError as exc:
            raise CoreDispatchError("Unknown job id: {!r}".format(job_id)) from exc
        return {
            "job": self._serialize_managed_job(info),
        }

    def _resolve_library_database_path(self) -> str | None:
        db = getattr(self.library, "database", None)
        metadata = getattr(db, "metadata", None) if db is not None else None
        if not isinstance(metadata, Mapping):
            return None
        value = metadata.get("database_path")
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    def _resolve_library_db_type(self) -> str:
        db = getattr(self.library, "database", None)
        value = getattr(db, "type", None) if db is not None else None
        text = str(value or "").strip()
        return text or "SQLite"

    @staticmethod
    def _normalize_sync_job_kwargs(payload: Mapping[str, Any]) -> dict[str, Any]:
        raw = payload.get("sync_kwargs", {})
        if not isinstance(raw, Mapping):
            raise CoreDispatchError("`sync_kwargs` must be a mapping.")
        return dict(raw)

    def _handle_sync_store_start_command(self, runtime: "CoreRuntime", command: CoreCommand) -> dict[str, Any]:
        del runtime
        payload = dict(command.payload or {})
        sync_kwargs = self._normalize_sync_job_kwargs(payload)

        if not str(sync_kwargs.get("database_path", "")).strip():
            resolved_path = self._resolve_library_database_path()
            if not resolved_path:
                raise CoreDispatchError(
                    "sync.store.start requires `sync_kwargs.database_path` when core library database path is unavailable."
                )
            sync_kwargs["database_path"] = resolved_path
        if not str(sync_kwargs.get("db_type", "")).strip():
            sync_kwargs["db_type"] = self._resolve_library_db_type()

        timeout_value = payload.get("job_timeout_s", None)
        timeout_s = None if timeout_value is None else float(timeout_value)
        manager_timeout = -1.0 if timeout_s is None else float(timeout_s)

        no_output = bool(payload.get("job_no_output", False))
        backend = payload.get("job_backend", None)
        label = str(payload.get("label", "")).strip() or str(payload.get("job_label", "")).strip() or None

        request = JobRequest(
            module_name="LiuXin_alpha.interfaces.terminal.commands.sync",
            function_name="run_sync_store_job",
            kwargs=sync_kwargs,
        )
        job_id = self.job_manager.submit(
            request,
            timeout=manager_timeout,
            no_output=no_output,
            backend=backend,
            label=label,
        )

        self.emit_event(
            "sync.store.job_submitted",
            {
                "job_id": job_id,
                "label": label or "",
                "backend": "" if backend is None else str(backend),
                "timeout_s": "none" if timeout_s is None else timeout_s,
                "no_output": no_output,
            },
        )
        return {
            "job_id": job_id,
            "label": label or "",
            "backend": "" if backend is None else str(backend),
            "timeout_s": None if timeout_s is None else timeout_s,
            "no_output": no_output,
        }

    def _handle_sync_store_cancel_command(self, runtime: "CoreRuntime", command: CoreCommand) -> dict[str, Any]:
        del runtime
        payload = dict(command.payload or {})
        job_id = self._normalize_job_id(payload)
        result = self._cancel_job_by_id(job_id=job_id, event_type="sync.store.job_cancel_requested")
        return result

    def _cancel_job_by_id(self, *, job_id: str, event_type: str) -> dict[str, Any]:
        cancelled = bool(self.job_manager.cancel(job_id))
        state = "unknown"
        try:
            info = self.job_manager.get(job_id)
            state = str(info.state)
        except Exception:
            state = "unknown"

        payload = {
            "job_id": job_id,
            "cancelled": cancelled,
            "state": state,
        }
        self.emit_event(event_type, payload)
        return payload

    def _handle_jobs_cancel_command(self, runtime: "CoreRuntime", command: CoreCommand) -> dict[str, Any]:
        del runtime
        payload = dict(command.payload or {})
        job_id = self._normalize_job_id(payload)
        return self._cancel_job_by_id(job_id=job_id, event_type="jobs.cancel_requested")


__all__ = [
    "CoreRuntime",
]
