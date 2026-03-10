"""Phase-1 core runtime scaffold.

The runtime is transport-agnostic and can be hosted directly in-process or by a
daemon wrapper.
"""

from __future__ import annotations

import threading
import uuid

from typing import Any, Callable, Mapping, Optional

from LiuXin_alpha.core.api import CoreAPI
from LiuXin_alpha.core.commands import CoreCommand, CoreCommandResult
from LiuXin_alpha.core.errors import CoreDispatchError, CoreHandlerError, CoreShutdownError
from LiuXin_alpha.core.events import CoreEvent, make_core_event
from LiuXin_alpha.core.queries import CoreQuery, CoreQueryResult


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
    ) -> None:
        self._library = library
        self._core_uuid = str(core_uuid or uuid.uuid4())
        self._core_version = str(core_version)
        self._shutdown = False

        self._command_handlers: dict[str, CommandHandler] = {}
        self._query_handlers: dict[str, QueryHandler] = {}
        self._event_subscribers: list[EventSubscriber] = []

        # Write-path serialization.
        self._command_lock = threading.RLock()
        # Protects handler and subscriber maps.
        self._state_lock = threading.RLock()

        self.register_command_handler("invoke", self._handle_invoke_command)
        self.register_command_handler("shutdown", self._handle_shutdown_command)
        self.register_query_handler("invoke", self._handle_invoke_query)
        self.register_query_handler("health", self._handle_health_query)

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
    def is_shutdown(self) -> bool:
        return bool(self._shutdown)

    def shutdown(self) -> int:
        if self._shutdown:
            return 0
        self._shutdown = True
        self.emit_event("core.shutdown", {"reason": "explicit"})
        return 0

    def register_command_handler(self, name: str, handler: CommandHandler) -> None:
        token = str(name).strip().lower()
        if not token:
            raise ValueError("Command name cannot be blank.")
        with self._state_lock:
            self._command_handlers[token] = handler

    def register_query_handler(self, name: str, handler: QueryHandler) -> None:
        token = str(name).strip().lower()
        if not token:
            raise ValueError("Query name cannot be blank.")
        with self._state_lock:
            self._query_handlers[token] = handler

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

    def _resolve_target(self, token: str) -> Any:
        key = str(token).strip().lower()
        if key in {"library", "lib"}:
            return self.library
        if key in {"database", "db"}:
            db = getattr(self.library, "database", None)
            if db is None:
                raise CoreDispatchError("Library has no `database` target.")
            return db
        if key in {"storage", "stores", "store_manager"}:
            storage = getattr(self.library, "storage", None)
            if storage is None:
                raise CoreDispatchError("Library has no `storage` target.")
            return storage
        raise CoreDispatchError("Unknown invoke target: {!r}".format(token))

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


__all__ = [
    "CoreRuntime",
]
