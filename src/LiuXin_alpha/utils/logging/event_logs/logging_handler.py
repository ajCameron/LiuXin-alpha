"""Standard-library logging bridge for LiuXin structured event logs."""

from __future__ import annotations

import dataclasses
import logging
import traceback

from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import cast, final, override
from uuid import UUID

from LiuXin_alpha.utils.logging.api import EventLogAPI


def json_safe(value: object, *, depth: int = 0) -> object:
    """Bound arbitrary logging context without losing useful diagnostics."""

    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (Path, UUID)):
        return str(value)
    if isinstance(value, bytes):
        return {
            "type": "bytes",
            "size": len(value),
            "preview_hex": value[:64].hex(),
        }
    if isinstance(value, BaseException):
        return {
            "type": type(value).__name__,
            "message": str(value) or type(value).__name__,
        }
    if depth >= 5:
        return _bounded_repr(value)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: json_safe(
                cast(object, getattr(value, field.name)), depth=depth + 1
            )
            for field in dataclasses.fields(value)
            if not field.name.startswith("_")
        }
    if isinstance(value, Mapping):
        mapping_result: dict[str, object] = {}
        mapping = cast(Mapping[object, object], value)
        for index, (key, item) in enumerate(mapping.items()):
            if index >= 200:
                mapping_result["__truncated__"] = "mapping exceeded 200 entries"
                break
            mapping_result[str(key)] = json_safe(item, depth=depth + 1)
        return mapping_result
    if isinstance(value, (list, tuple, set, frozenset)):
        items = list(cast(list[object] | tuple[object, ...] | set[object] | frozenset[object], value))
        sequence_result = [
            json_safe(item, depth=depth + 1) for item in items[:200]
        ]
        if len(items) > 200:
            sequence_result.append("... sequence exceeded 200 entries")
        return sequence_result
    return _bounded_repr(value)


def _bounded_repr(value: object, *, max_length: int = 2_000) -> str:
    try:
        rendered = repr(value)
    except Exception as error:  # pragma: no cover - defensive logging boundary
        rendered = f"<unrepresentable {type(value).__name__}: {error!r}>"
    if len(rendered) > max_length:
        return rendered[: max_length - 3] + "..."
    return rendered


@final
class EventLogHandler(logging.Handler):
    """Write Python log records into a LiuXin :class:`EventLogAPI`.

    Records emitted by LiuXin workflows may provide ``liuxin_event`` and
    ``liuxin_context`` in ``extra``. Compatibility logger ``vars`` and
    exception fields are retained as well. Tracebacks are rendered into the
    JSONL context so they survive a detached terminal or process supervisor.
    """

    def __init__(
        self,
        event_log: EventLogAPI,
        *,
        level: int = logging.NOTSET,
        close_event_log: bool = False,
    ) -> None:
        super().__init__(level)
        self.event_log = event_log
        self.close_event_log = bool(close_event_log)

    @override
    def emit(self, record: logging.LogRecord) -> None:
        try:
            context: dict[str, object] = {
                "logger": record.name,
                "module": record.module,
                "function": record.funcName,
                "line": record.lineno,
                "process_id": record.process,
                "process_name": record.processName,
                "thread_id": record.thread,
                "thread_name": record.threadName,
            }
            event = getattr(record, "liuxin_event", None)
            if event is not None:
                context["event"] = str(cast(object, event))
            details = getattr(record, "liuxin_context", None)
            if details is not None:
                context["details"] = json_safe(cast(object, details))
            variables = getattr(record, "vars", None)
            if variables is not None:
                context["variables"] = json_safe(cast(object, variables))
            for name in ("exception_type", "exception", "liuxin_json_fallback"):
                value = getattr(record, name, None)
                if value is not None:
                    context[name] = json_safe(cast(object, value))
            if record.exc_info:
                context["traceback"] = "".join(
                    traceback.format_exception(*record.exc_info)
                )
            if record.stack_info:
                context["stack"] = str(record.stack_info)
            _ = self.event_log.put_event(
                record.getMessage(),
                level=int(record.levelno),
                ts=datetime.fromtimestamp(record.created, timezone.utc),
                context=context,
            )
        except Exception:
            self.handleError(record)

    @override
    def flush(self) -> None:
        self.event_log.flush()

    @override
    def close(self) -> None:
        try:
            self.flush()
            if self.close_event_log:
                self.event_log.close()
        finally:
            super().close()


__all__ = ["EventLogHandler", "json_safe"]
