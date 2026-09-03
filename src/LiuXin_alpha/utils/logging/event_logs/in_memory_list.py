"""Thread-safe retained event log with optional durable JSONL output."""

from __future__ import annotations

import json
import threading

from collections import deque
from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import TextIO, cast, final, override

from LiuXin_alpha.utils.logging.api import Event, EventLogAPI


@final
class InMemoryEventLog(EventLogAPI):
    """Retain recent events in memory and optionally append all events to JSONL.

    The ring is useful for local queries and followers. When ``persist_path``
    is set, the append-only file is the complete record and is flushed after
    every event. One line-buffered file handle is retained so object-level
    archive logging does not open a file millions of times.
    """

    def __init__(
        self,
        max_entries: int = 10_000,
        persist_path: Path | None = None,
        encoding: str = "utf-8",
        utc_timestamps: bool = True,
        normalize_multiline: bool = True,
        level_names: Mapping[int, str] | None = None,
        include_level_name_in_jsonl: bool = False,
    ) -> None:
        if max_entries <= 0:
            raise ValueError("max_entries must be > 0")

        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._events: deque[Event] = deque(maxlen=max_entries)
        self._next_id = 1
        self._closed = False
        self._persist_path = Path(persist_path) if persist_path is not None else None
        self._persist_file: TextIO | None = None
        self._encoding = encoding
        self._utc_timestamps = utc_timestamps
        self._normalize_multiline = normalize_multiline
        self._include_level_name_in_jsonl = include_level_name_in_jsonl
        self._level_names = dict(self.DEFAULT_LEVEL_NAMES)
        if level_names:
            validated_names = self._validated_level_names(level_names)
            self._level_names.update(validated_names)

        if self._persist_path is not None:
            self._persist_path.parent.mkdir(parents=True, exist_ok=True)
            self._persist_file = self._persist_path.open(
                "a",
                encoding=self._encoding,
                errors="backslashreplace",
                buffering=1,
                newline="\n",
            )

    @property
    @override
    def max_entries(self) -> int:
        with self._lock:
            maximum = self._events.maxlen
            return int(maximum) if maximum is not None else 0

    @override
    def set_max_entries(self, max_entries: int) -> None:
        if max_entries <= 0:
            raise ValueError("max_entries must be > 0")
        with self._cond:
            self._ensure_open()
            if self._events.maxlen == max_entries:
                return
            current = list(self._events)
            if len(current) > max_entries:
                current = current[-max_entries:]
            self._events = deque(current, maxlen=max_entries)
            self._cond.notify_all()

    @override
    def level_name(self, level: int) -> str:
        with self._lock:
            return self._level_names.get(level, f"LVL{level}")

    @override
    def set_level_names(
        self,
        level_names: Mapping[int, str],
        *,
        replace: bool = False,
    ) -> None:
        validated_names = self._validated_level_names(level_names)
        with self._cond:
            self._ensure_open()
            if replace:
                self._level_names = validated_names
            else:
                self._level_names.update(validated_names)
            self._cond.notify_all()

    @override
    def get_level_names(self) -> Mapping[int, str]:
        with self._lock:
            return dict(self._level_names)

    @override
    def put(self, message: str) -> None:
        _ = self.put_event(message, level=20)

    @override
    def put_event(
        self,
        message: str,
        *,
        level: int = 20,
        ts: datetime | None = None,
        context: dict[str, object] | None = None,
    ) -> int:
        message = self._validated_message(message)
        level = self._validated_level(level)
        ctx = self._validated_context(context)
        normalized = self._normalize_message(message)
        timestamp = ts
        if timestamp is None:
            timestamp = datetime.now(
                timezone.utc if self._utc_timestamps else None
            )

        with self._cond:
            self._ensure_open()
            event_id = self._next_id
            self._next_id += 1
            event = Event(
                id=event_id,
                ts=timestamp,
                level=level,
                message=normalized,
                context=ctx,
            )
            self._events.append(event)
            if self._persist_file is not None:
                self._append_jsonl(event)
            self._cond.notify_all()
            return event_id

    @override
    def get(self, num: int | None = None) -> Iterable[str]:
        events = self.get_events(limit=num, reverse=False)
        return [self._render_event(event) for event in events]

    @override
    def get_events(
        self,
        *,
        limit: int | None = None,
        since_id: int | None = None,
        since_ts: datetime | None = None,
        level_min: int | None = None,
        contains: str | None = None,
        reverse: bool = True,
    ) -> Iterable[Event]:
        with self._lock:
            snapshot = list(self._events)
        if since_id is not None:
            snapshot = [event for event in snapshot if event.id > since_id]
        if since_ts is not None:
            snapshot = [event for event in snapshot if event.ts > since_ts]
        if level_min is not None:
            snapshot = [event for event in snapshot if event.level >= level_min]
        if contains is not None:
            snapshot = [event for event in snapshot if contains in event.message]
        if reverse:
            snapshot.reverse()
        if limit is not None:
            if limit <= 0:
                return ()
            snapshot = snapshot[:limit]
        return snapshot

    @override
    def follow(
        self,
        *,
        after_id: int | None = None,
        poll_interval_s: float = 0.25,
    ) -> Iterable[Event]:
        if poll_interval_s <= 0:
            raise ValueError("poll_interval_s must be > 0")
        with self._cond:
            cursor = self._next_id - 1 if after_id is None else after_id

        while True:
            with self._cond:
                new_events = [event for event in self._events if event.id > cursor]
                if new_events:
                    cursor = new_events[-1].id
                elif self._closed:
                    return
                else:
                    _ = self._cond.wait(timeout=poll_interval_s)
                    continue
            yield from new_events

    @override
    def flush(self) -> None:
        with self._lock:
            if self._persist_file is not None:
                self._persist_file.flush()

    @override
    def close(self) -> None:
        with self._cond:
            if self._closed:
                return
            if self._persist_file is not None:
                self._persist_file.flush()
                self._persist_file.close()
                self._persist_file = None
            self._closed = True
            self._cond.notify_all()

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("Event log is closed")

    def _normalize_message(self, message: str) -> str:
        if not self._normalize_multiline:
            return message
        return (
            message.replace("\r\n", "\n")
            .replace("\r", "\n")
            .replace("\n", "\\n")
        )

    def _render_event(self, event: Event) -> str:
        timestamp = event.ts.isoformat(timespec="milliseconds")
        rendered = (
            f"{timestamp} [{event.id:08d}] "
            f"{self.level_name(event.level)} {event.message}"
        )
        if event.context:
            context = json.dumps(
                event.context,
                ensure_ascii=True,
                separators=(",", ":"),
                default=self._json_default,
            )
            return f"{rendered} {context}"
        return rendered

    def _append_jsonl(self, event: Event) -> None:
        assert self._persist_file is not None
        payload: dict[str, object] = {
            "id": event.id,
            "ts": event.ts.isoformat(timespec="milliseconds"),
            "level": event.level,
            "message": event.message,
            "context": event.context,
        }
        if self._include_level_name_in_jsonl:
            # ``put_event`` already owns the non-reentrant condition lock.
            payload["level_name"] = self._level_names.get(
                event.level,
                f"LVL{event.level}",
            )
        line = json.dumps(
            payload,
            ensure_ascii=True,
            default=self._json_default,
            separators=(",", ":"),
        )
        _ = self._persist_file.write(line)
        _ = self._persist_file.write("\n")
        self._persist_file.flush()

    @staticmethod
    def _json_default(value: object) -> str:
        return repr(value)

    @staticmethod
    def _validated_message(value: object) -> str:
        if not isinstance(value, str):
            raise TypeError("message must be a str")
        return value

    @staticmethod
    def _validated_level(value: object) -> int:
        if not isinstance(value, int):
            raise TypeError("level must be an int")
        return value

    @staticmethod
    def _validated_context(value: object) -> dict[str, object]:
        if value is None:
            return {}
        if not isinstance(value, dict):
            raise TypeError("context must be a dict[str, object] or None")
        result: dict[str, object] = {}
        for key, item in cast(dict[object, object], value).items():
            if not isinstance(key, str):
                raise TypeError("context keys must be strings")
            result[key] = item
        return result

    @staticmethod
    def _validated_level_names(value: object) -> dict[int, str]:
        if not isinstance(value, Mapping):
            raise TypeError("level_names must be a Mapping[int, str]")
        result: dict[int, str] = {}
        for level, name in cast(Mapping[object, object], value).items():
            if not isinstance(level, int):
                raise TypeError(
                    f"level_names key must be int, got {type(level)!r}"
                )
            if not isinstance(name, str):
                raise TypeError(
                    f"level_names value must be str, got {type(name)!r}"
                )
            if not name:
                raise ValueError("level_names values must be non-empty strings")
            result[level] = name
        return result


__all__ = ["InMemoryEventLog"]
