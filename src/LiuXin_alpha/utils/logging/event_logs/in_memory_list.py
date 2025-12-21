
"""

"""

from __future__ import annotations

import threading

import json

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Deque, Iterable, Optional, List, Dict, Mapping, Tuple, Union

from LiuXin_alpha.utils.logging.api import EventLogAPI, Event

# ---------------------------
# Implementation
# ---------------------------

class InMemoryEventLog(EventLogAPI):
    """
    Sufficient prototype event log.

    - Ring buffer in memory (last `max_entries`).
    - Optional JSONL persistence to disk (one event per line).
    - Thread-safe for concurrent put/get/follow and live resizing/config.
    """

    def __init__(
        self,
        max_entries: int = 10_000,
        persist_path: Optional[Path] = None,
        encoding: str = "utf-8",
        utc_timestamps: bool = True,
        normalize_multiline: bool = True,
        level_names: Optional[Mapping[int, str]] = None,
        include_level_name_in_jsonl: bool = False,
    ) -> None:
        """
        Startup the logger.

        :param max_entries:
        :param persist_path:
        :param encoding:
        :param utc_timestamps:
        :param normalize_multiline:
        :param level_names:
        :param include_level_name_in_jsonl:
        """
        if max_entries <= 0:
            raise ValueError("max_entries must be > 0")

        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)

        self._events: Deque[Event] = deque(maxlen=max_entries)
        self._next_id = 1
        self._closed = False

        self._persist_path = Path(persist_path) if persist_path is not None else None
        self._encoding = encoding
        self._utc_timestamps = utc_timestamps
        self._normalize_multiline = normalize_multiline
        self._include_level_name_in_jsonl = include_level_name_in_jsonl

        # Per-instance level mapping (mutable, guarded by lock)
        self._level_names: Dict[int, str] = dict(self.DEFAULT_LEVEL_NAMES)
        if level_names:
            self._validate_level_names(level_names)
            self._level_names.update(dict(level_names))

        if self._persist_path is not None:
            self._persist_path.parent.mkdir(parents=True, exist_ok=True)
            self._persist_path.touch(exist_ok=True)

    # ----- retention -----

    @property
    def max_entries(self) -> int:
        """
        Maximum number of events to log.

        :return:
        """
        with self._lock:
            ml = self._events.maxlen
            return int(ml) if ml is not None else 0

    def set_max_entries(self, max_entries: int) -> None:
        """
        Change the maximum number of events to log.

        :param max_entries:
        :return:
        """
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

    # ----- level name mapping -----

    def level_name(self, level: int) -> str:
        with self._lock:
            return self._level_names.get(level, f"LVL{level}")

    def set_level_names(self, level_names: Mapping[int, str], *, replace: bool = False) -> None:
        self._validate_level_names(level_names)
        with self._cond:
            self._ensure_open()
            if replace:
                # You might prefer to keep defaults always; here we truly replace.
                self._level_names = dict(level_names)
            else:
                self._level_names.update(dict(level_names))
            self._cond.notify_all()

    def get_level_names(self) -> Mapping[int, str]:
        with self._lock:
            # Return a copy so callers can't mutate internal state.
            return dict(self._level_names)

    # ----- core API -----

    def put(self, message: str) -> None:
        self.put_event(message, level=20)

    def put_event(
        self,
        message: str,
        *,
        level: int = 20,
        ts: Optional[datetime] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> int:
        if not isinstance(message, str):
            raise TypeError("message must be a str")
        if not isinstance(level, int):
            raise TypeError("level must be an int")
        if context is not None and not isinstance(context, dict):
            raise TypeError("context must be a dict[str, Any] or None")

        msg = self._normalize_message(message)
        ctx: Dict[str, Any] = dict(context) if context else {}

        if ts is None:
            ts = datetime.now(timezone.utc if self._utc_timestamps else None)

        with self._cond:
            self._ensure_open()

            event_id = self._next_id
            self._next_id += 1

            ev = Event(id=event_id, ts=ts, level=level, message=msg, context=ctx)
            self._events.append(ev)

            if self._persist_path is not None:
                self._append_jsonl(ev)

            self._cond.notify_all()
            return event_id

    def get(self, num: Optional[int] = None) -> Iterable[str]:
        events = list(self.get_events(limit=num, reverse=False))
        return [self._render_event(e) for e in events]

    def get_events(
        self,
        *,
        limit: Optional[int] = None,
        since_id: Optional[int] = None,
        since_ts: Optional[datetime] = None,
        level_min: Optional[int] = None,
        contains: Optional[str] = None,
        reverse: bool = True,
    ) -> Iterable[Event]:
        with self._lock:
            snapshot: List[Event] = list(self._events)

        if since_id is not None:
            snapshot = [e for e in snapshot if e.id > since_id]
        if since_ts is not None:
            snapshot = [e for e in snapshot if e.ts > since_ts]
        if level_min is not None:
            snapshot = [e for e in snapshot if e.level >= level_min]
        if contains is not None:
            snapshot = [e for e in snapshot if contains in e.message]

        if reverse:
            snapshot.reverse()

        if limit is not None:
            if limit <= 0:
                return []
            snapshot = snapshot[:limit]

        return snapshot

    def follow(
        self,
        *,
        after_id: Optional[int] = None,
        poll_interval_s: float = 0.25,
    ) -> Iterable[Event]:
        if poll_interval_s <= 0:
            raise ValueError("poll_interval_s must be > 0")

        with self._cond:
            cursor = (self._next_id - 1) if after_id is None else after_id

        while True:
            with self._cond:
                if self._closed:
                    return

                new_events = [e for e in self._events if e.id > cursor]
                if not new_events:
                    self._cond.wait(timeout=poll_interval_s)
                    continue

                cursor = new_events[-1].id

            for e in new_events:
                yield e

    def flush(self) -> None:
        return None

    def close(self) -> None:
        with self._cond:
            self._closed = True
            self._cond.notify_all()

    # ----- helpers -----

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

    def _render_event(self, e: Event) -> str:
        ts_iso = e.ts.isoformat(timespec="milliseconds")
        lvl = self.level_name(e.level)
        base = f"{ts_iso} [{e.id:08d}] {lvl} {e.message}"
        if e.context:
            ctx = json.dumps(
                e.context,
                ensure_ascii=False,
                separators=(",", ":"),
                default=self._json_default,
            )
            return f"{base} {ctx}"
        return base

    @staticmethod
    def _json_default(obj: Any) -> str:
        return repr(obj)

    def _append_jsonl(self, e: Event) -> None:
        assert self._persist_path is not None

        payload: Dict[str, Any] = {
            "id": e.id,
            "ts": e.ts.isoformat(timespec="milliseconds"),
            "level": e.level,
            "message": e.message,
            "context": e.context,
        }
        if self._include_level_name_in_jsonl:
            payload["level_name"] = self.level_name(e.level)

        line = json.dumps(payload, ensure_ascii=False, default=self._json_default)
        with self._persist_path.open("a", encoding=self._encoding, newline="\n") as f:
            f.write(line)
            f.write("\n")
            f.flush()

    @staticmethod
    def _validate_level_names(level_names: Mapping[int, str]) -> None:
        if not isinstance(level_names, Mapping):
            raise TypeError("level_names must be a Mapping[int, str]")
        for k, v in level_names.items():
            if not isinstance(k, int):
                raise TypeError(f"level_names key must be int, got {type(k)!r}")
            if not isinstance(v, str):
                raise TypeError(f"level_names value must be str, got {type(v)!r}")
            if not v:
                raise ValueError("level_names values must be non-empty strings")
