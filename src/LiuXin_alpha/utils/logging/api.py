from __future__ import annotations

import abc
import json
import threading
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, Iterable, List, Mapping, Optional


# ---------------------------
# Data model
# ---------------------------

@dataclass(frozen=True)
class Event:
    id: int
    ts: datetime
    level: int              # e.g. 10=debug, 20=info, 30=warning, 40=error
    message: str
    context: Dict[str, Any]


# ---------------------------
# API
# ---------------------------

class EventLogAPI(abc.ABC):
    """
    Common interface for the event log class.
    """

    DEFAULT_LEVEL_NAMES: Mapping[int, str] = {
        10: "DEBUG",
        20: "INFO",
        30: "WARNING",
        40: "ERROR",
        50: "CRITICAL",
    }

    @abc.abstractmethod
    def put(self, message: str) -> None:
        ...

    @abc.abstractmethod
    def put_event(
        self,
        message: str,
        *,
        level: int = 20,
        ts: Optional[datetime] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> int:
        ...

    @abc.abstractmethod
    def get(self, num: Optional[int] = None) -> Iterable[str]:
        ...

    @abc.abstractmethod
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
        ...

    @abc.abstractmethod
    def follow(
        self,
        *,
        after_id: Optional[int] = None,
        poll_interval_s: float = 0.25,
    ) -> Iterable[Event]:
        ...

    @property
    @abc.abstractmethod
    def max_entries(self) -> int:
        ...

    @abc.abstractmethod
    def set_max_entries(self, max_entries: int) -> None:
        ...

    def level_name(self, level: int) -> str:
        return self.DEFAULT_LEVEL_NAMES.get(level, f"LVL{level}")

    @abc.abstractmethod
    def set_level_names(self, level_names: Mapping[int, str], *, replace: bool = False) -> None:
        """
        Update the mapping from numeric level -> display name.

        - If replace=False (default): merge into existing mapping.
        - If replace=True: replace mapping entirely (DEFAULT_LEVEL_NAMES may be re-added by impls if desired).
        """
        ...

    @abc.abstractmethod
    def get_level_names(self) -> Mapping[int, str]:
        """
        Return the current level mapping (should be safe to treat as read-only).
        """
        ...

    def flush(self) -> None:
        return None

    def close(self) -> None:
        return None

    def __enter__(self) -> "EventLogAPI":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

