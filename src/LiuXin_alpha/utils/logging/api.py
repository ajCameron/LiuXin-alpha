"""Common structured event-log data and interface."""

from __future__ import annotations

import abc

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from types import TracebackType
from typing import ClassVar, Self


@dataclass(slots=True, frozen=True)
class Event:
    """One structured, severity-labelled log event."""

    id: int
    ts: datetime
    level: int
    message: str
    context: dict[str, object]


class EventLogAPI(abc.ABC):
    """Thread-compatible storage interface for structured events."""

    DEFAULT_LEVEL_NAMES: ClassVar[Mapping[int, str]] = {
        10: "DEBUG",
        20: "INFO",
        30: "WARNING",
        40: "ERROR",
        50: "CRITICAL",
    }

    @abc.abstractmethod
    def put(self, message: str) -> None:
        """Record an informational event."""

    @abc.abstractmethod
    def put_event(
        self,
        message: str,
        *,
        level: int = 20,
        ts: datetime | None = None,
        context: dict[str, object] | None = None,
    ) -> int:
        """Record an event and return its monotonically increasing ID."""

    @abc.abstractmethod
    def get(self, num: int | None = None) -> Iterable[str]:
        """Return rendered retained events."""

    @abc.abstractmethod
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
        """Return retained events matching the supplied filters."""

    @abc.abstractmethod
    def follow(
        self,
        *,
        after_id: int | None = None,
        poll_interval_s: float = 0.25,
    ) -> Iterable[Event]:
        """Yield newly recorded events until the log closes."""

    @property
    @abc.abstractmethod
    def max_entries(self) -> int:
        """Return the in-process retention ceiling."""

    @abc.abstractmethod
    def set_max_entries(self, max_entries: int) -> None:
        """Change the in-process retention ceiling."""

    def level_name(self, level: int) -> str:
        """Return the configured display name for a numeric level."""

        return self.DEFAULT_LEVEL_NAMES.get(level, f"LVL{level}")

    @abc.abstractmethod
    def set_level_names(
        self,
        level_names: Mapping[int, str],
        *,
        replace: bool = False,
    ) -> None:
        """Merge or replace numeric-level display names."""

    @abc.abstractmethod
    def get_level_names(self) -> Mapping[int, str]:
        """Return a safe copy of the numeric-level display names."""

    def flush(self) -> None:
        """Flush durable output, if any."""

    def close(self) -> None:
        """Close durable output and wake followers, if applicable."""

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback_value: TracebackType | None,
    ) -> None:
        del exc_type, exc, traceback_value
        self.close()


__all__ = ["Event", "EventLogAPI"]
