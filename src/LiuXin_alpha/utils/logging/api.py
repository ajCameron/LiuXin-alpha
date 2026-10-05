"""
Define the compatibility logging API, levels, records and stream contracts.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise api through a consuming regression::

        python -m pytest -q tests/utils/logging/test_compat_logger.py
"""

from __future__ import annotations

import abc

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from types import TracebackType
from typing import ClassVar, Self


@dataclass(slots=True, frozen=True)
class Event:
    """
    One structured, severity-labelled log event.

    Example:
        Exercise Event through a consuming regression::

            python -m pytest -q tests/utils/logging/test_compat_logger.py
    """

    id: int
    ts: datetime
    level: int
    message: str
    context: dict[str, object]


class EventLogAPI(abc.ABC):
    """
    Thread-compatible storage interface for structured events.

    Example:
        Exercise EventLogAPI through a consuming regression::

            python -m pytest -q tests/utils/logging/test_compat_logger.py
    """

    DEFAULT_LEVEL_NAMES: ClassVar[Mapping[int, str]] = {
        10: "DEBUG",
        20: "INFO",
        30: "WARNING",
        40: "ERROR",
        50: "CRITICAL",
    }

    @abc.abstractmethod
    def put(self, message: str) -> None:
        """
        Record an informational event.

        Example:
            Exercise EventLogAPI.put through a consuming regression::

                python -m pytest -q tests/utils/logging/test_compat_logger.py


        :param message: Value supplied for message under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    @abc.abstractmethod
    def put_event(
        self,
        message: str,
        *,
        level: int = 20,
        ts: datetime | None = None,
        context: dict[str, object] | None = None,
    ) -> int:
        """
        Record an event and return its monotonically increasing ID.

        Example:
            Exercise EventLogAPI.put event through a consuming regression::

                python -m pytest -q tests/utils/logging/test_compat_logger.py


        :param message: Value supplied for message under the utility contract.
        :param level: Value supplied for level under the utility contract.
        :param ts: Value supplied for ts under the utility contract.
        :param context: Value supplied for context under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    @abc.abstractmethod
    def get(self, num: int | None = None) -> Iterable[str]:
        """
        Return rendered retained events.

        Example:
            Exercise EventLogAPI.get through a consuming regression::

                python -m pytest -q tests/utils/logging/test_compat_logger.py


        :param num: Value supplied for num under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

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
        """
        Return retained events matching the supplied filters.

        Example:
            Exercise EventLogAPI.get events through a consuming regression::

                python -m pytest -q tests/utils/logging/test_compat_logger.py


        :param limit: Value supplied for limit under the utility contract.
        :param since_id: Value supplied for since id under the utility contract.
        :param since_ts: Value supplied for since ts under the utility contract.
        :param level_min: Value supplied for level min under the utility contract.
        :param contains: Value supplied for contains under the utility contract.
        :param reverse: Value supplied for reverse under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    @abc.abstractmethod
    def follow(
        self,
        *,
        after_id: int | None = None,
        poll_interval_s: float = 0.25,
    ) -> Iterable[Event]:
        """
        Yield newly recorded events until the log closes.

        Example:
            Exercise EventLogAPI.follow through a consuming regression::

                python -m pytest -q tests/utils/logging/test_compat_logger.py


        :param after_id: Value supplied for after id under the utility contract.
        :param poll_interval_s: Value supplied for poll interval s under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    @property
    @abc.abstractmethod
    def max_entries(self) -> int:
        """
        Return the in-process retention ceiling.

        Example:
            Exercise EventLogAPI.max entries through a consuming regression::

                python -m pytest -q tests/utils/logging/test_compat_logger.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    @abc.abstractmethod
    def set_max_entries(self, max_entries: int) -> None:
        """
        Change the in-process retention ceiling.

        Example:
            Exercise EventLogAPI.set max entries through a consuming regression::

                python -m pytest -q tests/utils/logging/test_compat_logger.py


        :param max_entries: Value supplied for max entries under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    def level_name(self, level: int) -> str:
        """
        Return the configured display name for a numeric level.

        Example:
            Exercise EventLogAPI.level name through a consuming regression::

                python -m pytest -q tests/utils/logging/test_compat_logger.py


        :param level: Value supplied for level under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        return self.DEFAULT_LEVEL_NAMES.get(level, f"LVL{level}")

    @abc.abstractmethod
    def set_level_names(
        self,
        level_names: Mapping[int, str],
        *,
        replace: bool = False,
    ) -> None:
        """
        Merge or replace numeric-level display names.

        Example:
            Exercise EventLogAPI.set level names through a consuming regression::

                python -m pytest -q tests/utils/logging/test_compat_logger.py


        :param level_names: Value supplied for level names under the utility contract.
        :param replace: Value supplied for replace under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    @abc.abstractmethod
    def get_level_names(self) -> Mapping[int, str]:
        """
        Return a safe copy of the numeric-level display names.

        Example:
            Exercise EventLogAPI.get level names through a consuming regression::

                python -m pytest -q tests/utils/logging/test_compat_logger.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    def flush(self) -> None:
        """
        Flush durable output, if any.

        Example:
            Exercise EventLogAPI.flush through a consuming regression::

                python -m pytest -q tests/utils/logging/test_compat_logger.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    def close(self) -> None:
        """
        Close durable output and wake followers, if applicable.

        Example:
            Exercise EventLogAPI.close through a consuming regression::

                python -m pytest -q tests/utils/logging/test_compat_logger.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    def __enter__(self) -> Self:
        """
        Implement the resource's enter lifecycle operation.

        Example:
            Exercise EventLogAPI.  enter   through a consuming regression::

                python -m pytest -q tests/utils/logging/test_compat_logger.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback_value: TracebackType | None,
    ) -> None:
        """
        Implement the resource's exit lifecycle operation.

        Example:
            Exercise EventLogAPI.  exit   through a consuming regression::

                python -m pytest -q tests/utils/logging/test_compat_logger.py


        :param exc_type: Value supplied for exc type under the utility contract.
        :param exc: Value supplied for exc under the utility contract.
        :param traceback_value: Value supplied for traceback value under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        del exc_type, exc, traceback_value
        self.close()


__all__ = ["Event", "EventLogAPI"]
