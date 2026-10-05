"""
Configure process logging destinations, levels and lifecycle cleanup.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise run logging through a consuming regression::

        python -m pytest -q tests/utils/logging/test_compat_logger.py
"""

from __future__ import annotations

import dataclasses
import json
import logging
import threading
import time

from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from types import TracebackType
from typing import cast, final, override
from uuid import UUID

from LiuXin_alpha.utils.logging.event_logs import EventLogHandler, InMemoryEventLog
from LiuXin_alpha.utils.logging.event_logs.logging_handler import json_safe


@dataclasses.dataclass(slots=True, frozen=True)
class RunLogPaths:
    """
    Human-readable and structured forensic log paths for one run.

    Example:
        Exercise RunLogPaths through a consuming regression::

            python -m pytest -q tests/utils/logging/test_compat_logger.py
    """

    human_log: Path
    event_log: Path


@final
class LiuXinTextLogFormatter(logging.Formatter):
    """
    UTC text formatter that retains LiuXin event names and context.

    Example:
        Exercise LiuXinTextLogFormatter through a consuming regression::

            python -m pytest -q tests/utils/logging/test_compat_logger.py
    """

    converter = time.gmtime

    @override
    def format(self, record: logging.LogRecord) -> str:
        """
        Perform the format utility operation under explicit compatibility rules.

        Example:
            Exercise LiuXinTextLogFormatter.format through a consuming regression::

                python -m pytest -q tests/utils/logging/test_compat_logger.py


        :param record: Value supplied for record under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        rendered = super().format(record)
        event = getattr(record, "liuxin_event", None)
        details = getattr(record, "liuxin_context", None)
        suffix: list[str] = []
        if event is not None:
            suffix.append(f"event={event}")
        if details is not None:
            suffix.append(
                "context="
                + json.dumps(
                    json_safe(cast(object, details)),
                    ensure_ascii=True,
                    default=repr,
                    sort_keys=True,
                    separators=(",", ":"),
                )
            )
        if suffix:
            rendered += "\n  " + " ".join(suffix)
        return rendered


@final
class LoggingTextStream:
    """
    Line-buffered text stream that sends legacy output through logging.

    Example:
        Exercise LoggingTextStream through a consuming regression::

            python -m pytest -q tests/utils/logging/test_compat_logger.py
    """

    encoding = "utf-8"
    errors = "backslashreplace"

    def __init__(
        self,
        logger: logging.Logger,
        *,
        level: int = logging.INFO,
        stream_name: str = "stdout",
    ) -> None:
        """
        Initialize and validate the LoggingTextStream state.

        Example:
            Exercise LoggingTextStream.  init   through a consuming regression::

                python -m pytest -q tests/utils/logging/test_compat_logger.py


        :param logger: Value supplied for logger under the utility contract.
        :param level: Value supplied for level under the utility contract.
        :param stream_name: Value supplied for stream name under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.logger = logger
        self.level = int(level)
        self.stream_name = stream_name
        self._buffer = ""
        self._lock = threading.Lock()

    def write(self, value: str) -> int:
        """
        Forward the write operation while preserving adapter ownership rules.

        Example:
            Exercise LoggingTextStream.write through a consuming regression::

                python -m pytest -q tests/utils/logging/test_compat_logger.py


        :param value: Value normalized, stored, formatted or returned.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        complete_lines: list[str] = []
        with self._lock:
            self._buffer += value
            while "\n" in self._buffer:
                line, self._buffer = self._buffer.split("\n", 1)
                complete_lines.append(line.rstrip("\r"))
        for line in complete_lines:
            self._emit(line)
        return len(value)

    def flush(self) -> None:
        """
        Forward the flush operation while preserving adapter ownership rules.

        Example:
            Exercise LoggingTextStream.flush through a consuming regression::

                python -m pytest -q tests/utils/logging/test_compat_logger.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        line = ""
        with self._lock:
            if self._buffer:
                line = self._buffer.rstrip("\r")
                self._buffer = ""
        if line:
            self._emit(line)

    def isatty(self) -> bool:
        """
        Perform the isatty utility operation under explicit compatibility rules.

        Example:
            Exercise LoggingTextStream.isatty through a consuming regression::

                python -m pytest -q tests/utils/logging/test_compat_logger.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return False

    def writable(self) -> bool:
        """
        Perform the writable utility operation under explicit compatibility rules.

        Example:
            Exercise LoggingTextStream.writable through a consuming regression::

                python -m pytest -q tests/utils/logging/test_compat_logger.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return True

    def _emit(self, line: str) -> None:
        """
        Perform the emit utility operation under explicit compatibility rules.

        Example:
            Exercise LoggingTextStream. emit through a consuming regression::

                python -m pytest -q tests/utils/logging/test_compat_logger.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not line:
            return
        self.logger.log(
            self.level,
            line,
            extra={
                "liuxin_event": "captured_output",
                "liuxin_context": {"stream": self.stream_name},
            },
        )


@final
class RunLoggingSession:
    """
    Attach LiuXin JSONL and rotating text handlers for one operation.

    Example:
        Exercise RunLoggingSession through a consuming regression::

            python -m pytest -q tests/utils/logging/test_compat_logger.py
    """

    def __init__(
        self,
        log_directory: str | Path,
        *,
        run_id: UUID,
        prefix: str = "liuxin-run",
        level: int = logging.DEBUG,
        max_text_bytes: int = 100 * 1024 * 1024,
        text_backup_count: int = 10,
        memory_events: int = 10_000,
    ) -> None:
        """
        Initialize and validate the RunLoggingSession state.

        Example:
            Exercise RunLoggingSession.  init   through a consuming regression::

                python -m pytest -q tests/utils/logging/test_compat_logger.py


        :param log_directory: Value supplied for log directory under the utility contract.
        :param run_id: Value supplied for run id under the utility contract.
        :param prefix: Text prepended to the formatted or selected result.
        :param level: Value supplied for level under the utility contract.
        :param max_text_bytes: Value supplied for max text bytes under the utility contract.
        :param text_backup_count: Value supplied for text backup count under the utility
            contract.
        :param memory_events: Value supplied for memory events under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        if not prefix.strip():
            raise ValueError("prefix must not be empty.")
        if max_text_bytes < 1:
            raise ValueError("max_text_bytes must be positive.")
        if text_backup_count < 0:
            raise ValueError("text_backup_count must not be negative.")
        if memory_events < 1:
            raise ValueError("memory_events must be positive.")
        self.log_directory = Path(log_directory).expanduser().resolve(strict=False)
        self.run_id = run_id
        self.prefix = prefix.strip()
        self.level = int(level)
        self.max_text_bytes = int(max_text_bytes)
        self.text_backup_count = int(text_backup_count)
        self.memory_events = int(memory_events)
        self.paths: RunLogPaths | None = None
        self.event_log: InMemoryEventLog | None = None
        self._event_handler: EventLogHandler | None = None
        self._text_handler: RotatingFileHandler | None = None
        self._root_logger: logging.Logger | None = None
        self._old_root_level: int | None = None
        self._entered = False

    def __enter__(self) -> "RunLoggingSession":
        """
        Implement the resource's enter lifecycle operation.

        Example:
            Exercise RunLoggingSession.  enter   through a consuming regression::

                python -m pytest -q tests/utils/logging/test_compat_logger.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self._entered:
            raise RuntimeError("RunLoggingSession cannot be entered twice.")
        self.log_directory.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
        stem = f"{self.prefix}-{stamp}-{self.run_id}"
        paths = RunLogPaths(
            human_log=self.log_directory / f"{stem}.log",
            event_log=self.log_directory / f"{stem}.jsonl",
        )
        event_log: InMemoryEventLog | None = None
        event_handler: EventLogHandler | None = None
        text_handler: RotatingFileHandler | None = None
        try:
            event_log = InMemoryEventLog(
                max_entries=self.memory_events,
                persist_path=paths.event_log,
                include_level_name_in_jsonl=True,
            )
            event_handler = EventLogHandler(event_log, level=self.level)
            text_handler = RotatingFileHandler(
                paths.human_log,
                maxBytes=self.max_text_bytes,
                backupCount=self.text_backup_count,
                encoding="utf-8",
                errors="backslashreplace",
                delay=False,
            )
        except BaseException:
            if event_handler is not None:
                event_handler.close()
            if event_log is not None:
                event_log.close()
            raise
        assert event_handler is not None
        assert text_handler is not None
        text_handler.setLevel(self.level)
        text_handler.setFormatter(
            LiuXinTextLogFormatter(
                "%(asctime)s.%(msecs)03dZ %(levelname)s "
                + "%(process)d/%(threadName)s %(name)s: %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            )
        )
        root = logging.getLogger()
        old_level = root.level
        root.setLevel(min(old_level, self.level) if old_level else self.level)
        root.addHandler(event_handler)
        root.addHandler(text_handler)
        self.paths = paths
        self.event_log = event_log
        self._event_handler = event_handler
        self._text_handler = text_handler
        self._root_logger = root
        self._old_root_level = old_level
        self._entered = True
        return self

    def flush(self) -> None:
        """
        Forward the flush operation while preserving adapter ownership rules.

        Example:
            Exercise RunLoggingSession.flush through a consuming regression::

                python -m pytest -q tests/utils/logging/test_compat_logger.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self._event_handler is not None:
            self._event_handler.flush()
        if self._text_handler is not None:
            self._text_handler.flush()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback_value: TracebackType | None,
    ) -> None:
        """
        Implement the resource's exit lifecycle operation.

        Example:
            Exercise RunLoggingSession.  exit   through a consuming regression::

                python -m pytest -q tests/utils/logging/test_compat_logger.py


        :param exc_type: Value supplied for exc type under the utility contract.
        :param exc: Value supplied for exc under the utility contract.
        :param traceback_value: Value supplied for traceback value under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        del exc_type, exc, traceback_value
        root = self._root_logger
        event_handler = self._event_handler
        text_handler = self._text_handler
        try:
            self.flush()
            if root is not None and event_handler is not None:
                root.removeHandler(event_handler)
            if root is not None and text_handler is not None:
                root.removeHandler(text_handler)
            if event_handler is not None:
                event_handler.close()
            if text_handler is not None:
                text_handler.close()
            if self.event_log is not None:
                self.event_log.close()
        finally:
            if root is not None and self._old_root_level is not None:
                root.setLevel(self._old_root_level)
            self._entered = False


__all__ = [
    "LiuXinTextLogFormatter",
    "LoggingTextStream",
    "RunLogPaths",
    "RunLoggingSession",
]
