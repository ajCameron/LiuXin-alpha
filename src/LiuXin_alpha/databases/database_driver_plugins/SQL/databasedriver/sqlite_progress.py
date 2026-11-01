"""Opt SQLite drivers into progress callbacks without an idle VM handler.

These hooks cover the stdlib connections returned by both SQLite drivers. They
are independent of ingest progress events and test resource telemetry.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterable

ProgressCallback = Callable[[], bool | int | None]


class SQLiteProgressMixin:
    """Configure optional callbacks on existing and future driver connections.

    Settings belong to one driver instance. Callers must serialize changes with
    connection use, as they do other driver operations. Connection initialization
    finishes before callbacks are installed. No callback is registered by default.
    """

    _progress_callbacks: tuple[ProgressCallback, ...] = ()
    _progress_instruction_interval: int = 1000
    _open_connections: list[sqlite3.Connection]
    conn: sqlite3.Connection | None

    def set_progress_callbacks(
        self,
        callbacks: Iterable[ProgressCallback] = (),
        *,
        instruction_interval: int = 1000,
    ) -> None:
        """Replace callbacks, or remove the VM handler when the collection is empty.

        Callbacks run in registration order every requested number of SQLite VM
        instructions. None/false continues; the first truthy return interrupts the
        query and stops dispatch. Callback exceptions also interrupt through SQLite.
        Callbacks must not execute SQL or reconfigure this driver themselves.

        Example:
            ``driver.set_progress_callbacks((lambda: cancelled,),
            instruction_interval=1000)`` enables cancellation. Calling
            ``driver.set_progress_callbacks()`` removes all registered callbacks.

        :param callbacks: Zero-argument observers/cancellation callbacks.
        :param instruction_interval: Positive instruction interval, excluding bool.
        :return: None after updating live handles; closed tracked handles are skipped.
        """
        callbacks = tuple(callbacks)
        if any(not callable(callback) for callback in callbacks):
            raise TypeError("Progress callbacks must be callable")
        if (
            isinstance(instruction_interval, bool)
            or not isinstance(instruction_interval, int)
            or not 0 < instruction_interval <= 2**31 - 1
        ):
            raise ValueError("Progress interval must be a positive 32-bit integer")
        self._progress_callbacks = callbacks
        self._progress_instruction_interval = instruction_interval
        seen = set()
        for connection in (*self._open_connections, self.conn):
            if connection is None or id(connection) in seen:
                continue
            seen.add(id(connection))
            try:
                # The driver tracks handles which a borrower may already have closed.
                _ = connection.in_transaction
            except sqlite3.ProgrammingError:
                continue
            if callbacks:
                self._configure_progress_handler(connection)
            else:
                connection.set_progress_handler(None, 0)

    def _configure_progress_handler(self, connection: sqlite3.Connection) -> None:
        """Install the configured callbacks on a fully initialized new connection."""
        callbacks = self._progress_callbacks
        if not callbacks:
            return

        def dispatch() -> int:
            return int(any(callback() for callback in callbacks))

        connection.set_progress_handler(dispatch, self._progress_instruction_interval)
