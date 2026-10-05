"""
Run background tasks for the Tk interface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise tasks through a consuming regression::

        python -m pytest -q tests/surfaces/test_tkinter_gui.py
"""

from __future__ import annotations

import queue
import threading
import time
import uuid

from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Callable


TaskCallback = Callable[["TkGuiTaskResult"], None]


@dataclass(frozen=True)
class TkGuiTaskResult:
    """
    Success or failure delivered from a worker back to the Tk thread.

    Example:
        Exercise TkGuiTaskResult through a consuming regression::

            python -m pytest -q tests/surfaces/test_tkinter_gui.py
    """

    task_id: str
    name: str
    ok: bool
    result: Any = None
    error: str = ""
    exception: BaseException | None = None


@dataclass(frozen=True)
class TkGuiTaskHandle:
    """
    Caller-facing identity and cancellation handle for background work.

    Example:
        Exercise TkGuiTaskHandle through a consuming regression::

            python -m pytest -q tests/surfaces/test_tkinter_gui.py
    """

    task_id: str
    name: str
    future: Future

    def cancel(self) -> bool:
        """
        Perform the cancel operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiTaskHandle.cancel through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return bool(self.future.cancel())


@dataclass(frozen=True)
class _TaskCallbacks:
    """
    Provide the taskcallbacks contract for validated ebook processing.

    Example:
        Exercise  TaskCallbacks through a consuming regression::

            python -m pytest -q tests/surfaces/test_tkinter_gui.py
    """
    on_success: TaskCallback | None = None
    on_error: TaskCallback | None = None
    on_done: TaskCallback | None = None


class TkGuiTaskRunner:
    """
    Run blocking work off the Tk thread and deliver results on poll.

    Example:
        Exercise TkGuiTaskRunner through a consuming regression::

            python -m pytest -q tests/surfaces/test_tkinter_gui.py
    """

    def __init__(
        self,
        *,
        after: Callable[[int, Callable[[], None]], object] | None = None,
        poll_interval_ms: int = 50,
        max_workers: int = 1,
        thread_name_prefix: str = "liuxin-tk-gui",
    ) -> None:
        """
        Initialize and validate the tkguitaskrunner state.

        Example:
            Exercise TkGuiTaskRunner.  init   through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param after: Value supplied for after under the utility contract.
        :param poll_interval_ms: Value supplied for poll interval ms under the utility
            contract.
        :param max_workers: Value supplied for max workers under the utility contract.
        :param thread_name_prefix: Value supplied for thread name prefix under the utility
            contract.
        :return: None; validated state is stored on the receiving object.
        """
        self._after = after
        self.poll_interval_ms = max(1, int(poll_interval_ms))
        self._executor = ThreadPoolExecutor(
            max_workers=max(1, int(max_workers)),
            thread_name_prefix=str(thread_name_prefix),
        )
        self._results: queue.Queue[TkGuiTaskResult] = queue.Queue()
        self._callbacks: dict[str, _TaskCallbacks] = {}
        self._futures: dict[str, Future] = {}
        self._lock = threading.RLock()
        self._closed = False
        self._polling = False

    @property
    def closed(self) -> bool:
        """
        Perform the closed operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiTaskRunner.closed through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return bool(self._closed)

    @property
    def pending_count(self) -> int:
        """
        Perform the pending count operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiTaskRunner.pending count through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        with self._lock:
            return len(self._futures)

    def submit(
        self,
        name: str,
        func: Callable[..., Any],
        *args: Any,
        on_success: TaskCallback | None = None,
        on_error: TaskCallback | None = None,
        on_done: TaskCallback | None = None,
        **kwargs: Any,
    ) -> TkGuiTaskHandle:
        """
        Perform the submit operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiTaskRunner.submit through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param func: Value supplied for func under the utility contract.
        :param args: Positional values forwarded to the compatibility implementation.
        :param on_success: Value supplied for on success under the utility contract.
        :param on_error: Value supplied for on error under the utility contract.
        :param on_done: Value supplied for on done under the utility contract.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self._closed:
            raise RuntimeError("Task runner is closed.")

        task_id = str(uuid.uuid4())
        task_name = str(name or "task")
        callbacks = _TaskCallbacks(
            on_success=on_success,
            on_error=on_error,
            on_done=on_done,
        )
        with self._lock:
            self._callbacks[task_id] = callbacks

        future = self._executor.submit(
            self._execute_task,
            task_id,
            task_name,
            func,
            args,
            kwargs,
        )
        with self._lock:
            self._futures[task_id] = future
        future.add_done_callback(lambda completed: self._mark_cancelled(task_id, task_name, completed))
        return TkGuiTaskHandle(task_id=task_id, name=task_name, future=future)

    def _execute_task(
        self,
        task_id: str,
        name: str,
        func: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> None:
        """
        Perform the execute task operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiTaskRunner. execute task through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param task_id: Value supplied for task id under the utility contract.
        :param name: Field, file, function or resource name addressed by the operation.
        :param func: Value supplied for func under the utility contract.
        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        try:
            result = func(*args, **kwargs)
        except BaseException as exc:
            self._results.put(
                TkGuiTaskResult(
                    task_id=task_id,
                    name=name,
                    ok=False,
                    error=self._summarize_exception(exc),
                    exception=exc,
                )
            )
            return
        self._results.put(
            TkGuiTaskResult(
                task_id=task_id,
                name=name,
                ok=True,
                result=result,
            )
        )

    def _mark_cancelled(self, task_id: str, name: str, completed: Future) -> None:
        """
        Perform the mark cancelled operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiTaskRunner. mark cancelled through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param task_id: Value supplied for task id under the utility contract.
        :param name: Field, file, function or resource name addressed by the operation.
        :param completed: Value supplied for completed under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if completed.cancelled():
            self._results.put(
                TkGuiTaskResult(
                    task_id=task_id,
                    name=name,
                    ok=False,
                    error="Task cancelled.",
                )
            )

    @staticmethod
    def _summarize_exception(exc: BaseException) -> str:
        """
        Perform the summarize exception operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiTaskRunner. summarize exception through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param exc: Value supplied for exc under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        text = str(exc).strip()
        if text:
            return "{}: {}".format(exc.__class__.__name__, text)
        return exc.__class__.__name__

    def poll(self, *, max_results: int | None = None) -> int:
        """
        Perform the poll operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiTaskRunner.poll through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param max_results: Value supplied for max results under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        processed = 0
        while max_results is None or processed < int(max_results):
            try:
                result = self._results.get_nowait()
            except queue.Empty:
                break
            with self._lock:
                callbacks = self._callbacks.pop(result.task_id, _TaskCallbacks())
                self._futures.pop(result.task_id, None)
            if result.ok:
                if callbacks.on_success is not None:
                    callbacks.on_success(result)
            elif callbacks.on_error is not None:
                callbacks.on_error(result)
            if callbacks.on_done is not None:
                callbacks.on_done(result)
            processed += 1
        return processed

    def start_polling(self) -> bool:
        """
        Perform the start polling operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiTaskRunner.start polling through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self._after is None or self._closed or self._polling:
            return False
        self._polling = True
        self._schedule_poll()
        return True

    def _schedule_poll(self) -> None:
        """
        Perform the schedule poll operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiTaskRunner. schedule poll through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self._after is None or self._closed or not self._polling:
            return
        self._after(self.poll_interval_ms, self._poll_once)

    def _poll_once(self) -> None:
        """
        Perform the poll once operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiTaskRunner. poll once through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self._closed:
            return
        self.poll()
        self._schedule_poll()

    def wait_for_idle(self, *, timeout_s: float = 1.0) -> bool:
        """
        Perform the wait for idle operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiTaskRunner.wait for idle through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param timeout_s: Value supplied for timeout s under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        deadline = time.monotonic() + max(0.0, float(timeout_s))
        while time.monotonic() <= deadline:
            self.poll()
            if self.pending_count <= 0:
                self.poll()
                return True
            time.sleep(0.01)
        self.poll()
        return self.pending_count <= 0

    def close(self, *, wait: bool = False, cancel_pending: bool = False) -> None:
        """
        Perform the close operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiTaskRunner.close through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param wait: Value supplied for wait under the utility contract.
        :param cancel_pending: Value supplied for cancel pending under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self._closed:
            return
        self._closed = True
        self._polling = False
        if cancel_pending:
            with self._lock:
                self._callbacks.clear()
        self._executor.shutdown(wait=bool(wait), cancel_futures=bool(cancel_pending))
        if wait and not cancel_pending:
            self.poll()


__all__ = [
    "TkGuiTaskHandle",
    "TkGuiTaskResult",
    "TkGuiTaskRunner",
]
