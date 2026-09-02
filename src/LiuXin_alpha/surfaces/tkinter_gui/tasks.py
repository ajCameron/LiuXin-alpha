"""Threaded task runner for Tkinter-safe background work."""

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
    """Success or failure delivered from a worker back to the Tk thread."""

    task_id: str
    name: str
    ok: bool
    result: Any = None
    error: str = ""
    exception: BaseException | None = None


@dataclass(frozen=True)
class TkGuiTaskHandle:
    """Caller-facing identity and cancellation handle for background work."""

    task_id: str
    name: str
    future: Future

    def cancel(self) -> bool:
        return bool(self.future.cancel())


@dataclass(frozen=True)
class _TaskCallbacks:
    on_success: TaskCallback | None = None
    on_error: TaskCallback | None = None
    on_done: TaskCallback | None = None


class TkGuiTaskRunner:
    """Run blocking work off the Tk thread and deliver results on poll."""

    def __init__(
        self,
        *,
        after: Callable[[int, Callable[[], None]], object] | None = None,
        poll_interval_ms: int = 50,
        max_workers: int = 1,
        thread_name_prefix: str = "liuxin-tk-gui",
    ) -> None:
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
        return bool(self._closed)

    @property
    def pending_count(self) -> int:
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
        text = str(exc).strip()
        if text:
            return "{}: {}".format(exc.__class__.__name__, text)
        return exc.__class__.__name__

    def poll(self, *, max_results: int | None = None) -> int:
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
        if self._after is None or self._closed or self._polling:
            return False
        self._polling = True
        self._schedule_poll()
        return True

    def _schedule_poll(self) -> None:
        if self._after is None or self._closed or not self._polling:
            return
        self._after(self.poll_interval_ms, self._poll_once)

    def _poll_once(self) -> None:
        if self._closed:
            return
        self.poll()
        self._schedule_poll()

    def wait_for_idle(self, *, timeout_s: float = 1.0) -> bool:
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
