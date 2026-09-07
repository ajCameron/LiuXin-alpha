"""State and named cross-pane calls required by curses driver composition."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections import deque
from pathlib import Path

from .models import CursesWindow, WindowedBrowser, WindowedUiConfig


class WindowedState(ABC):
    """Driver-initialized state and required methods implemented by pane owners."""

    core: object
    config: WindowedUiConfig
    history_file: Path | None
    browser: WindowedBrowser | None
    _stdscr: CursesWindow | None
    _status_win: CursesWindow | None
    _telemetry_win: CursesWindow | None
    _job_output_win: CursesWindow | None
    _console_win: CursesWindow | None
    _status_last_render: float
    _lines: deque[str]
    _current_prompt: str
    _current_input: str
    _history: list[str]
    _history_cursor: int | None
    _telemetry_tables: tuple[str, ...] | None
    _telemetry_started_at: float | None
    _telemetry_baseline_counts: dict[str, int | None]
    _telemetry_last_counts: dict[str, int | None]
    _job_output_job_id: str | None
    _jobs_status_error: str | None
    _job_output_error: str | None
    _console_scroll_offset: int
    _job_output_scroll_offset: int
    _job_output_last_wrap_width: int | None
    _job_output_last_wrapped_count: int
    _scroll_focus: str
    _completion_hint: str | None
    _completion_matches: tuple[str, ...]
    _completion_base_input: str | None
    _completion_active_input: str | None
    _completion_token_start: int
    _completion_token_end: int
    _completion_index: int | None

    @property
    @abstractmethod
    def terminal_width(self) -> int: ...

    @staticmethod
    @abstractmethod
    def _format_error(prefix: str, exc: BaseException) -> str: ...

    @staticmethod
    @abstractmethod
    def _wrap_lines_for_width(lines: list[str], *, width: int) -> list[str]: ...

    @abstractmethod
    def _render_compact_sections(
        self,
        sections: list[tuple[str, list[tuple[str, object]]]],
        *,
        title: str | None = None,
    ) -> list[str]: ...

    @staticmethod
    @abstractmethod
    def _clamp_scroll_offset(
        total_lines: int, visible_rows: int, offset: int
    ) -> int: ...

    @abstractmethod
    def _console_visible_log_rows(self) -> int: ...

    @abstractmethod
    def _job_output_visible_rows(self) -> int: ...

    @abstractmethod
    def _visible_console_lines(
        self, *, width: int | None = None, visible_rows: int | None = None
    ) -> list[str]: ...

    @abstractmethod
    def _scroll_console_relative(
        self, delta: int, *, width: int | None = None, visible_rows: int | None = None
    ) -> bool: ...

    @abstractmethod
    def _scroll_console_to_top(
        self, *, width: int | None = None, visible_rows: int | None = None
    ) -> bool: ...

    @abstractmethod
    def _scroll_console_to_bottom(self) -> bool: ...

    @abstractmethod
    def _visible_job_output_lines(
        self, *, width: int | None = None, visible_rows: int | None = None
    ) -> list[str]: ...

    @abstractmethod
    def _scroll_job_output_relative(
        self, delta: int, *, width: int | None = None, visible_rows: int | None = None
    ) -> bool: ...

    @abstractmethod
    def _scroll_job_output_to_top(
        self, *, width: int | None = None, visible_rows: int | None = None
    ) -> bool: ...

    @abstractmethod
    def _scroll_job_output_to_bottom(self) -> bool: ...

    @abstractmethod
    def _active_scroll_target(self) -> str: ...

    @abstractmethod
    def _reset_completion(self, *, keep_hint: bool = False) -> None: ...

    @abstractmethod
    def _complete_current_input(self, *, direction: int = 1) -> bool: ...

    @abstractmethod
    def _rebuild_windows(self) -> None: ...

    @abstractmethod
    def _build_status_lines(self) -> list[str]: ...

    @abstractmethod
    def _build_telemetry_lines(self, *, max_lines: int) -> list[str]: ...

    @abstractmethod
    def _render(self, *, force_status: bool) -> None: ...
