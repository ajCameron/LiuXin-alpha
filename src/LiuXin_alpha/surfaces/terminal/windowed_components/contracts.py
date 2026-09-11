"""
Declare shared curses-driver state and the named calls between pane/input/rendering owners.

The root driver initializes annotated fields; this ABC neither creates windows
nor initializes buffers, history, selections, caches, or scroll offsets. Abstract
methods keep incomplete compositions uninstantiable. Their documentation links
to the maintained implementation and does not provide an executable fallback.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections import deque
from pathlib import Path

from .models import CursesWindow, WindowedBrowser, WindowedUiConfig


class WindowedState(ABC):
    """
    Require initialized curses-pane state and the cross-owner capabilities used by the composed UI driver.

    Fields cover window references, console/history/input buffers, telemetry snapshots,
    job output, scroll focus, and completion state. None of them is allocated or
    runtime-validated here. Concrete owner mixins must implement every abstract method;
    the actual driver remains responsible for constructing a coherent state.

    Example:
        >>> import inspect
        >>> inspect.isabstract(WindowedState)
        True
    """

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
    def terminal_width(self) -> int:
        """
        Report the screen width with a minimum of forty, or a pre-session default of 120.

        See :meth:`LiuXin_alpha.surfaces.terminal.windowed_components.layout.LayoutMixin.terminal_width`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host.terminal_width  # doctest: +SKIP


        :return: Integer formatting width; the minimum may exceed the actual screen width.
        """
        ...

    @staticmethod
    @abstractmethod
    def _format_error(prefix: str, exc: BaseException) -> str:
        """
        Combine an operation label, exception class, and nonblank exception message.

        See :meth:`LiuXin_alpha.surfaces.terminal.windowed_components.presentation.PresentationMixin._format_error`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._format_error(prefix, exc)  # doctest: +SKIP


        :param prefix: Context label to place before the exception details.
        :param exc: Exception whose class name and string message are displayed.
        :return: Diagnostic text without a traceback.
        """
        ...

    @staticmethod
    @abstractmethod
    def _wrap_lines_for_width(lines: list[str], *, width: int) -> list[str]:
        """
        Split each input string into fixed-width character slices, retaining blanks.

        See :meth:`LiuXin_alpha.surfaces.terminal.windowed_components.presentation.PresentationMixin._wrap_lines_for_width`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._wrap_lines_for_width(lines, width=width)  # doctest: +SKIP


        :param lines: Logical lines to stringify and wrap in their original order.
        :param width: Maximum Python characters per returned nonempty line.
        :return: New list of wrapped lines, including one entry per empty input line.
        """
        ...

    @abstractmethod
    def _render_compact_sections(
        self,
        sections: list[tuple[str, list[tuple[str, object]]]],
        *,
        title: str | None = None,
    ) -> list[str]:
        """
        Build one compact line per nonempty section, optionally preceded by a title.

        See :meth:`LiuXin_alpha.surfaces.terminal.windowed_components.presentation.PresentationMixin._render_compact_sections`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._render_compact_sections(sections, title=title)  # doctest: +SKIP


        :param sections: Ordered section labels and ordered key/value rows.
        :param title: Optional leading title, included verbatim when truthy.
        :return: New list containing the title and each nonempty section line.
        """
        ...

    @staticmethod
    @abstractmethod
    def _clamp_scroll_offset(total_lines: int, visible_rows: int, offset: int) -> int:
        """
        Bound a bottom-relative scroll offset to the available hidden line count.

        See :meth:`LiuXin_alpha.surfaces.terminal.windowed_components.presentation.PresentationMixin._clamp_scroll_offset`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._clamp_scroll_offset(total_lines, visible_rows, offset)  # doctest: +SKIP


        :param total_lines: Number of wrapped content lines.
        :param visible_rows: Number of lines that the viewport can show.
        :param offset: Requested number of newest lines to keep below the viewport.
        :return: Integer offset between zero and the maximum available scrollback.
        """
        ...

    @abstractmethod
    def _console_visible_log_rows(self) -> int:
        """
        Reserve one console row for input and report the remaining log capacity.

        See :meth:`LiuXin_alpha.surfaces.terminal.windowed_components.console.ConsoleMixin._console_visible_log_rows`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._console_visible_log_rows()  # doctest: +SKIP


        :return: At least one log row for an existing window, or ten before it exists.
        """
        ...

    @abstractmethod
    def _job_output_visible_rows(self) -> int:
        """
        Report all job-window rows, or the configured height before the window exists.

        See :meth:`LiuXin_alpha.surfaces.terminal.windowed_components.job_output.JobOutputMixin._job_output_visible_rows`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._job_output_visible_rows()  # doctest: +SKIP


        :return: Pane/configured height converted to an integer and clamped to at least one.
        """
        ...

    @abstractmethod
    def _visible_console_lines(
        self, *, width: int | None = None, visible_rows: int | None = None
    ) -> list[str]:
        """
        Select wrapped lines at the current scroll offset and clamp that stored offset.

        See :meth:`LiuXin_alpha.surfaces.terminal.windowed_components.console.ConsoleMixin._visible_console_lines`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._visible_console_lines(width=width, visible_rows=visible_rows)  # doctest: +SKIP


        :param width: Wrapping width, or ``None`` to use the current console width.
        :param visible_rows: Viewport height, or ``None`` to use the console log capacity.
        :return: Visible lines in chronological order, possibly fewer than the viewport height.
        """
        ...

    @abstractmethod
    def _scroll_console_relative(
        self, delta: int, *, width: int | None = None, visible_rows: int | None = None
    ) -> bool:
        """
        Move the console viewport by wrapped lines and redraw only if its offset changes.

        See :meth:`LiuXin_alpha.surfaces.terminal.windowed_components.console.ConsoleMixin._scroll_console_relative`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._scroll_console_relative(delta, width=width, visible_rows=visible_rows)  # doctest: +SKIP


        :param delta: Signed number of wrapped lines to add to the scrollback offset.
        :param width: Optional wrapping width instead of the current console width.
        :param visible_rows: Optional viewport height, clamped to zero or greater.
        :return: Whether the clamped stored offset differs from its previous value.
        """
        ...

    @abstractmethod
    def _scroll_console_to_top(
        self, *, width: int | None = None, visible_rows: int | None = None
    ) -> bool:
        """
        Select the oldest available console viewport and redraw if its offset changes.

        See :meth:`LiuXin_alpha.surfaces.terminal.windowed_components.console.ConsoleMixin._scroll_console_to_top`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._scroll_console_to_top(width=width, visible_rows=visible_rows)  # doctest: +SKIP


        :param width: Optional wrapping width instead of the current console width.
        :param visible_rows: Optional viewport height, clamped to zero or greater.
        :return: Whether selecting the maximum scrollback changed the stored offset.
        """
        ...

    @abstractmethod
    def _scroll_console_to_bottom(self) -> bool:
        """
        Reset scrollback to follow the newest output and redraw only when needed.

        See :meth:`LiuXin_alpha.surfaces.terminal.windowed_components.console.ConsoleMixin._scroll_console_to_bottom`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._scroll_console_to_bottom()  # doctest: +SKIP


        :return: Whether the console previously had a nonzero scroll offset.
        """
        ...

    @abstractmethod
    def _visible_job_output_lines(
        self, *, width: int | None = None, visible_rows: int | None = None
    ) -> list[str]:
        """
        Fetch/wrap job content, synchronize scrollback, and select its visible slice.

        See :meth:`LiuXin_alpha.surfaces.terminal.windowed_components.job_output.JobOutputMixin._visible_job_output_lines`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._visible_job_output_lines(width=width, visible_rows=visible_rows)  # doctest: +SKIP


        :param width: Optional wrapping width; ``None`` uses the current job pane width.
        :param visible_rows: Optional height clamped to zero, or the pane's default capacity.
        :return: Visible wrapped lines in content order, possibly fewer than the viewport height.
        """
        ...

    @abstractmethod
    def _scroll_job_output_relative(
        self, delta: int, *, width: int | None = None, visible_rows: int | None = None
    ) -> bool:
        """
        Refresh job content, synchronize growth, then scroll by a signed wrapped-line delta.

        See :meth:`LiuXin_alpha.surfaces.terminal.windowed_components.job_output.JobOutputMixin._scroll_job_output_relative`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._scroll_job_output_relative(delta, width=width, visible_rows=visible_rows)  # doctest: +SKIP


        :param delta: Signed number of wrapped lines to add to the synchronized offset.
        :param width: Optional character width used to wrap freshly fetched content.
        :param visible_rows: Optional nonnegative viewport height instead of the default capacity.
        :return: Whether the requested move changed the synchronized offset after clamping.
        """
        ...

    @abstractmethod
    def _scroll_job_output_to_top(
        self, *, width: int | None = None, visible_rows: int | None = None
    ) -> bool:
        """
        Refresh/synchronize content and select the oldest available job viewport.

        See :meth:`LiuXin_alpha.surfaces.terminal.windowed_components.job_output.JobOutputMixin._scroll_job_output_to_top`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._scroll_job_output_to_top(width=width, visible_rows=visible_rows)  # doctest: +SKIP


        :param width: Optional character width used for the refreshed content.
        :param visible_rows: Optional viewport height, clamped to zero or greater.
        :return: Whether selecting maximum scrollback changed the synchronized offset; redraw if so.
        """
        ...

    @abstractmethod
    def _scroll_job_output_to_bottom(self) -> bool:
        """
        Follow the newest job output, redrawing only when scrollback was active.

        See :meth:`LiuXin_alpha.surfaces.terminal.windowed_components.job_output.JobOutputMixin._scroll_job_output_to_bottom`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._scroll_job_output_to_bottom()  # doctest: +SKIP


        :return: Whether the stored job-output offset changed to zero.
        """
        ...

    @abstractmethod
    def _active_scroll_target(self) -> str:
        """
        Resolve effective scroll focus, falling back to console when the job panel is disabled.

        See :meth:`LiuXin_alpha.surfaces.terminal.windowed_components.input.InputMixin._active_scroll_target`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._active_scroll_target()  # doctest: +SKIP


        :return: ``job`` only for job focus with a selected job; otherwise ``console``.
        """
        ...

    @abstractmethod
    def _reset_completion(self, *, keep_hint: bool = False) -> None:
        """
        Clear cached candidates, token bounds, and cycling state without editing input.

        See :meth:`LiuXin_alpha.surfaces.terminal.windowed_components.completion.CompletionMixin._reset_completion`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._reset_completion(keep_hint=keep_hint)  # doctest: +SKIP


        :param keep_hint: Preserve the displayed hint while clearing the cycle cache.
        :return: ``None``; completion state on the driver is reset in place.
        """
        ...

    @abstractmethod
    def _complete_current_input(self, *, direction: int = 1) -> bool:
        """
        Expand or select completion at the input end, or cycle an active selection.

        See :meth:`LiuXin_alpha.surfaces.terminal.windowed_components.completion.CompletionMixin._complete_current_input`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._complete_current_input(direction=direction)  # doctest: +SKIP


        :param direction: Integer cycle step; negative values start a fresh cycle last.
        :return: Whether a prefix expansion or candidate selection was handled.
        """
        ...

    @abstractmethod
    def _rebuild_windows(self) -> None:
        """
        Replace pane windows using the current screen dimensions and active selections.

        See :meth:`LiuXin_alpha.surfaces.terminal.windowed_components.layout.LayoutMixin._rebuild_windows`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._rebuild_windows()  # doctest: +SKIP


        :return: ``None``; window references are assigned without drawing their contents.
        """
        ...

    @abstractmethod
    def _build_status_lines(self) -> list[str]:
        """
        Build the current title and ordered runtime, context, rows, jobs, panels, and UI lines.

        See :meth:`LiuXin_alpha.surfaces.terminal.windowed_components.status.StatusMixin._build_status_lines`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._build_status_lines()  # doctest: +SKIP


        :return: Compact status lines, beginning with the timestamped application title.
        """
        ...

    @abstractmethod
    def _build_telemetry_lines(self, *, max_lines: int) -> list[str]:
        """
        Sample telemetry and return a height-limited activity/count/event summary.

        See :meth:`LiuXin_alpha.surfaces.terminal.windowed_components.telemetry.TelemetryMixin._build_telemetry_lines`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._build_telemetry_lines(max_lines=max_lines)  # doctest: +SKIP


        :param max_lines: Maximum number of compact lines to return, including the title.
        :return: Telemetry lines in display order, possibly truncated within the fixed sections.
        """
        ...

    @abstractmethod
    def _render(self, *, force_status: bool) -> None:
        """
        Reconcile pane geometry, throttle status/telemetry updates, and redraw job output and console.

        See :meth:`LiuXin_alpha.surfaces.terminal.windowed_components.layout.LayoutMixin._render`
        for the maintained owner's state transitions and error boundaries.

        Example:
            >>> result = host._render(force_status=force_status)  # doctest: +SKIP


        :param force_status: Refresh status and telemetry even before the configured interval elapses.
        :return: ``None`` after the applicable pane draws, unless a later rendering error propagates.
        """
        ...
