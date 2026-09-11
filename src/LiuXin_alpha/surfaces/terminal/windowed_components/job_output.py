"""
Build the selected job's status/log pane and maintain its independent scrollback.

Content generation fetches a fresh job snapshot and reads its complete advertised
log through shared helpers. Wrapping and viewport selection happen afterward;
this owner does not maintain a bounded incremental log reader.
"""

from __future__ import annotations

from LiuXin_alpha.surfaces.terminal.job_view import (
    TerminalJobView,
    fetch_terminal_job_view,
    read_terminal_job_log_view,
)

from .contracts import WindowedState


class JobOutputMixin(WindowedState):
    """
    Manage job-panel selection, current content, and bottom-relative scroll position.

    The composed driver provides browser access, layout, text wrapping, and rendering.
    Selecting a job does not validate its existence before enabling the panel.

    Example:
        >>> changed = driver.set_job_output_job("job-123")  # doctest: +SKIP
    """

    def set_job_output_job(self, job_id: str | None) -> bool:
        """
        Select a stripped job ID or disable the panel for blank/``None`` input.

        A changed selection resets scroll/wrapping history. Disabling a job-focused
        panel returns focus to the console. Layout rebuild and status redraw happen
        even when the selected ID is unchanged.

        Example:
            >>> changed = driver.set_job_output_job(" job-123 ")  # doctest: +SKIP


        :param job_id: Job identifier to display, or a blank/``None`` value to disable the pane.
        :return: Whether the normalized selection changed, not whether the job exists.
        """
        previous = self._job_output_job_id
        normalized = str(job_id).strip() if job_id is not None else ""
        self._job_output_job_id = normalized or None
        changed = previous != self._job_output_job_id
        if changed:
            self._job_output_scroll_offset = 0
            self._job_output_last_wrap_width = None
            self._job_output_last_wrapped_count = 0
        if self._job_output_job_id is None and self._scroll_focus == "job":
            self._scroll_focus = "console"
        self._rebuild_windows()
        self._render(force_status=True)
        return changed

    def clear_job_output_job(self) -> bool:
        """
        Disable the job pane through the normal selection, focus, and redraw path.

        Example:
            >>> was_enabled = driver.clear_job_output_job()  # doctest: +SKIP


        :return: Whether clearing the selected job ID changed the panel selection.
        """
        return self.set_job_output_job(None)

    def _job_output_content_width(self) -> int:
        """
        Reserve the job pane's last column, falling back to terminal width before creation.

        Example:
            >>> width = driver._job_output_content_width()  # doctest: +SKIP


        :return: At least one usable character column after reserving the final column.
        """
        win = self._job_output_win
        if win is not None:
            _, cols = win.getmaxyx()
            return max(1, cols - 1)
        return max(1, self.terminal_width - 1)

    def _job_output_visible_rows(self) -> int:
        """
        Report all job-window rows, or the configured height before the window exists.

        Example:
            >>> rows = driver._job_output_visible_rows()  # doctest: +SKIP


        :return: Pane/configured height converted to an integer and clamped to at least one.
        """
        win = self._job_output_win
        if win is not None:
            rows, _ = win.getmaxyx()
            return max(1, rows)
        return max(1, int(self.config.job_panel_height))

    def _build_job_output_content_lines(self) -> list[str]:
        """
        Fetch job metadata and log content, returning status messages for unavailable resources.

        No selected job returns no lines. Missing browser and snapshot-fetch failures
        produce explicit status/error sections. Otherwise include job/state/path and
        either all log lines or the shared log reader's status message. Exceptions
        outside snapshot fetching are not caught here; no height/width limit is applied.

        Example:
            >>> lines = driver._build_job_output_content_lines()  # doctest: +SKIP


        :return: Header/status/log lines in display order, before wrapping or viewport selection.
        """
        job_id = str(self._job_output_job_id or "").strip()
        if not job_id:
            return []
        if self.browser is None:
            return self._render_compact_sections(
                [("Job", [("job_id", job_id), ("status", "browser unavailable")])],
                title="Job output",
            )

        try:
            info = self._get_job(job_id)
        except Exception as exc:
            return self._render_compact_sections(
                [
                    ("Job", [("job_id", job_id), ("status", "unavailable")]),
                    ("Error", [("", str(exc))]),
                ],
                title="Job output",
            )

        state = str(info.state or "")
        log_view = read_terminal_job_log_view(info)
        log_path = str(log_view.log_path or "")

        lines: list[str] = self._render_compact_sections(
            [
                (
                    "Job",
                    [
                        ("job_id", job_id),
                        ("state", state),
                        ("log_path", log_path or "<none>"),
                    ],
                )
            ],
            title="Job output",
        )
        if not log_view.lines:
            lines.extend(
                self._render_compact_sections([("Output", [("", log_view.message)])])
            )
            return lines

        lines.append("Log tail")
        lines.extend(log_view.lines)
        return lines

    def _build_job_output_lines(self, *, max_lines: int) -> list[str]:
        """
        Build fresh job content and retain its last requested logical lines.

        A nonpositive budget avoids fetching entirely. A short tail may omit metadata headings.

        Example:
            >>> tail = driver._build_job_output_lines(max_lines=10)  # doctest: +SKIP


        :param max_lines: Maximum number of unwrapped trailing content lines to return.
        :return: New tail list, or an empty list for a nonpositive budget.
        """
        if max_lines <= 0:
            return []
        lines = self._build_job_output_content_lines()
        return lines[-max_lines:]

    def _wrapped_job_output_lines(self, *, width: int | None = None) -> list[str]:
        """
        Fetch fresh complete job content and wrap it without updating scroll state.

        Example:
            >>> lines = driver._wrapped_job_output_lines(width=80)  # doctest: +SKIP


        :param width: Explicit character width clamped to one, or ``None`` for the pane width.
        :return: All wrapped status and log lines from the current snapshot.
        """
        return self._wrap_lines_for_width(
            self._build_job_output_content_lines(),
            width=max(
                1, self._job_output_content_width() if width is None else int(width)
            ),
        )

    def _sync_job_output_scroll_state(
        self, *, wrapped_count: int, width: int, visible_rows: int
    ) -> None:
        """
        Compensate active scrollback for growth at unchanged width, then clamp and save measurements.

        Width changes or shrinkage do not receive growth compensation. Measurements
        count wrapped status lines as well as log lines, not stable log-record identities.

        Example:
            >>> driver._sync_job_output_scroll_state(  # doctest: +SKIP
            ...     wrapped_count=40, width=80, visible_rows=10
            ... )


        :param wrapped_count: New total wrapped-line count for the selected job content.
        :param width: Width used to produce that count, stored for the next comparison.
        :param visible_rows: Viewport height used to bound the resulting scroll offset.
        :return: ``None``; scroll offset and previous measurements are updated in place.
        """
        if (
            self._job_output_scroll_offset > 0
            and self._job_output_last_wrap_width == width
            and wrapped_count > self._job_output_last_wrapped_count
        ):
            self._job_output_scroll_offset += (
                wrapped_count - self._job_output_last_wrapped_count
            )
        self._job_output_last_wrap_width = int(width)
        self._job_output_last_wrapped_count = int(wrapped_count)
        self._job_output_scroll_offset = self._clamp_scroll_offset(
            wrapped_count, visible_rows, self._job_output_scroll_offset
        )

    def _visible_job_output_lines(
        self, *, width: int | None = None, visible_rows: int | None = None
    ) -> list[str]:
        """
        Fetch/wrap job content, synchronize scrollback, and select its visible slice.

        Even an explicitly zero-height viewport fetches content and updates scroll state.

        Example:
            >>> lines = driver._visible_job_output_lines(visible_rows=10)  # doctest: +SKIP


        :param width: Optional wrapping width; ``None`` uses the current job pane width.
        :param visible_rows: Optional height clamped to zero, or the pane's default capacity.
        :return: Visible wrapped lines in content order, possibly fewer than the viewport height.
        """
        width_value = max(
            1, self._job_output_content_width() if width is None else int(width)
        )
        wrapped = self._wrapped_job_output_lines(width=width_value)
        rows = (
            self._job_output_visible_rows()
            if visible_rows is None
            else max(0, int(visible_rows))
        )
        self._sync_job_output_scroll_state(
            wrapped_count=len(wrapped), width=width_value, visible_rows=rows
        )
        if rows <= 0:
            return []
        end = len(wrapped) - self._job_output_scroll_offset
        start = max(0, end - rows)
        return wrapped[start:end]

    def _scroll_job_output_relative(
        self, delta: int, *, width: int | None = None, visible_rows: int | None = None
    ) -> bool:
        """
        Refresh job content, synchronize growth, then scroll by a signed wrapped-line delta.

        Positive values move toward older output. Redraw and the result reflect the
        requested move relative to the synchronized offset, not any earlier growth adjustment.

        Example:
            >>> moved = driver._scroll_job_output_relative(10)  # doctest: +SKIP


        :param delta: Signed number of wrapped lines to add to the synchronized offset.
        :param width: Optional character width used to wrap freshly fetched content.
        :param visible_rows: Optional nonnegative viewport height instead of the default capacity.
        :return: Whether the requested move changed the synchronized offset after clamping.
        """
        width_value = max(
            1, self._job_output_content_width() if width is None else int(width)
        )
        wrapped = self._wrapped_job_output_lines(width=width_value)
        rows = (
            self._job_output_visible_rows()
            if visible_rows is None
            else max(0, int(visible_rows))
        )
        self._sync_job_output_scroll_state(
            wrapped_count=len(wrapped), width=width_value, visible_rows=rows
        )
        previous = self._job_output_scroll_offset
        self._job_output_scroll_offset = self._clamp_scroll_offset(
            len(wrapped), rows, previous + int(delta)
        )
        changed = self._job_output_scroll_offset != previous
        if changed:
            self._render(force_status=False)
        return changed

    def _scroll_job_output_to_top(
        self, *, width: int | None = None, visible_rows: int | None = None
    ) -> bool:
        """
        Refresh/synchronize content and select the oldest available job viewport.

        Example:
            >>> moved = driver._scroll_job_output_to_top()  # doctest: +SKIP


        :param width: Optional character width used for the refreshed content.
        :param visible_rows: Optional viewport height, clamped to zero or greater.
        :return: Whether selecting maximum scrollback changed the synchronized offset; redraw if so.
        """
        width_value = max(
            1, self._job_output_content_width() if width is None else int(width)
        )
        wrapped = self._wrapped_job_output_lines(width=width_value)
        rows = (
            self._job_output_visible_rows()
            if visible_rows is None
            else max(0, int(visible_rows))
        )
        self._sync_job_output_scroll_state(
            wrapped_count=len(wrapped), width=width_value, visible_rows=rows
        )
        previous = self._job_output_scroll_offset
        self._job_output_scroll_offset = self._clamp_scroll_offset(
            len(wrapped), rows, len(wrapped)
        )
        changed = self._job_output_scroll_offset != previous
        if changed:
            self._render(force_status=False)
        return changed

    def _scroll_job_output_to_bottom(self) -> bool:
        """
        Follow the newest job output, redrawing only when scrollback was active.

        Example:
            >>> moved = driver._scroll_job_output_to_bottom()  # doctest: +SKIP


        :return: Whether the stored job-output offset changed to zero.
        """
        previous = self._job_output_scroll_offset
        self._job_output_scroll_offset = 0
        changed = self._job_output_scroll_offset != previous
        if changed:
            self._render(force_status=False)
        return changed

    def _get_job(self, job_id: str) -> TerminalJobView:
        """
        Fetch a nonwaiting normalized job snapshot and label retrieval failures for display.

        Clear the prior error first. Fetch failures store a Core/local diagnostic and
        raise a chained RuntimeError. An absent browser raises immediately without
        storing a diagnostic; capability-check errors while choosing a label also propagate.

        Example:
            >>> snapshot = driver._get_job("job-123")  # doctest: +SKIP


        :param job_id: Job identifier stringified before passing it to the shared fetcher.
        :return: Normalized job snapshot from the selected Core or local backend.
        :raises RuntimeError: If no browser is bound or job retrieval fails.
        """
        self._job_output_error = None
        if self.browser is None:
            raise RuntimeError("browser unavailable")
        try:
            return fetch_terminal_job_view(
                self.browser, job_id=str(job_id), do_wait=False, wait_timeout=None
            )
        except Exception as exc:
            label = (
                "core jobs.get failed"
                if hasattr(self.browser, "supports_core_queries")
                and bool(self.browser.supports_core_queries())
                else "local job unavailable"
            )
            message = self._format_error(label, exc)
            self._job_output_error = message
            raise RuntimeError(message) from exc
