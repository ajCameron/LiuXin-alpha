"""Active job snapshots, log presentation, and independent scrollback."""

from __future__ import annotations

from LiuXin_alpha.surfaces.terminal.job_view import (
    TerminalJobView,
    fetch_terminal_job_view,
    read_terminal_job_log_view,
)

from .contracts import WindowedState


class JobOutputMixin(WindowedState):
    """Active job snapshots, log presentation, and independent scrollback."""

    def set_job_output_job(self, job_id: str | None) -> bool:
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
        return self.set_job_output_job(None)

    def _job_output_content_width(self) -> int:
        win = self._job_output_win
        if win is not None:
            _, cols = win.getmaxyx()
            return max(1, cols - 1)
        return max(1, self.terminal_width - 1)

    def _job_output_visible_rows(self) -> int:
        win = self._job_output_win
        if win is not None:
            rows, _ = win.getmaxyx()
            return max(1, rows)
        return max(1, int(self.config.job_panel_height))

    def _build_job_output_content_lines(self) -> list[str]:
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
        if max_lines <= 0:
            return []
        lines = self._build_job_output_content_lines()
        return lines[-max_lines:]

    def _wrapped_job_output_lines(self, *, width: int | None = None) -> list[str]:
        return self._wrap_lines_for_width(
            self._build_job_output_content_lines(),
            width=max(
                1, self._job_output_content_width() if width is None else int(width)
            ),
        )

    def _sync_job_output_scroll_state(
        self, *, wrapped_count: int, width: int, visible_rows: int
    ) -> None:
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
        previous = self._job_output_scroll_offset
        self._job_output_scroll_offset = 0
        changed = self._job_output_scroll_offset != previous
        if changed:
            self._render(force_status=False)
        return changed

    def _get_job(self, job_id: str) -> TerminalJobView:
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
