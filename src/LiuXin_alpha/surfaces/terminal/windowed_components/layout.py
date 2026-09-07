"""Pane allocation, terminal resizing, and curses drawing."""

from __future__ import annotations

import curses
import time

from .contracts import WindowedState


class LayoutMixin(WindowedState):
    """Pane allocation, terminal resizing, and curses drawing."""

    @property
    def terminal_width(self) -> int:
        if self._stdscr is None:
            return 120
        _, width = self._stdscr.getmaxyx()
        return max(40, int(width))

    def _allocate_aux_panel_heights(
        self, *, rows: int, status_h: int
    ) -> tuple[int, int]:
        active: list[tuple[str, int]] = []
        if self._telemetry_tables:
            active.append(
                ("telemetry", max(4, int(self.config.telemetry_panel_height)))
            )
        if self._job_output_job_id:
            active.append(("job", max(4, int(self.config.job_panel_height))))
        if not active:
            return (0, 0)

        remaining = max(0, int(rows) - int(status_h) - 3)
        allocations = {"telemetry": 0, "job": 0}
        if remaining < 4:
            return (0, 0)

        minimum_total = 4 * len(active)
        if remaining < minimum_total:
            for name, _desired in active:
                if remaining < 4:
                    break
                allocations[name] = 4
                remaining -= 4
            return (allocations["telemetry"], allocations["job"])

        self._distribute_panel_space(active, allocations, remaining)

        return (allocations["telemetry"], allocations["job"])

    def _rebuild_windows(self) -> None:
        if self._stdscr is None:
            return
        rows, cols = self._stdscr.getmaxyx()
        status_h = max(5, min(int(self.config.status_height), max(5, rows - 4)))
        telemetry_h, job_h = self._allocate_aux_panel_heights(
            rows=rows, status_h=status_h
        )
        console_h = max(3, rows - status_h - telemetry_h - job_h)
        self._status_win = curses.newwin(status_h, cols, 0, 0)
        y = status_h
        if telemetry_h > 0:
            self._telemetry_win = curses.newwin(telemetry_h, cols, y, 0)
            y += telemetry_h
        else:
            self._telemetry_win = None
        if job_h > 0:
            self._job_output_win = curses.newwin(job_h, cols, y, 0)
            y += job_h
        else:
            self._job_output_win = None
        self._console_win = curses.newwin(console_h, cols, y, 0)

    def _render(self, *, force_status: bool) -> None:
        if self._stdscr is None:
            return
        try:
            rows, cols = self._stdscr.getmaxyx()
            if self._status_win is None or self._console_win is None:
                self._rebuild_windows()
            else:
                s_rows, s_cols = self._status_win.getmaxyx()
                c_rows, c_cols = self._console_win.getmaxyx()
                t_rows = 0
                t_cols = cols
                j_rows = 0
                j_cols = cols
                if self._telemetry_win is not None:
                    t_rows, t_cols = self._telemetry_win.getmaxyx()
                if self._job_output_win is not None:
                    j_rows, j_cols = self._job_output_win.getmaxyx()
                if (
                    s_cols != cols
                    or c_cols != cols
                    or t_cols != cols
                    or j_cols != cols
                    or (s_rows + c_rows + t_rows + j_rows) != rows
                ):
                    self._rebuild_windows()
        except Exception:
            return

        now = time.monotonic()
        refresh_interval = max(0.2, float(self.config.status_refresh_s))
        should_render_status = force_status or (
            (now - self._status_last_render) >= refresh_interval
        )
        if should_render_status:
            self._render_status()
            self._render_telemetry()
            self._status_last_render = now
        self._render_job_output()
        self._render_console()

    def _render_status(self) -> None:
        win = self._status_win
        if win is None:
            return
        win.erase()
        rows, cols = win.getmaxyx()
        content_rows = max(0, rows - 1)
        lines = self._wrap_lines_for_width(
            self._build_status_lines(), width=max(1, cols - 1)
        )
        for idx in range(min(content_rows, len(lines))):
            line = str(lines[idx])
            try:
                win.addstr(idx, 0, line)
            except Exception:
                continue
        try:
            win.hline(rows - 1, 0, curses.ACS_HLINE, max(0, cols - 1))
        except Exception:
            pass
        win.noutrefresh()

    def _render_console(self) -> None:
        win = self._console_win
        if win is None:
            return
        win.erase()
        rows, cols = win.getmaxyx()
        visible_log_rows = max(0, rows - 1)

        tail = self._visible_console_lines(
            width=max(1, cols - 1), visible_rows=visible_log_rows
        )
        start_row = max(0, visible_log_rows - len(tail))
        for idx, line in enumerate(tail, start=start_row):
            text = str(line)
            try:
                win.addstr(idx, 0, text)
            except Exception:
                continue

        prompt_line = f"{self._current_prompt}{self._current_input}"
        if len(prompt_line) >= cols:
            prompt_line = prompt_line[-(cols - 1) :]
        try:
            win.addstr(rows - 1, 0, prompt_line)
        except Exception:
            pass

        cursor_x = min(max(0, len(prompt_line)), max(0, cols - 1))
        try:
            win.move(rows - 1, cursor_x)
        except Exception:
            pass
        win.noutrefresh()
        curses.doupdate()

    def _render_telemetry(self) -> None:
        win = self._telemetry_win
        if win is None:
            return
        win.erase()
        rows, cols = win.getmaxyx()
        lines = self._wrap_lines_for_width(
            self._build_telemetry_lines(max_lines=max(1, rows)),
            width=max(1, cols - 1),
        )
        tail = lines[-rows:]
        for idx, text in enumerate(tail):
            try:
                win.addstr(idx, 0, text)
            except Exception:
                continue
        win.noutrefresh()

    def _render_job_output(self) -> None:
        win = self._job_output_win
        if win is None:
            return
        win.erase()
        rows, cols = win.getmaxyx()
        lines = self._visible_job_output_lines(
            width=max(1, cols - 1), visible_rows=max(1, rows)
        )
        for idx, text in enumerate(lines):
            try:
                win.addstr(idx, 0, text)
            except Exception:
                continue
        win.noutrefresh()

    @staticmethod
    def _distribute_panel_space(
        active: list[tuple[str, int]], allocations: dict[str, int], remaining: int
    ) -> None:
        """Allocate minima, then share spare rows in the existing pane order."""
        extras: dict[str, int] = {}
        for name, desired in active:
            allocations[name] = 4
            remaining -= 4
            extras[name] = max(0, int(desired) - 4)
        while remaining > 0 and any(extras[name] > 0 for name, _ in active):
            for name, _desired in active:
                if remaining <= 0:
                    break
                if extras[name] <= 0:
                    continue
                allocations[name] += 1
                extras[name] -= 1
                remaining -= 1
