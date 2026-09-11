"""
Allocate terminal panes, detect geometry changes, and stage/flush curses drawing.

Status and telemetry refresh at a throttled cadence; job output and console draw
on each render. Individual text-drawing failures are tolerated, not every window
or content-generation error.
"""

from __future__ import annotations

import curses
import time

from .contracts import WindowedState


class LayoutMixin(WindowedState):
    """
    Supply geometry and drawing operations for the composed curses driver's panes.

    Pane owners provide content, while the final console draw flushes all staged
    window updates. Minimum sizes are layout preferences, not proof that a tiny
    terminal can accommodate the requested windows.

    Example:
        >>> driver._render(force_status=True)  # doctest: +SKIP
    """

    @property
    def terminal_width(self) -> int:
        """
        Report the screen width with a minimum of forty, or a pre-session default of 120.

        Example:
            >>> width = driver.terminal_width  # doctest: +SKIP


        :return: Integer formatting width; the minimum may exceed the actual screen width.
        """
        if self._stdscr is None:
            return 120
        _, width = self._stdscr.getmaxyx()
        return max(40, int(width))

    def _allocate_aux_panel_heights(
        self, *, rows: int, status_h: int
    ) -> tuple[int, int]:
        """
        Divide spare screen rows between enabled telemetry/job panes after reserving status and console.

        Reserve three console rows. Each enabled auxiliary pane needs four rows;
        when both minima cannot fit, telemetry gets first priority. Otherwise share
        spare rows round-robin up to configured desired heights, leaving excess to console.

        Example:
            >>> heights = driver._allocate_aux_panel_heights(rows=30, status_h=5)  # doctest: +SKIP


        :param rows: Available screen height in rows.
        :param status_h: Height already reserved for the status pane.
        :return: Telemetry and job heights in that order; zero disables an unfitted pane.
        """
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
        """
        Replace pane windows using the current screen dimensions and active selections.

        Stack status, optional telemetry, optional job output, then console. Status
        and console retain minimum heights of five and three. No screen is a no-op;
        creation errors propagate and can leave some windows already replaced.

        Example:
            >>> driver._rebuild_windows()  # doctest: +SKIP


        :return: ``None``; window references are assigned without drawing their contents.
        """
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
        """
        Reconcile pane geometry, throttle status/telemetry updates, and redraw job output and console.

        Missing screen or geometry/rebuild exceptions return without drawing. Later
        content/drawing errors are not globally swallowed. Status/telemetry cadence
        uses monotonic time and a minimum 0.2-second interval; forced updates bypass
        the timer. Its timestamp advances after both throttled panes render successfully.

        Example:
            >>> driver._render(force_status=False)  # doctest: +SKIP


        :param force_status: Refresh status and telemetry even before the configured interval elapses.
        :return: ``None`` after the applicable pane draws, unless a later rendering error propagates.
        """
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
        """
        Stage the first wrapped status lines and a bottom separator in an existing status pane.

        Reserve the last row for the separator and the last column from text width.
        Individual text/separator errors are ignored; erase, content, and refresh errors propagate.

        Example:
            >>> driver._render_status()  # doctest: +SKIP


        :return: ``None``; update is staged with ``noutrefresh``, not physically flushed here.
        """
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
        """
        Draw bottom-aligned console history and the input line, then flush all staged panes.

        Reserve the last row for prompt/input. Long input keeps its trailing visible
        characters and places the cursor at that displayed end. Text/cursor errors
        are ignored individually; erase, viewport, and refresh/update errors propagate.

        Example:
            >>> driver._render_console()  # doctest: +SKIP


        :return: ``None``; an existing console pane is staged and ``curses.doupdate`` is called.
        """
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
        """
        Stage the trailing wrapped telemetry lines that fit the existing telemetry pane.

        The content builder receives a pane-height budget before wrapping, then
        excess wrapped leading lines are dropped. Individual text errors are ignored.

        Example:
            >>> driver._render_telemetry()  # doctest: +SKIP


        :return: ``None``; no pane is a no-op and an existing pane's update is staged only.
        """
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
        """
        Stage the selected job viewport at the top of its pane, ignoring individual text errors.

        Viewport construction fetches fresh content and synchronizes job scrollback.
        Other window/content errors propagate rather than becoming empty output.

        Example:
            >>> driver._render_job_output()  # doctest: +SKIP


        :return: ``None``; absent panes are ignored and existing ones use ``noutrefresh``.
        """
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
        """
        Assign four rows per active pane, then distribute spare rows in repeated input-order rounds.

        Stop at each desired height, leaving unneeded rows undistributed. Callers
        supply unique pane names and a budget sufficient for all four-row minima.

        Example:
            >>> allocations = {"telemetry": 0, "job": 0}
            >>> LayoutMixin._distribute_panel_space(
            ...     [("telemetry", 6), ("job", 6)], allocations, 9
            ... )
            >>> allocations
            {'telemetry': 5, 'job': 4}


        :param active: Ordered pane-name/desired-height pairs participating in allocation.
        :param allocations: Mapping to update; entries for inactive panes remain untouched.
        :param remaining: Total auxiliary row budget before assigning any minima.
        :return: ``None``; active pane heights are written into ``allocations``.
        """
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
