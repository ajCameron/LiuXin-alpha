"""Console output buffering, wrapping, and stable scrollback."""

from __future__ import annotations

from .contracts import WindowedState


class ConsoleMixin(WindowedState):
    """Console output buffering, wrapping, and stable scrollback."""

    def clear_console_output(self) -> bool:
        had_content = bool(self._lines) or self._console_scroll_offset != 0
        self._lines.clear()
        self._console_scroll_offset = 0
        self._render(force_status=False)
        return had_content

    def append_output(self, text: str, *, end: str = "\n") -> None:
        payload = str(text) + str(end)
        if payload == "":
            return
        chunks = payload.splitlines()
        added_wrapped_lines = len(
            self._wrap_lines_for_width(chunks, width=self._console_content_width())
        )
        if self._console_scroll_offset > 0 and added_wrapped_lines > 0:
            self._console_scroll_offset += added_wrapped_lines
        for line in chunks:
            self._lines.append(line)
        self._render(force_status=False)

    def _console_content_width(self) -> int:
        win = self._console_win
        if win is not None:
            _, cols = win.getmaxyx()
            return max(1, cols - 1)
        return max(1, self.terminal_width - 1)

    def _console_visible_log_rows(self) -> int:
        win = self._console_win
        if win is not None:
            rows, _ = win.getmaxyx()
            return max(1, rows - 1)
        return 10

    def _wrapped_console_lines(self, *, width: int | None = None) -> list[str]:
        return self._wrap_lines_for_width(
            list(self._lines),
            width=max(
                1, self._console_content_width() if width is None else int(width)
            ),
        )

    def _visible_console_lines(
        self, *, width: int | None = None, visible_rows: int | None = None
    ) -> list[str]:
        wrapped = self._wrapped_console_lines(width=width)
        rows = (
            self._console_visible_log_rows()
            if visible_rows is None
            else max(0, int(visible_rows))
        )
        self._console_scroll_offset = self._clamp_scroll_offset(
            len(wrapped), rows, self._console_scroll_offset
        )
        if rows <= 0:
            return []
        end = len(wrapped) - self._console_scroll_offset
        start = max(0, end - rows)
        return wrapped[start:end]

    def _scroll_console_relative(
        self, delta: int, *, width: int | None = None, visible_rows: int | None = None
    ) -> bool:
        wrapped = self._wrapped_console_lines(width=width)
        rows = (
            self._console_visible_log_rows()
            if visible_rows is None
            else max(0, int(visible_rows))
        )
        previous = self._console_scroll_offset
        self._console_scroll_offset = self._clamp_scroll_offset(
            len(wrapped), rows, previous + int(delta)
        )
        changed = self._console_scroll_offset != previous
        if changed:
            self._render(force_status=False)
        return changed

    def _scroll_console_to_top(
        self, *, width: int | None = None, visible_rows: int | None = None
    ) -> bool:
        wrapped = self._wrapped_console_lines(width=width)
        rows = (
            self._console_visible_log_rows()
            if visible_rows is None
            else max(0, int(visible_rows))
        )
        previous = self._console_scroll_offset
        self._console_scroll_offset = self._clamp_scroll_offset(
            len(wrapped), rows, len(wrapped)
        )
        changed = self._console_scroll_offset != previous
        if changed:
            self._render(force_status=False)
        return changed

    def _scroll_console_to_bottom(self) -> bool:
        previous = self._console_scroll_offset
        self._console_scroll_offset = 0
        changed = self._console_scroll_offset != previous
        if changed:
            self._render(force_status=False)
        return changed
