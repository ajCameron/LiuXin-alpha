"""
Buffer console output and select a bottom-relative viewport for the curses pane.

Logical lines live in the driver's bounded deque; wrapping is recalculated for
the current width. New output advances a scrolled-back offset to preserve the
view where possible, subject to deque eviction and viewport clamping.
"""

from __future__ import annotations

from .contracts import WindowedState


class ConsoleMixin(WindowedState):
    """
    Supply console buffering and scroll operations to the composed curses driver.

    The driver initializes the line deque and scroll offset, and provides wrapping,
    bounds, and drawing hooks. A zero offset follows the newest output.

    Example:
        >>> driver.append_output("Conversion queued")  # doctest: +SKIP
        >>> moved = driver._scroll_console_to_top()  # doctest: +SKIP
    """

    def clear_console_output(self) -> bool:
        """
        Empty the logical-line buffer, reset scrollback, and redraw the console.

        A redraw occurs even when the buffer and offset were already empty.

        Example:
            >>> had_output = driver.clear_console_output()  # doctest: +SKIP


        :return: Whether buffered lines or a nonzero scroll offset existed beforehand.
        """
        had_content = bool(self._lines) or self._console_scroll_offset != 0
        self._lines.clear()
        self._console_scroll_offset = 0
        self._render(force_status=False)
        return had_content

    def append_output(self, text: str, *, end: str = "\n") -> None:
        """
        Append line-split output and redraw while compensating an active scrollback.

        Text and terminator are stringified together, then split with ``splitlines``.
        Each call appends separate logical lines; an unterminated fragment is not
        joined to the previous call. The bounded deque may evict old content. A
        scrolled-back offset grows by the new wrapped-line count before redraw.

        Example:
            >>> driver.append_output("Queued", end="")  # doctest: +SKIP


        :param text: Console text to append, possibly containing multiple line breaks.
        :param end: Suffix appended before splitting; defaults to a newline.
        :return: ``None``; a completely empty combined payload makes no changes.
        """
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
        """
        Reserve the console's final column and return at least one usable character.

        Example:
            >>> width = driver._console_content_width()  # doctest: +SKIP


        :return: Window width minus one, or terminal width minus one before window creation.
        """
        win = self._console_win
        if win is not None:
            _, cols = win.getmaxyx()
            return max(1, cols - 1)
        return max(1, self.terminal_width - 1)

    def _console_visible_log_rows(self) -> int:
        """
        Reserve one console row for input and report the remaining log capacity.

        Example:
            >>> rows = driver._console_visible_log_rows()  # doctest: +SKIP


        :return: At least one log row for an existing window, or ten before it exists.
        """
        win = self._console_win
        if win is not None:
            rows, _ = win.getmaxyx()
            return max(1, rows - 1)
        return 10

    def _wrapped_console_lines(self, *, width: int | None = None) -> list[str]:
        """
        Wrap a snapshot of all buffered logical lines without changing scroll state.

        Example:
            >>> lines = driver._wrapped_console_lines(width=40)  # doctest: +SKIP


        :param width: Explicit character width, clamped to one; ``None`` uses the pane width.
        :return: Newly wrapped lines in buffer order, including preserved blank lines.
        """
        return self._wrap_lines_for_width(
            list(self._lines),
            width=max(
                1, self._console_content_width() if width is None else int(width)
            ),
        )

    def _visible_console_lines(
        self, *, width: int | None = None, visible_rows: int | None = None
    ) -> list[str]:
        """
        Select wrapped lines at the current scroll offset and clamp that stored offset.

        The default viewport reserves space for the prompt. An explicit nonpositive
        height returns no lines but still updates the offset to the valid range.

        Example:
            >>> visible = driver._visible_console_lines(  # doctest: +SKIP
            ...     width=40, visible_rows=8
            ... )


        :param width: Wrapping width, or ``None`` to use the current console width.
        :param visible_rows: Viewport height, or ``None`` to use the console log capacity.
        :return: Visible lines in chronological order, possibly fewer than the viewport height.
        """
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
        """
        Move the console viewport by wrapped lines and redraw only if its offset changes.

        Positive deltas move toward older output; negative deltas move toward the
        newest output. Bounds use the requested wrapping width and viewport height.

        Example:
            >>> moved = driver._scroll_console_relative(10)  # doctest: +SKIP


        :param delta: Signed number of wrapped lines to add to the scrollback offset.
        :param width: Optional wrapping width instead of the current console width.
        :param visible_rows: Optional viewport height, clamped to zero or greater.
        :return: Whether the clamped stored offset differs from its previous value.
        """
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
        """
        Select the oldest available console viewport and redraw if its offset changes.

        Example:
            >>> moved = driver._scroll_console_to_top(visible_rows=8)  # doctest: +SKIP


        :param width: Optional wrapping width instead of the current console width.
        :param visible_rows: Optional viewport height, clamped to zero or greater.
        :return: Whether selecting the maximum scrollback changed the stored offset.
        """
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
        """
        Reset scrollback to follow the newest output and redraw only when needed.

        Example:
            >>> moved = driver._scroll_console_to_bottom()  # doctest: +SKIP


        :return: Whether the console previously had a nonzero scroll offset.
        """
        previous = self._console_scroll_offset
        self._console_scroll_offset = 0
        changed = self._console_scroll_offset != previous
        if changed:
            self._render(force_status=False)
        return changed
