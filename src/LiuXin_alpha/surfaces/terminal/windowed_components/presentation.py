"""
Format compact pane text and compute character-based wrapping and scroll bounds.

These helpers produce strings and offsets without drawing curses windows. Their
width calculations use Python string lengths, not terminal display-cell widths.
"""

from __future__ import annotations

from .contracts import WindowedState


class PresentationMixin(WindowedState):
    """
    Supply shared formatting operations to the composed curses driver.

    The mixin does not initialize driver state or draw panes. Static helpers can also
    be used independently to format errors, wrap text, and constrain scrollback.

    Example:
        >>> PresentationMixin._wrap_lines_for_width(["abcdef", ""], width=4)
        ['abcd', 'ef', '']
    """

    @staticmethod
    def _format_error(prefix: str, exc: BaseException) -> str:
        """
        Combine an operation label, exception class, and nonblank exception message.

        Leading and trailing message whitespace is stripped. An empty message
        leaves just the label and class; internal line breaks are not removed.

        Example:
            >>> PresentationMixin._format_error("query failed", ValueError("bad id"))
            'query failed: ValueError: bad id'


        :param prefix: Context label to place before the exception details.
        :param exc: Exception whose class name and string message are displayed.
        :return: Diagnostic text without a traceback.
        """
        text = str(exc).strip()
        name = exc.__class__.__name__
        if text:
            return f"{prefix}: {name}: {text}"
        return f"{prefix}: {name}"

    @staticmethod
    def _wrap_lines_for_width(lines: list[str], *, width: int) -> list[str]:
        """
        Split each input string into fixed-width character slices, retaining blanks.

        Width is converted to an integer and clamped to at least one. Wrapping
        neither seeks word boundaries nor interprets tabs, ANSI codes, or wide
        characters; callers supply already separated logical lines.

        Example:
            >>> PresentationMixin._wrap_lines_for_width(["ab cd", ""], width=3)
            ['ab ', 'cd', '']


        :param lines: Logical lines to stringify and wrap in their original order.
        :param width: Maximum Python characters per returned nonempty line.
        :return: New list of wrapped lines, including one entry per empty input line.
        """
        usable = max(1, int(width))
        wrapped: list[str] = []
        for raw in lines:
            text = str(raw)
            if text == "":
                wrapped.append("")
                continue
            start = 0
            while start < len(text):
                wrapped.append(text[start : start + usable])
                start += usable
        return wrapped

    @staticmethod
    def _stringify_compact_value(value: object) -> str:
        """
        Convert a status value to trimmed text with each line break replaced by a space.

        ``None`` becomes an empty string. Other internal whitespace is retained;
        CRLF is treated as one line break rather than two spaces.

        Example:
            >>> PresentationMixin._stringify_compact_value(" ready " + chr(10) + "now ")
            'ready  now'
            >>> PresentationMixin._stringify_compact_value(None)
            ''


        :param value: Pane value to display without embedded line breaks.
        :return: Stripped string, or an empty string for ``None``.
        """
        if value is None:
            return ""
        return (
            str(value)
            .replace("\r\n", " ")
            .replace("\r", " ")
            .replace("\n", " ")
            .strip()
        )

    def _render_compact_sections(
        self,
        sections: list[tuple[str, list[tuple[str, object]]]],
        *,
        title: str | None = None,
    ) -> list[str]:
        """
        Build one compact line per nonempty section, optionally preceded by a title.

        Nonblank values become ``key=value`` segments, or bare values for blank
        keys. Section labels are retained even without values. Segments are joined
        with `` | ``; only values have line breaks flattened. No wrapping is done.

        Example:
            >>> driver._render_compact_sections(  # doctest: +SKIP
            ...     [("Jobs", [("running", 2), ("error", None)])], title="Status"
            ... )
            ['Status', 'Jobs | running=2']


        :param sections: Ordered section labels and ordered key/value rows.
        :param title: Optional leading title, included verbatim when truthy.
        :return: New list containing the title and each nonempty section line.
        """
        lines: list[str] = []
        if title:
            lines.append(str(title))
        for section_title, rows in sections:
            parts: list[str] = []
            label = str(section_title).strip()
            if label:
                parts.append(label)
            for key, value in rows:
                value_text = self._stringify_compact_value(value)
                if not value_text:
                    continue
                key_text = str(key).strip()
                if key_text:
                    parts.append(f"{key_text}={value_text}")
                else:
                    parts.append(value_text)
            if parts:
                lines.append(" | ".join(parts))
        return lines

    @staticmethod
    def _clamp_scroll_offset(total_lines: int, visible_rows: int, offset: int) -> int:
        """
        Bound a bottom-relative scroll offset to the available hidden line count.

        Zero selects the newest lines. Negative viewport sizes behave as zero,
        and content shorter than the viewport permits no scrollback.

        Example:
            >>> PresentationMixin._clamp_scroll_offset(12, 5, 20)
            7
            >>> PresentationMixin._clamp_scroll_offset(3, 5, -1)
            0


        :param total_lines: Number of wrapped content lines.
        :param visible_rows: Number of lines that the viewport can show.
        :param offset: Requested number of newest lines to keep below the viewport.
        :return: Integer offset between zero and the maximum available scrollback.
        """
        max_offset = max(0, int(total_lines) - max(0, int(visible_rows)))
        return max(0, min(max_offset, int(offset)))
