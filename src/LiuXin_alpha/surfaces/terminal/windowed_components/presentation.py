"""Compact status text, line wrapping, and scroll bounds shared by panes."""

from __future__ import annotations

from .contracts import WindowedState


class PresentationMixin(WindowedState):
    """Compact status text, line wrapping, and scroll bounds shared by panes."""

    @staticmethod
    def _format_error(prefix: str, exc: BaseException) -> str:
        text = str(exc).strip()
        name = exc.__class__.__name__
        if text:
            return f"{prefix}: {name}: {text}"
        return f"{prefix}: {name}"

    @staticmethod
    def _wrap_lines_for_width(lines: list[str], *, width: int) -> list[str]:
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
        max_offset = max(0, int(total_lines) - max(0, int(visible_rows)))
        return max(0, min(max_offset, int(offset)))
