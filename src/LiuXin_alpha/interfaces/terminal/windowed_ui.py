"""Curses-based split-pane UI for the terminal database browser.

Top pane:
- status/report board (database + jobs summary)

Bottom pane:
- command/output console with editable prompt
"""

from __future__ import annotations

import curses
import time

from collections import Counter, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from LiuXin_alpha.interfaces.terminal.text_browser import TextDatabaseBrowser


@dataclass
class WindowedUiConfig:
    """Configuration for the split-pane curses UI."""

    status_refresh_s: float = 1.0
    status_height: int = 9
    job_panel_height: int = 10
    max_console_lines: int = 4000


class _CursesUiDriver:
    """Low-level curses drawing/input helpers shared by windowed shell methods."""

    def __init__(self, db, *, config: WindowedUiConfig, history_file: Optional[str | Path]) -> None:
        self.db = db
        self.config = config
        self.history_file = Path(history_file).expanduser() if history_file else None

        self._stdscr = None
        self._status_win = None
        self._job_output_win = None
        self._console_win = None
        self._status_last_render = 0.0

        self._lines: deque[str] = deque(maxlen=max(100, int(config.max_console_lines)))
        self._current_prompt = ""
        self._current_input = ""
        self._history: list[str] = []
        self._history_cursor: Optional[int] = None
        self._job_output_job_id: Optional[str] = None
        self._jobs_status_error: Optional[str] = None
        self._job_output_error: Optional[str] = None
        self._console_scroll_offset = 0
        self._completion_hint: Optional[str] = None
        self._completion_matches: tuple[str, ...] = ()
        self._completion_base_input: Optional[str] = None
        self._completion_active_input: Optional[str] = None
        self._completion_token_start = 0
        self._completion_token_end = 0
        self._completion_index: Optional[int] = None
        self.browser: Optional[TextDatabaseBrowser] = None

    @property
    def terminal_width(self) -> int:
        if self._stdscr is None:
            return 120
        _, width = self._stdscr.getmaxyx()
        return max(40, int(width))

    def bind_browser(self, browser: TextDatabaseBrowser) -> None:
        self.browser = browser

    def set_job_output_job(self, job_id: Optional[str]) -> bool:
        previous = self._job_output_job_id
        normalized = str(job_id).strip() if job_id is not None else ""
        self._job_output_job_id = normalized or None
        changed = previous != self._job_output_job_id
        self._rebuild_windows()
        self._render(force_status=True)
        return changed

    def clear_job_output_job(self) -> bool:
        return self.set_job_output_job(None)

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
        added_wrapped_lines = len(self._wrap_lines_for_width(chunks, width=self._console_content_width()))
        if self._console_scroll_offset > 0 and added_wrapped_lines > 0:
            self._console_scroll_offset += added_wrapped_lines
        for line in chunks:
            self._lines.append(line)
        self._render(force_status=False)

    @staticmethod
    def _format_error(prefix: str, exc: BaseException) -> str:
        text = str(exc).strip()
        name = exc.__class__.__name__
        if text:
            return "{}: {}: {}".format(prefix, name, text)
        return "{}: {}".format(prefix, name)

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
        return str(value).replace("\r\n", " ").replace("\r", " ").replace("\n", " ").strip()

    def _render_compact_sections(
        self,
        sections: list[tuple[str, list[tuple[str, object]]]],
        *,
        title: Optional[str] = None,
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
                    parts.append("{}={}".format(key_text, value_text))
                else:
                    parts.append(value_text)
            if parts:
                lines.append(" | ".join(parts))
        return lines

    @staticmethod
    def _clamp_scroll_offset(total_lines: int, visible_rows: int, offset: int) -> int:
        max_offset = max(0, int(total_lines) - max(0, int(visible_rows)))
        return max(0, min(max_offset, int(offset)))

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

    def _wrapped_console_lines(self, *, width: Optional[int] = None) -> list[str]:
        return self._wrap_lines_for_width(
            list(self._lines),
            width=max(1, self._console_content_width() if width is None else int(width)),
        )

    def _visible_console_lines(
        self,
        *,
        width: Optional[int] = None,
        visible_rows: Optional[int] = None,
    ) -> list[str]:
        wrapped = self._wrapped_console_lines(width=width)
        rows = self._console_visible_log_rows() if visible_rows is None else max(0, int(visible_rows))
        self._console_scroll_offset = self._clamp_scroll_offset(len(wrapped), rows, self._console_scroll_offset)
        if rows <= 0:
            return []
        end = len(wrapped) - self._console_scroll_offset
        start = max(0, end - rows)
        return wrapped[start:end]

    def _scroll_console_relative(
        self,
        delta: int,
        *,
        width: Optional[int] = None,
        visible_rows: Optional[int] = None,
    ) -> bool:
        wrapped = self._wrapped_console_lines(width=width)
        rows = self._console_visible_log_rows() if visible_rows is None else max(0, int(visible_rows))
        previous = self._console_scroll_offset
        self._console_scroll_offset = self._clamp_scroll_offset(len(wrapped), rows, previous + int(delta))
        changed = self._console_scroll_offset != previous
        if changed:
            self._render(force_status=False)
        return changed

    def _scroll_console_to_top(self, *, width: Optional[int] = None, visible_rows: Optional[int] = None) -> bool:
        wrapped = self._wrapped_console_lines(width=width)
        rows = self._console_visible_log_rows() if visible_rows is None else max(0, int(visible_rows))
        previous = self._console_scroll_offset
        self._console_scroll_offset = self._clamp_scroll_offset(len(wrapped), rows, len(wrapped))
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

    @staticmethod
    def _shared_prefix(values: tuple[str, ...]) -> str:
        if not values:
            return ""
        prefix = values[0]
        for value in values[1:]:
            limit = min(len(prefix), len(value))
            idx = 0
            while idx < limit and prefix[idx] == value[idx]:
                idx += 1
            prefix = prefix[:idx]
            if not prefix:
                break
        return prefix

    def _reset_completion(self, *, keep_hint: bool = False) -> None:
        self._completion_matches = ()
        self._completion_base_input = None
        self._completion_active_input = None
        self._completion_token_start = 0
        self._completion_token_end = 0
        self._completion_index = None
        if not keep_hint:
            self._completion_hint = None

    def _format_completion_hint(self, matches: tuple[str, ...], *, selected_index: Optional[int] = None) -> str:
        preview = list(matches[:6])
        preview_text = ", ".join(preview)
        remaining = len(matches) - len(preview)
        if remaining > 0:
            preview_text += " (+{} more)".format(remaining)
        if selected_index is None or selected_index < 0 or selected_index >= len(matches):
            return "completion: {}".format(preview_text)
        return "completion: {} ({}/{}) | matches: {}".format(
            matches[selected_index],
            selected_index + 1,
            len(matches),
            preview_text,
        )

    def _apply_completion_candidate(self, candidate: str) -> str:
        base = str(self._completion_base_input or self._current_input)
        start = max(0, int(self._completion_token_start))
        end = max(start, int(self._completion_token_end))
        completed = "{}{}{}".format(base[:start], candidate, base[end:])
        if not completed.endswith(" "):
            completed += " "
        return completed

    def _complete_current_input(self, *, direction: int = 1) -> bool:
        browser = self.browser
        if browser is None or not hasattr(browser, "command_completion_candidates"):
            self._reset_completion()
            return False

        if (
            self._completion_matches
            and self._completion_base_input is not None
            and self._completion_active_input == self._current_input
            and self._completion_index is not None
        ):
            next_index = (self._completion_index + int(direction)) % len(self._completion_matches)
            self._completion_index = next_index
            self._current_input = self._apply_completion_candidate(self._completion_matches[next_index])
            self._completion_active_input = self._current_input
            self._completion_hint = self._format_completion_hint(
                self._completion_matches,
                selected_index=next_index,
            )
            self._render(force_status=False)
            return True

        try:
            completion = browser.command_completion_candidates(self._current_input, cursor=len(self._current_input))
        except Exception:
            self._reset_completion()
            return False

        matches = tuple(str(candidate) for candidate in completion.candidates if str(candidate))
        if not matches:
            self._reset_completion()
            return False

        self._reset_completion(keep_hint=True)
        shared_prefix = self._shared_prefix(matches)
        if len(matches) > 1 and len(shared_prefix) > len(str(completion.prefix)):
            self._current_input = (
                self._current_input[: completion.token_start]
                + shared_prefix
                + self._current_input[completion.token_end :]
            )
            self._completion_hint = self._format_completion_hint(matches)
            self._render(force_status=False)
            return True

        if len(matches) == 1:
            self._completion_base_input = self._current_input
            self._completion_token_start = int(completion.token_start)
            self._completion_token_end = int(completion.token_end)
            self._current_input = self._apply_completion_candidate(matches[0])
            self._completion_active_input = self._current_input
            self._completion_hint = self._format_completion_hint(matches, selected_index=0)
            self._render(force_status=False)
            return True

        self._completion_matches = matches
        self._completion_base_input = self._current_input
        self._completion_token_start = int(completion.token_start)
        self._completion_token_end = int(completion.token_end)
        self._completion_index = 0 if int(direction) >= 0 else (len(matches) - 1)
        self._current_input = self._apply_completion_candidate(matches[self._completion_index])
        self._completion_active_input = self._current_input
        self._completion_hint = self._format_completion_hint(matches, selected_index=self._completion_index)
        self._render(force_status=False)
        return True

    def read_line(self, prompt: str, *, default: Optional[str] = None) -> str:
        self._current_prompt = str(prompt)
        self._current_input = "" if default is None else str(default)
        self._history_cursor = None
        self._reset_completion()

        while True:
            self._render(force_status=False)
            ch = self._read_key_with_refresh()
            if ch is None:
                continue

            if ch in ("\n", "\r", curses.KEY_ENTER):
                value = self._current_input
                if value.strip():
                    self._history.append(value)
                self._console_scroll_offset = 0
                self._current_prompt = ""
                self._current_input = ""
                self._history_cursor = None
                self._reset_completion()
                self._render(force_status=True)
                return value

            if ch == "\t":
                self._complete_current_input(direction=1)
                continue

            if ch == getattr(curses, "KEY_BTAB", None):
                self._complete_current_input(direction=-1)
                continue

            if ch == curses.KEY_PPAGE:
                self._scroll_console_relative(self._console_visible_log_rows())
                continue

            if ch == curses.KEY_NPAGE:
                self._scroll_console_relative(-self._console_visible_log_rows())
                continue

            if ch == curses.KEY_HOME:
                self._scroll_console_to_top()
                continue

            if ch == curses.KEY_END:
                self._scroll_console_to_bottom()
                continue

            if ch in (curses.KEY_BACKSPACE, "\b", "\x7f"):
                if self._current_input:
                    self._current_input = self._current_input[:-1]
                    self._reset_completion()
                continue

            if ch == curses.KEY_UP:
                if not self._history:
                    continue
                if self._history_cursor is None:
                    self._history_cursor = len(self._history) - 1
                elif self._history_cursor > 0:
                    self._history_cursor -= 1
                self._current_input = self._history[self._history_cursor]
                self._reset_completion()
                continue

            if ch == curses.KEY_DOWN:
                if self._history_cursor is None:
                    continue
                if self._history_cursor >= len(self._history) - 1:
                    self._history_cursor = None
                    self._current_input = ""
                else:
                    self._history_cursor += 1
                    self._current_input = self._history[self._history_cursor]
                self._reset_completion()
                continue

            if ch == "\x04":  # Ctrl-D
                if not self._current_input:
                    self._current_prompt = ""
                    self._current_input = ""
                    self._history_cursor = None
                    self._reset_completion()
                    self._render(force_status=True)
                    return ""
                continue

            if ch == "\x03":  # Ctrl-C
                raise KeyboardInterrupt

            if isinstance(ch, str) and ch.isprintable():
                self._current_input += ch
                self._reset_completion()

    def load_history(self) -> None:
        path = self.history_file
        if path is None:
            return
        try:
            if path.exists():
                text = path.read_text(encoding="utf-8", errors="replace")
                self._history = [line.rstrip("\n") for line in text.splitlines() if line.strip()]
        except Exception:
            self._history = []

    def save_history(self) -> None:
        path = self.history_file
        if path is None:
            return
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            lines = self._history[-1000:]
            payload = "\n".join(lines)
            if payload and not payload.endswith("\n"):
                payload += "\n"
            path.write_text(payload, encoding="utf-8")
        except Exception:
            return

    def run_with_curses(self, runner) -> int:
        return int(curses.wrapper(self._wrapped_main, runner))

    def _wrapped_main(self, stdscr, runner) -> int:
        self._stdscr = stdscr
        curses.curs_set(1)
        stdscr.keypad(True)
        stdscr.timeout(200)
        self._rebuild_windows()
        self.load_history()
        self._render(force_status=True)
        try:
            return int(runner())
        finally:
            self.save_history()

    def _read_key_with_refresh(self):
        if self._stdscr is None:
            return None
        try:
            key = self._stdscr.get_wch()
            return key
        except curses.error:
            self._render(force_status=False)
            return None

    def _rebuild_windows(self) -> None:
        if self._stdscr is None:
            return
        rows, cols = self._stdscr.getmaxyx()
        status_h = max(5, min(int(self.config.status_height), max(5, rows - 4)))
        job_h = 0
        if self._job_output_job_id:
            remaining_after_status = max(3, rows - status_h)
            if remaining_after_status >= 7:
                job_h = max(4, min(int(self.config.job_panel_height), remaining_after_status - 3))
        console_h = max(3, rows - status_h - job_h)
        self._status_win = curses.newwin(status_h, cols, 0, 0)
        y = status_h
        if job_h > 0:
            self._job_output_win = curses.newwin(job_h, cols, y, 0)
            y += job_h
        else:
            self._job_output_win = None
        self._console_win = curses.newwin(console_h, cols, y, 0)

    @staticmethod
    def _job_field(info: object, key: str, default=None):
        if isinstance(info, dict):
            return info.get(key, default)
        return getattr(info, key, default)

    def _list_jobs(self) -> list[object]:
        self._jobs_status_error = None
        if self.browser is None:
            return []
        if hasattr(self.browser, "supports_core_queries") and bool(self.browser.supports_core_queries()):
            try:
                result = self.browser.execute_core_query(
                    "jobs.list",
                    payload={"offset": 0, "limit": 5000},
                )
                return list((result or {}).get("jobs", ()) or ())
            except Exception as exc:
                self._jobs_status_error = self._format_error("core jobs.list failed", exc)
                return []
        try:
            return list(self.browser.job_manager.list())
        except Exception as exc:
            self._jobs_status_error = self._format_error("local jobs unavailable", exc)
            return []

    def _get_job(self, job_id: str) -> object:
        self._job_output_error = None
        if self.browser is None:
            raise RuntimeError("browser unavailable")
        if hasattr(self.browser, "supports_core_queries") and bool(self.browser.supports_core_queries()):
            try:
                result = self.browser.execute_core_query("jobs.get", payload={"job_id": str(job_id)})
                return (result or {}).get("job", {})
            except Exception as exc:
                message = self._format_error("core jobs.get failed", exc)
                self._job_output_error = message
                raise RuntimeError(message) from exc
        try:
            return self.browser.job_manager.get(job_id)
        except Exception as exc:
            message = self._format_error("local job unavailable", exc)
            self._job_output_error = message
            raise RuntimeError(message) from exc

    def _build_status_lines(self) -> list[str]:
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        db_path = ""
        if self.browser is not None:
            try:
                db_path = str(self.browser.database_path)
            except Exception:
                db_path = ""
        title = "LiuXin Terminal UI | {}".format(now)
        if db_path:
            title += " | db={}".format(db_path)

        sections: list[tuple[str, list[tuple[str, object]]]] = []
        if self.browser is None:
            return self._render_compact_sections([], title=title)

        core_status = ""
        if hasattr(self.browser, "core_runtime_status_summary"):
            try:
                core_status = str(self.browser.core_runtime_status_summary() or "").strip()
            except Exception:
                core_status = ""
        if core_status:
            sections.append(("Runtime", [("", core_status)]))

        table = self.browser.current_table or "<none>"
        window = self.browser.window
        if window is None:
            window_text = "window: <none>"
        else:
            window_text = "window: {} limit={} offset={}".format(window.table, window.limit, window.offset)
        sections.append(
            (
                "Context",
                [
                    ("table", table),
                    ("page_size", self.browser.page_size),
                    ("window", window_text.replace("window: ", "", 1)),
                ],
            )
        )

        table_counts: list[str] = []
        for name in ("works", "expressions", "manifestations", "items", "files", "stores"):
            try:
                count = self.browser.get_table_row_count(name)
            except Exception:
                count = None
            if count is None:
                continue
            table_counts.append("{}={}".format(name, count))
        if table_counts:
            sections.append(("Rows", [("", " | ".join(table_counts))]))

        jobs = self._list_jobs()
        counter = Counter(str(self._job_field(one, "state", "") or "") for one in jobs)
        job_rows: list[tuple[str, object]] = [("total", len(jobs))]
        if jobs:
            for key in sorted(counter.keys()):
                job_rows.append((key, counter.get(key, 0)))
        sections.append(("Jobs", job_rows))
        if self._jobs_status_error:
            sections.append(("Errors", [("jobs_error", self._jobs_status_error)]))
        if self._job_output_job_id:
            sections.append(("Panels", [("job_panel", self._job_output_job_id)]))
        ui_rows: list[tuple[str, object]] = []
        if self._console_scroll_offset > 0:
            ui_rows.append(("console_scrollback", "+{} | PgUp/PgDn Home/End".format(self._console_scroll_offset)))
        if self._completion_hint:
            ui_rows.append(("", self._completion_hint))
        if ui_rows:
            sections.append(("UI", ui_rows))
        return self._render_compact_sections(sections, title=title)

    def _build_job_output_lines(self, *, max_lines: int) -> list[str]:
        if max_lines <= 0:
            return []
        job_id = str(self._job_output_job_id or "").strip()
        if not job_id:
            return []
        if self.browser is None:
            return self._render_compact_sections(
                [("Job", [("job_id", job_id), ("status", "browser unavailable")])],
                title="Job output",
            )[:max_lines]

        try:
            info = self._get_job(job_id)
        except Exception as exc:
            return self._render_compact_sections(
                [
                    ("Job", [("job_id", job_id), ("status", "unavailable")]),
                    ("Error", [("", str(exc))]),
                ],
                title="Job output",
            )[:max_lines]

        state = str(self._job_field(info, "state", "") or "")
        log_path = str(self._job_field(info, "log_path", "") or "")
        execution = self._job_field(info, "execution", None)
        if not log_path and execution is not None:
            if isinstance(execution, dict):
                log_path = str(execution.get("log_path", "") or "")
            else:
                log_path = str(getattr(execution, "log_path", "") or "")

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
        if not log_path:
            lines.extend(self._render_compact_sections([("Output", [("", "No log path is available for this job.")])]))
            return lines[:max_lines]

        path = Path(log_path)
        if not path.exists():
            lines.extend(self._render_compact_sections([("Output", [("", "Log file not found yet: {}".format(log_path))])]))
            return lines[:max_lines]

        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except Exception as exc:
            lines.extend(
                self._render_compact_sections([("Output", [("", "Failed reading log {}: {}".format(log_path, exc))])])
            )
            return lines[:max_lines]

        payload_lines = text.splitlines()
        if not payload_lines:
            lines.extend(self._render_compact_sections([("Output", [("", "(no log output yet)")])]))
            return lines[:max_lines]

        keep = max(1, max_lines - len(lines))
        tail = payload_lines[-keep:]
        lines.append("Log tail")
        lines.extend(tail)
        return lines[-max_lines:]

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
                j_rows = 0
                j_cols = cols
                if self._job_output_win is not None:
                    j_rows, j_cols = self._job_output_win.getmaxyx()
                if s_cols != cols or c_cols != cols or j_cols != cols or (s_rows + c_rows + j_rows) != rows:
                    self._rebuild_windows()
        except Exception:
            return

        now = time.monotonic()
        refresh_interval = max(0.2, float(self.config.status_refresh_s))
        should_render_status = force_status or ((now - self._status_last_render) >= refresh_interval)
        if should_render_status:
            self._render_status()
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
        lines = self._wrap_lines_for_width(self._build_status_lines(), width=max(1, cols - 1))
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

        tail = self._visible_console_lines(width=max(1, cols - 1), visible_rows=visible_log_rows)
        start_row = max(0, visible_log_rows - len(tail))
        for idx, line in enumerate(tail, start=start_row):
            text = str(line)
            try:
                win.addstr(idx, 0, text)
            except Exception:
                continue

        prompt_line = "{}{}".format(self._current_prompt, self._current_input)
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

    def _render_job_output(self) -> None:
        win = self._job_output_win
        if win is None:
            return
        win.erase()
        rows, cols = win.getmaxyx()
        lines = self._wrap_lines_for_width(
            self._build_job_output_lines(max_lines=max(1, rows)),
            width=max(1, cols - 1),
        )
        tail = lines[-rows:]
        for idx, text in enumerate(tail):
            try:
                win.addstr(idx, 0, text)
            except Exception:
                continue
        win.noutrefresh()


class _WindowedTextDatabaseBrowser(TextDatabaseBrowser):
    """Text browser variant that reads/writes through curses panes."""

    def __init__(self, *args, ui_driver: _CursesUiDriver, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._ui_driver = ui_driver
        ui_driver.bind_browser(self)

    def _write(self, text: str, *, end: str = "\n") -> None:  # type: ignore[override]
        self._ui_driver.append_output(text, end=end)

    def _read_command_line(self) -> str:  # type: ignore[override]
        line = self._ui_driver.read_line(self.prompt)
        if line == "":
            return ""
        return line + "\n"

    def clear_output(self) -> bool:  # type: ignore[override]
        return self._ui_driver.clear_console_output()

    def prompt_text(self, prompt: str, *, default: Optional[str] = None) -> str:  # type: ignore[override]
        suffix = ""
        if default is not None:
            suffix = " [{}]".format(default)
        value = self._ui_driver.read_line("{}{}: ".format(prompt, suffix), default=default)
        if value == "" and default is not None:
            return default
        return value

    def prompt_yes_no(self, prompt: str, *, default: bool) -> bool:  # type: ignore[override]
        hint = "Y/n" if default else "y/N"
        raw = self.prompt_text("{} ({})".format(prompt, hint), default=None).strip().lower()
        if raw == "":
            return default
        if raw in {"y", "yes", "1", "true", "t"}:
            return True
        if raw in {"n", "no", "0", "false", "f"}:
            return False
        self._write("Invalid response {!r}; using default {}".format(raw, "yes" if default else "no"))
        return default

    def get_terminal_width(self) -> int:  # type: ignore[override]
        return self._ui_driver.terminal_width

    def supports_job_output_panel(self) -> bool:  # type: ignore[override]
        return True

    def attach_job_output_panel(self, job_id: str) -> bool:  # type: ignore[override]
        return self._ui_driver.set_job_output_job(job_id)

    def detach_job_output_panel(self) -> bool:  # type: ignore[override]
        return self._ui_driver.clear_job_output_job()


def run_windowed_browser(
    db,
    *,
    page_size: int = 20,
    history_file: Optional[str | Path] = None,
    config: Optional[WindowedUiConfig] = None,
) -> int:
    """Run the split-pane curses UI wrapper around `TextDatabaseBrowser`."""
    ui = _CursesUiDriver(
        db,
        config=config or WindowedUiConfig(),
        history_file=history_file,
    )

    def _runner() -> int:
        shell = _WindowedTextDatabaseBrowser(
            db,
            page_size=page_size,
            history_file=history_file,
            ui_driver=ui,
        )
        return shell.run()

    return ui.run_with_curses(_runner)


__all__ = [
    "WindowedUiConfig",
    "run_windowed_browser",
]
