"""Curses session lifetime, key handling, focus selection, and input history."""

from __future__ import annotations

import curses
from collections.abc import Callable

from .contracts import WindowedState
from .models import CursesWindow


class InputMixin(WindowedState):
    """Curses session lifetime, key handling, focus selection, and input history."""

    def _active_scroll_target(self) -> str:
        if self._scroll_focus == "job" and self._job_output_job_id:
            return "job"
        return "console"

    def _cycle_scroll_focus(self) -> bool:
        targets = ["console"]
        if self._job_output_job_id:
            targets.append("job")
        if len(targets) < 2:
            return False
        current = self._active_scroll_target()
        next_index = (targets.index(current) + 1) % len(targets)
        next_target = targets[next_index]
        changed = next_target != self._scroll_focus
        self._scroll_focus = next_target
        if changed:
            self._render(force_status=False)
        return changed

    @staticmethod
    def _focus_toggle_key() -> object:
        key_fn = getattr(curses, "KEY_F", None)
        if callable(key_fn):
            try:
                return key_fn(6)
            except Exception:
                return getattr(curses, "KEY_F6", None)
        return getattr(curses, "KEY_F6", None)

    def read_line(self, prompt: str, *, default: str | None = None) -> str:
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
                return self._accept_current_input()
            if self._handle_navigation_key(ch) or self._handle_buffer_key(ch):
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

    def _accept_current_input(self) -> str:
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

    def _handle_navigation_key(self, ch: str | int) -> bool:
        if ch == "\t":
            self._complete_current_input(direction=1)
            return True

        if ch == getattr(curses, "KEY_BTAB", None):
            self._complete_current_input(direction=-1)
            return True

        if ch == self._focus_toggle_key():
            self._cycle_scroll_focus()
            return True

        return self._handle_scroll_key(ch)

    def _handle_scroll_key(self, ch: str | int) -> bool:
        if ch == curses.KEY_PPAGE:
            if self._active_scroll_target() == "job":
                self._scroll_job_output_relative(self._job_output_visible_rows())
            else:
                self._scroll_console_relative(self._console_visible_log_rows())
            return True

        if ch == curses.KEY_NPAGE:
            if self._active_scroll_target() == "job":
                self._scroll_job_output_relative(-self._job_output_visible_rows())
            else:
                self._scroll_console_relative(-self._console_visible_log_rows())
            return True

        if ch == curses.KEY_HOME:
            if self._active_scroll_target() == "job":
                self._scroll_job_output_to_top()
            else:
                self._scroll_console_to_top()
            return True

        if ch == curses.KEY_END:
            if self._active_scroll_target() == "job":
                self._scroll_job_output_to_bottom()
            else:
                self._scroll_console_to_bottom()
            return True

        return False

    def _handle_buffer_key(self, ch: str | int) -> bool:
        if ch in (curses.KEY_BACKSPACE, "\b", "\x7f"):
            if self._current_input:
                self._current_input = self._current_input[:-1]
                self._reset_completion()
            return True

        if ch == curses.KEY_UP:
            self._previous_history_entry()
            return True

        if ch == curses.KEY_DOWN:
            self._next_history_entry()
            return True

        return False

    def _previous_history_entry(self) -> None:
        if not self._history:
            return
        if self._history_cursor is None:
            self._history_cursor = len(self._history) - 1
        elif self._history_cursor > 0:
            self._history_cursor -= 1
        self._current_input = self._history[self._history_cursor]
        self._reset_completion()
        return

    def _next_history_entry(self) -> None:
        if self._history_cursor is None:
            return
        if self._history_cursor >= len(self._history) - 1:
            self._history_cursor = None
            self._current_input = ""
        else:
            self._history_cursor += 1
            self._current_input = self._history[self._history_cursor]
        self._reset_completion()
        return

    def load_history(self) -> None:
        path = self.history_file
        if path is None:
            return
        try:
            if path.exists():
                text = path.read_text(encoding="utf-8", errors="replace")
                self._history = [
                    line.rstrip("\n") for line in text.splitlines() if line.strip()
                ]
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

    def run_with_curses(self, runner: Callable[[], int]) -> int:
        return int(curses.wrapper(self._wrapped_main, runner))

    def _wrapped_main(self, stdscr: CursesWindow, runner: Callable[[], int]) -> int:
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

    def _read_key_with_refresh(self) -> str | int | None:
        if self._stdscr is None:
            return None
        try:
            key = self._stdscr.get_wch()
            return key
        except curses.error:
            self._render(force_status=False)
            return None
