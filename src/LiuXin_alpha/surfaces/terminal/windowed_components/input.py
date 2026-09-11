"""
Handle curses session setup, end-of-buffer editing, pane navigation, and text history.

History persistence is best-effort. Interactive state and drawing are supplied by
the composed driver; key dispatch does not implement arbitrary cursor-position editing.
"""

from __future__ import annotations

import curses
from collections.abc import Callable

from .contracts import WindowedState
from .models import CursesWindow


class InputMixin(WindowedState):
    """
    Supply a synchronous input loop and session hooks to the composed curses driver.

    Printable input appends at the end; history replaces the whole buffer. Completion
    and scrolling delegate to their owners, while the session wrapper manages curses.

    Example:
        >>> response = driver.read_line("Name: ", default="Untitled")  # doctest: +SKIP
    """

    def _active_scroll_target(self) -> str:
        """
        Resolve effective scroll focus, falling back to console when the job panel is disabled.

        Example:
            >>> target = driver._active_scroll_target()  # doctest: +SKIP


        :return: ``job`` only for job focus with a selected job; otherwise ``console``.
        """
        if self._scroll_focus == "job" and self._job_output_job_id:
            return "job"
        return "console"

    def _cycle_scroll_focus(self) -> bool:
        """
        Toggle console/job focus when a job is selected, redrawing on a changed target.

        Without a job panel, return without rewriting the stored focus.

        Example:
            >>> changed = driver._cycle_scroll_focus()  # doctest: +SKIP


        :return: Whether the stored focus changed to another available target.
        """
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
        """
        Resolve F6 from a callable curses key factory, falling back to its named constant.

        Factory errors use the same fallback as an unavailable factory.

        Example:
            >>> key = InputMixin._focus_toggle_key()


        :return: Result of ``KEY_F(6)``, or ``KEY_F6``/``None`` when that path is unavailable.
        """
        key_fn = getattr(curses, "KEY_F", None)
        if callable(key_fn):
            try:
                return key_fn(6)
            except Exception:
                return getattr(curses, "KEY_F6", None)
        return getattr(curses, "KEY_F6", None)

    def read_line(self, prompt: str, *, default: str | None = None) -> str:
        """
        Read an editable line while refreshing panes and routing history/completion/scroll keys.

        Start from the supplied default, resetting history selection and completion.
        Enter accepts the buffer. Ctrl-D returns empty only for an empty buffer;
        otherwise it is ignored. Ctrl-C raises without clearing the current input
        state. Other printable strings append at the end; unhandled keys are ignored.

        Example:
            >>> value = driver.read_line("Search: ")  # doctest: +SKIP


        :param prompt: Prompt string displayed before the editable buffer.
        :param default: Optional initial editable text, not a fallback applied after acceptance.
        :return: Accepted text without a newline; empty input and empty-buffer Ctrl-D both return ``""``.
        :raises KeyboardInterrupt: If Ctrl-C is received.
        """
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
        """
        Save nonblank input to history, reset console/input state, and force a status redraw.

        Saved and returned text retains whitespace and duplicates. Blank input is not saved.

        Example:
            >>> accepted = driver._accept_current_input()  # doctest: +SKIP


        :return: Original buffer text, after successful state reset and redraw.
        """
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
        """
        Route Tab/back-Tab, F6, and scroll keys to completion or pane navigation.

        Recognized completion/focus keys count as handled even when their operation makes no change.

        Example:
            >>> handled = driver._handle_navigation_key(curses.KEY_NPAGE)  # doctest: +SKIP


        :param ch: Character or curses key code read from the active window.
        :return: Whether the key belongs to a supported navigation operation.
        """
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
        """
        Route page-up/down and home/end to the effectively focused console or job pane.

        Page moves use that pane's visible row count; home/end select its oldest/newest view.

        Example:
            >>> handled = driver._handle_scroll_key(curses.KEY_HOME)  # doctest: +SKIP


        :param ch: Curses key code or character to check for scroll actions.
        :return: Whether a scroll key was recognized, even if the viewport could not move.
        """
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
        """
        Handle trailing-character deletion and previous/next history selection.

        Backspace on an empty buffer is still handled; completion resets only after a deletion.

        Example:
            >>> handled = driver._handle_buffer_key(curses.KEY_BACKSPACE)  # doctest: +SKIP


        :param ch: Character or curses code to check for backspace and history keys.
        :return: Whether the key was recognized, irrespective of buffer/history changes.
        """
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
        """
        Recall the newest history entry or move toward the oldest, resetting completion.

        Empty history is a no-op; reaching the oldest entry keeps it selected.

        Example:
            >>> driver._previous_history_entry()  # doctest: +SKIP


        :return: ``None``; history position and input are updated without a direct redraw.
        """
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
        """
        Recall a newer history entry, or leave history with an empty input buffer.

        No selected history entry is a no-op. Moving past the newest entry does not restore a draft.

        Example:
            >>> driver._next_history_entry()  # doctest: +SKIP


        :return: ``None``; an active history transition also clears completion state.
        """
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
        """
        Replace history from an existing UTF-8 file, discarding blank lines and replacing decode errors.

        No configured path or an absent file leaves current history intact. Any
        existence/read exception clears it. Whitespace on retained lines is preserved.

        Example:
            >>> driver.load_history()  # doctest: +SKIP


        :return: ``None``; file failures are swallowed after clearing the history list.
        """
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
        """
        Overwrite the configured UTF-8 history file with at most the latest 1,000 entries.

        Create missing parents and terminate a nonempty payload with a newline.
        No path is a no-op. Errors are swallowed; writes are not atomic and the
        in-memory history is neither truncated nor cleared.

        Example:
            >>> driver.save_history()  # doctest: +SKIP


        :return: ``None`` regardless of whether persistence succeeds.
        """
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
        """
        Invoke the configured session through ``curses.wrapper`` and convert its result to an integer.

        Example:
            >>> status = driver.run_with_curses(run_browser)  # doctest: +SKIP


        :param runner: No-argument session callable passed to the driver's wrapped setup method.
        :return: Integer session result; setup, execution, and conversion exceptions propagate.
        """
        return int(curses.wrapper(self._wrapped_main, runner))

    def _wrapped_main(self, stdscr: CursesWindow, runner: Callable[[], int]) -> int:
        """
        Bind/configure the screen, load history, render, then run the session with history saving.

        Enable the cursor/keypad and a 200 ms read timeout. History saving is in
        the runner's ``finally`` block; earlier setup/load/initial-render failures
        occur before that block and do not trigger saving here.

        Example:
            >>> status = driver._wrapped_main(screen, run_browser)  # doctest: +SKIP


        :param stdscr: Main curses window supplied by the outer wrapper.
        :param runner: No-argument callable to invoke after successful UI initialization.
        :return: Runner result converted to an integer; errors propagate after applicable cleanup.
        """
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
        """
        Read one wide-character/key event, refreshing after a curses read error or timeout.

        No bound screen returns immediately without rendering. Errors from the
        refresh itself or non-curses read exceptions propagate.

        Example:
            >>> key = driver._read_key_with_refresh()  # doctest: +SKIP


        :return: Character/key code, or ``None`` for no screen or a caught curses read error.
        """
        if self._stdscr is None:
            return None
        try:
            key = self._stdscr.get_wch()
            return key
        except curses.error:
            self._render(force_status=False)
            return None
