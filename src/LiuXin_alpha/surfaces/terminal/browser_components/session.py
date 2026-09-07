"""Interactive and scripted session lifecycle, readline editing, and persistent history."""

from __future__ import annotations

import sys
from collections.abc import Sequence

from .contracts import BrowserState
from .models import ReadlineBackend

_readline: ReadlineBackend | None
try:
    import readline

    _readline = readline
except Exception:  # pragma: no cover - platform dependent (e.g. minimal Windows builds)
    _readline = None


class SessionMixin[HostT](BrowserState[HostT]):
    """Interactive and scripted session lifecycle, readline editing, and persistent history."""

    def run(self) -> int:
        """Run the interactive command loop until `quit`/`exit` is entered."""
        self.startup()
        self._write("LiuXin text browser. Type `help` for commands.")
        exit_code = 0
        try:
            while True:
                line = self._read_command_line()
                if line == "":
                    self._write("")
                    self.request_shutdown("eof")
                    return exit_code
                try:
                    keep_going = self.execute_line(line)
                except Exception as exc:
                    self._write(f"ERROR: {exc}")
                    continue
                if not keep_going:
                    return exit_code
        finally:
            self.shutdown(reason=self._shutdown_reason or "run_complete")

    def _can_use_readline_prompt(self) -> bool:
        """Whether we can safely use readline-backed `input()` prompt editing."""
        if _readline is None:
            return False
        if self.input is not sys.stdin or self.output is not sys.stdout:
            return False
        input_is_tty = bool(getattr(self.input, "isatty", lambda: False)())
        output_is_tty = bool(getattr(self.output, "isatty", lambda: False)())
        return input_is_tty and output_is_tty

    def _read_command_line(self) -> str:
        """Read one command line, enabling arrow-key history on interactive TTY."""
        if self._can_use_readline_prompt():
            self._configure_readline_completion()
            try:
                line = input(self.prompt)
            except EOFError:
                return ""
            if line.strip():
                try:
                    assert _readline is not None
                    _readline.add_history(line)
                except Exception:
                    pass
            return line + "\n"

        self._write(self.prompt, end="")
        return self.input.readline()

    def run_commands(self, commands: Sequence[str]) -> int:
        """Run commands non-interactively with full lifecycle hooks."""
        self.startup()
        exit_code = 0
        try:
            for command in commands:
                keep_going = self.execute_line(command)
                if not keep_going:
                    break
            return exit_code
        finally:
            self.shutdown(reason=self._shutdown_reason or "commands_complete")

    def startup(self) -> None:
        """Run startup lifecycle hooks once."""
        if self._started:
            return
        self._started = True
        self._load_command_history()
        core_warning = self.core_runtime_startup_warning()
        if core_warning:
            self._write(f"WARNING: {core_warning}")
        for plugin in self._lifecycle_plugins:
            plugin_name = getattr(plugin, "name", plugin.__class__.__name__)
            try:
                plugin.on_startup(self._extension_host)
            except Exception as exc:
                raise RuntimeError(
                    f"Lifecycle startup plugin {plugin_name!r} failed: {exc}"
                ) from exc

    def shutdown(self, *, reason: str) -> None:
        """Run shutdown lifecycle hooks once."""
        if self._closed:
            return
        self._closed = True
        self._shutdown_reason = str(reason)
        self._save_command_history()
        errors: list[str] = []
        for plugin in reversed(self._lifecycle_plugins):
            plugin_name = getattr(plugin, "name", plugin.__class__.__name__)
            try:
                plugin.on_shutdown(self._extension_host, reason=self._shutdown_reason)
            except Exception as exc:
                errors.append(f"plugin {plugin_name!r}: {exc}")
        if errors:
            self._write("WARNING: lifecycle shutdown errors:")
            for err in errors:
                self._write(f"  {err}")
        if self._compatibility_core_session is not None:
            self._compatibility_core_session.close()

    def request_shutdown(self, reason: str) -> None:
        """Mark preferred shutdown reason for this browser session."""
        self._shutdown_reason = str(reason)

    def _load_command_history(self) -> None:
        """
        Load persistent readline history for interactive sessions.

        This is intentionally best-effort and should never block startup.
        """
        if self._history_loaded:
            return
        self._history_loaded = True
        if _readline is None:
            return
        if not self._can_use_readline_prompt():
            return
        try:
            if self._history_file.exists():
                _readline.read_history_file(str(self._history_file))
            self._history_enabled_for_session = True
        except Exception:
            self._history_enabled_for_session = False

    def _save_command_history(self) -> None:
        """
        Persist readline history for interactive sessions.

        This is intentionally best-effort and should never block shutdown.
        """
        if _readline is None:
            return
        if not self._history_enabled_for_session:
            return
        try:
            self._history_file.parent.mkdir(parents=True, exist_ok=True)
            _readline.write_history_file(str(self._history_file))
        except Exception:
            return

    def _configure_readline_completion(self) -> None:
        """Install one readline completer for this browser session."""
        if self._readline_completion_configured:
            return
        readline_mod = _readline
        if readline_mod is None:
            return
        try:
            parse_and_bind = getattr(readline_mod, "parse_and_bind", None)
            if callable(parse_and_bind):
                parse_and_bind("tab: complete")
        except Exception:
            pass
        try:
            set_completer_delims = getattr(readline_mod, "set_completer_delims", None)
            if callable(set_completer_delims):
                set_completer_delims(" \t\n")
        except Exception:
            pass
        try:
            set_completer = getattr(readline_mod, "set_completer", None)
            if callable(set_completer):
                set_completer(self._readline_completer)
                self._readline_completion_configured = True
        except Exception:
            self._readline_completion_configured = False

    def _readline_completer(self, text: str, state: int) -> str | None:
        """Readline callback returning one completion match at a time."""
        if state == 0:
            readline_mod = _readline
            line_buffer = str(text)
            endidx = len(line_buffer)
            if readline_mod is not None:
                try:
                    line_buffer = str(readline_mod.get_line_buffer())
                except Exception:
                    line_buffer = str(text)
                try:
                    endidx = int(readline_mod.get_endidx())
                except Exception:
                    endidx = len(line_buffer)
            completion = self.command_completion_candidates(line_buffer, cursor=endidx)
            self._readline_completion_matches = list(completion.candidates)
        if state < 0 or state >= len(self._readline_completion_matches):
            return None
        return self._readline_completion_matches[state]
