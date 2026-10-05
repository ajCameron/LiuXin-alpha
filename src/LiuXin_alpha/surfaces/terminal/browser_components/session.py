"""
Own terminal command loops, lifecycle hooks, optional editing, and history I/O.

Interactive command failures are reported and the loop continues; scripted
command failures propagate. Readline configuration and history persistence stay
best-effort so missing terminal support does not prevent stream-based browsing.
"""

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
    """
    Supply session lifecycle and command-loop behavior to a composed browser.

    Startup and shutdown flags prevent duplicate lifecycle calls. Concrete
    composition supplies command execution, output, completion, and the extension
    host passed to plugins; this mixin is not a standalone browser constructor.

    Example:
        >>> browser.run_commands(['help', 'quit'])  # doctest: +SKIP
    """

    def run(self) -> int:
        """
        Run commands interactively until shutdown is requested by a command or EOF.

        Command-execution errors are printed before accepting another command.
        Once startup succeeds, leaving the loop runs shutdown even if input fails.

        Example:
            >>> exit_code = browser.run()  # doctest: +SKIP


        :return: Zero after an ordinary command-requested or end-of-input exit.
        :raises RuntimeError: If a startup lifecycle plugin fails before the loop starts.
        """
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
        """
        Check that readline editing can use the browser's actual standard streams.

        Both streams must be the process's current stdin/stdout and report a TTY;
        redirected or substituted streams use the plain stream-reading path.

        Example:
            >>> editable = browser._can_use_readline_prompt()  # doctest: +SKIP


        :return: Whether readline exists and both stream identity/TTY checks pass.
        """
        if _readline is None:
            return False
        if self.input is not sys.stdin or self.output is not sys.stdout:
            return False
        input_is_tty = bool(getattr(self.input, "isatty", lambda: False)())
        output_is_tty = bool(getattr(self.output, "isatty", lambda: False)())
        return input_is_tty and output_is_tty

    def _read_command_line(self) -> str:
        """
        Read one command with optional editing while preserving stream EOF semantics.

        Editable input gains a trailing newline; an empty entered command is
        therefore distinct from EOF. Nonblank edited commands enter history on a
        best-effort basis. Other streams retain their native ``readline`` behavior.

        Example:
            >>> line = browser._read_command_line()  # doctest: +SKIP


        :return: Command text with its newline, or an empty string at end of input.
        :raises KeyboardInterrupt: If keyboard input is interrupted by the operator.
        """
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
        """
        Execute an ordered command sequence with startup and shutdown hooks.

        A command returning false stops the sequence. Unlike the interactive loop,
        execution failures propagate to the caller after shutdown has been run.

        Example:
            >>> browser.run_commands(['help', 'quit'])  # doctest: +SKIP


        :param commands: Complete command lines to execute in order.
        :return: Zero when the sequence completes or a command requests its end.
        :raises RuntimeError: If a startup lifecycle plugin fails.
        """
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
        """
        Initialize history, report Core warnings, and run each startup plugin once.

        The started flag is set before hooks run, so a failed hook is not retried
        implicitly by another startup call. Plugins run in registration order.

        Example:
            >>> browser.startup()  # doctest: +SKIP


        :return: ``None`` after startup, or immediately if startup was already attempted.
        :raises RuntimeError: If a plugin fails; the message identifies that plugin.
        """
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
        """
        Save history, unwind lifecycle plugins, and close an owned compatibility session.

        Plugin shutdown runs in reverse registration order. Failures are collected
        and printed without preventing the remaining plugins from running; later
        shutdown calls do nothing because the closed flag is set first.

        Example:
            >>> browser.shutdown(reason='operator_exit')  # doctest: +SKIP


        :param reason: Shutdown explanation stored on the browser and passed to plugins.
        :return: ``None`` after cleanup, or immediately if already closed.
        """
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
        """
        Record the preferred reason to use when the command loop later shuts down.

        This changes the reason only: it does not stop the loop, run hooks, or
        close Core resources by itself.

        Example:
            >>> browser.request_shutdown('quit_command')  # doctest: +SKIP


        :param reason: Explanation to retain for the eventual shutdown call.
        :return: ``None`` after storing the reason as text.
        """
        self._shutdown_reason = str(reason)

    def _load_command_history(self) -> None:
        """
        Load persistent readline history for interactive sessions.

        Attempt loading at most once. A missing history file is acceptable and
        still enables saving later; other loading failures disable persistence for
        this session without interrupting startup.

        Example:
            >>> browser._load_command_history()  # doctest: +SKIP


        :return: ``None`` after the attempt or when editing/history is unavailable.
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

        Create missing parent directories only when history is enabled. Backend
        and filesystem errors are ignored so persistence cannot prevent shutdown.

        Example:
            >>> browser._save_command_history()  # doctest: +SKIP


        :return: ``None`` whether history is saved, disabled, or unavailable.
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
        """
        Configure tab completion and install the browser callback when supported.

        Optional binding, delimiter, and callback setters are tried independently.
        Mark configuration complete only after callback registration succeeds;
        unavailable or failing setters do not prevent reading commands.

        Example:
            >>> browser._configure_readline_completion()  # doctest: +SKIP


        :return: ``None`` after best-effort configuration or when already configured.
        """
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
        """
        Serve cached completion candidates using readline's numbered callback protocol.

        State zero rebuilds candidates from the current buffer and completion end
        index. If backend inspection fails, the supplied text is used as the
        buffer. Subsequent states index the same candidate list.

        Example:
            >>> first = browser._readline_completer('he', 0)  # doctest: +SKIP


        :param text: Readline's current token, also used as the fallback buffer.
        :param state: Zero-based candidate index; zero starts a fresh request.
        :return: One full candidate token, or ``None`` for negative/exhausted indices.
        """
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
