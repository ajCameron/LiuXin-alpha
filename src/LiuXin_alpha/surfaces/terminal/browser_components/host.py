"""
Expose Core dispatch, output, prompts, and optional browser capabilities to extensions.

The stream-based browser provides command/query dispatch but no dedicated job or
telemetry panes. Curses adapters override those optional capabilities. Output and
prompt helpers use the browser's configured streams instead of global print calls.
"""

from __future__ import annotations

import shutil
from collections.abc import Sequence
from typing import Any

from LiuXin_alpha.surfaces.terminal.presentation import (
    ask_text as _ask_text,
)
from LiuXin_alpha.surfaces.terminal.presentation import (
    ask_yes_no as _ask_yes_no,
)

from .contracts import BrowserState


class HostMixin[HostT](BrowserState[HostT]):
    """
    Supply extension-facing services shared by terminal browser compositions.

    Core availability methods advertise dispatch support rather than performing
    a live health check. Concrete Core operations remain responsible for reporting
    connection, query, and command failures.

    Example:
        >>> browser.emit('Import completed')  # doctest: +SKIP
    """

    def emit(self, text: str, *, end: str = "\n") -> None:
        """
        Send command output through the browser's overridable writing hook.

        Example:
            >>> browser.emit('Ready', end=': ')  # doctest: +SKIP


        :param text: Text to send without implicit string coercion.
        :param end: Suffix appended to the text; defaults to a newline.
        :return: ``None`` after the output hook completes.
        """
        self._write(text, end=end)

    def notify_write_completed(self) -> bool:
        """
        Request a best-effort refresh of attached metadata views after a write.

        Refresh failures are contained by the shared helper and do not turn an
        already completed write into a reported write failure.

        Example:
            >>> refreshed = browser.notify_write_completed()  # doctest: +SKIP


        :return: Whether an attached read source reported a successful refresh.
        """
        from LiuXin_alpha.surfaces.write_refresh import (
            refresh_metadata_read_source_after_write,
        )

        return refresh_metadata_read_source_after_write(self)

    def supports_core_commands(self) -> bool:
        """
        Advertise the browser's Core write-command dispatch capability.

        Example:
            >>> browser.supports_core_commands()  # doctest: +SKIP


        :return: ``True``; this does not probe whether the current Core connection is healthy.
        """
        return True

    def supports_core_queries(self) -> bool:
        """
        Advertise the browser's Core read-query dispatch capability.

        Example:
            >>> browser.supports_core_queries()  # doctest: +SKIP


        :return: ``True``; query execution remains responsible for reporting runtime failures.
        """
        return True

    def core_runtime_status_summary(self) -> str:
        """
        Supply the static Core-enabled label used by browser status displays.

        Example:
            >>> label = browser.core_runtime_status_summary()  # doctest: +SKIP


        :return: ``"core: enabled"``, not a live connectivity or readiness assessment.
        """
        return "core: enabled"

    def core_runtime_startup_warning(self) -> str | None:
        """
        Provide the optional startup-warning hook for browser compositions.

        Example:
            >>> warning = browser.core_runtime_startup_warning()  # doctest: +SKIP


        :return: ``None`` in this composition; overrides may supply user-facing warning text.
        """
        return None

    def execute_core_command(
        self, name: str, *, payload: dict[str, object] | None = None
    ) -> Any:
        """
        Execute a named Core write command and return its unmodified result payload.

        Copy the supplied argument mapping before dispatch. Core failures propagate
        to the command-loop or caller boundary rather than becoming empty results.

        Example:
            >>> result = browser.execute_core_command(command_name, payload=arguments)  # doctest: +SKIP


        :param name: Registered command name, coerced to text before dispatch.
        :param payload: Command arguments; ``None`` supplies an empty mapping.
        :return: The command-specific result returned by the bound Core client.
        """
        return self.core.command(str(name), dict(payload or {}))

    def execute_core_query(
        self, name: str, *, payload: dict[str, object] | None = None
    ) -> Any:
        """
        Execute a named Core read query and return its unmodified result payload.

        Copy the supplied argument mapping before dispatch. Lookup, validation,
        and transport failures remain errors for the caller to handle.

        Example:
            >>> result = browser.execute_core_query(query_name, payload=arguments)  # doctest: +SKIP


        :param name: Registered query name, coerced to text before dispatch.
        :param payload: Query arguments; ``None`` supplies an empty mapping.
        :return: The query-specific result returned by the bound Core client.
        """
        return self.core.query(str(name), dict(payload or {}))

    def supports_job_output_panel(self) -> bool:
        """
        Report whether this browser composition offers a dedicated job-output pane.

        Example:
            >>> supported = browser.supports_job_output_panel()  # doctest: +SKIP


        :return: ``False`` for this stream-based implementation.
        """
        return False

    def attach_job_output_panel(self, job_id: str) -> bool:
        """
        Decline job-pane attachment in the stream-based browser.

        Example:
            >>> attached = browser.attach_job_output_panel('job-42')  # doctest: +SKIP


        :param job_id: Requested job identifier, unused by this implementation.
        :return: ``False`` because no dedicated pane exists here.
        """
        del job_id
        return False

    def detach_job_output_panel(self) -> bool:
        """
        Decline job-pane detachment when the composition has no such pane.

        Example:
            >>> detached = browser.detach_job_output_panel()  # doctest: +SKIP


        :return: ``False`` without changing any job or output stream.
        """
        return False

    def supports_telemetry_panel(self) -> bool:
        """
        Report whether this browser composition offers a database-telemetry pane.

        Example:
            >>> supported = browser.supports_telemetry_panel()  # doctest: +SKIP


        :return: ``False`` for this stream-based implementation.
        """
        return False

    def attach_telemetry_panel(self, tables: Sequence[str] | None = None) -> bool:
        """
        Decline telemetry-pane attachment in the stream-based browser.

        Example:
            >>> attached = browser.attach_telemetry_panel(['works'])  # doctest: +SKIP


        :param tables: Optional requested table scope, unused by this implementation.
        :return: ``False`` because this composition has no telemetry pane.
        """
        del tables
        return False

    def detach_telemetry_panel(self) -> bool:
        """
        Decline telemetry-pane detachment when the composition has no such pane.

        Example:
            >>> detached = browser.detach_telemetry_panel()  # doctest: +SKIP


        :return: ``False`` without changing telemetry collection or output.
        """
        return False

    def clear_output(self) -> bool:
        """
        Clear a seekable output buffer or emit a terminal clear-and-home sequence.

        Prefer seek/truncate/flush when supported. If that fails, try terminal
        escape output only for a TTY; unsupported or failed attempts return false.

        Example:
            >>> cleared = browser.clear_output()  # doctest: +SKIP


        :return: Whether a buffer clear or terminal clear sequence completed successfully.
        """
        stream = self.output
        if hasattr(stream, "seek") and hasattr(stream, "truncate"):
            try:
                stream.seek(0)
                stream.truncate(0)
                stream.flush()
                return True
            except Exception:
                pass

        is_tty = bool(getattr(stream, "isatty", lambda: False)())
        if is_tty:
            try:
                stream.write("\x1b[2J\x1b[H")
                stream.flush()
                return True
            except Exception:
                pass
        return False

    def prompt_text(self, prompt: str, *, default: str | None = None) -> str:
        """
        Read a stripped response through the browser's configured streams.

        Example:
            >>> title = browser.prompt_text('Title', default='Untitled')  # doctest: +SKIP


        :param prompt: Question displayed before reading one line.
        :param default: Value used for a blank response or EOF, when supplied.
        :return: Stripped input, the supplied default, or an empty string when neither exists.
        """
        return _ask_text(
            prompt,
            default=default,
            input_stream=self.input,
            output_stream=self.output,
        )

    def prompt_yes_no(self, prompt: str, *, default: bool) -> bool:
        """
        Read a boolean response, reporting invalid text before using the default.

        Common yes/no, true/false, and one/zero spellings are accepted. Blank input
        and EOF select the default without another prompt.

        Example:
            >>> confirmed = browser.prompt_yes_no('Continue', default=False)  # doctest: +SKIP


        :param prompt: Question displayed with a hint showing the default choice.
        :param default: Decision used for blank, invalid, or exhausted input.
        :return: Parsed decision or the configured default.
        """
        return _ask_yes_no(
            prompt,
            default=bool(default),
            input_stream=self.input,
            output_stream=self.output,
        )

    def get_terminal_width(self) -> int:
        """
        Choose a usable rendering width from terminal detection or a safe fallback.

        Example:
            >>> width = browser.get_terminal_width()  # doctest: +SKIP


        :return: At least 40 columns, using 120 when terminal-size detection fails.
        """
        try:
            columns = int(shutil.get_terminal_size(fallback=(120, 30)).columns)
        except Exception:
            columns = 120
        return max(40, columns)

    def _write(self, text: str, *, end: str = "\n") -> None:
        """
        Write one text fragment and immediately flush the configured output stream.

        Example:
            >>> browser._write('Ready', end=': ')  # doctest: +SKIP


        :param text: Output fragment without implicit string coercion.
        :param end: Suffix appended before flushing; defaults to a newline.
        :return: ``None`` after both write and flush complete.
        :raises OSError: If the underlying stream cannot write or flush.
        """
        self.output.write(text + end)
        self.output.flush()
