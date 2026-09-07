"""Core dispatch, output, prompts, and optional capabilities exposed to extensions."""

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
    """Core dispatch, output, prompts, and optional capabilities exposed to extensions."""

    def emit(self, text: str, *, end: str = "\n") -> None:
        """Public output sink for command implementations."""
        self._write(text, end=end)

    def notify_write_completed(self) -> bool:
        """Refresh any attached metadata read source after a successful write."""
        from LiuXin_alpha.surfaces.write_refresh import (
            refresh_metadata_read_source_after_write,
        )

        return refresh_metadata_read_source_after_write(self)

    def supports_core_commands(self) -> bool:
        """Whether this browser can dispatch write commands through core runtime."""
        return True

    def supports_core_queries(self) -> bool:
        """Whether this browser can dispatch read queries through core runtime."""
        return True

    def core_runtime_status_summary(self) -> str:
        """Summarize core runtime availability for status surfaces."""
        return "core: enabled"

    def core_runtime_startup_warning(self) -> str | None:
        """User-facing startup warning for core runtime bootstrap failures."""
        return None

    def execute_core_command(
        self, name: str, *, payload: dict[str, object] | None = None
    ) -> Any:
        """
        Execute one core write command and return command result payload.

        Raises a user-facing error when core runtime is unavailable.
        """
        return self.core.command(str(name), dict(payload or {}))

    def execute_core_query(
        self, name: str, *, payload: dict[str, object] | None = None
    ) -> Any:
        """
        Execute one core read query and return query result payload.

        Raises a user-facing error when core runtime is unavailable.
        """
        return self.core.query(str(name), dict(payload or {}))

    def supports_job_output_panel(self) -> bool:
        """Whether this browser can route one job log stream to a dedicated panel."""
        return False

    def attach_job_output_panel(self, job_id: str) -> bool:
        """Attach the dedicated job output panel to a specific job id."""
        del job_id
        return False

    def detach_job_output_panel(self) -> bool:
        """Detach any active dedicated job output panel."""
        return False

    def supports_telemetry_panel(self) -> bool:
        """Whether this browser can route DB telemetry to a dedicated panel."""
        return False

    def attach_telemetry_panel(self, tables: Sequence[str] | None = None) -> bool:
        """Attach a dedicated telemetry panel, optionally scoped to specific tables."""
        del tables
        return False

    def detach_telemetry_panel(self) -> bool:
        """Detach any active dedicated telemetry panel."""
        return False

    def clear_output(self) -> bool:
        """Clear terminal output buffer or screen when supported."""
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
        """Prompt for one text input line using browser input/output streams."""
        return _ask_text(
            prompt,
            default=default,
            input_stream=self.input,
            output_stream=self.output,
        )

    def prompt_yes_no(self, prompt: str, *, default: bool) -> bool:
        """Prompt for a yes/no decision using browser input/output streams."""
        return _ask_yes_no(
            prompt,
            default=bool(default),
            input_stream=self.input,
            output_stream=self.output,
        )

    def get_terminal_width(self) -> int:
        """Return detected terminal width for table rendering."""
        try:
            columns = int(shutil.get_terminal_size(fallback=(120, 30)).columns)
        except Exception:
            columns = 120
        return max(40, columns)

    def _write(self, text: str, *, end: str = "\n") -> None:
        self.output.write(text + end)
        self.output.flush()
