"""Public curses composition and browser adapter.

Pane content, input, completion, history, and drawing are owned by the bounded
windowed_components modules. Startup remains in the terminal application.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from .browser import TextDatabaseBrowser
from .windowed_components.completion import CompletionMixin
from .windowed_components.console import ConsoleMixin
from .windowed_components.input import InputMixin
from .windowed_components.job_output import JobOutputMixin
from .windowed_components.layout import LayoutMixin
from .windowed_components.models import WindowedBrowser, WindowedUiConfig
from .windowed_components.presentation import PresentationMixin
from .windowed_components.status import StatusMixin
from .windowed_components.telemetry import TelemetryMixin


class _CursesUiDriver(
    ConsoleMixin,
    JobOutputMixin,
    TelemetryMixin,
    StatusMixin,
    CompletionMixin,
    InputMixin,
    LayoutMixin,
    PresentationMixin,
):
    """Compose pane, input, and rendering owners around one curses session."""

    def __init__(
        self, core: object, *, config: WindowedUiConfig, history_file: str | Path | None
    ) -> None:
        self.core = core
        self.config = config
        self.history_file = Path(history_file).expanduser() if history_file else None

        self._stdscr = None
        self._status_win = None
        self._telemetry_win = None
        self._job_output_win = None
        self._console_win = None
        self._status_last_render = 0.0

        self._lines: deque[str] = deque(maxlen=max(100, int(config.max_console_lines)))
        self._current_prompt = ""
        self._current_input = ""
        self._history: list[str] = []
        self._history_cursor: int | None = None
        self._telemetry_tables: tuple[str, ...] | None = None
        self._telemetry_started_at: float | None = None
        self._telemetry_baseline_counts: dict[str, int | None] = {}
        self._telemetry_last_counts: dict[str, int | None] = {}
        self._job_output_job_id: str | None = None
        self._jobs_status_error: str | None = None
        self._job_output_error: str | None = None
        self._console_scroll_offset = 0
        self._job_output_scroll_offset = 0
        self._job_output_last_wrap_width: int | None = None
        self._job_output_last_wrapped_count = 0
        self._scroll_focus = "console"
        self._completion_hint: str | None = None
        self._completion_matches: tuple[str, ...] = ()
        self._completion_base_input: str | None = None
        self._completion_active_input: str | None = None
        self._completion_token_start = 0
        self._completion_token_end = 0
        self._completion_index: int | None = None
        self.browser: WindowedBrowser | None = None

    def bind_browser(self, browser: WindowedBrowser) -> None:
        self.browser = browser


class _WindowedTextDatabaseBrowser(TextDatabaseBrowser):
    """Text browser variant that reads/writes through curses panes."""

    def __init__(self, *args: Any, ui_driver: _CursesUiDriver, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._ui_driver = ui_driver
        ui_driver.bind_browser(self)

    def _write(self, text: str, *, end: str = "\n") -> None:
        self._ui_driver.append_output(text, end=end)

    def _read_command_line(self) -> str:
        line = self._ui_driver.read_line(self.prompt)
        if line == "":
            return ""
        return line + "\n"

    def clear_output(self) -> bool:
        return self._ui_driver.clear_console_output()

    def prompt_text(self, prompt: str, *, default: str | None = None) -> str:
        suffix = ""
        if default is not None:
            suffix = f" [{default}]"
        value = self._ui_driver.read_line(f"{prompt}{suffix}: ", default=default)
        if value == "" and default is not None:
            return default
        return value

    def prompt_yes_no(self, prompt: str, *, default: bool) -> bool:
        hint = "Y/n" if default else "y/N"
        raw = self.prompt_text(f"{prompt} ({hint})", default=None).strip().lower()
        if raw == "":
            return default
        if raw in {"y", "yes", "1", "true", "t"}:
            return True
        if raw in {"n", "no", "0", "false", "f"}:
            return False
        self._write(
            "Invalid response {!r}; using default {}".format(
                raw, "yes" if default else "no"
            )
        )
        return default

    def get_terminal_width(self) -> int:
        return self._ui_driver.terminal_width

    def supports_job_output_panel(self) -> bool:
        return True

    def attach_job_output_panel(self, job_id: str) -> bool:
        return self._ui_driver.set_job_output_job(job_id)

    def detach_job_output_panel(self) -> bool:
        return self._ui_driver.clear_job_output_job()

    def supports_telemetry_panel(self) -> bool:
        return True

    def attach_telemetry_panel(self, tables: Sequence[str] | None = None) -> bool:
        return self._ui_driver.set_telemetry_tables(tables)

    def detach_telemetry_panel(self) -> bool:
        return self._ui_driver.clear_telemetry_panel()


def run_windowed_browser(
    core: Any,
    *,
    page_size: int = 20,
    history_file: str | Path | None = None,
    config: WindowedUiConfig | None = None,
) -> int:
    """Run the split-pane curses UI wrapper around `TextDatabaseBrowser`."""
    ui = _CursesUiDriver(
        core,
        config=config or WindowedUiConfig(),
        history_file=history_file,
    )

    def _runner() -> int:
        shell = _WindowedTextDatabaseBrowser(
            core,
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
