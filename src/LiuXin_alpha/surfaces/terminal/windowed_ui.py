"""
Compose the curses driver, adapt the text browser to its panes, and run a shared Core-backed session.

Pane content, input, completion, history, and drawing are owned by the bounded
windowed_components modules. Startup remains in the terminal application.
The adapter adds no alternate command implementation; it redirects browser I/O
and panel capabilities. Curses setup/restoration and history handling are delegated
to the driver's input/session owner, while browser lifecycle belongs to its shell.
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
    """
    Initialize shared pane/input state and compose the owners used by one curses session.

    Construction creates no windows, reads no history, and does not query Core.
    Binding a browser supplies later query/completion services without starting it.

    Example:
        >>> driver = _CursesUiDriver(object(), config=WindowedUiConfig(), history_file=None)
        >>> driver.browser is None and driver._stdscr is None
        True
    """

    def __init__(
        self, core: object, *, config: WindowedUiConfig, history_file: str | Path | None
    ) -> None:
        """
        Retain configuration/Core references and initialize empty windows, buffers, selections, and observation caches.

        Console capacity is integer-converted and clamped to at least 100 logical
        lines. Other configuration values are retained for their owner to interpret;
        no broad configuration validation or active curses setup is performed.

        Example:
            >>> driver = _CursesUiDriver(object(), config=WindowedUiConfig(max_console_lines=2), history_file=None)
            >>> driver._lines.maxlen
            100


        :param core: Object retained as the driver's Core reference without coercion or queries here.
        :param config: Windowed UI configuration retained by reference; max_console_lines sizes the initial deque.
        :param history_file: Optional path expanded for a leading user marker; any falsey value disables the driver's history file.
        :return: None after state initialization, before a browser is bound or any curses windows exist.
        """
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
        """
        Replace the browser reference used by pane queries, completion, and job access without resetting driver state.

        Example:
            >>> driver.bind_browser(browser)  # doctest: +SKIP


        :param browser: Browser capability provider retained directly, without runtime validation or lifecycle calls.
        :return: None after assignment; existing windows, output, selections, and history are unchanged.
        """
        self.browser = browser


class _WindowedTextDatabaseBrowser(TextDatabaseBrowser):
    """
    Reuse the text browser's commands/lifecycle while routing prompts, output, width, and panels through a curses driver.

    Driver binding follows base-browser construction. An empty command response
    remains empty rather than receiving a newline, so the inherited loop treats
    both an accepted blank command and empty-buffer Ctrl-D as EOF.

    Example:
        >>> browser = _WindowedTextDatabaseBrowser(core, ui_driver=driver)  # doctest: +SKIP
    """

    def __init__(self, *args: Any, ui_driver: _CursesUiDriver, **kwargs: Any) -> None:
        """
        Initialize the ordinary text-browser state before attaching this instance to the supplied driver.

        Base construction failures leave the driver unbound to this instance;
        a later bind failure propagates without undoing completed base initialization.

        Example:
            >>> browser = _WindowedTextDatabaseBrowser(core, page_size=10, ui_driver=driver)  # doctest: +SKIP


        :param args: Positional arguments forwarded unchanged to TextDatabaseBrowser construction.
        :param ui_driver: Driver retained on the browser and then asked to bind this instance.
        :param kwargs: Remaining keyword arguments forwarded to the base browser constructor.
        :return: None after base initialization and driver binding succeed.
        """
        super().__init__(*args, **kwargs)
        self._ui_driver = ui_driver
        ui_driver.bind_browser(self)

    def _write(self, text: str, *, end: str = "\n") -> None:
        """
        Forward one output fragment and its terminator to the driver's console buffer/redraw path.

        The maintained driver splits each call into independent logical lines;
        an unterminated fragment is not joined to the previous call's last line.

        Example:
            >>> browser._write("Queued", end="")  # doctest: +SKIP


        :param text: Output text passed unchanged to append_output for stringification and line splitting.
        :param end: Terminator appended by the driver, defaulting to a newline.
        :return: None after delegation; driver errors propagate.
        """
        self._ui_driver.append_output(text, end=end)

    def _read_command_line(self) -> str:
        """
        Read a driver command and add a newline only when its response is nonempty.

        Unlike the ordinary readline adapter, an accepted blank line is
        indistinguishable from EOF to the inherited browser loop. Input interrupts
        and rendering errors propagate.

        Example:
            >>> line = browser._read_command_line()  # doctest: +SKIP


        :return: Empty string for a blank/EOF response, otherwise the driver's text plus one newline.
        """
        line = self._ui_driver.read_line(self.prompt)
        if line == "":
            return ""
        return line + "\n"

    def clear_output(self) -> bool:
        """
        Ask the driver to empty its console buffer, reset scrollback, and redraw.

        Example:
            >>> had_content = browser.clear_output()  # doctest: +SKIP


        :return: Driver-reported prior content/scroll-offset presence, not a general capability flag.
        """
        return self._ui_driver.clear_console_output()

    def prompt_text(self, prompt: str, *, default: str | None = None) -> str:
        """
        Display a labeled driver prompt with an optional bracketed default and apply that default to an empty response.

        The driver also receives the default as its initial editable buffer.
        Nonempty responses retain whitespace; this adapter does not trim or validate them.

        Example:
            >>> name = browser.prompt_text("Name", default="Untitled")  # doctest: +SKIP


        :param prompt: Prompt label followed by the optional default suffix and a colon-space terminator.
        :param default: Optional displayed/initial value, also returned unchanged if the driver supplies empty text.
        :return: Nonempty driver response, supplied default for empty input when non-None, or empty text otherwise.
        """
        suffix = ""
        if default is not None:
            suffix = f" [{default}]"
        value = self._ui_driver.read_line(f"{prompt}{suffix}: ", default=default)
        if value == "" and default is not None:
            return default
        return value

    def prompt_yes_no(self, prompt: str, *, default: bool) -> bool:
        """
        Ask once for a boolean response, accepting normalized word/digit aliases and falling back without reprompting.

        Blank input uses the default silently. Recognized yes/no values include
        y/n, yes/no, 1/0, true/false, and t/f. Invalid input emits a warning using
        the stripped/lowercased response and returns the default.

        Example:
            >>> confirmed = browser.prompt_yes_no("Continue", default=False)  # doctest: +SKIP


        :param prompt: Prompt label augmented with Y/n or y/N according to the default.
        :param default: Boolean returned for blank or invalid input and advertised in the prompt hint.
        :return: Parsed boolean or the supplied default after any required warning is emitted.
        """
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
        """
        Expose the driver's current width, including its pre-window fallback and lower bound.

        Example:
            >>> width = browser.get_terminal_width()  # doctest: +SKIP


        :return: Driver terminal_width value without an additional adapter clamp or probe.
        """
        return self._ui_driver.terminal_width

    def supports_job_output_panel(self) -> bool:
        """
        Advertise job-panel integration independently of whether a pane or selected job currently exists.

        Example:
            >>> browser.supports_job_output_panel()  # doctest: +SKIP
            True


        :return: True, declaring this adapter's job-panel capability rather than live job availability.
        """
        return True

    def attach_job_output_panel(self, job_id: str) -> bool:
        """
        Delegate job selection to the driver, including its normalization, scroll reset, and layout/redraw effects.

        A blank selector disables the panel; a nonblank ID is not validated for
        existence before selection. An unchanged selection still triggers layout/redraw.

        Example:
            >>> changed = browser.attach_job_output_panel("job-123")  # doctest: +SKIP


        :param job_id: Job selector stringified/stripped by the driver, with blank text clearing the selection.
        :return: Whether the normalized selection changed, not whether attachment found a real job.
        """
        return self._ui_driver.set_job_output_job(job_id)

    def detach_job_output_panel(self) -> bool:
        """
        Delegate clearing the selected job and any associated job scroll focus through the normal redraw path.

        Example:
            >>> was_selected = browser.detach_job_output_panel()  # doctest: +SKIP


        :return: Whether the driver's selected job changed to no selection.
        """
        return self._ui_driver.clear_job_output_job()

    def supports_telemetry_panel(self) -> bool:
        """
        Advertise telemetry-panel integration without checking current selection, windows, or count availability.

        Example:
            >>> browser.supports_telemetry_panel()  # doctest: +SKIP
            True


        :return: True as a static adapter capability declaration.
        """
        return True

    def attach_telemetry_panel(self, tables: Sequence[str] | None = None) -> bool:
        """
        Select telemetry tables and resample the driver's baseline even when the normalized selection is unchanged.

        The driver strips/deduplicates names without schema validation and uses
        defaults for omitted, empty, or entirely blank input. Setup can query counts
        and redraw; the returned flag is not a change-only indicator.

        Example:
            >>> browser.attach_telemetry_panel(["works", "items"])  # doctest: +SKIP
            True


        :param tables: Optional ordered table names forwarded to set_telemetry_tables.
        :return: True after successful maintained-driver setup; underlying sampling/layout errors propagate.
        """
        return self._ui_driver.set_telemetry_tables(tables)

    def detach_telemetry_panel(self) -> bool:
        """
        Delegate telemetry selection/cache clearing and layout redraw even when the pane was already disabled.

        Example:
            >>> was_selected = browser.detach_telemetry_panel()  # doctest: +SKIP


        :return: Whether a telemetry table selection existed before clearing.
        """
        return self._ui_driver.clear_telemetry_panel()


def run_windowed_browser(
    core: Any,
    *,
    page_size: int = 20,
    history_file: str | Path | None = None,
    config: WindowedUiConfig | None = None,
) -> int:
    """
    Construct a driver, enter its curses wrapper, and run a newly bound windowed text-browser session.

    The browser is constructed inside the wrapper after screen setup/history loading.
    The input owner manages applicable history saving and curses restoration; shell
    startup/shutdown remains delegated to its session owner. Setup, construction,
    input, and result-conversion errors propagate rather than becoming exit codes here.

    Example:
        >>> status = run_windowed_browser(core, page_size=20)  # doctest: +SKIP


    :param core: Core client or legacy database passed to browser coercion, and retained unchanged by the driver.
    :param page_size: Initial page length forwarded to the browser's integer/clamp initialization.
    :param history_file: Shared requested path; the driver disables history for falsey input, while the browser chooses its default for None.
    :param config: Driver configuration, with None or another falsey value selecting a new WindowedUiConfig.
    :return: Driver wrapper's integer session result, normally zero for ordinary browser termination.
    """
    ui = _CursesUiDriver(
        core,
        config=config or WindowedUiConfig(),
        history_file=history_file,
    )

    def _runner() -> int:
        """
        Construct and bind the captured windowed browser after curses setup, then run its command lifecycle.

        Example:
            >>> exit_code = _runner()  # doctest: +SKIP


        :return: Browser run result; construction and session exceptions propagate to the enclosing curses wrapper.
        """
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
