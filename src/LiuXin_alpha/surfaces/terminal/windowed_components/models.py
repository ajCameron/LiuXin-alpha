"""
Define configuration and structural capabilities consumed by the curses owners.

Neither the concrete browser nor the driver is imported here. Test windows and
alternate browser hosts can implement the same small named capabilities.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from LiuXin_alpha.utils.jobs.manager import JobManagerAPI

from ..browser_components.models import BrowseWindow, CommandCompletion


class CursesWindow(Protocol):
    """
    Describe the curses window operations used for input and pane rendering.

    Test doubles need this capability set, not inheritance from a concrete curses
    window. Coordinates are zero-based and local to the supplied window.

    Example:
        >>> window.addstr(0, 0, 'Ready')  # doctest: +SKIP
    """

    def getmaxyx(self) -> tuple[int, int]:
        """
        Read the current window dimensions for layout and clipping.

        Example:
            >>> rows, columns = window.getmaxyx()  # doctest: +SKIP


        :return: Window height and width, in character cells.
        """
        ...

    def get_wch(self) -> str | int:
        """
        Read a Unicode character or a translated special-key code.

        Example:
            >>> key = window.get_wch()  # doctest: +SKIP


        :return: A character string, or an integer for a special key.
        :raises curses.error: If no key is available before the configured timeout.
        """
        ...

    def erase(self) -> None:
        """
        Clear the window's contents before drawing a new frame.

        Example:
            >>> window.erase()  # doctest: +SKIP


        :return: ``None`` after clearing the window's backing contents.
        """
        ...

    def addstr(self, y: int, x: int, text: str, /) -> None:
        """
        Draw text at a window-relative row and column.

        Example:
            >>> window.addstr(1, 0, 'Database connected')  # doctest: +SKIP


        :param y: Zero-based destination row.
        :param x: Zero-based starting column.
        :param text: Text already fitted to the pane by its presentation owner.
        :return: ``None`` after updating the window's contents.
        """
        ...

    def hline(self, y: int, x: int, ch: int, n: int, /) -> None:
        """
        Draw a horizontal run of one character for a pane separator.

        Example:
            >>> window.hline(2, 0, ord('-'), 20)  # doctest: +SKIP


        :param y: Zero-based row containing the line.
        :param x: Zero-based starting column.
        :param ch: Character code or curses line-drawing constant to repeat.
        :param n: Number of character cells to draw.
        :return: ``None`` after drawing the separator.
        """
        ...

    def noutrefresh(self) -> None:
        """
        Stage window changes for the next shared physical-screen update.

        Example:
            >>> window.noutrefresh()  # doctest: +SKIP


        :return: ``None``; the caller still controls the final ``curses.doupdate``.
        """
        ...

    def move(self, y: int, x: int, /) -> None:
        """
        Position the window cursor for subsequent input or output.

        Example:
            >>> window.move(0, 4)  # doctest: +SKIP


        :param y: Zero-based cursor row.
        :param x: Zero-based cursor column.
        :return: ``None`` after moving the window cursor.
        """
        ...

    def keypad(self, flag: bool, /) -> None:
        """
        Control whether escape sequences are translated into special-key codes.

        Example:
            >>> window.keypad(True)  # doctest: +SKIP


        :param flag: Whether to enable curses keypad processing for this window.
        :return: ``None`` after changing the input mode.
        """
        ...

    def timeout(self, delay: int, /) -> None:
        """
        Set the wait policy used by subsequent keyboard reads.

        Example:
            >>> window.timeout(100)  # doctest: +SKIP


        :param delay: Milliseconds to wait; zero polls and a negative value blocks.
        :return: ``None`` after configuring the window's input timeout.
        """
        ...


class WindowedBrowser(Protocol):
    """
    Describe browser state and queries needed by curses input and status panes.

    The driver depends on this structural view instead of importing the concrete
    browser. Completion and query execution remain owned by that browser.

    Example:
        >>> candidates = browser.command_completion_candidates('he')  # doctest: +SKIP
    """

    @property
    def database_path(self) -> str:
        """
        Read the connected database's best-effort display path.

        Example:
            >>> label = browser.database_path  # doctest: +SKIP


        :return: Database location text, or an empty string when metadata lacks it.
        """
        ...

    @property
    def current_table(self) -> str | None:
        """
        Identify the table currently selected for browsing.

        Example:
            >>> table = browser.current_table  # doctest: +SKIP


        :return: Selected table token, or ``None`` before a table is selected.
        """
        ...

    @property
    def window(self) -> BrowseWindow | None:
        """
        Expose the active page cursor for status and navigation displays.

        Example:
            >>> page = browser.window  # doctest: +SKIP


        :return: Current browsing window, or ``None`` when no page is active.
        """
        ...

    @property
    def page_size(self) -> int:
        """
        Read the browser's configured default number of rows per page.

        Example:
            >>> limit = browser.page_size  # doctest: +SKIP


        :return: Default page length used by browsing commands.
        """
        ...

    @property
    def job_manager(self) -> JobManagerAPI:
        """
        Expose the browser's job manager for job status and output panes.

        Example:
            >>> manager = browser.job_manager  # doctest: +SKIP


        :return: Job-management API associated with the current browser session.
        """
        ...

    def supports_core_queries(self) -> bool:
        """
        Report whether the host supports read queries through Core.

        Example:
            >>> enabled = browser.supports_core_queries()  # doctest: +SKIP


        :return: Whether panes may use the Core query capability.
        """
        ...

    def execute_core_query(
        self, name: str, *, payload: dict[str, object] | None = None
    ) -> Any:
        """
        Dispatch one named Core read query without interpreting its result.

        Query failures propagate to the pane owner, which controls presentation.

        Example:
            >>> result = browser.execute_core_query(query_name, payload=arguments)  # doctest: +SKIP


        :param name: Registered Core query name.
        :param payload: Query arguments; ``None`` supplies an empty payload.
        :return: Query-specific result payload returned by Core.
        """
        ...

    def core_runtime_status_summary(self) -> str:
        """
        Produce the short Core availability label used by status displays.

        Example:
            >>> summary = browser.core_runtime_status_summary()  # doctest: +SKIP


        :return: Human-readable runtime availability text.
        """
        ...

    def get_table_row_count(self, table: str) -> int | None:
        """
        Obtain a table's row count when the host can supply one.

        Example:
            >>> count = browser.get_table_row_count('works')  # doctest: +SKIP


        :param table: Resolved table token whose size should be displayed.
        :return: Nonnegative row count, or ``None`` when the count is unavailable.
        """
        ...

    def command_completion_candidates(
        self, line: str, *, cursor: int | None = None
    ) -> CommandCompletion:
        """
        Find candidate tokens and the buffer span to replace at the cursor.

        Example:
            >>> completion = browser.command_completion_candidates('he', cursor=2)  # doctest: +SKIP


        :param line: Complete current command buffer.
        :param cursor: Character index to complete at; ``None`` means the buffer end.
        :return: Token bounds, typed prefix, and ordered replacement candidates.
        """
        ...


@dataclass
class WindowedUiConfig:
    """
    Configure refresh timing, requested pane heights, and console retention.

    Layout owners fit these requested heights to the terminal's available rows.
    Constructing this value neither initializes curses nor starts a refresh loop.

    Example:
        >>> WindowedUiConfig(status_refresh_s=0.5).max_console_lines
        4000

    :ivar status_refresh_s: Target interval between status refreshes, in seconds.
    :ivar status_height: Requested row allocation for the status pane.
    :ivar telemetry_panel_height: Requested row allocation for database telemetry.
    :ivar job_panel_height: Requested row allocation for the attached job's output.
    :ivar max_console_lines: Maximum number of console lines retained by the driver.
    """

    status_refresh_s: float = 1.0
    status_height: int = 9
    telemetry_panel_height: int = 9
    job_panel_height: int = 10
    max_console_lines: int = 4000
