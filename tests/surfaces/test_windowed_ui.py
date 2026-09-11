"""
Exercise windowed terminal content, completion, and scrollback without a real curses screen.

Small Core/local-job fakes supply deterministic snapshots; temporary log files
exercise job-output reading. These tests do not open a database or initialize curses.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

from LiuXin_alpha.surfaces.terminal.windowed_ui import _CursesUiDriver, WindowedUiConfig


class _FakeJobManager:
    """
    Supply minimal running-job dictionaries for local-manager fallback tests.

    IDs passed to ``get`` are echoed without checking that a job was registered.

    Example:
        >>> _FakeJobManager().list()[0]["state"]
        'running'
    """

    def list(self):
        """
        Return one synthetic running job to expose accidental local fallback during Core failures.

        Example:
            >>> _FakeJobManager().list()
            [{'job_id': 'local-job', 'state': 'running'}]


        :return: Fresh one-element list containing the fixed local-job record.
        """
        return [{"job_id": "local-job", "state": "running"}]

    def get(self, job_id: str):
        """
        Describe any supplied ID as a running local job, without existence validation.

        Example:
            >>> _FakeJobManager().get("j1")
            {'job_id': 'j1', 'state': 'running'}


        :param job_id: Identifier to stringify and echo in the returned record.
        :return: Fresh dictionary containing the requested ID and running state.
        """
        return {"job_id": str(job_id), "state": "running"}


class _FakeTelemetryDb:
    """
    Return a fixed write-activity snapshot with two distinct event sources.

    The snapshot separates total observed activity, queued work, persisted work,
    and recent file/folder events for pane-format assertions.

    Example:
        >>> _FakeTelemetryDb().get_write_telemetry_snapshot()["observed_total"]
        5
    """

    def get_write_telemetry_snapshot(self, *, recent_limit: int = 8):
        """
        Build the deterministic activity/count/event payload used by the fake Core query.

        Always include both events regardless of the requested recent-event limit.

        Example:
            >>> len(_FakeTelemetryDb().get_write_telemetry_snapshot(recent_limit=1)["recent_events"])
            2


        :param recent_limit: Compatibility argument deliberately ignored by this fake.
        :return: New nested telemetry dictionary with fixed counts and two timestamped events.
        """
        del recent_limit
        return {
            "observed_total": 5,
            "queue_size": 2,
            "persisted_queue_size": 1,
            "source_counts": {
                "dirty_queue": 3,
                "trigger_dirty_record": 2,
            },
            "recent_events": [
                {
                    "timestamp": 1700000000.0,
                    "source": "dirty_queue",
                    "table": "files",
                    "row_id": 42,
                    "reason": "update",
                },
                {
                    "timestamp": 1700000001.0,
                    "source": "trigger_dirty_record",
                    "table": "folders",
                    "row_id": 7,
                    "reason": "DIRTY_RECORD",
                },
            ],
        }


class _FakeBrowser:
    """
    Emulate a Core-capable browser whose telemetry succeeds but job queries fail.

    Mutable table counts let tests observe telemetry deltas. Fixed completion cases
    exercise unique matches, cycling, and shared prefixes without a command registry.

    Example:
        >>> _FakeBrowser().get_table_row_count("files")
        10
    """

    def __init__(self) -> None:
        """
        Initialize deterministic context, local fallback jobs, telemetry, and editable row counts.

        The database path is display metadata only; no database file is opened.

        Example:
            >>> browser = _FakeBrowser()
            >>> (browser.current_table, browser.page_size)
            ('stores', 20)


        :return: ``None``; a fresh independent fake state is initialized.
        """
        self.database_path = "/tmp/test.sqlite"
        self.current_table = "stores"
        self.window = None
        self.page_size = 20
        self.job_manager = _FakeJobManager()
        self.db = _FakeTelemetryDb()
        self._counts = {
            "works": 0,
            "expressions": 0,
            "manifestations": 0,
            "items": 0,
            "files": 10,
            "folders": 4,
            "stores": 1,
        }

    def supports_core_queries(self) -> bool:
        """
        Advertise Core support so failing job queries must remain visible rather than use local jobs.

        Example:
            >>> _FakeBrowser().supports_core_queries()
            True


        :return: ``True`` for every call on this Core-capable fake.
        """
        return True

    def execute_core_query(self, name: str, *, payload=None):
        """
        Return telemetry snapshots and reject every other query with a named RPC-failure message.

        Example:
            >>> _FakeBrowser().execute_core_query("jobs.get")
            Traceback (most recent call last):
            ...
            RuntimeError: rpc down for jobs.get


        :param name: Core query name, with only ``database.telemetry`` implemented.
        :param payload: Optional dictionary supplying a recent-limit value converted to an integer.
        :return: Telemetry dictionary for the supported query.
        :raises RuntimeError: For any other query name, including job listing/retrieval.
        """
        if name == "database.telemetry":
            return self.db.get_write_telemetry_snapshot(
                recent_limit=int((payload or {}).get("recent_limit", 8))
            )
        raise RuntimeError("rpc down for {}".format(name))

    def get_table_row_count(self, _table: str):
        """
        Read a mutable fake count, treating unspecified tables as empty.

        Example:
            >>> _FakeBrowser().get_table_row_count("not_configured")
            0


        :param _table: Table name used directly as the fake count-map key.
        :return: Stored count, or zero for a missing key.
        """
        return self._counts.get(_table, 0)

    def core_runtime_status_summary(self) -> str:
        """
        Provide the fixed runtime marker asserted by status-pane tests.

        Example:
            >>> _FakeBrowser().core_runtime_status_summary()
            'core: enabled'


        :return: Literal text indicating that the fake advertises Core support.
        """
        return "core: enabled"

    def command_completion_candidates(self, line: str, *, cursor: int | None = None):
        """
        Return canned completion spans for ``he``, ``add s``, and ``help a`` prefixes.

        Slice at the supplied cursor using normal Python slicing, not production
        cursor clamping. Unrecognized input gets an empty span at that prefix's end.

        Example:
            >>> _FakeBrowser().command_completion_candidates("he").candidates
            ('help',)


        :param line: Input text compared with the three exact fixture prefixes.
        :param cursor: Optional slice endpoint; ``None`` uses the full line.
        :return: Immutable fake completion record with the canned or empty candidates.
        """
        prefix = str(line[: len(line) if cursor is None else cursor])
        if prefix == "he":
            return _FakeCompletion(
                token_start=0, token_end=2, prefix="he", candidates=("help",)
            )
        if prefix == "add s":
            return _FakeCompletion(
                token_start=4,
                token_end=5,
                prefix="s",
                candidates=("series", "store", "subject"),
            )
        if prefix == "help a":
            return _FakeCompletion(
                token_start=5,
                token_end=6,
                prefix="a",
                candidates=("add", "add-store", "add_store"),
            )
        return _FakeCompletion(
            token_start=len(prefix), token_end=len(prefix), prefix="", candidates=()
        )


class _FakeLogJobManager:
    """
    Advertise a supplied local log path in running-job records without reading the file.

    Example:
        >>> _FakeLogJobManager(Path("job.log")).get("j1")["log_path"]
        'job.log'
    """

    def __init__(self, log_path: Path) -> None:
        """
        Store the log path as display text for later list/get responses.

        Example:
            >>> manager = _FakeLogJobManager(Path("job.log"))


        :param log_path: File path to advertise; existence is not checked here.
        :return: ``None``; the stringified path is retained on the fake.
        """
        self._log_path = str(log_path)

    def list(self):
        """
        Return one running fixture job carrying the configured log path.

        Example:
            >>> _FakeLogJobManager(Path("job.log")).list()[0]["job_id"]
            'job-123'


        :return: New list containing the fixed job ID, running state, and advertised path.
        """
        return [{"job_id": "job-123", "state": "running", "log_path": self._log_path}]

    def get(self, job_id: str):
        """
        Echo a requested ID in a running-job record with the configured log path.

        Example:
            >>> _FakeLogJobManager(Path("job.log")).get("j2")["job_id"]
            'j2'


        :param job_id: Identifier to stringify without checking registration.
        :return: New job dictionary advertising the same log path for any requested ID.
        """
        return {"job_id": str(job_id), "state": "running", "log_path": self._log_path}


class _FakeLocalJobBrowser(_FakeBrowser):
    """
    Reuse fake browser context while directing job operations to a log-advertising local manager.

    Example:
        >>> _FakeLocalJobBrowser(Path("job.log")).supports_core_queries()
        False
    """

    def __init__(self, log_path: Path) -> None:
        """
        Initialize base browser state and replace its manager with the log-path fake.

        Example:
            >>> browser = _FakeLocalJobBrowser(Path("job.log"))


        :param log_path: Local path for the manager to advertise to the real log reader.
        :return: ``None``; the new manager replaces the base fake's manager.
        """
        super().__init__()
        self.job_manager = _FakeLogJobManager(log_path)

    def supports_core_queries(self) -> bool:
        """
        Disable Core job routing so pane tests exercise the local manager path.

        Example:
            >>> _FakeLocalJobBrowser(Path("job.log")).supports_core_queries()
            False


        :return: ``False`` for every call on this local-only variant.
        """
        return False


@dataclass(frozen=True)
class _FakeCompletion:
    """
    Carry replacement bounds, original prefix, and ordered completion candidates for tests.

    The frozen record mirrors the attributes consumed by the driver, without
    validating bounds or deriving suggestions from a real registry.

    Example:
        >>> _FakeCompletion(0, 2, "he", ("help",)).token_end
        2
    """

    token_start: int
    token_end: int
    prefix: str
    candidates: tuple[str, ...]


def test_windowed_ui_append_output_does_not_double_space() -> None:
    """
    Verify default newline terminators produce two logical lines without extra blank entries.

    Example:
        >>> test_windowed_ui_append_output_does_not_double_space()


    :return: ``None`` when the buffered lines exactly match the two appended messages.
    """
    ui = _CursesUiDriver(None, config=WindowedUiConfig(), history_file=None)

    ui.append_output("hello")
    ui.append_output("world")

    assert list(ui._lines) == ["hello", "world"]


def test_windowed_ui_status_lines_surface_core_jobs_query_failures() -> None:
    """
    Verify status retains the Core marker and exposes failed job listing beside an empty total.

    The fake has a nonempty local manager, so the asserted zero total also protects
    against presenting fallback local jobs as successful Core results.

    Example:
        >>> test_windowed_ui_status_lines_surface_core_jobs_query_failures()


    :return: ``None`` when runtime, empty job total, and the exact Core error are visible.
    """
    ui = _CursesUiDriver(None, config=WindowedUiConfig(), history_file=None)
    ui.bind_browser(_FakeBrowser())

    lines = ui._build_status_lines()

    assert any("core: enabled" in line for line in lines)
    assert any(line.startswith("Jobs | total=0") for line in lines)
    assert any(
        "jobs_error=core jobs.list failed: RuntimeError: rpc down for jobs.list" in line
        for line in lines
    )


def test_windowed_ui_job_output_lines_surface_core_job_query_failures() -> None:
    """
    Verify a failed Core job lookup retains the job ID and a visible unavailable/error description.

    Example:
        >>> test_windowed_ui_job_output_lines_surface_core_job_query_failures()


    :return: ``None`` when the five-line panel contains its title, ID, status, and failure details.
    """
    ui = _CursesUiDriver(None, config=WindowedUiConfig(), history_file=None)
    ui.bind_browser(_FakeBrowser())
    ui._job_output_job_id = "job-123"

    lines = ui._build_job_output_lines(max_lines=5)

    assert lines[0] == "Job output"
    assert any("job_id=job-123" in line for line in lines)
    assert any("status=unavailable" in line for line in lines)
    assert any(
        "core jobs.get failed: RuntimeError: rpc down for jobs.get" in line
        for line in lines
    )


def test_windowed_ui_wrap_lines_for_width_preserves_blank_lines() -> None:
    """
    Verify character wrapping retains an empty logical line between separately wrapped words.

    Example:
        >>> test_windowed_ui_wrap_lines_for_width_preserves_blank_lines()


    :return: ``None`` when width-three wrapping preserves the expected blank entry and fragments.
    """
    wrapped = _CursesUiDriver._wrap_lines_for_width(["alpha", "", "beta"], width=3)

    assert wrapped == ["alp", "ha", "", "bet", "a"]


def test_windowed_ui_wrap_lines_for_width_splits_long_lines() -> None:
    """
    Verify a ten-character line wraps into two full-width fragments and one short remainder.

    Example:
        >>> test_windowed_ui_wrap_lines_for_width_splits_long_lines()


    :return: ``None`` when width-four slices preserve all characters in order.
    """
    wrapped = _CursesUiDriver._wrap_lines_for_width(["abcdefghij"], width=4)

    assert wrapped == ["abcd", "efgh", "ij"]


def test_windowed_ui_visible_console_lines_follow_latest_by_default() -> None:
    """
    Verify zero scrollback shows the latest two of four buffered console messages.

    Example:
        >>> test_windowed_ui_visible_console_lines_follow_latest_by_default()


    :return: ``None`` when the default viewport follows the newest output in chronological order.
    """
    ui = _CursesUiDriver(None, config=WindowedUiConfig(), history_file=None)

    for line in ("one", "two", "three", "four"):
        ui.append_output(line)

    visible = ui._visible_console_lines(width=10, visible_rows=2)

    assert visible == ["three", "four"]


def test_windowed_ui_console_scrollback_clamps_and_selects_older_lines() -> None:
    """
    Verify relative console scrolling selects older lines and clamps an oversized jump to the top.

    Example:
        >>> test_windowed_ui_console_scrollback_clamps_and_selects_older_lines()


    :return: ``None`` when offset, selected lines, change result, and maximum scrollback match.
    """
    ui = _CursesUiDriver(None, config=WindowedUiConfig(), history_file=None)

    for line in ("one", "two", "three", "four", "five"):
        ui.append_output(line)

    changed = ui._scroll_console_relative(2, width=10, visible_rows=2)
    visible = ui._visible_console_lines(width=10, visible_rows=2)

    assert changed is True
    assert ui._console_scroll_offset == 2
    assert visible == ["two", "three"]

    ui._scroll_console_relative(999, width=10, visible_rows=2)
    assert ui._console_scroll_offset == 3


def test_windowed_ui_scrollback_preserves_view_when_new_output_arrives() -> None:
    """
    Verify appending one console message increases active scrollback to keep the same visible lines.

    Example:
        >>> test_windowed_ui_scrollback_preserves_view_when_new_output_arrives()


    :return: ``None`` when both viewports retain the oldest pair and the offset grows by one.
    """
    ui = _CursesUiDriver(None, config=WindowedUiConfig(), history_file=None)

    for line in ("one", "two", "three", "four"):
        ui.append_output(line)

    ui._console_scroll_offset = 2
    before = ui._visible_console_lines(width=10, visible_rows=2)
    ui.append_output("five")
    after = ui._visible_console_lines(width=10, visible_rows=2)

    assert before == ["one", "two"]
    assert after == ["one", "two"]
    assert ui._console_scroll_offset == 3


def test_windowed_ui_clear_console_output_resets_buffer_and_scrollback() -> None:
    """
    Verify clearing populated, scrolled-back console output empties the buffer and resets its offset.

    Example:
        >>> test_windowed_ui_clear_console_output_resets_buffer_and_scrollback()


    :return: ``None`` when clearing reports a change and removes content plus scrollback.
    """
    ui = _CursesUiDriver(None, config=WindowedUiConfig(), history_file=None)

    for line in ("one", "two", "three"):
        ui.append_output(line)
    ui._console_scroll_offset = 2

    changed = ui.clear_console_output()

    assert changed is True
    assert list(ui._lines) == []
    assert ui._console_scroll_offset == 0


def test_windowed_ui_tab_completion_applies_single_match() -> None:
    """
    Verify a unique completion replaces the prefix, appends a space, and identifies the selected match.

    Example:
        >>> test_windowed_ui_tab_completion_applies_single_match()


    :return: ``None`` when the resulting input, hint, and handled result match the help completion.
    """
    ui = _CursesUiDriver(None, config=WindowedUiConfig(), history_file=None)
    ui.bind_browser(_FakeBrowser())
    ui._current_input = "he"

    changed = ui._complete_current_input(direction=1)

    assert changed is True
    assert ui._current_input == "help "
    assert ui._completion_hint == "completion: help (1/1) | matches: help"


def test_windowed_ui_tab_completion_cycles_and_surfaces_matches() -> None:
    """
    Verify forward/backward completion cycling returns to the first match and exposes candidate hints.

    Example:
        >>> test_windowed_ui_tab_completion_cycles_and_surfaces_matches()


    :return: ``None`` when all cycle steps are handled and the status includes the ordered matches.
    """
    ui = _CursesUiDriver(None, config=WindowedUiConfig(), history_file=None)
    ui.bind_browser(_FakeBrowser())
    ui._current_input = "add s"

    first = ui._complete_current_input(direction=1)
    status_lines = ui._build_status_lines()
    second = ui._complete_current_input(direction=1)
    third = ui._complete_current_input(direction=-1)

    assert first is True
    assert ui._completion_hint is not None
    assert any("completion:" in line for line in status_lines)
    assert any("matches: series, store, subject" in line for line in status_lines)
    assert second is True
    assert third is True
    assert ui._current_input == "add series "


def test_windowed_ui_telemetry_lines_surface_counts_and_recent_events() -> None:
    """
    Verify telemetry combines activity counters, tracked tables, count deltas, and recent events.

    Counts change after baseline sampling so both since-start and previous-sample
    deltas are checked alongside the fixed Core activity snapshot.

    Example:
        >>> test_windowed_ui_telemetry_lines_surface_counts_and_recent_events()


    :return: ``None`` when the compact lines contain the expected totals, deltas, and event identity.
    """
    browser = _FakeBrowser()
    ui = _CursesUiDriver(None, config=WindowedUiConfig(), history_file=None)
    ui.bind_browser(browser)
    ui.set_telemetry_tables(("files", "folders"))

    browser._counts["files"] = 12
    browser._counts["folders"] = 5
    lines = ui._build_telemetry_lines(max_lines=12)

    assert lines[0] == "DB telemetry"
    assert any(
        "Activity | observed_total=5 | queue_depth=2 | persisted=1" in line
        for line in lines
    )
    assert any(
        "Tracking | files, folders" in line or "Tracking | files,folders" in line
        for line in lines
    )
    assert any(
        line.startswith("files | total=12 | since=+2 | last=+2") for line in lines
    )
    assert any(
        line.startswith("folders | total=5 | since=+1 | last=+1") for line in lines
    )
    assert "Recent events" in lines
    assert any("dirty_queue | files:42 | update" in line for line in lines)


def test_windowed_ui_status_lines_surface_active_telemetry_panel() -> None:
    """
    Verify the status summary names the currently selected telemetry tables.

    Example:
        >>> test_windowed_ui_status_lines_surface_active_telemetry_panel()


    :return: ``None`` when panel metadata includes the ordered file/folder table selection.
    """
    browser = _FakeBrowser()
    ui = _CursesUiDriver(None, config=WindowedUiConfig(), history_file=None)
    ui.bind_browser(browser)
    ui.set_telemetry_tables(("files", "folders"))

    lines = ui._build_status_lines()

    assert any("telemetry_panel=files,folders" in line for line in lines)


def test_windowed_ui_visible_job_output_lines_follow_latest_by_default(
    tmp_path,
) -> None:
    """
    Verify the default job viewport follows the newest three log lines after metadata headings.

    Example:
        >>> test_windowed_ui_visible_job_output_lines_follow_latest_by_default(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for a four-line UTF-8 job log.
    :return: ``None`` when the selected local job displays the final three log records.
    """
    log_path = tmp_path / "job.log"
    log_path.write_text("one\ntwo\nthree\nfour\n", encoding="utf-8")
    ui = _CursesUiDriver(None, config=WindowedUiConfig(), history_file=None)
    ui.bind_browser(_FakeLocalJobBrowser(log_path))
    ui.set_job_output_job("job-123")

    visible = ui._visible_job_output_lines(width=200, visible_rows=3)

    assert visible == ["two", "three", "four"]


def test_windowed_ui_job_output_scrollback_clamps_and_selects_older_lines(
    tmp_path,
) -> None:
    """
    Verify job scrollback includes preceding content and clamps large moves using wrapped-content size.

    Example:
        >>> test_windowed_ui_job_output_scrollback_clamps_and_selects_older_lines(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory holding the local job's log fixture.
    :return: ``None`` when the intermediate viewport and maximum offset include headers consistently.
    """
    log_path = tmp_path / "job.log"
    log_path.write_text("one\ntwo\nthree\nfour\n", encoding="utf-8")
    ui = _CursesUiDriver(None, config=WindowedUiConfig(), history_file=None)
    ui.bind_browser(_FakeLocalJobBrowser(log_path))
    ui.set_job_output_job("job-123")

    changed = ui._scroll_job_output_relative(2, width=200, visible_rows=3)
    visible = ui._visible_job_output_lines(width=200, visible_rows=3)

    assert changed is True
    assert ui._job_output_scroll_offset == 2
    assert visible == ["Log tail", "one", "two"]

    ui._scroll_job_output_relative(999, width=200, visible_rows=3)
    assert ui._job_output_scroll_offset == max(
        0, len(ui._wrapped_job_output_lines(width=200)) - 3
    )


def test_windowed_ui_job_scrollback_preserves_view_when_new_output_arrives(
    tmp_path,
) -> None:
    """
    Verify growing a selected job's log advances active scrollback without shifting the visible content.

    Example:
        >>> test_windowed_ui_job_scrollback_preserves_view_when_new_output_arrives(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for the log rewritten with one additional line.
    :return: ``None`` when the old/new viewports match and the offset accounts for growth.
    """
    log_path = tmp_path / "job.log"
    log_path.write_text("one\ntwo\nthree\nfour\n", encoding="utf-8")
    ui = _CursesUiDriver(None, config=WindowedUiConfig(), history_file=None)
    ui.bind_browser(_FakeLocalJobBrowser(log_path))
    ui.set_job_output_job("job-123")

    ui._scroll_job_output_relative(2, width=200, visible_rows=3)
    before = ui._visible_job_output_lines(width=200, visible_rows=3)
    log_path.write_text("one\ntwo\nthree\nfour\nfive\n", encoding="utf-8")
    after = ui._visible_job_output_lines(width=200, visible_rows=3)

    assert before == ["Log tail", "one", "two"]
    assert after == ["Log tail", "one", "two"]
    assert ui._job_output_scroll_offset == 3


def test_windowed_ui_status_lines_surface_job_focus_and_scrollback(tmp_path) -> None:
    """
    Verify switching to job focus and scrolling exposes both focus-switch and scrollback-key hints.

    Example:
        >>> test_windowed_ui_status_lines_surface_job_focus_and_scrollback(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory containing the selected job's log.
    :return: ``None`` when focus changes and status describes the active job viewport controls.
    """
    log_path = tmp_path / "job.log"
    log_path.write_text("one\ntwo\nthree\nfour\n", encoding="utf-8")
    ui = _CursesUiDriver(None, config=WindowedUiConfig(), history_file=None)
    ui.bind_browser(_FakeLocalJobBrowser(log_path))
    ui.set_job_output_job("job-123")

    changed = ui._cycle_scroll_focus()
    ui._scroll_job_output_relative(2, width=200, visible_rows=3)
    lines = ui._build_status_lines()

    assert changed is True
    assert any("focus=job | F6 switch pane" in line for line in lines)
    assert any("job_scrollback=+2 | PgUp/PgDn Home/End" in line for line in lines)
