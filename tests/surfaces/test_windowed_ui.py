from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

pytest.importorskip(
    "LiuXin_alpha.surfaces.terminal",
    reason="Terminal package is not exposed under surfaces/ in this checkout.",
)

from LiuXin_alpha.surfaces.terminal.windowed_ui import _CursesUiDriver, WindowedUiConfig


class _FakeJobManager:
    def list(self):
        return [{"job_id": "local-job", "state": "running"}]

    def get(self, job_id: str):
        return {"job_id": str(job_id), "state": "running"}


class _FakeTelemetryDb:
    def get_write_telemetry_snapshot(self, *, recent_limit: int = 8):
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
    def __init__(self) -> None:
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
        return True

    def execute_core_query(self, name: str, *, payload=None):
        raise RuntimeError("rpc down for {}".format(name))

    def get_table_row_count(self, _table: str):
        return self._counts.get(_table, 0)

    def core_runtime_status_summary(self) -> str:
        return "core: enabled"

    def command_completion_candidates(self, line: str, *, cursor: int | None = None):
        prefix = str(line[: len(line) if cursor is None else cursor])
        if prefix == "he":
            return _FakeCompletion(token_start=0, token_end=2, prefix="he", candidates=("help",))
        if prefix == "add s":
            return _FakeCompletion(token_start=4, token_end=5, prefix="s", candidates=("series", "store", "subject"))
        if prefix == "help a":
            return _FakeCompletion(
                token_start=5,
                token_end=6,
                prefix="a",
                candidates=("add", "add-store", "add_store"),
            )
        return _FakeCompletion(token_start=len(prefix), token_end=len(prefix), prefix="", candidates=())


class _FakeLogJobManager:
    def __init__(self, log_path: Path) -> None:
        self._log_path = str(log_path)

    def list(self):
        return [{"job_id": "job-123", "state": "running", "log_path": self._log_path}]

    def get(self, job_id: str):
        return {"job_id": str(job_id), "state": "running", "log_path": self._log_path}


class _FakeLocalJobBrowser(_FakeBrowser):
    def __init__(self, log_path: Path) -> None:
        super().__init__()
        self.job_manager = _FakeLogJobManager(log_path)

    def supports_core_queries(self) -> bool:
        return False


@dataclass(frozen=True)
class _FakeCompletion:
    token_start: int
    token_end: int
    prefix: str
    candidates: tuple[str, ...]


def test_windowed_ui_append_output_does_not_double_space() -> None:
    ui = _CursesUiDriver(None, config=WindowedUiConfig(), history_file=None)

    ui.append_output("hello")
    ui.append_output("world")

    assert list(ui._lines) == ["hello", "world"]


def test_windowed_ui_status_lines_surface_core_jobs_query_failures() -> None:
    ui = _CursesUiDriver(None, config=WindowedUiConfig(), history_file=None)
    ui.bind_browser(_FakeBrowser())

    lines = ui._build_status_lines()

    assert any("core: enabled" in line for line in lines)
    assert any(line.startswith("Jobs | total=0") for line in lines)
    assert any("jobs_error=core jobs.list failed: RuntimeError: rpc down for jobs.list" in line for line in lines)


def test_windowed_ui_job_output_lines_surface_core_job_query_failures() -> None:
    ui = _CursesUiDriver(None, config=WindowedUiConfig(), history_file=None)
    ui.bind_browser(_FakeBrowser())
    ui._job_output_job_id = "job-123"

    lines = ui._build_job_output_lines(max_lines=5)

    assert lines[0] == "Job output"
    assert any("job_id=job-123" in line for line in lines)
    assert any("status=unavailable" in line for line in lines)
    assert any("core jobs.get failed: RuntimeError: rpc down for jobs.get" in line for line in lines)


def test_windowed_ui_wrap_lines_for_width_preserves_blank_lines() -> None:
    wrapped = _CursesUiDriver._wrap_lines_for_width(["alpha", "", "beta"], width=3)

    assert wrapped == ["alp", "ha", "", "bet", "a"]


def test_windowed_ui_wrap_lines_for_width_splits_long_lines() -> None:
    wrapped = _CursesUiDriver._wrap_lines_for_width(["abcdefghij"], width=4)

    assert wrapped == ["abcd", "efgh", "ij"]


def test_windowed_ui_visible_console_lines_follow_latest_by_default() -> None:
    ui = _CursesUiDriver(None, config=WindowedUiConfig(), history_file=None)

    for line in ("one", "two", "three", "four"):
        ui.append_output(line)

    visible = ui._visible_console_lines(width=10, visible_rows=2)

    assert visible == ["three", "four"]


def test_windowed_ui_console_scrollback_clamps_and_selects_older_lines() -> None:
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
    ui = _CursesUiDriver(None, config=WindowedUiConfig(), history_file=None)

    for line in ("one", "two", "three"):
        ui.append_output(line)
    ui._console_scroll_offset = 2

    changed = ui.clear_console_output()

    assert changed is True
    assert list(ui._lines) == []
    assert ui._console_scroll_offset == 0


def test_windowed_ui_tab_completion_applies_single_match() -> None:
    ui = _CursesUiDriver(None, config=WindowedUiConfig(), history_file=None)
    ui.bind_browser(_FakeBrowser())
    ui._current_input = "he"

    changed = ui._complete_current_input(direction=1)

    assert changed is True
    assert ui._current_input == "help "
    assert ui._completion_hint == "completion: help (1/1) | matches: help"


def test_windowed_ui_tab_completion_cycles_and_surfaces_matches() -> None:
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
    browser = _FakeBrowser()
    ui = _CursesUiDriver(None, config=WindowedUiConfig(), history_file=None)
    ui.bind_browser(browser)
    ui.set_telemetry_tables(("files", "folders"))

    browser._counts["files"] = 12
    browser._counts["folders"] = 5
    lines = ui._build_telemetry_lines(max_lines=12)

    assert lines[0] == "DB telemetry"
    assert any("Activity | observed_total=5 | queue_depth=2 | persisted=1" in line for line in lines)
    assert any("Tracking | files, folders" in line or "Tracking | files,folders" in line for line in lines)
    assert any(line.startswith("files | total=12 | since=+2 | last=+2") for line in lines)
    assert any(line.startswith("folders | total=5 | since=+1 | last=+1") for line in lines)
    assert "Recent events" in lines
    assert any("dirty_queue | files:42 | update" in line for line in lines)


def test_windowed_ui_status_lines_surface_active_telemetry_panel() -> None:
    browser = _FakeBrowser()
    ui = _CursesUiDriver(None, config=WindowedUiConfig(), history_file=None)
    ui.bind_browser(browser)
    ui.set_telemetry_tables(("files", "folders"))

    lines = ui._build_status_lines()

    assert any("telemetry_panel=files,folders" in line for line in lines)


def test_windowed_ui_visible_job_output_lines_follow_latest_by_default(tmp_path) -> None:
    log_path = tmp_path / "job.log"
    log_path.write_text("one\ntwo\nthree\nfour\n", encoding="utf-8")
    ui = _CursesUiDriver(None, config=WindowedUiConfig(), history_file=None)
    ui.bind_browser(_FakeLocalJobBrowser(log_path))
    ui.set_job_output_job("job-123")

    visible = ui._visible_job_output_lines(width=200, visible_rows=3)

    assert visible == ["two", "three", "four"]


def test_windowed_ui_job_output_scrollback_clamps_and_selects_older_lines(tmp_path) -> None:
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
    assert ui._job_output_scroll_offset == max(0, len(ui._wrapped_job_output_lines(width=200)) - 3)


def test_windowed_ui_job_scrollback_preserves_view_when_new_output_arrives(tmp_path) -> None:
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
