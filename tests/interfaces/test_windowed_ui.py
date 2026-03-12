from __future__ import annotations

from dataclasses import dataclass

from LiuXin_alpha.interfaces.terminal.windowed_ui import _CursesUiDriver, WindowedUiConfig


class _FakeJobManager:
    def list(self):
        return [{"job_id": "local-job", "state": "running"}]

    def get(self, job_id: str):
        return {"job_id": str(job_id), "state": "running"}


class _FakeBrowser:
    def __init__(self) -> None:
        self.database_path = "/tmp/test.sqlite"
        self.current_table = "stores"
        self.window = None
        self.page_size = 20
        self.job_manager = _FakeJobManager()

    def supports_core_queries(self) -> bool:
        return True

    def execute_core_query(self, name: str, *, payload=None):
        raise RuntimeError("rpc down for {}".format(name))

    def get_table_row_count(self, _table: str):
        return 0

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

    assert "core: enabled" in lines
    assert any(line.startswith("jobs: total=0") for line in lines)
    assert any("jobs_error: core jobs.list failed: RuntimeError: rpc down for jobs.list" in line for line in lines)


def test_windowed_ui_job_output_lines_surface_core_job_query_failures() -> None:
    ui = _CursesUiDriver(None, config=WindowedUiConfig(), history_file=None)
    ui.bind_browser(_FakeBrowser())
    ui._job_output_job_id = "job-123"

    lines = ui._build_job_output_lines(max_lines=5)

    assert lines == [
        "Job output: job-123 | unavailable: core jobs.get failed: RuntimeError: rpc down for jobs.get"
    ]


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
    assert any(line.startswith("completion:") for line in status_lines)
    assert "matches: series, store, subject" in status_lines[-1]
    assert second is True
    assert third is True
    assert ui._current_input == "add series "
