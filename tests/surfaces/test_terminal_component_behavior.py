"""Headless contracts for extracted terminal parsing, completion, and key handling."""

from __future__ import annotations

import curses
import io
from collections import deque
from types import SimpleNamespace
from unittest.mock import create_autospec

import pytest

from LiuXin_alpha.core import CoreClientAPI
from LiuXin_alpha.surfaces.terminal.browser import TextDatabaseBrowser
from LiuXin_alpha.surfaces.terminal.browser_components.models import BrowseWindow
from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI
from LiuXin_alpha.surfaces.terminal.windowed_ui import WindowedUiConfig, _CursesUiDriver


class _Command(TerminalCommandAPI[TextDatabaseBrowser]):
    def __init__(self, name: str) -> None:
        self.name = name

    def execute(self, browser: TextDatabaseBrowser, args: list[str]) -> bool:
        return True


@pytest.fixture
def browser(monkeypatch) -> TextDatabaseBrowser:
    shell = TextDatabaseBrowser(
        create_autospec(CoreClientAPI, instance=True),
        input=io.StringIO(),
        output=io.StringIO(),
    )

    def resolve(token):
        if token in {"works", "work"}:
            return "works"
        raise ValueError("unknown table")

    monkeypatch.setattr(shell, "_resolve_table", resolve)
    return shell


@pytest.mark.parametrize("group", ["on", "off", "show"])
@pytest.mark.parametrize(
    ("args", "expected"),
    [
        (["works:1", "note", "text"], ["works:1", "text"]),
        (["works", "1", "note", "text"], ["works", "1", "text"]),
        (["bad", "note"], ["bad"]),
        ([], None),
        (["bad", "1", "unknown"], None),
    ],
)
def test_legacy_rewrites_preserve_lookup_before_validation(
    browser, group, args, expected
) -> None:
    command = _Command("note")
    rewrite = getattr(browser, f"_rewrite_{group}_legacy_group_args")
    result = rewrite({"note": command}, args)
    assert result == (None if expected is None else (command, expected))


@pytest.mark.parametrize("args", [["works:1"], ["works", "1"]])
def test_show_defaults_to_all_for_one_valid_id(browser, args) -> None:
    command = _Command("all")
    assert browser._rewrite_show_legacy_group_args({"all": command}, args) == (
        command,
        args,
    )


@pytest.mark.parametrize(
    "args", [["works:1,2"], ["works", "1-3"], ["works:1,2", "unknown"]]
)
def test_show_rejects_multiple_ids_before_unknown_kind(browser, args) -> None:
    with pytest.raises(ValueError, match="single row id only"):
        browser._rewrite_show_legacy_group_args({"all": _Command("all")}, args)


@pytest.mark.parametrize(
    ("command", "args", "expected"),
    [
        ("browse", [], ["table:x"]),
        ("browse", ["works"], []),
        ("row", [], ["ref:x"]),
        ("row", ["works"], ["id:works:x"]),
        ("set", ["works:1"], ["column:works:x"]),
        ("set", ["works:1", "name"], []),
        ("set", ["works", "1"], ["column:works:x"]),
        ("edit", ["works:1", "name"], ["column:works:x"]),
        ("delete", ["works:1"], []),
        ("links", ["works:1"], ["table:x"]),
        ("link", ["works", "to"], []),
        ("link", ["works", "1", "to"], ["ref:x"]),
        ("link", ["works:1", "to", "notes"], ["id:notes:x"]),
        ("unlink", ["works:1", "notes:2"], []),
    ],
)
def test_completion_handlers_keep_command_specific_argument_positions(
    browser, monkeypatch, command, args, expected
) -> None:
    monkeypatch.setattr(
        browser, "_table_token_completion_candidates", lambda token: [f"table:{token}"]
    )
    monkeypatch.setattr(
        browser, "_row_ref_token_completion_candidates", lambda token: [f"ref:{token}"]
    )
    monkeypatch.setattr(
        browser,
        "_table_scoped_id_completion_candidates",
        lambda table, token: [f"id:{table}:{token}"],
    )
    monkeypatch.setattr(
        browser,
        "_table_column_completion_candidates",
        lambda table, token: [f"column:{table}:{token}"],
    )
    assert (
        browser._completion_candidates_for_direct_command(_Command(command), args, "x")
        == expected
    )


def test_id_completion_preserves_visible_priority_and_fallback_scan(
    browser, monkeypatch
) -> None:
    browser.window = BrowseWindow("works", 2, 5)
    calls = []

    def visible(table, *, limit, offset):
        calls.append(("visible", table, limit, offset))
        return [{"id": "12"}, {"id": "11"}]

    def all_rows(table, *, iterator_return):
        calls.append(("all", table, iterator_return))
        return iter([{"id": "11"}, {}, {"id": "13"}, {"id": "2"}])

    monkeypatch.setattr(browser, "_table_id_column", lambda table: "id")
    monkeypatch.setattr(browser, "_table_slice", visible)
    monkeypatch.setattr(browser, "db", SimpleNamespace(get_all_rows=all_rows))
    assert browser._row_id_completion_candidates("works", "1", max_candidates=3) == [
        "12",
        "11",
        "13",
    ]
    assert calls == [("visible", "works", 2, 5), ("all", "works", True)]
    calls.clear()
    assert browser._row_id_completion_candidates("works", "") == ["12", "11"]
    assert calls == [("visible", "works", 2, 5)]


@pytest.mark.parametrize(
    ("keys", "initial", "history", "expected", "saved"),
    [
        ([None, "a", "b", "\b", "c", "\n"], None, [], "ac", ["ac"]),
        (
            [curses.KEY_UP, curses.KEY_UP, curses.KEY_DOWN, "\n"],
            None,
            ["one", "two"],
            "two",
            ["one", "two", "two"],
        ),
        ([curses.KEY_UP, curses.KEY_DOWN, "\n"], None, ["one"], "", ["one"]),
        (["\x04"], None, [], "", []),
        (["\x04", "\n"], "kept", [], "kept", ["kept"]),
    ],
)
def test_curses_input_keeps_history_editing_and_eof_behavior(
    monkeypatch, keys, initial, history, expected, saved
) -> None:
    driver = _CursesUiDriver(object(), config=WindowedUiConfig(), history_file=None)
    pending = deque(keys)
    renders = []
    driver._history = list(history)
    monkeypatch.setattr(driver, "_read_key_with_refresh", pending.popleft)
    monkeypatch.setattr(
        driver, "_render", lambda *, force_status: renders.append(force_status)
    )
    assert driver.read_line("prompt> ", default=initial) == expected
    assert driver._history == saved
    assert driver._current_prompt == driver._current_input == ""
    assert driver._history_cursor is None
    assert renders[-1] is True


def test_curses_control_c_remains_an_interrupt(monkeypatch) -> None:
    driver = _CursesUiDriver(object(), config=WindowedUiConfig(), history_file=None)
    monkeypatch.setattr(driver, "_read_key_with_refresh", lambda: "\x03")
    with pytest.raises(KeyboardInterrupt):
        driver.read_line("prompt> ")


@pytest.mark.parametrize(
    ("rows", "expected"),
    [
        (11, (0, 0)),
        (12, (4, 0)),
        (15, (4, 0)),
        (16, (4, 4)),
        (18, (5, 5)),
        (40, (9, 10)),
    ],
)
def test_panel_allocation_keeps_minima_and_round_robin_priority(rows, expected) -> None:
    driver = _CursesUiDriver(object(), config=WindowedUiConfig(), history_file=None)
    driver._telemetry_tables = ("works",)
    driver._job_output_job_id = "job"
    assert driver._allocate_aux_panel_heights(rows=rows, status_h=5) == expected
