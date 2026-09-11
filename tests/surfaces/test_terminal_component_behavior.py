"""
Characterize terminal legacy parsing, completion routing, visible-ID priority, curses key handling, and pane allocation.

Core clients, candidate providers, and input keys are test doubles. These tests
exercise the real composed owners without starting an interactive terminal or
reading a production database; source snippets and expected call order preserve
the observed compatibility boundaries of the extraction.
"""

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
    """
    Supply a named, no-op command so parsing/completion tests can select real command objects.

    Example:
        >>> _Command("note").name
        'note'
    """

    def __init__(self, name: str) -> None:
        """
        Store the supplied command name unchanged for dispatch/completion lookup tests.

        Example:
            >>> _Command("show").name
            'show'


        :param name: Command token exposed through this test command's name attribute.
        :return: None after assigning the name; no browser registration occurs here.
        """
        self.name = name

    def execute(self, browser: TextDatabaseBrowser, args: list[str]) -> bool:
        """
        Accept any test dispatch without changing browser state or interpreting arguments.

        Example:
            >>> _Command("note").execute(object.__new__(TextDatabaseBrowser), [])
            True


        :param browser: Ignored browser host supplied by the command contract.
        :param args: Ignored command arguments; parser behavior is tested separately.
        :return: True so an accidental test dispatch requests session continuation.
        """
        return True


@pytest.fixture
def browser(monkeypatch) -> TextDatabaseBrowser:
    """
    Construct an in-memory-stream browser with an autospecced Core client and a narrow works-table resolver.

    No command loop or lifecycle startup is run. The normal default job-manager
    selection remains part of construction; all client operations are mocks.

    Example:
        >>> shell = browser.__wrapped__(patch)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture replacing only this shell's _resolve_table method for the test lifetime.
    :return: TextDatabaseBrowser accepting work/works table tokens and rejecting every other token in the patched resolver.
    """
    shell = TextDatabaseBrowser(
        create_autospec(CoreClientAPI, instance=True),
        input=io.StringIO(),
        output=io.StringIO(),
    )

    def resolve(token):
        """
        Resolve the two literal works-table spellings used by this fixture and reject other tokens.

        Example:
            >>> resolve("work")  # doctest: +SKIP
            'works'


        :param token: Unnormalized table token compared exactly with work and works.
        :return: Canonical works table name for either accepted spelling.
        :raises ValueError: If the token is outside the fixture's two accepted spellings.
        """
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
    """
    Check target-first legacy rewriting across on/off/show groups without changing command identity or argument order.

    Example:
        >>> test_legacy_rewrites_preserve_lookup_before_validation(shell, "on", ["works:1", "note", "text"], ["works:1", "text"])  # doctest: +SKIP


    :param browser: Headless browser fixture with a narrow works-table resolver.
    :param group: Parametrized legacy group whose rewrite method is selected by name.
    :param args: Original tokens including valid/invalid compact or split targets.
    :param expected: Expected rewritten argument list, or None when no matching command should be selected.
    :return: None after command-object identity and exact rewrite result are checked.
    """
    command = _Command("note")
    rewrite = getattr(browser, f"_rewrite_{group}_legacy_group_args")
    result = rewrite({"note": command}, args)
    assert result == (None if expected is None else (command, expected))


@pytest.mark.parametrize("args", [["works:1"], ["works", "1"]])
def test_show_defaults_to_all_for_one_valid_id(browser, args) -> None:
    """
    Select the all subcommand for one valid compact or split show target while retaining its tokens unchanged.

    Example:
        >>> test_show_defaults_to_all_for_one_valid_id(shell, ["works:1"])  # doctest: +SKIP


    :param browser: Headless browser fixture supplying the real show-rewrite implementation.
    :param args: Parametrized one-ID compact or split target token list.
    :return: None after the selected all command and unchanged arguments match expectations.
    """
    command = _Command("all")
    assert browser._rewrite_show_legacy_group_args({"all": command}, args) == (
        command,
        args,
    )


@pytest.mark.parametrize(
    "args", [["works:1,2"], ["works", "1-3"], ["works:1,2", "unknown"]]
)
def test_show_rejects_multiple_ids_before_unknown_kind(browser, args) -> None:
    """
    Preserve multi-ID rejection before resolving an optional unsupported show kind.

    Example:
        >>> test_show_rejects_multiple_ids_before_unknown_kind(shell, ["works:1,2", "unknown"])  # doctest: +SKIP


    :param browser: Headless browser fixture with real legacy target validation.
    :param args: Parametrized list/range selector, optionally followed by an unknown kind.
    :return: None after ValueError specifically reports that only a single row ID is allowed.
    """
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
    """
    Distinguish table, row-reference, scoped-ID, and column completion at each command's current argument position.

    Recording token strings identify the selected provider and forwarded table;
    these mocks do not query schema or rows.

    Example:
        >>> test_completion_handlers_keep_command_specific_argument_positions(shell, patch, "row", ["works"], ["id:works:x"])  # doctest: +SKIP


    :param browser: Headless fixture whose command-specific completion routing is exercised.
    :param monkeypatch: Fixture installing deterministic candidate providers on that browser.
    :param command: Parametrized direct-command name wrapped in a no-op _Command.
    :param args: Already-entered argument tokens preceding the fixed current token x.
    :param expected: Exact candidate list expected for this command/argument position.
    :return: None after the completion route and propagated selector/token values match.
    """
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
    """
    Prefer active-page IDs, deduplicate fallback results, and avoid an all-row scan for an empty completion token.

    Example:
        >>> test_id_completion_preserves_visible_priority_and_fallback_scan(shell, patch)  # doctest: +SKIP


    :param browser: Headless fixture with a selected works page assigned by this test.
    :param monkeypatch: Fixture replacing ID-column, page-read, and all-row providers with deterministic recorders.
    :return: None after candidate order, cap, and exact visible/fallback calls are asserted.
    """
    browser.window = BrowseWindow("works", 2, 5)
    calls = []

    def visible(table, *, limit, offset):
        """
        Record the requested visible page and return two deliberately reverse-ordered ID rows.

        Example:
            >>> visible("works", limit=2, offset=5)  # doctest: +SKIP


        :param table: Table token recorded unchanged for call-order assertions.
        :param limit: Requested page length recorded without limiting this fixed fixture result.
        :param offset: Requested page start recorded without slicing the fixed fixture rows.
        :return: Two row dictionaries containing IDs 12 and 11 in that order.
        """
        calls.append(("visible", table, limit, offset))
        return [{"id": "12"}, {"id": "11"}]

    def all_rows(table, *, iterator_return):
        """
        Record the fallback scan and return rows covering duplicate, missing, matching, and nonmatching IDs.

        Example:
            >>> rows = all_rows("works", iterator_return=True)  # doctest: +SKIP


        :param table: Table name retained in the enclosing call log.
        :param iterator_return: Requested iterator flag recorded without changing the returned iterator shape.
        :return: Fresh iterator over four fixed row dictionaries used by the completion test.
        """
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
    """
    Replay finite key sequences through the real input loop and check accepted text, history, reset state, and final redraw.

    Example:
        >>> test_curses_input_keeps_history_editing_and_eof_behavior(patch, ["a", chr(10)], None, [], "a", ["a"])  # doctest: +SKIP


    :param monkeypatch: Fixture substituting queued key events and recording redraw requests.
    :param keys: Parametrized key/timeout sequence ending in acceptance or empty-buffer EOF.
    :param initial: Optional initial editable text supplied as the read_line default.
    :param history: Starting history entries copied into the driver's mutable list.
    :param expected: Exact accepted text expected from read_line.
    :param saved: Expected final history, including retained duplicates and omitted blank entries.
    :return: None after result, history, cleared input state, and forced final status redraw are checked.
    """
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
    """
    Preserve KeyboardInterrupt from Ctrl-C rather than treating it as ordinary EOF or an accepted command.

    Example:
        >>> test_curses_control_c_remains_an_interrupt(patch)  # doctest: +SKIP


    :param monkeypatch: Fixture making the headless driver's next key event Ctrl-C.
    :return: None after read_line raises the expected KeyboardInterrupt.
    """
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
    """
    Preserve auxiliary-pane minimum heights and allocation order when both telemetry and job panes are enabled.

    Example:
        >>> test_panel_allocation_keeps_minima_and_round_robin_priority(16, (4, 4))


    :param rows: Parametrized available terminal height passed to the layout allocator.
    :param expected: Expected telemetry/job height pair with a fixed five-row status pane.
    :return: None after allocated pane heights match the established layout contract.
    """
    driver = _CursesUiDriver(object(), config=WindowedUiConfig(), history_file=None)
    driver._telemetry_tables = ("works",)
    driver._job_output_job_id = "job"
    assert driver._allocate_aux_panel_heights(rows=rows, status_h=5) == expected
