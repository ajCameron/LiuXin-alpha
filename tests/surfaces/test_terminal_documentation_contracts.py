"""
Check source-linked terminal documentation against headless construction, lookup, paging, preview, and input behavior.

Recording Core/row/driver doubles isolate the documented boundaries without
opening databases, loading history, starting jobs, or initializing a real curses
screen. Abstract contract links are resolved and checked against their owner docs.
"""

from __future__ import annotations

import importlib
import inspect
import io
import re
from collections.abc import Iterator, Mapping
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest

from LiuXin_alpha.core import CoreClientAPI
from LiuXin_alpha.surfaces.terminal.browser import TextDatabaseBrowser
from LiuXin_alpha.surfaces.terminal.browser_components.contracts import BrowserState
from LiuXin_alpha.surfaces.terminal.browser_components.rows import (
    _lookup_row_value,
    _RowSummary,
)
from LiuXin_alpha.surfaces.terminal.windowed_components.contracts import WindowedState
from LiuXin_alpha.surfaces.terminal.windowed_ui import (
    WindowedUiConfig,
    _CursesUiDriver,
    _WindowedTextDatabaseBrowser,
)


def _browser() -> TextDatabaseBrowser:
    """
    Construct a concrete browser with borrowed mock Core/job managers and in-memory streams, without starting it.

    Example:
        >>> _browser().window is None
        True


    :return: Fresh initialized browser whose Core and job operations are recording mocks.
    """
    return TextDatabaseBrowser(
        Mock(spec=CoreClientAPI),
        input=io.StringIO(),
        output=io.StringIO(),
        job_manager=Mock(),
    )


def test_browser_and_driver_keep_distinct_empty_history_paths() -> None:
    """
    Keep browser Path('.') versus disabled driver history for an explicit empty path, and clamp their own numeric bounds.

    Example:
        >>> test_browser_and_driver_keep_distinct_empty_history_paths()


    :return: None after history-path, page-size, buffer-capacity, read-source fallback, and unstarted-state assertions.
    """
    shell = TextDatabaseBrowser(
        Mock(spec=CoreClientAPI),
        page_size=-2,
        history_file="",
        metadata_read_source=[],
        input=io.StringIO(),
        output=io.StringIO(),
        job_manager=Mock(),
    )
    driver = _CursesUiDriver(
        object(), config=WindowedUiConfig(max_console_lines=1), history_file=""
    )
    assert shell._history_file == Path(".") and driver.history_file is None
    assert shell.page_size == 1 and driver._lines.maxlen == 100
    assert shell.metadata_read_source is shell.model
    assert shell._extension_host is shell
    assert shell._started is False and driver.browser is None


def test_catalog_counts_and_pages_keep_error_and_bound_semantics() -> None:
    """
    Preserve unknown counts, raw nonpositive page-limit behavior, public bound clamping, and visible iterator failures.

    Example:
        >>> test_catalog_counts_and_pages_keep_error_and_bound_semantics()


    :return: None after count sentinels, page contents, and propagation of a mid-iteration failure are checked.
    """
    shell = _browser()
    shell.db = Mock()
    shell.db.get_record_count.side_effect = [0, -3, RuntimeError("count failed")]
    assert [shell.get_table_row_count("works") for _ in range(3)] == [0, None, None]
    rows = [{"work_id": 1}, {"work_id": 2}, {"work_id": 3}]
    shell.db.get_all_rows.side_effect = [iter(rows), iter(rows)]
    assert shell._table_slice("works", limit=0, offset=1) == [rows[1]]
    assert shell.table_slice("works", limit=0, offset=-5) == [rows[0]]
    assert all(
        call.kwargs == {"iterator_return": True}
        for call in shell.db.get_all_rows.call_args_list
    )

    def broken_rows() -> Iterator[dict[str, int]]:
        """
        Yield one row before simulating a failing backend iterator to test partial-page error propagation.

        Example:
            >>> iterator = broken_rows()  # doctest: +SKIP


        :return: Iterator yielding one work-ID mapping before raising RuntimeError on its next advancement.
        """
        yield {"work_id": 1}
        raise RuntimeError("iteration failed")

    shell.db.get_all_rows.side_effect = None
    shell.db.get_all_rows.return_value = broken_rows()
    with pytest.raises(RuntimeError, match="iteration failed"):
        shell.table_slice("works", limit=2)


def test_catalog_name_resolution_preserves_lookup_precedence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Prefer exact table/schema names to aliases and retain ambiguity after repeated display-name collisions.

    Example:
        >>> with pytest.MonkeyPatch.context() as patch:
        ...     test_catalog_name_resolution_preserves_lookup_precedence(patch)


    :param monkeypatch: Fixture supplying intentionally colliding full/display schema names on a headless browser.
    :return: None after table aliases, current-table reuse, exact-column priority, and ambiguous prefixes are checked.
    """
    shell = _browser()
    shell.db = Mock()
    shell.db.get_tables.return_value = ["works", "work", "categories"]
    assert shell.resolve_table("work") == "work"
    assert shell.resolve_table("category") == "categories"
    shell.current_table = "works"
    assert shell.resolve_table(None) == "works"
    with pytest.raises(ValueError, match="blank"):
        shell.resolve_table("")
    monkeypatch.setattr(
        shell, "get_table_columns", Mock(return_value=["name", "other"])
    )
    monkeypatch.setattr(
        shell, "get_table_display_columns", Mock(return_value=["name", "name"])
    )
    assert shell.resolve_table_column("works", " NAME ") == "name"
    with pytest.raises(ValueError, match="Ambiguous column"):
        shell.resolve_table_column("works", "na")
    assert shell._column_display_aliases(["a", "b", "a"], ["x", "x", "x"]) == {
        "x": None
    }
    assert shell._column_prefix_matches("", ["a", "b", "a"], ["x", "y", "x"]) == [
        "a",
        "b",
    ]
    assert shell.current_table == "works"


def test_language_lookup_does_not_trim_stored_values_or_hide_errors() -> None:
    """
    Retain casefold-only scan matching and propagate search/conversion failures rather than silently returning no match.

    Example:
        >>> test_language_lookup_does_not_trim_stored_values_or_hide_errors()


    :return: None after stored-whitespace, usable first matching ID, search failure, and digit-only conversion checks.
    """
    shell = _browser()
    shell.model = Mock()
    shell.model.table_exists.return_value = True
    shell.model.columns.return_value = ["language_id", "language_name"]
    shell.model.id_column.return_value = "language_id"
    shell.model.search.return_value = []
    padded = SimpleNamespace(row_id=1, get=Mock(return_value=" English "))
    exact = SimpleNamespace(row_id=2, get=Mock(return_value="ENGLISH"))
    shell.model.rows.return_value = [padded]
    assert shell.resolve_language_id("english") is None
    shell.model.rows.return_value = [padded, exact]
    assert shell.resolve_language_id(" english ") == 2
    shell.model.search.side_effect = RuntimeError("search failed")
    with pytest.raises(RuntimeError, match="search failed"):
        shell.resolve_language_id("english")
    with pytest.raises(ValueError):
        shell.resolve_language_id("²")


def test_row_lookup_tolerates_only_non_mapping_access_failures() -> None:
    """
    Retry failed indexed access on non-Mapping rows while leaving Mapping get errors visible and retaining present None.

    Example:
        >>> test_row_lookup_tolerates_only_non_mapping_access_failures()


    :return: None after both access policies and exact retry count are asserted.
    """
    indexed = MagicMock()
    indexed.__contains__.return_value = True
    indexed.__getitem__.side_effect = [RuntimeError("first lookup failed"), None]
    assert _lookup_row_value(indexed, "title") == (True, None)
    assert indexed.__getitem__.call_count == 2
    mapping = MagicMock(spec=Mapping)
    mapping.__contains__.return_value = True
    mapping.get.side_effect = RuntimeError("mapping get failed")
    with pytest.raises(RuntimeError, match="mapping get failed"):
        _lookup_row_value(mapping, "title")
    assert _lookup_row_value({}, "title") == (False, None)


def test_row_previews_deduplicate_rendered_text_and_keep_full_fallback() -> None:
    """
    Deduplicate truncated preview text, omit falsey previews, and retain present falsey values in full repr fallback.

    Example:
        >>> test_row_previews_deduplicate_rendered_text_and_keep_full_fallback()


    :return: None after normalization/collision behavior and the final full-row representation are checked.
    """
    summary = _RowSummary()
    for value in [0, False, "a" * 70 + "X", "a" * 70 + "Y"]:
        summary.add_value(value)
    assert summary.parts == ["a" * 61 + "..."]
    shell = _browser()
    shell.db = Mock()
    shell.db.driver_wrapper.get_id_column.return_value = "work_id"
    shell.db.get_column_headings.return_value = ["work_id", "work_title", "work_count"]
    assert (
        shell.format_row("works", {"work_id": None, "work_title": "", "work_count": 0})
        == "work_id=None | work_title='' | work_count=0"
    )
    assert (
        shell.format_row(
            "works", {"work_id": 7, "work_title": "Title", "work_count": 12}
        )
        == "#7 | Title"
    )


def test_windowed_adapter_preserves_blank_eof_and_single_boolean_prompt() -> None:
    """
    Keep blank command responses as EOF and use the configured boolean default after one invalid normalized response.

    The browser is allocated without base initialization because only the adapter's
    forwarding methods and default class prompt are under test.

    Example:
        >>> test_windowed_adapter_preserves_blank_eof_and_single_boolean_prompt()


    :return: None after newline handling, one-shot input, and exact warning/default behavior are checked.
    """
    shell = object.__new__(_WindowedTextDatabaseBrowser)
    driver = Mock()
    driver.read_line.side_effect = ["", "help", " MAYBE "]
    shell._ui_driver = driver
    assert shell._read_command_line() == ""
    assert shell._read_command_line() == "help\n"
    assert shell.prompt_yes_no("Continue", default=False) is False
    assert driver.read_line.call_count == 3
    driver.read_line.assert_called_with("Continue (y/N): ", default=None)
    driver.append_output.assert_called_once_with(
        "Invalid response 'maybe'; using default no", end="\n"
    )


@pytest.mark.parametrize("state", [BrowserState, WindowedState])
def test_abstract_contract_docs_resolve_to_current_owner_methods(state: type) -> None:
    """
    Resolve every abstract method's reST owner link and compare its summary, fields, argument names, and descriptor kind.

    Example:
        >>> test_abstract_contract_docs_resolve_to_current_owner_methods(BrowserState)


    :param state: Parametrized browser or windowed ABC inspected without instantiation or invocation of its abstract bodies.
    :return: None after every abstract method's documentation matches its linked maintained implementation.
    """
    assert inspect.isabstract(state)
    for name in state.__abstractmethods__:
        descriptor = inspect.getattr_static(state, name)
        abstract = (
            descriptor.fget
            if isinstance(descriptor, property)
            else getattr(state, name)
        )
        description = inspect.getdoc(abstract)
        assert description is not None
        links = re.findall(r":meth:`([^`]+)`", description)
        assert len(links) == 1
        module_name, class_name, method_name = links[0].rsplit(".", 2)
        assert method_name == name
        owner_class = getattr(importlib.import_module(module_name), class_name)
        owner_descriptor = inspect.getattr_static(owner_class, method_name)
        assert isinstance(descriptor, property) == isinstance(
            owner_descriptor, property
        )
        assert isinstance(descriptor, staticmethod) == isinstance(
            owner_descriptor, staticmethod
        )
        owner = (
            owner_descriptor.fget
            if isinstance(owner_descriptor, property)
            else getattr(owner_class, method_name)
        )
        owner_doc = inspect.getdoc(owner)
        assert owner_doc is not None
        assert description.splitlines()[0] == owner_doc.splitlines()[0]
        fields = re.search(r"^:(?:param |return:)", description, re.M)
        owner_fields = re.search(r"^:(?:param |return:)", owner_doc, re.M)
        assert fields is not None and owner_fields is not None
        assert description[fields.start() :] == owner_doc[owner_fields.start() :]
        assert list(inspect.signature(abstract).parameters) == list(
            inspect.signature(owner).parameters
        )
