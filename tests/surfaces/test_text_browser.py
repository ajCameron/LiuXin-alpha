from __future__ import annotations

import builtins
import io
import signal

from pathlib import Path

import pytest

pytest.importorskip(
    "LiuXin_alpha.surfaces.terminal",
    reason="Terminal package is not exposed under surfaces/ in this checkout.",
)

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.surfaces.terminal.commands import DEFAULT_COMMAND_CLASSES
from LiuXin_alpha.surfaces.terminal.commands import db as db_command_module
from LiuXin_alpha.surfaces.terminal.commands import off as off_commands
from LiuXin_alpha.surfaces.terminal.commands import on as on_commands
from LiuXin_alpha.surfaces.terminal.commands import sync as sync_command_module
from LiuXin_alpha.surfaces.terminal.plugins import TerminalLifecyclePluginAPI
from LiuXin_alpha.surfaces.terminal import text_browser as text_browser_module
from LiuXin_alpha.surfaces.terminal.text_browser import TextDatabaseBrowser, main as browser_main
from LiuXin_alpha.library.library import Library
from LiuXin_alpha.metadata.standardization import make_tag_search_term, make_title_search_term, standardize_genre
from LiuXin_alpha.storage.store_backend_plugins.rclone_http_readonly import (
    rclone_http_storage_backend as rclone_backend_module,
)
from LiuXin_alpha.storage.store_backend_plugins.native_html_readonly import (
    native_html_storage_backend as native_html_backend_module,
)
from LiuXin_alpha.storage.store_backend_plugins.wget_html_readonly import (
    wget_html_storage_backend as wget_backend_module,
)
from LiuXin_alpha.storage.store_backend_plugins.wget_html_readonly.wget_utils import WgetResult
from LiuXin_alpha.utils.jobs import JobRequest
from LiuXin_alpha.utils.jobs.manager import InMemoryJobManager
from tests.support._surface_storage_tables import ensure_surface_asset_tables


def _preferred_tag_table(db: Database) -> str:
    tables = set(db.get_tables())
    if "tags" in tables:
        return "tags"
    if "labels" in tables:
        return "labels"
    pytest.fail("Schema has neither labels nor tags table")


def _search_tag_rows(db: Database, tag_text: str):
    norm = make_tag_search_term(tag_text)
    table = _preferred_tag_table(db)
    if table == "tags":
        return "tags", db.search("tags", "tag_phash", norm)
    return "labels", db.search("labels", "label_text_norm", norm)


def _insert_store_row(
    db: Database,
    *,
    name: str,
    kind: str,
    root_uri: str,
    access_protocol: str = "file",
    is_read_only: int = 0,
    online_status: str = "online",
) -> int:
    ensure_surface_asset_tables(db, include_file_store_links=True)
    row = Row.from_idless_row_dict(
        db,
        row_dict={
            "store_name": name,
            "store_kind": kind,
            "store_access_protocol": access_protocol,
            "store_root_uri": root_uri,
            "store_is_read_only": int(is_read_only),
            "store_online_status": online_status,
        },
        table="stores",
    )
    return int(row["store_id"])


def _insert_folder_row(
    db: Database,
    *,
    store_id: int,
    name: str = "root",
    relpath: str = "root",
) -> int:
    row = Row.from_idless_row_dict(
        db,
        row_dict={
            "folder_store_id": int(store_id),
            "folder_name": name,
            "folder_relpath": relpath,
        },
        table="folders",
    )
    return int(row["folder_id"])


class _PanelAwareBrowser(TextDatabaseBrowser):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.panel_job_id: str | None = None
        self.panel_attach_calls = 0
        self.panel_detach_calls = 0
        self.telemetry_tables: tuple[str, ...] | None = None
        self.telemetry_attach_calls = 0
        self.telemetry_detach_calls = 0

    def supports_job_output_panel(self) -> bool:
        return True

    def attach_job_output_panel(self, job_id: str) -> bool:
        self.panel_attach_calls += 1
        self.panel_job_id = str(job_id).strip() or None
        return True

    def detach_job_output_panel(self) -> bool:
        self.panel_detach_calls += 1
        had = self.panel_job_id is not None
        self.panel_job_id = None
        return had

    def supports_telemetry_panel(self) -> bool:
        return True

    def attach_telemetry_panel(self, tables=None) -> bool:
        self.telemetry_attach_calls += 1
        self.telemetry_tables = tuple(str(one) for one in (tables or ()))
        return True

    def detach_telemetry_panel(self) -> bool:
        self.telemetry_detach_calls += 1
        had = self.telemetry_tables is not None
        self.telemetry_tables = None
        return had


def test_text_browser_session_basic_browsing(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_session.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_id = _insert_store_row(
            db,
            name="browser_store",
            kind="on_disk_existing_managed_drive",
            root_uri=str(tmp_path / "store_root"),
        )

        shell = TextDatabaseBrowser(db, page_size=5, output=output)
        assert shell.execute_line("tables")
        assert shell.execute_line("use stores")
        assert shell.execute_line("schema")
        assert shell.execute_line("count")
        assert shell.execute_line("browse 5 0")
        assert shell.execute_line("search stores store_name browser_store 5")
        assert shell.execute_line("search stores browser_store --limit 5")
        assert shell.execute_line("row stores {}".format(store_id))
        assert shell.execute_line("summary 3")
        assert shell.execute_line("pagesize 3")
        assert shell.page_size == 3

    rendered = output.getvalue()
    assert "stores" in rendered
    assert "Current table: stores" in rendered
    assert "Schema for stores" in rendered
    assert "store_name" in rendered
    assert "browser_store" in rendered
    assert "Search stores.store_name" in rendered
    assert "Search stores contains 'browser_store'" in rendered
    assert "Summary: matches_total=" in rendered
    assert "Summary: scanned_rows=" in rendered
    assert "Database summary" in rendered
    assert "Largest tables" in rendered


def test_text_browser_main_non_interactive(driver_spec, tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "browser_main.sqlite"

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        _insert_store_row(
            db,
            name="main_store",
            kind="on_disk_existing_managed_drive",
            root_uri=str(tmp_path / "store_root"),
        )

    rc = browser_main(
        [
            "--database",
            str(db_path),
            "--db-type",
            driver_spec.db_type,
            "--command",
            "tables",
            "--command",
            "use stores",
            "--command",
            "browse 1 0",
            "--command",
            "summary 2",
        ]
    )
    assert rc == 0

    out = capsys.readouterr().out
    assert "stores" in out
    assert "Current table: stores" in out
    assert "Browsing stores rows" in out
    assert "Database summary" in out


def test_text_browser_parser_accepts_windowed_mode_options() -> None:
    parser = text_browser_module.build_parser()
    args = parser.parse_args(
        [
            "--database",
            "library.sqlite",
            "--ui-mode",
            "windowed",
            "--windowed-status-refresh-s",
            "2.5",
            "--windowed-status-height",
            "11",
            "--windowed-job-panel-height",
            "12",
            "--windowed-telemetry-panel-height",
            "13",
        ]
    )
    assert args.database == "library.sqlite"
    assert args.ui_mode == "windowed"
    assert args.windowed_status_refresh_s == pytest.approx(2.5)
    assert args.windowed_status_height == 11
    assert args.windowed_job_panel_height == 12
    assert args.windowed_telemetry_panel_height == 13


def test_text_browser_main_windowed_mode_dispatches(driver_spec, tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "windowed_mode.sqlite"
    history_path = tmp_path / "windowed_history.txt"
    observed: dict[str, object] = {}

    def _fake_run_windowed(db, **kwargs):
        observed["database_path"] = str(getattr(db, "metadata", {}).get("database_path", ""))
        observed.update(kwargs)
        return 17

    monkeypatch.setattr(text_browser_module, "run_windowed_text_browser", _fake_run_windowed)

    rc = browser_main(
        [
            "--database",
            str(db_path),
            "--db-type",
            driver_spec.db_type,
            "--ui-mode",
            "windowed",
            "--page-size",
            "33",
            "--windowed-status-refresh-s",
            "2.5",
            "--windowed-status-height",
            "11",
            "--windowed-job-panel-height",
            "12",
            "--windowed-telemetry-panel-height",
            "13",
            "--history-file",
            str(history_path),
        ]
    )
    assert rc == 17
    assert observed["database_path"] == str(db_path)
    assert observed["page_size"] == 33
    assert observed["history_file"] == str(history_path)
    assert observed["status_refresh_s"] == pytest.approx(2.5)
    assert observed["status_height"] == 11
    assert observed["job_panel_height"] == 12
    assert observed["telemetry_panel_height"] == 13


def test_text_browser_main_command_mode_overrides_windowed(driver_spec, tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "windowed_ignored.sqlite"

    def _unexpected_windowed(*_args, **_kwargs):
        raise AssertionError("windowed UI should not run when --command is provided")

    monkeypatch.setattr(text_browser_module, "run_windowed_text_browser", _unexpected_windowed)

    rc = browser_main(
        [
            "--database",
            str(db_path),
            "--db-type",
            driver_spec.db_type,
            "--ui-mode",
            "windowed",
            "--command",
            "tables",
        ]
    )
    assert rc == 0
    out = capsys.readouterr().out
    assert "stores" in out


def test_text_browser_help_command_shows_specific_direct_command(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_help_direct.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, output=output)

        assert shell.execute_line("help clear")

    rendered = output.getvalue()
    assert "Command: clear" in rendered
    assert "Usage: clear" in rendered
    assert "Aliases: cls" in rendered


def test_text_browser_help_command_shows_group_details(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_help_group.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, output=output)

        assert shell.execute_line("help add")

    rendered = output.getvalue()
    assert "Command group: add" in rendered
    assert "Aliases: new" in rendered
    assert "add store" in rendered
    assert "Use `help add <subcommand>` for details." in rendered


def test_text_browser_help_command_shows_group_subcommand_details(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_help_subcommand.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, output=output)

        assert shell.execute_line("help add store")

    rendered = output.getvalue()
    assert "Command: add store" in rendered
    assert "Usage: add store" in rendered
    assert "Direct names: store, new-store, new_store, add-store, add_store" in rendered
    assert "Group aliases: new" in rendered


def test_text_browser_clear_command_truncates_seekable_output(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_clear.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, output=output)
        output.write("stale output")

        assert shell.execute_line("clear")

    assert output.getvalue() == ""


def test_text_browser_startup_warns_when_core_runtime_bootstrap_fails(
    driver_spec, tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "browser_core_runtime_warn.sqlite"
    history_path = tmp_path / "history.txt"
    output = io.StringIO()

    def _fail_core_runtime(*_args, **_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(text_browser_module, "_build_default_core_runtime", _fail_core_runtime)

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, output=output, history_file=history_path)
        shell.startup()
        shell.shutdown(reason="test")

    rendered = output.getvalue()
    assert "WARNING: Core runtime unavailable; using local-only mode." in rendered
    assert "RuntimeError: boom" in rendered
    assert shell.supports_core_commands() is False


def test_sync_store_options_default_to_incremental_crawler_db_writes() -> None:
    options = sync_command_module._parse_sync_store_options(["1"], usage="sync store <id>")
    assert options.crawler_incremental_db_writes is True


def test_sync_store_options_can_disable_incremental_crawler_db_writes() -> None:
    options = sync_command_module._parse_sync_store_options(
        ["1", "--crawler-no-incremental-db-writes"],
        usage="sync store <id>",
    )
    assert options.crawler_incremental_db_writes is False


def test_text_browser_help_includes_registered_search_command(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_help_search.sqlite"
    output = io.StringIO()
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("help")

    rendered = output.getvalue()
    assert "search <table> <term> [--limit n]" in rendered
    assert "Search rows in a table." in rendered


def test_text_browser_core_command_aliases_are_registered(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_command_aliases.sqlite"
    output = io.StringIO()
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("h")
        assert shell.execute_line("?")
        assert shell.execute_line("columns stores")
        assert shell.execute_line("ls stores 1 0")

    rendered = output.getvalue()
    assert "Commands:" in rendered
    assert "Schema for stores" in rendered
    assert "Browsing stores rows" in rendered


def test_text_browser_jobs_group_lists_subcommands(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_jobs_group.sqlite"
    output = io.StringIO()
    manager = InMemoryJobManager(max_workers=1, default_backend="serial")
    try:
        with Database(
            metadata={"database_path": str(db_path)},
            db_type=driver_spec.db_type,
            create=True,
            backup=False,
            storage_startup_on_add=False,
        ) as db:
            shell = TextDatabaseBrowser(db, output=output, job_manager=manager)
            assert shell.execute_line("jobs")
    finally:
        manager.shutdown(wait=True, cancel_pending=True)

    rendered = output.getvalue()
    assert "Available `jobs` subcommands:" in rendered
    assert "jobs list" in rendered
    assert "jobs show" in rendered
    assert "jobs tail" in rendered
    assert "jobs cancel" in rendered
    assert "jobs panel" in rendered


def test_text_browser_db_group_lists_subcommands(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_db_group.sqlite"
    output = io.StringIO()
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("db")

    rendered = output.getvalue()
    assert "Available `db` subcommands:" in rendered
    assert "db unlock" in rendered


def test_text_browser_db_unlock_locked_without_kill_raises(driver_spec, tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "browser_db_unlock_locked.sqlite"
    output = io.StringIO()

    holder = db_command_module._FileHolder(pid=4242, command="python", path=str(db_path))
    monkeypatch.setattr(
        db_command_module,
        "_probe_database_write_lock",
        lambda *args, **kwargs: (False, "database is locked"),
    )
    monkeypatch.setattr(
        db_command_module,
        "_list_file_holders",
        lambda *args, **kwargs: [holder],
    )

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, output=output)
        with pytest.raises(ValueError):
            shell.execute_line("db unlock")

    rendered = output.getvalue()
    assert "Write lock probe: LOCKED" in rendered
    assert "pid=4242" in rendered


def test_text_browser_db_unlock_can_kill_external_holder(driver_spec, tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "browser_db_unlock_kill.sqlite"
    output = io.StringIO()

    state = {"killed": False}
    holder = db_command_module._FileHolder(pid=7777, command="python", path=str(db_path))
    sent_signals: list[tuple[set[int], int]] = []

    def _fake_probe(*args, **kwargs):
        if state["killed"]:
            return True, ""
        return False, "database is locked"

    def _fake_list(*args, **kwargs):
        if state["killed"]:
            return []
        return [holder]

    def _fake_signal(pids: set[int], sig: int):
        sent_signals.append((set(pids), int(sig)))
        state["killed"] = True
        return sorted(pids)

    monkeypatch.setattr(db_command_module, "_probe_database_write_lock", _fake_probe)
    monkeypatch.setattr(db_command_module, "_list_file_holders", _fake_list)
    monkeypatch.setattr(db_command_module, "_send_signal_to_pids", _fake_signal)
    monkeypatch.setattr(
        db_command_module,
        "_run_recovery_pragmas",
        lambda *args, **kwargs: {
            "checkpoint_busy": 0,
            "checkpoint_log_frames": 0,
            "checkpoint_frames_checkpointed": 0,
            "integrity_check": None,
        },
    )

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("db unlock --kill --no-check")

    assert sent_signals
    assert sent_signals[0][0] == {7777}
    assert sent_signals[0][1] == int(signal.SIGTERM)
    rendered = output.getvalue()
    assert "Sent SIGTERM" in rendered
    assert "DB unlock completed." in rendered


def test_text_browser_db_unlock_can_escalate_to_sudo(driver_spec, tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "browser_db_unlock_sudo.sqlite"
    output = io.StringIO()

    state = {"killed": False}
    holder = db_command_module._FileHolder(pid=9191, command="python", path=str(db_path))
    sent_local: list[tuple[set[int], int]] = []
    sent_sudo: list[tuple[set[int], int]] = []

    def _fake_probe(*args, **kwargs):
        if state["killed"]:
            return True, ""
        return False, "database is locked"

    def _fake_list(*args, **kwargs):
        if state["killed"]:
            return []
        return [holder]

    def _fake_local_signal(pids: set[int], sig: int):
        sent_local.append((set(pids), int(sig)))
        # Simulate permission denied/no effect from local kill attempt.
        return []

    def _fake_sudo_signal(pids: set[int], sig: int):
        sent_sudo.append((set(pids), int(sig)))
        state["killed"] = True
        return sorted(pids)

    monkeypatch.setattr(db_command_module, "_probe_database_write_lock", _fake_probe)
    monkeypatch.setattr(db_command_module, "_list_file_holders", _fake_list)
    monkeypatch.setattr(db_command_module, "_send_signal_to_pids", _fake_local_signal)
    monkeypatch.setattr(db_command_module, "_send_signal_to_pids_via_sudo", _fake_sudo_signal)
    monkeypatch.setattr(
        db_command_module,
        "_run_recovery_pragmas",
        lambda *args, **kwargs: {
            "checkpoint_busy": 0,
            "checkpoint_log_frames": 0,
            "checkpoint_frames_checkpointed": 0,
            "integrity_check": None,
        },
    )

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("db unlock --kill --sudo --no-check")

    assert sent_local
    assert sent_local[0][0] == {9191}
    assert sent_local[0][1] == int(signal.SIGTERM)
    assert sent_sudo
    assert sent_sudo[0][0] == {9191}
    assert sent_sudo[0][1] == int(signal.SIGTERM)
    rendered = output.getvalue()
    assert "Sent sudo SIGTERM" in rendered
    assert "DB unlock completed." in rendered


def test_text_browser_jobs_list_and_show(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_jobs_list_show.sqlite"
    output = io.StringIO()
    manager = InMemoryJobManager(max_workers=1, default_backend="serial")
    try:
        job_id = manager.submit(
            JobRequest(module_name="math", function_name="sqrt", args=(81,)),
            no_output=True,
            label="sqrt81",
        )
        manager.wait(job_id, timeout=2.0)

        with Database(
            metadata={"database_path": str(db_path)},
            db_type=driver_spec.db_type,
            create=True,
            backup=False,
            storage_startup_on_add=False,
        ) as db:
            shell = TextDatabaseBrowser(db, output=output, job_manager=manager)
            assert shell.execute_line("jobs list")
            assert shell.execute_line("jobs show {}".format(job_id))
    finally:
        manager.shutdown(wait=True, cancel_pending=True)

    rendered = output.getvalue()
    assert "Summary: total=" in rendered
    assert job_id in rendered
    assert "state" in rendered
    assert "succeeded" in rendered
    assert "result_preview" in rendered
    assert "9.0" in rendered


def test_text_browser_jobs_commands_route_via_core_when_available(driver_spec, tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "browser_jobs_core_route.sqlite"
    output = io.StringIO()
    manager = InMemoryJobManager(max_workers=1, default_backend="serial")
    try:
        job_id = manager.submit(
            JobRequest(module_name="math", function_name="sqrt", args=(81,)),
            no_output=True,
            label="sqrt81-core",
        )
        manager.wait(job_id, timeout=2.0)

        with Database(
            metadata={"database_path": str(db_path)},
            db_type=driver_spec.db_type,
            create=True,
            backup=False,
            storage_startup_on_add=False,
        ) as db:
            shell = TextDatabaseBrowser(db, output=output, job_manager=manager)
            query_calls: list[str] = []
            command_calls: list[str] = []

            original_query = shell.execute_core_query
            original_command = shell.execute_core_command

            def _record_query(name: str, *, payload=None):
                query_calls.append(str(name))
                return original_query(name, payload=payload)

            def _record_command(name: str, *, payload=None):
                command_calls.append(str(name))
                return original_command(name, payload=payload)

            monkeypatch.setattr(shell, "execute_core_query", _record_query)
            monkeypatch.setattr(shell, "execute_core_command", _record_command)

            assert shell.execute_line("jobs list")
            assert shell.execute_line("jobs show {}".format(job_id))
            assert shell.execute_line("jobs cancel {}".format(job_id))
    finally:
        manager.shutdown(wait=True, cancel_pending=True)

    assert "jobs.list" in query_calls
    assert "jobs.get" in query_calls
    assert "jobs.cancel" in command_calls


def test_text_browser_jobs_cancel_pending_job(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_jobs_cancel.sqlite"
    output = io.StringIO()
    manager = InMemoryJobManager(max_workers=1, default_backend="serial")
    try:
        source = """
import time

def run(seconds):
    time.sleep(seconds)
    return seconds
"""
        _first_job = manager.submit(
            JobRequest(module_name=source, function_name="run", args=(0.4,), module_is_source_code=True),
            no_output=True,
            label="blocker",
        )
        second_job = manager.submit(
            JobRequest(module_name=source, function_name="run", args=(0.01,), module_is_source_code=True),
            no_output=True,
            label="to-cancel",
        )

        with Database(
            metadata={"database_path": str(db_path)},
            db_type=driver_spec.db_type,
            create=True,
            backup=False,
            storage_startup_on_add=False,
        ) as db:
            shell = TextDatabaseBrowser(db, output=output, job_manager=manager)
            assert shell.execute_line("jobs cancel {}".format(second_job))
            assert shell.execute_line("jobs show {} --wait=1".format(second_job))
    finally:
        manager.shutdown(wait=True, cancel_pending=True)

    rendered = output.getvalue()
    assert (
        "Cancel requested for {}".format(second_job) in rendered
        or "No cancellable job found for {}".format(second_job) in rendered
    )
    assert "state" in rendered
    assert any(token in rendered for token in ("cancelled", "aborted", "succeeded"))


def test_text_browser_jobs_panel_command_attach_and_detach(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_jobs_panel.sqlite"
    output = io.StringIO()
    manager = InMemoryJobManager(max_workers=1, default_backend="process")
    try:
        source = """
import time

def run():
    print("panel hello")
    time.sleep(0.2)
    print("panel done")
    return 1
"""
        job_id = manager.submit(
            JobRequest(module_name=source, function_name="run", module_is_source_code=True),
            no_output=False,
            label="panel-test",
            timeout=5.0,
        )

        with Database(
            metadata={"database_path": str(db_path)},
            db_type=driver_spec.db_type,
            create=True,
            backup=False,
            storage_startup_on_add=False,
        ) as db:
            shell = _PanelAwareBrowser(db, output=output, job_manager=manager)
            assert shell.execute_line("jobs panel {}".format(job_id))
            assert shell.panel_job_id == job_id
            assert shell.execute_line("jobs panel off")
            assert shell.panel_job_id is None
    finally:
        manager.shutdown(wait=True, cancel_pending=True)

    rendered = output.getvalue()
    assert "Job output panel attached to {}".format(job_id) in rendered
    assert "Job output panel detached." in rendered


def test_text_browser_jobs_tail_shows_recent_log_lines(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_jobs_tail.sqlite"
    output = io.StringIO()
    manager = InMemoryJobManager(max_workers=1, default_backend="process")
    try:
        source = """
def run():
    print("tail first")
    print("tail second")
    print("tail third")
    return 1
"""
        job_id = manager.submit(
            JobRequest(module_name=source, function_name="run", module_is_source_code=True),
            no_output=False,
            label="tail-test",
            timeout=5.0,
        )
        manager.wait(job_id, timeout=5.0)

        with Database(
            metadata={"database_path": str(db_path)},
            db_type=driver_spec.db_type,
            create=True,
            backup=False,
            storage_startup_on_add=False,
        ) as db:
            shell = TextDatabaseBrowser(db, output=output, job_manager=manager)
            assert shell.execute_line("jobs tail {} 2".format(job_id))
    finally:
        manager.shutdown(wait=True, cancel_pending=True)

    rendered = output.getvalue()
    assert "Job tail {}".format(job_id) in rendered
    assert "tail second" in rendered
    assert "tail third" in rendered
    assert "tail first" not in rendered
    assert "Summary: total_lines=" in rendered


def test_text_browser_default_commands_and_aliases_registered(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_default_commands.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db)
        for command_class in DEFAULT_COMMAND_CLASSES:
            expected = command_class()
            names = [expected.name] + list(expected.aliases)

            if bool(getattr(expected, "expose_direct", True)):
                for raw_name in names:
                    token = shell._normalize_command_token(raw_name)
                    if not token:
                        continue
                    resolved = shell._commands.get(token)
                    assert resolved is not None
                    assert isinstance(resolved, command_class)

            group_name = shell._normalize_command_token(getattr(expected, "group", None))
            if not group_name:
                continue

            assert shell._group_alias_to_group[group_name] == group_name
            for raw_alias in getattr(expected, "group_aliases", ()) or ():
                alias = shell._normalize_command_token(raw_alias)
                if alias:
                    assert shell._group_alias_to_group[alias] == group_name
            if group_name == "add":
                assert shell._group_alias_to_group["new"] == "add"

            group_map = shell._command_groups[group_name]
            for raw_name in names:
                token = shell._normalize_command_token(raw_name)
                if not token:
                    continue
                resolved = group_map.get(token)
                assert resolved is not None
                assert isinstance(resolved, command_class)


def test_text_browser_read_command_line_non_tty_streams(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_readline_non_tty.sqlite"
    output = io.StringIO()
    input_stream = io.StringIO("tables\n")
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        line = shell._read_command_line()
    assert line == "tables\n"
    assert "liuxin-db> " in output.getvalue()


def test_text_browser_read_command_line_readline_mode_uses_input(monkeypatch, driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_readline_mode.sqlite"
    prompts: list[str] = []

    def _fake_input(prompt: str) -> str:
        prompts.append(prompt)
        return "tables"

    monkeypatch.setattr(builtins, "input", _fake_input)
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db)
        shell._can_use_readline_prompt = lambda: True  # type: ignore[method-assign]
        line = shell._read_command_line()
    assert line == "tables\n"
    assert prompts == ["liuxin-db> "]


def test_text_browser_command_completion_candidates_cover_help_and_groups(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_completion.sqlite"

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db)

        root = shell.command_completion_candidates("he")
        help_root = shell.command_completion_candidates("help a")
        help_group = shell.command_completion_candidates("help add st")
        group_alias = shell.command_completion_candidates("new s")

    assert "help" in root.candidates
    assert "add" in help_root.candidates
    assert "add-store" in help_root.candidates
    assert help_group.candidates == ("store",)
    assert set(group_alias.candidates) >= {"series", "store", "subject"}


def test_text_browser_command_completion_candidates_cover_table_slots(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_table_completion.sqlite"

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db)

        use_table = shell.command_completion_candidates("use st")
        row_table = shell.command_completion_candidates("row wo:")
        show_table = shell.command_completion_candidates("show tag st")
        on_table = shell.command_completion_candidates("on tag it")
        off_table = shell.command_completion_candidates("off note st")
        link_left = shell.command_completion_candidates("link wo")
        link_right = shell.command_completion_candidates("link works 1 st")
        link_right_compact = shell.command_completion_candidates("link works:1 st:")
        links_other = shell.command_completion_candidates("links works:1 st")
        set_column_short = shell.command_completion_candidates("set store:1 na")
        set_column_full = shell.command_completion_candidates("set store:1 store_n")
        edit_column = shell.command_completion_candidates("edit store:1 ro")
        delete_row_ref = shell.command_completion_candidates("delete wo:")
        delete_row_ids = shell.command_completion_candidates("delete works ")

    assert "stores" in use_table.candidates
    assert "works:" in row_table.candidates
    assert "stores" in show_table.candidates
    assert "items" in on_table.candidates
    assert "stores" in off_table.candidates
    assert "works" in link_left.candidates
    assert "stores" in link_right.candidates
    assert "stores:" in link_right_compact.candidates
    assert "stores" in links_other.candidates
    assert "name" in set_column_short.candidates
    assert "store_name" in set_column_full.candidates
    assert "root_uri" in edit_column.candidates
    assert "works:" in delete_row_ref.candidates
    assert delete_row_ids.candidates == ()


def test_text_browser_command_completion_candidates_cover_row_id_slots(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_row_id_completion.sqlite"

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_rows = [
            Row.from_idless_row_dict(
                db,
                row_dict={
                    "work_title": "Completion Work {}".format(idx),
                    "work_canonical_title": "Completion Work {}".format(idx),
                    "work_sort_title": "Completion Work {}".format(idx),
                },
                table="works",
            )
            for idx in range(1, 4)
        ]
        store_ids = [
            _insert_store_row(
                db,
                name="completion_store_{}".format(idx),
                kind="on_disk_existing_managed_drive",
                root_uri=str(tmp_path / "store_{}".format(idx)),
            )
            for idx in range(1, 3)
        ]

        shell = TextDatabaseBrowser(db)
        shell.execute_line("browse works 2 1")

        browse_window_ids = [str(work_rows[1]["work_id"]), str(work_rows[2]["work_id"])]
        store_id_texts = [str(one) for one in store_ids]

        row_ids = shell.command_completion_candidates("row works ")
        row_ref_ids = shell.command_completion_candidates("row works:")
        show_ids = shell.command_completion_candidates("show tag works ")
        on_ids = shell.command_completion_candidates("on tag works ")
        link_right_ids = shell.command_completion_candidates(
            "link works {} stores ".format(work_rows[0]["work_id"])
        )
        link_right_compact_ids = shell.command_completion_candidates(
            "link works:{} stores:".format(work_rows[0]["work_id"])
        )
        links_source_ids = shell.command_completion_candidates("links works ")

    assert row_ids.candidates == tuple(browse_window_ids)
    assert row_ref_ids.candidates == tuple("works:{}".format(one) for one in browse_window_ids)
    assert show_ids.candidates == tuple(browse_window_ids)
    assert on_ids.candidates == tuple(browse_window_ids)
    assert tuple(link_right_ids.candidates) == tuple(store_id_texts)
    assert tuple(link_right_compact_ids.candidates) == tuple("stores:{}".format(one) for one in store_id_texts)
    assert links_source_ids.candidates == tuple(browse_window_ids)


def test_text_browser_readline_mode_configures_command_completion(monkeypatch, driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_readline_completion.sqlite"
    prompts: list[str] = []

    class _FakeReadline:
        def __init__(self) -> None:
            self.binds: list[str] = []
            self.delims: list[str] = []
            self.completer = None
            self.line_buffer = ""
            self.endidx = 0

        def parse_and_bind(self, spec: str) -> None:
            self.binds.append(spec)

        def set_completer_delims(self, delims: str) -> None:
            self.delims.append(delims)

        def set_completer(self, completer) -> None:
            self.completer = completer

        def get_line_buffer(self) -> str:
            return self.line_buffer

        def get_endidx(self) -> int:
            return self.endidx

        def add_history(self, _line: str) -> None:
            return None

    fake_readline = _FakeReadline()
    monkeypatch.setattr(text_browser_module, "_readline", fake_readline, raising=False)

    def _fake_input(prompt: str) -> str:
        prompts.append(prompt)
        return "help add st"

    monkeypatch.setattr(builtins, "input", _fake_input)

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db)
        shell._can_use_readline_prompt = lambda: True  # type: ignore[method-assign]
        line = shell._read_command_line()

    fake_readline.line_buffer = "help add st"
    fake_readline.endidx = len(fake_readline.line_buffer)

    assert line == "help add st\n"
    assert prompts == ["liuxin-db> "]
    assert fake_readline.binds == ["tab: complete"]
    assert fake_readline.delims == [" \t\n"]
    assert fake_readline.completer is not None
    assert fake_readline.completer("st", 0) == "store"
    assert fake_readline.completer("st", 1) is None


def test_text_browser_history_loads_and_saves_for_interactive_readline(
    monkeypatch, driver_spec, tmp_path: Path
) -> None:
    db_path = tmp_path / "browser_history_interactive.sqlite"
    history_path = tmp_path / "history" / "liuxin_history.txt"
    history_path.parent.mkdir(parents=True, exist_ok=True)
    history_path.write_text("tables\n", encoding="utf-8")

    reads: list[str] = []
    writes: list[str] = []

    class _FakeReadline:
        def read_history_file(self, path: str) -> None:
            reads.append(path)

        def write_history_file(self, path: str) -> None:
            writes.append(path)

        def add_history(self, _line: str) -> None:
            return None

    monkeypatch.setattr(text_browser_module, "_readline", _FakeReadline(), raising=False)

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, history_file=history_path)
        shell._can_use_readline_prompt = lambda: True  # type: ignore[method-assign]
        shell.startup()
        shell.shutdown(reason="test")

    assert reads == [str(history_path)]
    assert writes == [str(history_path)]


def test_text_browser_history_not_used_without_interactive_readline(monkeypatch, driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_history_noninteractive.sqlite"
    history_path = tmp_path / "history" / "liuxin_history.txt"

    reads: list[str] = []
    writes: list[str] = []

    class _FakeReadline:
        def read_history_file(self, path: str) -> None:
            reads.append(path)

        def write_history_file(self, path: str) -> None:
            writes.append(path)

        def add_history(self, _line: str) -> None:
            return None

    monkeypatch.setattr(text_browser_module, "_readline", _FakeReadline(), raising=False)

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=io.StringIO(""), output=io.StringIO(), history_file=history_path)
        shell.startup()
        shell.shutdown(reason="test")

    assert reads == []
    assert writes == []


def test_text_browser_summary_invalid_args(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_summary_invalid.sqlite"
    output = io.StringIO()
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, page_size=5, output=output)
        with pytest.raises(ValueError):
            shell.execute_line("summary nope")


def test_text_browser_quit_command_aliases(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_quit.sqlite"
    output = io.StringIO()
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("quit") is False
        assert shell.execute_line("exit") is False
        assert shell.execute_line("q") is False


def test_text_browser_quit_command_rejects_args(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_quit_args.sqlite"
    output = io.StringIO()
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, output=output)
        with pytest.raises(ValueError):
            shell.execute_line("quit now")


class _RecorderLifecyclePlugin(TerminalLifecyclePluginAPI):
    name = "recorder"

    def __init__(self) -> None:
        self.started = 0
        self.stopped = 0
        self.shutdown_reasons: list[str] = []

    def on_startup(self, browser: TextDatabaseBrowser) -> None:
        self.started += 1

    def on_shutdown(self, browser: TextDatabaseBrowser, *, reason: str) -> None:
        self.stopped += 1
        self.shutdown_reasons.append(reason)


class _FailingShutdownPlugin(TerminalLifecyclePluginAPI):
    name = "failing_shutdown"

    def __init__(self) -> None:
        self.started = 0
        self.stopped = 0

    def on_startup(self, browser: TextDatabaseBrowser) -> None:
        self.started += 1

    def on_shutdown(self, browser: TextDatabaseBrowser, *, reason: str) -> None:
        self.stopped += 1
        raise RuntimeError("boom")


def test_text_browser_lifecycle_plugins_run_on_quit(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_lifecycle.sqlite"
    output = io.StringIO()
    plugin = _RecorderLifecyclePlugin()
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, output=output, lifecycle_plugins=[plugin])
        rc = shell.run_commands(["summary 1", "quit"])
        assert rc == 0

    assert plugin.started == 1
    assert plugin.stopped == 1
    assert plugin.shutdown_reasons == ["command:quit"]


def test_text_browser_lifecycle_shutdown_runs_all_plugins(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_lifecycle_shutdown.sqlite"
    output = io.StringIO()
    recorder = _RecorderLifecyclePlugin()
    failing = _FailingShutdownPlugin()
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(
            db,
            output=output,
            lifecycle_plugins=[recorder, failing],
        )
        rc = shell.run_commands(["quit"])
        assert rc == 0

    # startup called for both
    assert recorder.started == 1
    assert failing.started == 1
    # shutdown still called for both even when one fails
    assert recorder.stopped == 1
    assert failing.stopped == 1
    rendered = output.getvalue()
    assert "WARNING: lifecycle shutdown errors:" in rendered
    assert "failing_shutdown" in rendered


def test_text_browser_new_store_wizard_creates_row(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_store.sqlite"
    output = io.StringIO()
    store_dir = tmp_path / "managed_store"
    assert not store_dir.exists()

    # kind(default=1), root path, create dir yes, name default, read-only default(no),
    # online default(yes), refresh storage manager no
    input_stream = io.StringIO(
        "\n".join(
            [
                "",
                str(store_dir),
                "y",
                "",
                "",
                "",
                "n",
            ]
        )
        + "\n"
    )

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        rc = shell.run_commands(["add store"])
        assert rc == 0

        rows = db.search("stores", "store_root_uri", str(store_dir.resolve()))
        assert len(rows) == 1
        row = rows[0]
        assert row["store_kind"] == "on_disk_existing_managed_drive"
        assert int(row["store_is_read_only"]) == 0
        assert row["store_online_status"] == "online"

    rendered = output.getvalue()
    assert "New store wizard" in rendered
    assert "wget_html_readonly" in rendered
    assert "Store saved:" in rendered


def test_text_browser_new_store_wizard_updates_existing_row(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_store_update.sqlite"
    output = io.StringIO()
    store_dir = tmp_path / "existing_store"
    store_dir.mkdir(parents=True, exist_ok=True)

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        _insert_store_row(
            db,
            name="existing-store",
            kind="on_disk_existing_managed_drive",
            root_uri=str(store_dir.resolve()),
            is_read_only=0,
        )

        # kind=2 unmanaged, root path, name default, read-only default(yes), online default(yes),
        # update existing yes (default), refresh no
        input_stream = io.StringIO(
            "\n".join(
                [
                    "2",
                    str(store_dir),
                    "",
                    "",
                    "",
                    "",
                    "n",
                ]
            )
            + "\n"
        )

        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        rc = shell.run_commands(["add store"])
        assert rc == 0

        rows = db.search("stores", "store_root_uri", str(store_dir.resolve()))
        assert len(rows) == 1
        row = rows[0]
        assert row["store_kind"] == "on_disk_existing_unmanaged_drive"
        assert int(row["store_is_read_only"]) == 1


def test_text_browser_new_store_wizard_refresh_routes_via_core(driver_spec, tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "browser_new_store_core_refresh.sqlite"
    output = io.StringIO()
    store_dir = tmp_path / "core_refresh_store"
    query_calls: list[tuple[str, dict[str, object] | None]] = []
    command_calls: list[tuple[str, dict[str, object] | None]] = []

    input_stream = io.StringIO(
        "\n".join(
            [
                "",
                str(store_dir),
                "y",
                "",
                "",
                "",
                "",
            ]
        )
        + "\n"
    )

    def _unexpected_local_bootstrap(*args, **kwargs):
        del args, kwargs
        raise AssertionError("terminal should route store refresh via core when available")

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        monkeypatch.setattr(type(db), "bootstrap_storage_manager", _unexpected_local_bootstrap)

        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        monkeypatch.setattr(shell, "supports_core_queries", lambda: True)
        monkeypatch.setattr(shell, "supports_core_commands", lambda: True)

        def _record_query(name: str, *, payload=None):
            payload_dict = dict(payload or {})
            query_calls.append((str(name), payload_dict))
            return None

        def _record_command(name: str, *, payload=None):
            payload_dict = dict(payload or {})
            command_calls.append((str(name), payload_dict))
            method = str(payload_dict.get("method", ""))
            if method == "save_store_row":
                from LiuXin_alpha.library.library import Library

                return Library(database=db, close_database_on_close=False).save_store_row(
                    store_payload=payload_dict["kwargs"]["store_payload"]
                )
            return {
                "discovered_rows": 1,
                "loaded_stores": 1,
                "skipped_rows": 0,
                "failed_rows": 0,
            }

        monkeypatch.setattr(shell, "execute_core_query", _record_query)
        monkeypatch.setattr(shell, "execute_core_command", _record_command)

        rc = shell.run_commands(["add store"])
        assert rc == 0

        rows = db.search("stores", "store_root_uri", str(store_dir.resolve()))
        assert len(rows) == 1

    assert len(query_calls) == 1
    assert query_calls[0][0] == "invoke"
    assert query_calls[0][1]["target"] == "library"
    assert query_calls[0][1]["method"] == "find_existing_store"
    assert query_calls[0][1]["kwargs"]["root_uri"] == str(store_dir.resolve())
    assert len(command_calls) == 2
    assert command_calls[0][0] == "invoke"
    assert command_calls[0][1]["target"] == "library"
    assert command_calls[0][1]["method"] == "save_store_row"
    assert (
        query_calls[0][1]["kwargs"]["store_name"]
        == command_calls[0][1]["kwargs"]["store_payload"]["store_name"]
    )
    assert command_calls[0][1]["kwargs"]["store_payload"]["store_root_uri"] == str(store_dir.resolve())
    assert command_calls[1] == (
        "invoke",
        {
            "target": "library",
            "method": "refresh_storage",
            "kwargs": {"clear_existing": True},
        },
    )
    rendered = output.getvalue()
    assert "Store saved:" in rendered
    assert "Storage bootstrap: discovered=1 loaded=1 skipped=0 failed=0" in rendered


def test_text_browser_new_store_wizard_refresh_works_with_default_core_runtime(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_store_default_core_refresh.sqlite"
    output = io.StringIO()
    store_dir = tmp_path / "default_core_refresh_store"

    input_stream = io.StringIO(
        "\n".join(
            [
                "",
                str(store_dir),
                "y",
                "",
                "",
                "",
                "",
            ]
        )
        + "\n"
    )

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        rc = shell.run_commands(["add store"])
        assert rc == 0

        rows = db.search("stores", "store_root_uri", str(store_dir.resolve()))
        assert len(rows) == 1
        report = getattr(db, "storage_bootstrap_report", None)
        assert report is not None
        assert report.discovered_rows >= 1
        assert report.loaded_stores >= 1

    rendered = output.getvalue()
    assert "Storage bootstrap: discovered=" in rendered


def test_text_browser_new_store_wizard_rejects_args(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_store_args.sqlite"
    output = io.StringIO()
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, output=output)
        with pytest.raises(ValueError):
            shell.execute_line("add store now")


def test_text_browser_add_group_lists_subcommands(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_add_group.sqlite"
    output = io.StringIO()
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("add")
    rendered = output.getvalue()
    assert "Available `add` subcommands:" in rendered
    assert "add store" in rendered
    assert "add creator" in rendered
    assert "add expression" in rendered
    assert "add genre" in rendered
    assert "add item" in rendered
    assert "add note" in rendered
    assert "add organisation" in rendered
    assert "add publisher" in rendered
    assert "add series" in rendered
    assert "add subject" in rendered
    assert "add tag" in rendered
    assert "add title" in rendered
    assert "add work" in rendered
    assert "add manifestation" in rendered


def test_text_browser_new_group_alias_lists_add_subcommands(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_group_alias.sqlite"
    output = io.StringIO()
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("new")
    rendered = output.getvalue()
    assert "Available `add` subcommands:" in rendered
    assert "add series" in rendered
    assert "add work" in rendered


def test_text_browser_on_group_lists_subcommands(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_on_group.sqlite"
    output = io.StringIO()
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("on")
    rendered = output.getvalue()
    assert "Available `on` subcommands:" in rendered
    assert "on note" in rendered
    assert "on tag" in rendered
    assert "on genre" in rendered
    assert "on subject" in rendered
    assert "on language" in rendered
    assert "on series" in rendered


def test_text_browser_off_group_lists_subcommands(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_off_group.sqlite"
    output = io.StringIO()
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("off")
    rendered = output.getvalue()
    assert "Available `off` subcommands:" in rendered
    assert "off note" in rendered
    assert "off tag" in rendered
    assert "off genre" in rendered
    assert "off subject" in rendered
    assert "off language" in rendered
    assert "off series" in rendered


def test_text_browser_show_group_lists_subcommands(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_show_group.sqlite"
    output = io.StringIO()
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("show")
    rendered = output.getvalue()
    assert "Available `show` subcommands:" in rendered
    assert "show tags" in rendered
    assert "show notes" in rendered
    assert "show genres" in rendered
    assert "show subjects" in rendered
    assert "show language" in rendered
    assert "show series" in rendered
    assert "show all" in rendered


def test_text_browser_ingest_group_lists_subcommands(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_ingest_group.sqlite"
    output = io.StringIO()
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("ingest")
    rendered = output.getvalue()
    assert "Available `ingest` subcommands:" in rendered
    assert "ingest disk <path>" in rendered


def test_text_browser_ingest_disk_registers_ebook_files(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_ingest_disk.sqlite"
    output = io.StringIO()
    ingest_root = tmp_path / "ingest_root"
    (ingest_root / "nested").mkdir(parents=True, exist_ok=True)
    (ingest_root / "book1.epub").write_bytes(b"epub-one")
    (ingest_root / "nested" / "book2.mobi").write_bytes(b"mobi-two")
    (ingest_root / "ignore.bin").write_bytes(b"not-an-ebook")

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        ensure_surface_asset_tables(db, include_file_store_links=True)
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.run_commands(
            [
                'ingest disk "{}" --no-hash --no-refresh'.format(str(ingest_root)),
            ]
        ) == 0

        store_rows = db.search("stores", "store_root_uri", str(ingest_root.resolve()))
        assert len(store_rows) == 1
        store_id = int(store_rows[0]["store_id"])

        file_rows = db.search("files", "file_store_id", store_id)
        assert len(file_rows) == 2
        keys = {str(row["file_storage_key"]) for row in file_rows}
        assert "book1.epub" in keys
        assert "nested/book2.mobi" in keys
        assert "ignore.bin" not in keys

    rendered = output.getvalue()
    assert "Ingest completed:" in rendered
    assert "Results" in rendered
    assert "inserted_files" in rendered


def test_text_browser_ingest_disk_respects_extensions_filter(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_ingest_disk_extensions.sqlite"
    output = io.StringIO()
    ingest_root = tmp_path / "ingest_extensions_root"
    ingest_root.mkdir(parents=True, exist_ok=True)
    (ingest_root / "only.epub").write_bytes(b"epub")
    (ingest_root / "this.pdf").write_bytes(b"pdf")
    (ingest_root / "and.fb2").write_bytes(b"fb2")

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        ensure_surface_asset_tables(db, include_file_store_links=True)
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.run_commands(
            [
                'ingest disk "{}" --extensions pdf --no-hash --no-refresh'.format(str(ingest_root)),
            ]
        ) == 0

        store_rows = db.search("stores", "store_root_uri", str(ingest_root.resolve()))
        assert len(store_rows) == 1
        store_id = int(store_rows[0]["store_id"])

        file_rows = db.search("files", "file_store_id", store_id)
        assert len(file_rows) == 1
        assert str(file_rows[0]["file_storage_key"]) == "this.pdf"


def test_text_browser_sync_group_lists_subcommands(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_sync_group.sqlite"
    output = io.StringIO()
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("sync")
    rendered = output.getvalue()
    assert "Available `sync` subcommands:" in rendered
    assert "sync store <store_id|store_name> [to-db]" in rendered


def test_text_browser_sync_store_by_id_registers_ebook_files(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_sync_store_by_id.sqlite"
    output = io.StringIO()
    sync_root = tmp_path / "sync_root"
    (sync_root / "nested").mkdir(parents=True, exist_ok=True)
    (sync_root / "book1.epub").write_bytes(b"epub-one")
    (sync_root / "nested" / "book2.mobi").write_bytes(b"mobi-two")
    (sync_root / "ignore.bin").write_bytes(b"not-an-ebook")

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_id = _insert_store_row(
            db,
            name="sync-by-id-store",
            kind="on_disk_existing_unmanaged_drive",
            root_uri=str(sync_root.resolve()),
            is_read_only=1,
        )

        shell = TextDatabaseBrowser(db, output=output)
        assert shell.run_commands(
            [
                "sync store {} to-db --no-hash --no-refresh".format(store_id),
            ]
        ) == 0

        file_rows = db.search("files", "file_store_id", store_id)
        assert len(file_rows) == 2
        keys = {str(row["file_storage_key"]) for row in file_rows}
        assert "book1.epub" in keys
        assert "nested/book2.mobi" in keys
        assert "ignore.bin" not in keys

    rendered = output.getvalue()
    assert "Sync completed:" in rendered
    assert "Results" in rendered
    assert "inserted_files" in rendered


def test_text_browser_sync_store_compact_subcommand_ref(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_sync_store_compact_subcommand_ref.sqlite"
    output = io.StringIO()
    sync_root = tmp_path / "sync_compact_ref_root"
    sync_root.mkdir(parents=True, exist_ok=True)
    (sync_root / "book.epub").write_bytes(b"epub")

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_id = _insert_store_row(
            db,
            name="sync-compact-store",
            kind="on_disk_existing_unmanaged_drive",
            root_uri=str(sync_root.resolve()),
            is_read_only=1,
        )

        shell = TextDatabaseBrowser(db, output=output)
        assert shell.run_commands(
            [
                "sync store:{} --no-hash --no-refresh".format(store_id),
            ]
        ) == 0

        file_rows = db.search("files", "file_store_id", store_id)
        assert len(file_rows) == 1
        assert str(file_rows[0]["file_storage_key"]) == "book.epub"

    rendered = output.getvalue()
    assert "Sync completed:" in rendered


def test_text_browser_sync_store_by_name_respects_extensions_filter(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_sync_store_by_name.sqlite"
    output = io.StringIO()
    sync_root = tmp_path / "sync_by_name_root"
    sync_root.mkdir(parents=True, exist_ok=True)
    (sync_root / "first.epub").write_bytes(b"epub")
    (sync_root / "second.pdf").write_bytes(b"pdf")
    (sync_root / "third.fb2").write_bytes(b"fb2")

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_id = _insert_store_row(
            db,
            name="sync-by-name-store",
            kind="on_disk_existing_unmanaged_drive",
            root_uri=str(sync_root.resolve()),
            is_read_only=1,
        )

        shell = TextDatabaseBrowser(db, output=output)
        assert shell.run_commands(
            [
                'sync store "sync-by-name-store" to-db --extensions pdf --no-hash --no-refresh',
            ]
        ) == 0

        file_rows = db.search("files", "file_store_id", store_id)
        assert len(file_rows) == 1
        assert str(file_rows[0]["file_storage_key"]) == "second.pdf"


def test_text_browser_sync_store_no_progress_suppresses_progress_lines(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_sync_store_no_progress.sqlite"
    output = io.StringIO()
    sync_root = tmp_path / "sync_no_progress_root"
    sync_root.mkdir(parents=True, exist_ok=True)
    (sync_root / "first.epub").write_bytes(b"epub")

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_id = _insert_store_row(
            db,
            name="sync-no-progress-store",
            kind="on_disk_existing_unmanaged_drive",
            root_uri=str(sync_root.resolve()),
            is_read_only=1,
        )

        shell = TextDatabaseBrowser(db, output=output)
        assert shell.run_commands(
            [
                "sync store {} --no-hash --no-refresh --no-progress".format(store_id),
            ]
        ) == 0

    rendered = output.getvalue()
    assert "Sync completed:" in rendered
    assert "Sync started:" not in rendered
    assert "Sync progress:" not in rendered


def test_text_browser_sync_store_json_output_disables_progress_lines(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_sync_store_json_output.sqlite"
    output = io.StringIO()
    sync_root = tmp_path / "sync_json_root"
    sync_root.mkdir(parents=True, exist_ok=True)
    (sync_root / "first.epub").write_bytes(b"epub")

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_id = _insert_store_row(
            db,
            name="sync-json-store",
            kind="on_disk_existing_unmanaged_drive",
            root_uri=str(sync_root.resolve()),
            is_read_only=1,
        )

        shell = TextDatabaseBrowser(db, output=output)
        assert shell.run_commands(
            [
                "sync store {} --no-hash --no-refresh --json".format(store_id),
            ]
        ) == 0

    rendered = output.getvalue().strip()
    assert rendered.startswith("{")
    assert '"store_row_id"' in rendered
    assert "Sync started:" not in rendered
    assert "Sync progress:" not in rendered


def test_text_browser_sync_store_background_submits_job(driver_spec, tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "browser_sync_store_background.sqlite"
    output = io.StringIO()
    sync_root = tmp_path / "sync_background_root"
    sync_root.mkdir(parents=True, exist_ok=True)
    (sync_root / "first.epub").write_bytes(b"epub")

    manager = InMemoryJobManager(max_workers=1, default_backend="serial")
    captured_kwargs: dict[str, object] = {}

    def _fake_run_sync_store_job(**kwargs):
        captured_kwargs.update(kwargs)
        return {"store_row_id": 1, "inserted_files": 1, "errors": []}

    monkeypatch.setattr(sync_command_module, "run_sync_store_job", _fake_run_sync_store_job)

    try:
        with Database(
            metadata={"database_path": str(db_path)},
            db_type=driver_spec.db_type,
            create=True,
            backup=False,
            storage_startup_on_add=False,
        ) as db:
            store_id = _insert_store_row(
                db,
                name="sync-background-store",
                kind="on_disk_existing_unmanaged_drive",
                root_uri=str(sync_root.resolve()),
                is_read_only=1,
            )

            shell = TextDatabaseBrowser(db, output=output, job_manager=manager)
            assert shell.execute_line(
                "sync store {} --background --job-backend serial --job-timeout-s 5 --job-no-output".format(store_id)
            )

            jobs = manager.list()
            assert len(jobs) == 1
            info = manager.wait(jobs[0].job_id, timeout=2.0)
            assert info.state == "succeeded"
    finally:
        manager.shutdown(wait=True, cancel_pending=True)

    rendered = output.getvalue()
    assert "Sync job submitted:" in rendered
    assert "Use `jobs show " in rendered
    assert captured_kwargs.get("mode") == "local"
    assert captured_kwargs.get("database_path") == str(db_path)
    assert captured_kwargs.get("db_type") == driver_spec.db_type
    assert captured_kwargs.get("crawler_incremental_db_writes") is True


def test_text_browser_sync_store_background_job_panel_attaches(driver_spec, tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "browser_sync_store_background_panel.sqlite"
    output = io.StringIO()
    sync_root = tmp_path / "sync_background_panel_root"
    sync_root.mkdir(parents=True, exist_ok=True)
    (sync_root / "first.epub").write_bytes(b"epub")

    manager = InMemoryJobManager(max_workers=1, default_backend="serial")
    captured_kwargs: dict[str, object] = {}

    def _fake_run_sync_store_job(**kwargs):
        captured_kwargs.update(kwargs)
        return {"store_row_id": 1, "inserted_files": 1, "errors": []}

    monkeypatch.setattr(sync_command_module, "run_sync_store_job", _fake_run_sync_store_job)

    try:
        with Database(
            metadata={"database_path": str(db_path)},
            db_type=driver_spec.db_type,
            create=True,
            backup=False,
            storage_startup_on_add=False,
        ) as db:
            store_id = _insert_store_row(
                db,
                name="sync-background-panel-store",
                kind="on_disk_existing_unmanaged_drive",
                root_uri=str(sync_root.resolve()),
                is_read_only=1,
            )

            shell = _PanelAwareBrowser(db, output=output, job_manager=manager)
            assert shell.execute_line(
                "sync store {} --background --job-backend serial --job-timeout-s 5 --job-panel".format(store_id)
            )

            jobs = manager.list()
            assert len(jobs) == 1
            info = manager.wait(jobs[0].job_id, timeout=2.0)
            assert info.state == "succeeded"
            assert shell.panel_job_id == jobs[0].job_id
    finally:
        manager.shutdown(wait=True, cancel_pending=True)

    rendered = output.getvalue()
    assert "output_panel" in rendered
    assert "attached to job" in rendered
    assert captured_kwargs.get("mode") == "local"


def test_text_browser_telemetry_panel_attaches_and_detaches(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_telemetry_panel.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        _insert_store_row(
            db,
            name="telemetry-store",
            kind="on_disk_existing_unmanaged_drive",
            root_uri=str(tmp_path / "telemetry-root"),
            is_read_only=1,
        )

        shell = _PanelAwareBrowser(db, output=output)
        assert shell.execute_line("telemetry panel on files folders")
        assert shell.telemetry_tables == ("files", "folders")
        assert shell.execute_line("telemetry panel off")
        assert shell.telemetry_tables is None

    rendered = output.getvalue()
    assert "Telemetry panel attached." in rendered
    assert "files,folders" in rendered or "files, folders" in rendered
    assert "Telemetry panel detached." in rendered


def test_text_browser_sync_store_background_forwards_wget_incremental_flag(driver_spec, tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "browser_sync_store_background_wget_incremental_flag.sqlite"
    output = io.StringIO()
    sync_root = tmp_path / "sync_background_wget_incremental_flag_root"
    sync_root.mkdir(parents=True, exist_ok=True)
    (sync_root / "first.epub").write_bytes(b"epub")

    manager = InMemoryJobManager(max_workers=1, default_backend="serial")
    captured_kwargs: dict[str, object] = {}

    def _fake_run_sync_store_job(**kwargs):
        captured_kwargs.update(kwargs)
        return {"store_row_id": 1, "inserted_files": 1, "errors": []}

    monkeypatch.setattr(sync_command_module, "run_sync_store_job", _fake_run_sync_store_job)

    try:
        with Database(
            metadata={"database_path": str(db_path)},
            db_type=driver_spec.db_type,
            create=True,
            backup=False,
            storage_startup_on_add=False,
        ) as db:
            store_id = _insert_store_row(
                db,
                name="sync-background-wget-incremental-flag-store",
                kind="on_disk_existing_unmanaged_drive",
                root_uri=str(sync_root.resolve()),
                is_read_only=1,
            )

            shell = TextDatabaseBrowser(db, output=output, job_manager=manager)
            assert shell.execute_line(
                "sync store {} --background --job-backend serial --crawler-no-incremental-db-writes".format(
                    store_id
                )
            )
            jobs = manager.list()
            assert len(jobs) == 1
            info = manager.wait(jobs[0].job_id, timeout=2.0)
            assert info.state == "succeeded"
    finally:
        manager.shutdown(wait=True, cancel_pending=True)

    assert captured_kwargs.get("crawler_incremental_db_writes") is False


def test_text_browser_sync_store_background_rejects_json_mode(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_sync_store_background_json.sqlite"
    output = io.StringIO()
    sync_root = tmp_path / "sync_background_json_root"
    sync_root.mkdir(parents=True, exist_ok=True)

    manager = InMemoryJobManager(max_workers=1, default_backend="serial")
    try:
        with Database(
            metadata={"database_path": str(db_path)},
            db_type=driver_spec.db_type,
            create=True,
            backup=False,
            storage_startup_on_add=False,
        ) as db:
            store_id = _insert_store_row(
                db,
                name="sync-background-json-store",
                kind="on_disk_existing_unmanaged_drive",
                root_uri=str(sync_root.resolve()),
                is_read_only=1,
            )

            shell = TextDatabaseBrowser(db, output=output, job_manager=manager)
            with pytest.raises(ValueError):
                shell.execute_line("sync store {} --background --json".format(store_id))
    finally:
        manager.shutdown(wait=True, cancel_pending=True)


def test_text_browser_sync_store_job_panel_requires_background(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_sync_store_job_panel_requires_background.sqlite"
    output = io.StringIO()
    sync_root = tmp_path / "sync_job_panel_requires_background_root"
    sync_root.mkdir(parents=True, exist_ok=True)
    (sync_root / "first.epub").write_bytes(b"epub")

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_id = _insert_store_row(
            db,
            name="sync-job-panel-store",
            kind="on_disk_existing_unmanaged_drive",
            root_uri=str(sync_root.resolve()),
            is_read_only=1,
        )
        shell = TextDatabaseBrowser(db, output=output)
        with pytest.raises(ValueError):
            shell.execute_line("sync store {} --job-panel --no-refresh".format(store_id))


def test_text_browser_sync_store_background_core_requires_job_id(driver_spec, tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "browser_sync_store_background_core_job_id.sqlite"
    output = io.StringIO()
    sync_root = tmp_path / "sync_background_core_job_id_root"
    sync_root.mkdir(parents=True, exist_ok=True)
    (sync_root / "first.epub").write_bytes(b"epub")

    manager = InMemoryJobManager(max_workers=1, default_backend="serial")
    try:
        with Database(
            metadata={"database_path": str(db_path)},
            db_type=driver_spec.db_type,
            create=True,
            backup=False,
            storage_startup_on_add=False,
        ) as db:
            store_id = _insert_store_row(
                db,
                name="sync-background-core-job-id-store",
                kind="on_disk_existing_unmanaged_drive",
                root_uri=str(sync_root.resolve()),
                is_read_only=1,
            )

            shell = TextDatabaseBrowser(db, output=output, job_manager=manager)
            monkeypatch.setattr(shell, "supports_core_commands", lambda: True)
            monkeypatch.setattr(shell, "execute_core_command", lambda name, payload=None: {})

            with pytest.raises(RuntimeError, match="did not return a job id"):
                shell.execute_line("sync store {} --background --job-backend serial".format(store_id))

            assert manager.list() == []
    finally:
        manager.shutdown(wait=True, cancel_pending=True)


def test_text_browser_sync_store_rclone_uses_rate_limit_option(driver_spec, tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "browser_sync_store_rclone.sqlite"
    output = io.StringIO()
    captured_extra_args: list[tuple[str, ...]] = []

    def _fake_run_rclone_json(args, **kwargs):
        captured_extra_args.append(tuple(kwargs.get("extra_args", ())))
        if list(args[:3]) == ["lsjson", "-R", "--files-only"]:
            return [{"Path": "books/one.epub", "Name": "one.epub", "Size": 11, "ModTime": "2025-01-02T03:04:05Z"}]
        return []

    monkeypatch.setattr(rclone_backend_module, "run_rclone_json", _fake_run_rclone_json)

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_id = _insert_store_row(
            db,
            name="sync-rclone-store",
            kind="rclone_http_readonly",
            root_uri="remote:",
            access_protocol="rclone",
            is_read_only=1,
        )

        shell = TextDatabaseBrowser(db, output=output)
        assert shell.run_commands(
            [
                "sync store {} to-db --max-http-requests-per-hour 10 --no-refresh".format(store_id),
            ]
        ) == 0

        file_rows = db.search("files", "file_store_id", store_id)
        assert len(file_rows) == 1
        assert str(file_rows[0]["file_storage_key"]) == "books/one.epub"

    assert captured_extra_args
    tps_args = [arg for arg in captured_extra_args[0] if arg.startswith("--tpslimit=")]
    assert tps_args
    assert "0.00277778" in tps_args[0]


def test_text_browser_sync_store_rclone_listing_flags_are_forwarded(driver_spec, tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "browser_sync_store_rclone_listing_flags.sqlite"
    output = io.StringIO()
    captured_extra_args: list[tuple[str, ...]] = []

    def _fake_run_rclone_json(args, **kwargs):
        captured_extra_args.append(tuple(kwargs.get("extra_args", ())))
        if list(args[:3]) == ["lsjson", "-R", "--files-only"]:
            return [{"Path": "books/two.epub", "Name": "two.epub", "Size": 12, "ModTime": "2025-01-02T03:04:05Z"}]
        return []

    monkeypatch.setattr(rclone_backend_module, "run_rclone_json", _fake_run_rclone_json)

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_id = _insert_store_row(
            db,
            name="sync-rclone-flags-store",
            kind="rclone_http_readonly",
            root_uri="remote:",
            access_protocol="rclone",
            is_read_only=1,
        )

        shell = TextDatabaseBrowser(db, output=output)
        assert shell.run_commands(
            [
                "sync store {} --rclone-http-no-slash --rclone-http-no-head --no-refresh".format(store_id),
            ]
        ) == 0

    assert captured_extra_args
    extra_args = captured_extra_args[0]
    assert "--http-no-slash" in extra_args
    assert "--http-no-head" in extra_args


def test_text_browser_sync_store_rclone_plain_https_root_is_supported(driver_spec, tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "browser_sync_store_rclone_https.sqlite"
    output = io.StringIO()
    captured_roots: list[str] = []

    def _fake_run_rclone_json(args, **kwargs):
        if len(args) >= 4 and list(args[:3]) == ["lsjson", "-R", "--files-only"]:
            captured_roots.append(str(args[3]))
            return [{"Path": "books/plain.epub", "Name": "plain.epub", "Size": 7, "ModTime": "2025-01-02T03:04:05Z"}]
        return []

    monkeypatch.setattr(rclone_backend_module, "run_rclone_json", _fake_run_rclone_json)

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_id = _insert_store_row(
            db,
            name="sync-rclone-https-store",
            kind="rclone_http_readonly",
            root_uri="https://www.fadedpage.com/",
            access_protocol="https",
            is_read_only=1,
        )

        shell = TextDatabaseBrowser(db, output=output)
        assert shell.run_commands(
            [
                "sync store:{} --no-refresh".format(store_id),
            ]
        ) == 0

        file_rows = db.search("files", "file_store_id", store_id)
        assert len(file_rows) == 1
        assert str(file_rows[0]["file_storage_key"]) == "books/plain.epub"

    assert captured_roots
    assert captured_roots[0] == ':http,url="https://www.fadedpage.com":'


def test_text_browser_sync_store_wget_uses_rate_limit_option(driver_spec, tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "browser_sync_store_wget.sqlite"
    output = io.StringIO()
    captured_wget_args: list[list[str]] = []
    captured_timeout_s: list[object] = []

    def _fake_run_wget(args, **kwargs):
        captured_wget_args.append(list(args))
        captured_timeout_s.append(kwargs.get("timeout_s"))
        callback = kwargs.get("line_callback")
        if callable(callback):
            callback("Spider mode enabled")
        listing = "https://www.fadedpage.com/books/one.epub\n"
        return WgetResult(args=list(args), returncode=0, stdout=listing, stderr="")

    monkeypatch.setattr(wget_backend_module, "run_wget", _fake_run_wget)

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_id = _insert_store_row(
            db,
            name="sync-wget-store",
            kind="wget_html_readonly",
            root_uri="https://www.fadedpage.com/",
            access_protocol="wget",
            is_read_only=1,
        )

        shell = TextDatabaseBrowser(db, output=output)
        assert shell.run_commands(
            [
                "sync store {} to-db --max-http-requests-per-hour 30 --no-refresh".format(store_id),
            ]
        ) == 0

        file_rows = db.search("files", "file_store_id", store_id)
        assert len(file_rows) == 1
        assert str(file_rows[0]["file_storage_key"]) == "books/one.epub"

    assert captured_wget_args
    assert "--wait=120.000" in captured_wget_args[0]
    assert "--no-verbose" not in captured_wget_args[0]
    assert captured_timeout_s and captured_timeout_s[0] is None
    rendered = output.getvalue()
    assert "Wget: Spider mode enabled" in rendered
    assert "store_supports_checksums" in rendered
    assert "no" in rendered


def test_text_browser_sync_store_wget_kind_takes_precedence_over_https_protocol(
    driver_spec, tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "browser_sync_store_wget_kind_precedence.sqlite"
    output = io.StringIO()
    captured_wget_calls = {"count": 0}

    def _fake_run_wget(args, **kwargs):
        captured_wget_calls["count"] += 1
        listing = "https://example.com/books/one.epub\n"
        return WgetResult(args=list(args), returncode=0, stdout=listing, stderr="")

    def _fail_run_rclone_json(args, **kwargs):
        raise AssertionError("rclone should not be used for wget_html_readonly store kind")

    monkeypatch.setattr(wget_backend_module, "run_wget", _fake_run_wget)
    monkeypatch.setattr(rclone_backend_module, "run_rclone_json", _fail_run_rclone_json)

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_id = _insert_store_row(
            db,
            name="sync-wget-kind-wins",
            kind="wget_html_readonly",
            root_uri="https://example.com/",
            access_protocol="https",
            is_read_only=1,
        )

        shell = TextDatabaseBrowser(db, output=output)
        assert shell.run_commands(["sync store:{} --no-refresh".format(store_id)]) == 0

    assert captured_wget_calls["count"] >= 1


def test_text_browser_sync_store_wget_listing_flags_are_forwarded(driver_spec, tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "browser_sync_store_wget_listing_flags.sqlite"
    output = io.StringIO()
    captured_args: list[list[str]] = []
    captured_extra_args: list[tuple[str, ...]] = []

    def _fake_run_wget(args, **kwargs):
        captured_args.append(list(args))
        captured_extra_args.append(tuple(kwargs.get("extra_args", ())))
        listing = "https://example.com/books/two.epub\n"
        return WgetResult(args=list(args), returncode=0, stdout=listing, stderr="")

    monkeypatch.setattr(wget_backend_module, "run_wget", _fake_run_wget)

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_id = _insert_store_row(
            db,
            name="sync-wget-flags-store",
            kind="wget_html_readonly",
            root_uri="https://example.com/base/",
            access_protocol="wget",
            is_read_only=1,
        )

        shell = TextDatabaseBrowser(db, output=output)
        assert shell.run_commands(
            [
                "sync store {} --crawler-max-depth 2 --crawler-parent --crawler-span-hosts "
                "--crawler-ignore-robots --crawler-user-agent 'LiuXinTest/1.0' --wget-verbose "
                "--wget-arg=--timeout=5 --no-refresh".format(store_id),
            ]
        ) == 0

    assert captured_args
    sync_call = captured_args[0]
    assert "--recursive" in sync_call
    assert "--level=2" in sync_call
    assert "--span-hosts" in sync_call
    assert "--execute=robots=off" in sync_call
    assert "--user-agent=LiuXinTest/1.0" in sync_call
    assert "--no-parent" not in sync_call
    assert "--no-verbose" not in sync_call

    assert captured_extra_args
    assert "--timeout=5" in captured_extra_args[0]


def test_text_browser_sync_store_wget_no_verbose_flag_is_forwarded(driver_spec, tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "browser_sync_store_wget_no_verbose.sqlite"
    output = io.StringIO()
    captured_args: list[list[str]] = []

    def _fake_run_wget(args, **kwargs):
        captured_args.append(list(args))
        listing = "https://example.com/books/noisy.epub\n"
        return WgetResult(args=list(args), returncode=0, stdout=listing, stderr="")

    monkeypatch.setattr(wget_backend_module, "run_wget", _fake_run_wget)

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_id = _insert_store_row(
            db,
            name="sync-wget-no-verbose-store",
            kind="wget_html_readonly",
            root_uri="https://example.com/",
            access_protocol="wget",
            is_read_only=1,
        )

        shell = TextDatabaseBrowser(db, output=output)
        assert shell.run_commands(["sync store {} --wget-no-verbose --no-refresh".format(store_id)]) == 0

    assert captured_args
    assert "--no-verbose" in captured_args[0]


def test_text_browser_sync_store_wget_timeout_option_is_forwarded(driver_spec, tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "browser_sync_store_wget_timeout.sqlite"
    output = io.StringIO()
    captured_timeout_s: list[object] = []

    def _fake_run_wget(args, **kwargs):
        captured_timeout_s.append(kwargs.get("timeout_s"))
        listing = "https://example.com/books/slow.epub\n"
        return WgetResult(args=list(args), returncode=0, stdout=listing, stderr="")

    monkeypatch.setattr(wget_backend_module, "run_wget", _fake_run_wget)

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_id = _insert_store_row(
            db,
            name="sync-wget-timeout-store",
            kind="wget_html_readonly",
            root_uri="https://example.com/",
            access_protocol="wget",
            is_read_only=1,
        )

        shell = TextDatabaseBrowser(db, output=output)
        assert shell.run_commands(["sync store {} --crawler-timeout-s 1200 --no-refresh".format(store_id)]) == 0

    assert captured_timeout_s
    assert captured_timeout_s[0] == 1200.0


def test_text_browser_sync_store_native_html_routes_via_native_backend(driver_spec, tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "browser_sync_store_native_html.sqlite"
    output = io.StringIO()

    def _html_result(url: str, body: str) -> object:
        return native_html_backend_module._FetchResult(
            requested_url=url,
            final_url=url,
            status=200,
            content_type="text/html; charset=utf-8",
            body=body.encode("utf-8"),
            charset="utf-8",
        )

    responses = {
        "https://example.com/library/": _html_result(
            "https://example.com/library/",
            "<html><body><a href=\"files/one.epub\">One</a><a href=\"catalog/next\">Next</a></body></html>",
        ),
        "https://example.com/library/catalog/next": _html_result(
            "https://example.com/library/catalog/next",
            "<html><body><a href=\"../files/two.mobi\">Two</a></body></html>",
        ),
    }

    def _fake_fetch(self, url: str):
        return responses[url]

    monkeypatch.setattr(native_html_backend_module.NativeHtmlReadOnlyStorageBackend, "_fetch_url", _fake_fetch)

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_id = _insert_store_row(
            db,
            name="sync-native-store",
            kind="native_html_readonly",
            root_uri="https://example.com/library/",
            access_protocol="native_html",
            is_read_only=1,
        )

        shell = TextDatabaseBrowser(db, output=output)
        assert shell.run_commands(["sync store {} --no-refresh".format(store_id)]) == 0

        file_rows = db.search("files", "file_store_id", store_id)
        assert len(file_rows) == 2
        keys = {str(row["file_storage_key"]) for row in file_rows}
        assert "files/one.epub" in keys
        assert "files/two.mobi" in keys

    rendered = output.getvalue()
    assert "crawler_urls_observed" in rendered
    assert "Native: native fetch depth=0 https://example.com/library/" in rendered


def test_text_browser_sync_store_background_native_submits_job(driver_spec, tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "browser_sync_store_background_native.sqlite"
    output = io.StringIO()

    manager = InMemoryJobManager(max_workers=1, default_backend="serial")
    captured_kwargs: dict[str, object] = {}

    def _fake_run_sync_store_job(**kwargs):
        captured_kwargs.update(kwargs)
        return {"store_row_id": 1, "inserted_files": 1, "errors": []}

    monkeypatch.setattr(sync_command_module, "run_sync_store_job", _fake_run_sync_store_job)

    try:
        with Database(
            metadata={"database_path": str(db_path)},
            db_type=driver_spec.db_type,
            create=True,
            backup=False,
            storage_startup_on_add=False,
        ) as db:
            store_id = _insert_store_row(
                db,
                name="sync-background-native-store",
                kind="native_html_readonly",
                root_uri="https://example.com/library/",
                access_protocol="native_html",
                is_read_only=1,
            )

            shell = TextDatabaseBrowser(db, output=output, job_manager=manager)
            assert shell.execute_line(
                "sync store {} --background --job-backend serial --job-timeout-s 5 --job-no-output".format(store_id)
            )

            jobs = manager.list()
            assert len(jobs) == 1
            info = manager.wait(jobs[0].job_id, timeout=2.0)
            assert info.state == "succeeded"
    finally:
        manager.shutdown(wait=True, cancel_pending=True)

    assert captured_kwargs.get("mode") == "native"


def test_text_browser_sync_store_wget_surfaces_crawler_observation_summary(
    driver_spec, tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "browser_sync_store_wget_crawler_summary.sqlite"
    output = io.StringIO()

    def _fake_run_wget(args, **kwargs):
        callback = kwargs.get("line_callback")
        if callable(callback):
            callback("https://example.com/books/index")
            callback("https://example.com/books/one.epub")
            callback("https://example.com/books/guide.html")
            callback("https://other.example.com/books/author.html")
        return WgetResult(args=list(args), returncode=0, stdout="", stderr="")

    monkeypatch.setattr(wget_backend_module, "run_wget", _fake_run_wget)

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_id = _insert_store_row(
            db,
            name="sync-wget-crawler-summary-store",
            kind="wget_html_readonly",
            root_uri="https://example.com/books/",
            access_protocol="wget",
            is_read_only=1,
        )

        shell = TextDatabaseBrowser(db, output=output)
        assert shell.run_commands(["sync store {} --no-refresh".format(store_id)]) == 0

    rendered = output.getvalue()
    assert "Crawler" in rendered
    assert "crawler_urls_observed" in rendered
    assert "crawler_html_seen" in rendered
    assert "crawler_book_like_found" in rendered
    assert "crawler_html_rejected" in rendered
    assert "crawler_rejections" in rendered


def test_text_browser_store_group_lists_subcommands(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_store_group.sqlite"
    output = io.StringIO()
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("store")
    rendered = output.getvalue()
    assert "Available `store` subcommands:" in rendered
    assert "store list" in rendered
    assert "store show" in rendered
    assert "store files" in rendered


def test_text_browser_store_view_commands(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_store_view.sqlite"
    output = io.StringIO()
    store_root = tmp_path / "store_view_root"
    (store_root / "nested").mkdir(parents=True, exist_ok=True)
    (store_root / "alpha.epub").write_bytes(b"alpha")
    (store_root / "nested" / "beta.mobi").write_bytes(b"beta")

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_id = _insert_store_row(
            db,
            name="store-view",
            kind="on_disk_existing_unmanaged_drive",
            root_uri=str(store_root.resolve()),
            is_read_only=1,
        )

        shell = TextDatabaseBrowser(db, output=output)
        assert shell.run_commands(
            [
                "sync store {} --no-hash --no-refresh".format(store_id),
                "store list",
                'store show "store-view"',
                "store files {} 20 0".format(store_id),
            ]
        ) == 0

    rendered = output.getvalue()
    assert "Stores rows" in rendered
    assert "store-view" in rendered
    assert "Store details" in rendered
    assert "Inventory" in rendered
    assert "files_total" in rendered
    assert "Store {} files rows".format(store_id) in rendered
    assert "alpha.epub" in rendered
    assert "nested/beta.mobi" in rendered


def test_text_browser_store_list_filters_and_sort(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_store_list_filters.sqlite"
    output = io.StringIO()

    managed_root = tmp_path / "managed_root"
    unmanaged_root = tmp_path / "unmanaged_root"
    offline_root = tmp_path / "offline_root"
    managed_root.mkdir(parents=True, exist_ok=True)
    unmanaged_root.mkdir(parents=True, exist_ok=True)
    offline_root.mkdir(parents=True, exist_ok=True)

    (managed_root / "a.epub").write_bytes(b"a")
    (managed_root / "b.mobi").write_bytes(b"b")
    (unmanaged_root / "c.epub").write_bytes(b"c")

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        managed_id = _insert_store_row(
            db,
            name="managed-two",
            kind="on_disk_existing_managed_drive",
            root_uri=str(managed_root.resolve()),
            is_read_only=0,
            online_status="online",
        )
        _insert_store_row(
            db,
            name="unmanaged-one",
            kind="on_disk_existing_unmanaged_drive",
            root_uri=str(unmanaged_root.resolve()),
            is_read_only=1,
            online_status="online",
        )
        _insert_store_row(
            db,
            name="offline-empty",
            kind="on_disk_existing_unmanaged_drive",
            root_uri=str(offline_root.resolve()),
            is_read_only=1,
            online_status="offline",
        )

        shell = TextDatabaseBrowser(db, output=output)
        assert shell.run_commands(
            [
                "sync store {} --no-hash --no-refresh".format(managed_id),
                'sync store "unmanaged-one" --no-hash --no-refresh',
                "store list --status online --sort files --desc",
                "store list --kind on_disk_existing_unmanaged_drive --read-only --min-files 1",
            ]
        ) == 0

    rendered = output.getvalue()
    sections = rendered.split("Stores rows ")
    assert len(sections) >= 3
    first_list_section = "Stores rows " + sections[1]
    second_list_section = "Stores rows " + sections[2]

    managed_first = first_list_section.find("managed-two")
    unmanaged_first = first_list_section.find("unmanaged-one")
    assert managed_first != -1 and unmanaged_first != -1 and managed_first < unmanaged_first
    assert "offline-empty" not in first_list_section
    assert "filtered from" in first_list_section

    assert "managed-two" not in second_list_section
    assert "unmanaged-one" in second_list_section
    assert "offline-empty" not in second_list_section
    assert "filtered from" in second_list_section


def test_text_browser_new_store_legacy_alias_still_works(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_store_legacy.sqlite"
    output = io.StringIO()
    store_dir = tmp_path / "legacy_store"
    input_stream = io.StringIO(
        "\n".join(
            [
                "",
                str(store_dir),
                "y",
                "",
                "",
                "",
                "n",
            ]
        )
        + "\n"
    )

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        assert shell.run_commands(["new-store"]) == 0
        rows = db.search("stores", "store_root_uri", str(store_dir.resolve()))
        assert len(rows) == 1


def test_text_browser_top_command_shows_rows(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_top.sqlite"
    output = io.StringIO()
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        _insert_store_row(
            db,
            name="top-store",
            kind="on_disk_existing_managed_drive",
            root_uri=str(tmp_path / "store_root"),
        )
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("top stores 1")

    rendered = output.getvalue()
    assert "Top stores rows" in rendered
    assert "| name " in rendered
    assert "+-" in rendered
    assert "top-store" in rendered


def test_text_browser_list_alias_shows_rows(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_list_alias.sqlite"
    output = io.StringIO()
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        _insert_store_row(
            db,
            name="list-store",
            kind="on_disk_existing_managed_drive",
            root_uri=str(tmp_path / "store_root"),
        )
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("list stores 1")

    rendered = output.getvalue()
    assert "Top stores rows" in rendered
    assert "list-store" in rendered


def test_text_browser_top_command_respects_terminal_width(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_top_width.sqlite"
    output = io.StringIO()
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        _insert_store_row(
            db,
            name="width-store",
            kind="on_disk_existing_managed_drive",
            root_uri=str(tmp_path / "store_root"),
        )
        shell = TextDatabaseBrowser(db, output=output)
        # Force a narrow terminal width to exercise width-aware truncation and column pruning.
        shell.get_terminal_width = lambda: 72  # type: ignore[method-assign]
        assert shell.execute_line("top stores 1")

    rendered = output.getvalue()
    lines = [line for line in rendered.splitlines() if line.strip()]
    assert lines
    assert max(len(line) for line in lines) <= 72


def test_text_browser_top_command_invalid_args(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_top_invalid.sqlite"
    output = io.StringIO()
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, output=output)
        with pytest.raises(ValueError):
            shell.execute_line("top stores nope")
        with pytest.raises(ValueError):
            shell.execute_line("top")


def test_text_browser_show_tags_for_work(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_show_tags.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_row = Row.from_idless_row_dict(
            db,
            row_dict={
                "work_title": "Show Tags Work",
                "work_canonical_title": "Show Tags Work",
                "work_sort_title": "Show Tags Work",
            },
            table="works",
        )

        if _preferred_tag_table(db) == "tags":
            tag_row = Row.from_idless_row_dict(
                db,
                row_dict={
                    "tag": "Fish",
                    "tag_phash": make_tag_search_term("Fish"),
                },
                table="tags",
            )
        else:
            tag_row = Row.from_idless_row_dict(
                db,
                row_dict={
                    "label_text": "Fish",
                    "label_text_norm": make_tag_search_term("Fish"),
                },
                table="labels",
            )

        db.interlink_rows(primary_row=tag_row, secondary_row=work_row, priority=0)

        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("show tags work:{}".format(work_row["work_id"]))

    rendered = output.getvalue()
    assert "Tags for works:{} (1)".format(work_row["work_id"]) in rendered
    assert "  - Fish" in rendered


def test_text_browser_show_language_and_series_for_work(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_show_language_series.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_row = Row.from_idless_row_dict(
            db,
            row_dict={
                "work_title": "Show Language Series Work",
                "work_canonical_title": "Show Language Series Work",
                "work_sort_title": "Show Language Series Work",
            },
            table="works",
        )

        language_rows = db.search("languages", "language_code", "eng")
        assert language_rows
        language_row = language_rows[0]
        db.interlink_rows(primary_row=language_row, secondary_row=work_row, priority=0)

        series_rows = db.search("series", "series", "Culture")
        if series_rows:
            series_row = series_rows[0]
        else:
            series_row = Row.from_idless_row_dict(
                db,
                row_dict={
                    "series": "Culture",
                    "series_sort": "Culture",
                    "series_name_norm": make_title_search_term("Culture"),
                    "series_phash": "_culture",
                },
                table="series",
            )
        db.interlink_rows(primary_row=series_row, secondary_row=work_row, priority=0)

        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("show language work:{}".format(work_row["work_id"]))
        assert shell.execute_line("show series work:{}".format(work_row["work_id"]))

    rendered = output.getvalue()
    assert "Linked languages" in rendered
    assert "Linked series" in rendered


def test_text_browser_show_all_for_work(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_show_all.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_row = Row.from_idless_row_dict(
            db,
            row_dict={
                "work_title": "Show All Work",
                "work_canonical_title": "Show All Work",
                "work_sort_title": "Show All Work",
            },
            table="works",
        )
        note_row = Row.from_idless_row_dict(
            db,
            row_dict={"note": "Show all note"},
            table="notes",
        )
        db.interlink_rows(primary_row=note_row, secondary_row=work_row, priority=0)

        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("on tag work:{} fish,chips".format(work_row["work_id"]))
        assert shell.execute_line("show all work:{}".format(work_row["work_id"]))

    rendered = output.getvalue()
    assert "Linked notes" in rendered
    assert ("Linked labels" in rendered) or ("Linked tags" in rendered)
    assert "#{} | Show all note".format(note_row["note_id"]) in rendered


def test_text_browser_show_defaults_to_all_for_work_target(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_show_default_all.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_row = Row.from_idless_row_dict(
            db,
            row_dict={
                "work_title": "Show Default Work",
                "work_canonical_title": "Show Default Work",
                "work_sort_title": "Show Default Work",
            },
            table="works",
        )
        note_row = Row.from_idless_row_dict(
            db,
            row_dict={"note": "Default show note"},
            table="notes",
        )
        db.interlink_rows(primary_row=note_row, secondary_row=work_row, priority=0)

        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("show work:{}".format(work_row["work_id"]))
        assert shell.execute_line("show work {}".format(work_row["work_id"]))

    rendered = output.getvalue()
    assert rendered.count("Linked notes") >= 2


def test_text_browser_show_rejects_selector_targets(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_show_selector_targets.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, output=output)
        with pytest.raises(ValueError, match="single row id"):
            shell.execute_line("show work:1-3")
        with pytest.raises(ValueError, match="single row id"):
            shell.execute_line("show tags work:1-3")
        with pytest.raises(ValueError, match="single row id"):
            shell.execute_line("show work:1-3 tags")


def test_text_browser_show_rejects_unknown_kind(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_show_unknown_kind.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_row = Row.from_idless_row_dict(
            db,
            row_dict={
                "work_title": "Show Unknown Kind Work",
                "work_canonical_title": "Show Unknown Kind Work",
                "work_sort_title": "Show Unknown Kind Work",
            },
            table="works",
        )
        shell = TextDatabaseBrowser(db, output=output)
        with pytest.raises(ValueError, match="Unknown linked kind/table"):
            shell.execute_line("show work:{} bananas".format(work_row["work_id"]))


def test_text_browser_row_command_accepts_compact_table_id(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_row_compact.sqlite"
    output = io.StringIO()
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_id = _insert_store_row(
            db,
            name="row-compact-store",
            kind="on_disk_existing_managed_drive",
            root_uri=str(tmp_path / "store_root"),
        )
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("row store:{}".format(store_id))

    rendered = output.getvalue()
    assert "Identity" in rendered
    assert "Access" in rendered
    assert "| column " in rendered
    assert "| value " in rendered
    assert "| id " in rendered
    assert "| name " in rendered
    assert str(store_id) in rendered
    assert "row-compact-store" in rendered
    assert "\n\nAccess\n" in rendered


def test_text_browser_set_command_updates_row_with_display_column_token(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_set_command.sqlite"
    output = io.StringIO()
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_id = _insert_store_row(
            db,
            name="set-store",
            kind="on_disk_existing_managed_drive",
            root_uri=str(tmp_path / "store_root"),
        )
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("set store:{} name Set Store Updated".format(store_id))

        row = db.get_row_from_id("stores", store_id)
        assert row is not None
        assert row["store_name"] == "Set Store Updated"

    rendered = output.getvalue()
    assert "Updated stores:{} store_name='Set Store Updated'".format(store_id) in rendered


def test_text_browser_mutating_commands_refresh_attached_metadata_read_source(driver_spec, tmp_path: Path) -> None:
    class _RefreshableReadSource:
        def __init__(self) -> None:
            self.refresh_count = 0

        def refresh(self) -> bool:
            self.refresh_count += 1
            return True

    db_path = tmp_path / "browser_mutation_refreshes_read_source.sqlite"
    output = io.StringIO()
    read_source = _RefreshableReadSource()
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_id = _insert_store_row(
            db,
            name="refresh-store",
            kind="on_disk_existing_managed_drive",
            root_uri=str(tmp_path / "store_root"),
        )
        shell = TextDatabaseBrowser(db, output=output, metadata_read_source=read_source)

        assert shell.execute_line("row store:{}".format(store_id))
        assert read_source.refresh_count == 0

        assert shell.execute_line("set store:{} name Refreshed Store".format(store_id))
        assert read_source.refresh_count == 1


def test_text_browser_set_command_routes_via_core(driver_spec, tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "browser_set_command_core.sqlite"
    output = io.StringIO()
    query_calls: list[tuple[str, dict[str, object]]] = []
    command_calls: list[tuple[str, dict[str, object]]] = []

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_id = _insert_store_row(
            db,
            name="set-core-store",
            kind="on_disk_existing_managed_drive",
            root_uri=str(tmp_path / "store_root"),
        )
        shell = TextDatabaseBrowser(db, output=output)
        monkeypatch.setattr(shell, "supports_core_queries", lambda: True)
        monkeypatch.setattr(shell, "supports_core_commands", lambda: True)

        def _record_query(name: str, *, payload=None):
            payload_dict = dict(payload or {})
            query_calls.append((str(name), payload_dict))
            return Library(database=db, close_database_on_close=False).get_row(**payload_dict["kwargs"])

        def _record_command(name: str, *, payload=None):
            payload_dict = dict(payload or {})
            command_calls.append((str(name), payload_dict))
            return Library(database=db, close_database_on_close=False).update_row_fields(**payload_dict["kwargs"])

        monkeypatch.setattr(shell, "execute_core_query", _record_query)
        monkeypatch.setattr(shell, "execute_core_command", _record_command)

        assert shell.execute_line("set store:{} online_status offline".format(store_id))

        row = db.get_row_from_id("stores", store_id)
        assert row is not None
        assert row["store_online_status"] == "offline"

    assert query_calls == [
        (
            "invoke",
            {
                "target": "library",
                "method": "get_row",
                "kwargs": {
                    "table": "stores",
                    "row_id": store_id,
                },
            },
        )
    ]
    assert command_calls == [
        (
            "invoke",
            {
                "target": "library",
                "method": "update_row_fields",
                "kwargs": {
                    "table": "stores",
                    "row_id": store_id,
                    "updates": {"store_online_status": "offline"},
                },
            },
        )
    ]


def test_text_browser_edit_command_updates_selected_columns(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_edit_command.sqlite"
    output = io.StringIO()
    input_stream = io.StringIO("Edited Store\noffline\n")
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_id = _insert_store_row(
            db,
            name="edit-store",
            kind="on_disk_existing_managed_drive",
            root_uri=str(tmp_path / "store_root"),
        )
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        assert shell.execute_line("edit store:{} name online_status".format(store_id))

        row = db.get_row_from_id("stores", store_id)
        assert row is not None
        assert row["store_name"] == "Edited Store"
        assert row["store_online_status"] == "offline"

    rendered = output.getvalue()
    assert "Editing stores:{} | Enter keeps current value | type `null` to clear".format(store_id) in rendered
    assert "Identity" in rendered
    assert "Access" in rendered
    assert "Updated stores:{} (2 fields): store_name, store_online_status".format(store_id) in rendered


def test_text_browser_delete_command_with_force_removes_row(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_delete_command.sqlite"
    output = io.StringIO()
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_id = _insert_store_row(
            db,
            name="delete-store",
            kind="on_disk_existing_managed_drive",
            root_uri=str(tmp_path / "store_root"),
        )
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("delete store:{} --force".format(store_id))
        assert db.get_row_from_id("stores", store_id) is None

    rendered = output.getvalue()
    assert "Deleted stores:{}.".format(store_id) in rendered


def test_text_browser_delete_command_cancel_keeps_row(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_delete_command_cancel.sqlite"
    output = io.StringIO()
    input_stream = io.StringIO("n\n")
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_id = _insert_store_row(
            db,
            name="delete-cancel-store",
            kind="on_disk_existing_managed_drive",
            root_uri=str(tmp_path / "store_root"),
        )
        _insert_folder_row(db, store_id=store_id, relpath="delete-cancel-root")
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        assert shell.execute_line("delete store:{}".format(store_id))
        assert db.get_row_from_id("stores", store_id) is not None

    rendered = output.getvalue()
    assert "Delete preview for stores:{}".format(store_id) in rendered
    assert "Direct references:" in rendered
    assert "folders.folder_store_id: 1" in rendered
    assert "    - #1 | root | delete-cancel-root" in rendered
    assert "Delete may fail or cascade depending on schema constraints." in rendered
    assert "Delete canceled." in rendered


def test_text_browser_delete_command_preview_shows_linked_row_samples(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_delete_command_linked_samples.sqlite"
    output = io.StringIO()
    input_stream = io.StringIO("n\n")
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_row = Row.from_idless_row_dict(
            db,
            row_dict={
                "work_title": "Delete Preview Work",
                "work_canonical_title": "Delete Preview Work",
                "work_sort_title": "Delete Preview Work",
            },
            table="works",
        )

        if _preferred_tag_table(db) == "tags":
            tag_row = Row.from_idless_row_dict(
                db,
                row_dict={
                    "tag": "Fish",
                    "tag_phash": make_tag_search_term("Fish"),
                },
                table="tags",
            )
        else:
            tag_row = Row.from_idless_row_dict(
                db,
                row_dict={
                    "label_text": "Fish",
                    "label_text_norm": make_tag_search_term("Fish"),
                },
                table="labels",
            )

        db.interlink_rows(primary_row=tag_row, secondary_row=work_row, priority=0)

        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        assert shell.execute_line("delete work:{}".format(work_row["work_id"]))
        assert db.get_row_from_id("works", int(work_row["work_id"])) is not None

    rendered = output.getvalue()
    assert "Delete preview for works:{}".format(work_row["work_id"]) in rendered
    assert "Linked rows:" in rendered
    assert "    - #1 | Fish" in rendered
    assert "Delete canceled." in rendered


def test_text_browser_delete_command_routes_via_core(driver_spec, tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "browser_delete_command_core.sqlite"
    output = io.StringIO()
    query_calls: list[tuple[str, dict[str, object]]] = []
    command_calls: list[tuple[str, dict[str, object]]] = []

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_id = _insert_store_row(
            db,
            name="delete-core-store",
            kind="on_disk_existing_managed_drive",
            root_uri=str(tmp_path / "store_root"),
        )
        shell = TextDatabaseBrowser(db, output=output)
        monkeypatch.setattr(shell, "supports_core_queries", lambda: True)
        monkeypatch.setattr(shell, "supports_core_commands", lambda: True)

        def _record_query(name: str, *, payload=None):
            payload_dict = dict(payload or {})
            query_calls.append((str(name), payload_dict))
            method = str(payload_dict.get("method", ""))
            library = Library(database=db, close_database_on_close=False)
            if method == "get_row":
                return library.get_row(**payload_dict["kwargs"])
            if method == "describe_row_delete_impact":
                return library.describe_row_delete_impact(**payload_dict["kwargs"])
            raise AssertionError("unexpected core query method {!r}".format(method))

        def _record_command(name: str, *, payload=None):
            payload_dict = dict(payload or {})
            command_calls.append((str(name), payload_dict))
            return Library(database=db, close_database_on_close=False).delete_row(**payload_dict["kwargs"])

        monkeypatch.setattr(shell, "execute_core_query", _record_query)
        monkeypatch.setattr(shell, "execute_core_command", _record_command)

        assert shell.execute_line("delete store:{} --force".format(store_id))
        assert db.get_row_from_id("stores", store_id) is None

    assert query_calls == [
        (
            "invoke",
            {
                "target": "library",
                "method": "get_row",
                "kwargs": {
                    "table": "stores",
                    "row_id": store_id,
                },
            },
        ),
        (
            "invoke",
            {
                "target": "library",
                "method": "describe_row_delete_impact",
                "kwargs": {
                    "table": "stores",
                    "row_id": store_id,
                },
            },
        )
    ]
    assert command_calls == [
        (
            "invoke",
            {
                "target": "library",
                "method": "delete_row",
                "kwargs": {
                    "table": "stores",
                    "row_id": store_id,
                },
            },
        )
    ]


def test_text_browser_singular_table_tokens_resolve_in_core_commands(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_singular_table_tokens.sqlite"
    output = io.StringIO()
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_id = _insert_store_row(
            db,
            name="singular-store",
            kind="on_disk_existing_managed_drive",
            root_uri=str(tmp_path / "store_root"),
        )
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("use store")
        assert shell.execute_line("schema store")
        assert shell.execute_line("count store")
        assert shell.execute_line("browse store 1 0")
        assert shell.execute_line("row store {}".format(store_id))
        assert shell.execute_line("search store store_name singular-store 1")
        assert shell.execute_line("search store singular-store --limit 1")
        assert shell.execute_line("top store 1")

    rendered = output.getvalue()
    assert "Current table: stores" in rendered
    assert "Schema for stores" in rendered
    assert "stores rows:" in rendered
    assert "Browsing stores rows" in rendered
    assert "Search stores.store_name" in rendered
    assert "Search stores contains 'singular-store'" in rendered
    assert "Summary: matches_total=" in rendered
    assert "Summary: scanned_rows=" in rendered
    assert "Top stores rows" in rendered


def test_text_browser_table_wide_search_with_quoted_term(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_search_table_wide.sqlite"
    output = io.StringIO()
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        _insert_store_row(
            db,
            name="Wide Store Name",
            kind="on_disk_existing_managed_drive",
            root_uri=str(tmp_path / "store_root"),
        )
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line('search stores "Wide Store" --limit 5')

    rendered = output.getvalue()
    assert "Search stores contains 'Wide Store'" in rendered
    assert "matches_total=1" in rendered
    assert "Summary: scanned_rows=" in rendered


def test_text_browser_new_creator_wizard_creates_human_agent(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_creator.sqlite"
    output = io.StringIO()

    # name, type, sort(default), short, legal, birth, death, lang, bio, wiki, imdb, link, seminal, one_person, proceed
    input_stream = io.StringIO(
        "\n".join(
            [
                "Ursula K. Le Guin",
                "authors",
                "",
                "Le Guin",
                "Ursula Kroeber Le Guin",
                "1929-10-21",
                "2018-01-22",
                "",
                "American author of speculative fiction.",
                "https://en.wikipedia.org/wiki/Ursula_K._Le_Guin",
                "",
                "https://www.ursulakleguin.com",
                "The Left Hand of Darkness",
                "",
                "",
            ]
        )
        + "\n"
    )

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        rc = shell.run_commands(["add creator"])
        assert rc == 0

        agent_rows = db.search("agents", "agent_canonical_name", "Ursula K. Le Guin")
        assert agent_rows
        creator_row = agent_rows[0]
        assert creator_row["agent_type"] == "person"

        human_rows = db.search("human_agents", "human_agent_agent_id", creator_row["agent_id"])
        assert len(human_rows) == 1
        human_row = human_rows[0]
        assert human_row["human_agent_birth_date"] == "1929-10-21"
        assert human_row["human_agent_death_date"] == "2018-01-22"

        entity_rows = db.search("entity_identifiers", "entity_identifier_entity_id", creator_row["agent_id"])
        schemes = {row["entity_identifier_scheme"] for row in entity_rows}
        assert "wikipedia_url" in schemes
        assert "url" in schemes

    rendered = output.getvalue()
    assert "New creator wizard" in rendered
    assert "Creator created:" in rendered


def test_text_browser_new_creator_invalid_type_rejected(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_creator_invalid.sqlite"
    output = io.StringIO()
    input_stream = io.StringIO(
        "\n".join(
            [
                "Some Person",
                "not_a_real_creator_type",
            ]
        )
        + "\n"
    )

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        with pytest.raises(ValueError):
            shell.execute_line("add creator")


def test_text_browser_new_creator_legacy_alias_still_works(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_creator_legacy.sqlite"
    output = io.StringIO()
    input_stream = io.StringIO(
        "\n".join(
            [
                "Legacy Creator",
                "authors",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
            ]
        )
        + "\n"
    )

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        assert shell.run_commands(["new-creator"]) == 0
        rows = db.search("agents", "agent_canonical_name", "Legacy Creator")
        assert rows


def test_text_browser_new_work_wizard_creates_work(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_work.sqlite"
    output = io.StringIO()
    # title, canonical(default), sort(default), creator_sort, type, medium, language, original_date, original_year,
    # wiki, is_fiction(default yes), audience, completion_status, note, proceed(default yes)
    input_stream = io.StringIO(
        "\n".join(
            [
                "The Left Hand of Darkness",
                "",
                "",
                "Le Guin, Ursula K.",
                "novel",
                "text",
                "english",
                "1969-01-01",
                "1969",
                "https://en.wikipedia.org/wiki/The_Left_Hand_of_Darkness",
                "",
                "adult",
                "complete",
                "Classic speculative fiction work.",
                "",
            ]
        )
        + "\n"
    )

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        assert shell.run_commands(["add work"]) == 0

        rows = db.search("works", "work_canonical_title", "The Left Hand of Darkness")
        assert rows
        work_row = rows[0]
        assert work_row["work_title"] == "The Left Hand of Darkness"
        assert work_row["work_type"] == "novel"
        assert work_row["work_medium"] == "text"
        assert work_row["work_original_year"] == 1969
        assert int(work_row["work_is_fiction"]) == 1

    rendered = output.getvalue()
    assert "New work wizard" in rendered
    assert "Work created:" in rendered


def test_text_browser_new_work_invalid_year_rejected(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_work_invalid.sqlite"
    output = io.StringIO()
    input_stream = io.StringIO(
        "\n".join(
            [
                "Some Work",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "year-abc",
            ]
        )
        + "\n"
    )
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        with pytest.raises(ValueError):
            shell.execute_line("add work")


def test_text_browser_new_work_legacy_alias_still_works(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_work_legacy.sqlite"
    output = io.StringIO()
    input_stream = io.StringIO(
        "\n".join(
            [
                "Legacy Work",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
            ]
        )
        + "\n"
    )
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        assert shell.run_commands(["new-work"]) == 0
        rows = db.search("works", "work_canonical_title", "Legacy Work")
        assert rows


def test_text_browser_new_expression_wizard_creates_expression(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_expression.sqlite"
    output = io.StringIO()
    input_stream = io.StringIO(
        "\n".join(
            [
                "Author's preferred text",
                "Title Override",
                "textual",
                "Original Edition",
                "1969",
                "",
                "1969-01-01",
                "1969-01-01",
                "critical",
                "english",
                "reading",
                "102345",
                "4",
                "",
                "",
                "published",
                "Primary authored edition.",
                "",
            ]
        )
        + "\n"
    )

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        assert shell.run_commands(["add expression"]) == 0

        rows = db.search("expressions", "expression_label", "Original Edition")
        assert rows
        expression_row = rows[0]
        assert expression_row["expression_subtitle"] == "Author's preferred text"
        assert expression_row["expression_title_override"] == "Title Override"
        assert expression_row["expression_type"] == "textual"
        assert expression_row["expression_year"] == 1969
        assert int(expression_row["expression_is_preferred"]) == 1
        assert expression_row["expression_original_date"] is not None
        assert expression_row["expression_flags"] == "critical"
        assert expression_row["expression_language_id"] is not None
        assert expression_row["expression_mode"] == "reading"
        assert expression_row["expression_wordcount"] == 102345
        assert int(expression_row["expression_fiction_length_category"]) == 4
        assert expression_row["expression_status"] == "published"

    rendered = output.getvalue()
    assert "New expression wizard" in rendered
    assert "Expression created:" in rendered


def test_text_browser_new_expression_invalid_year_rejected(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_expression_invalid.sqlite"
    output = io.StringIO()
    input_stream = io.StringIO(
        "\n".join(
            [
                "Subtitle",
                "",
                "",
                "Label",
                "year-abc",
            ]
        )
        + "\n"
    )
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        with pytest.raises(ValueError):
            shell.execute_line("add expression")


def test_text_browser_new_expression_legacy_alias_still_works(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_expression_legacy.sqlite"
    output = io.StringIO()
    input_stream = io.StringIO(
        "\n".join(
            [
                "",
                "",
                "",
                "Legacy Expression",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
            ]
        )
        + "\n"
    )
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        assert shell.run_commands(["new-expression"]) == 0
        rows = db.search("expressions", "expression_label", "Legacy Expression")
        assert rows


def test_text_browser_new_manifestation_wizard_creates_manifestation(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_manifestation.sqlite"
    output = io.StringIO()
    input_stream = io.StringIO(
        "\n".join(
            [
                "Collector's edition",
                "ebook",
                "EPUB",
                "2nd edition",
                "2015",
                "2015-04-01",
                "drm-free",
                "352",
                "",
                "US",
                "available",
                "Primary digital release.",
                "",
            ]
        )
        + "\n"
    )

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        assert shell.run_commands(["add manifestation"]) == 0

        rows = db.search("manifestations", "manifestation_format_detail", "EPUB")
        assert rows
        manifestation_row = rows[0]
        assert manifestation_row["manifestation_subtitle"] == "Collector's edition"
        assert manifestation_row["manifestation_carrier_type"] == "ebook"
        assert manifestation_row["manifestation_edition_statement"] == "2nd edition"
        assert manifestation_row["manifestation_pub_year"] == 2015
        assert manifestation_row["manifestation_pub_date"] == "2015-04-01"
        assert manifestation_row["manifestation_page_count"] == 352
        assert manifestation_row["manifestation_status"] == "available"

    rendered = output.getvalue()
    assert "New manifestation wizard" in rendered
    assert "Manifestation created:" in rendered


def test_text_browser_new_item_wizard_creates_item(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_item.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        manifestation_row = Row.from_idless_row_dict(
            db,
            row_dict={"manifestation_format_detail": "EPUB"},
            table="manifestations",
        )
        manifestation_id = int(manifestation_row["manifestation_id"])

        input_stream = io.StringIO(
            "\n".join(
                [
                    str(manifestation_id),
                    "scanned",
                    "ebook",
                    "/library/primary",
                    "INV-0001",
                    "2022-02-03",
                    "2022-02-03",
                    "import",
                    "bulk migration",
                    "/mnt/books/example.epub",
                    "legacy_share",
                    "2022-06-01",
                    "1299.5",
                    "active",
                    "good",
                    "",
                ]
            )
            + "\n"
        )
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        assert shell.run_commands(["add item"]) == 0

        rows = db.search("items", "item_inventory_code", "INV-0001")
        assert rows
        item_row = rows[0]
        assert int(item_row["item_manifestation_id"]) == manifestation_id
        assert item_row["item_type"] == "ebook"
        assert item_row["item_location"] == "/library/primary"
        assert item_row["item_source"] == "import"
        assert item_row["item_lifecycle_status"] == "active"
        assert item_row["item_condition"] == "good"
        assert float(item_row["item_acquired_price_minor"]) == 1299.5
        assert item_row["item_original_date"] is not None

    rendered = output.getvalue()
    assert "New item wizard" in rendered
    assert "Item created:" in rendered


def test_text_browser_new_item_invalid_manifestation_id_rejected(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_item_invalid.sqlite"
    output = io.StringIO()
    input_stream = io.StringIO("not-an-int\n")
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        with pytest.raises(ValueError):
            shell.execute_line("add item")


def test_text_browser_new_item_legacy_alias_still_works(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_item_legacy.sqlite"
    output = io.StringIO()
    input_stream = io.StringIO(
        "\n".join(
            [
                "",
                "",
                "",
                "",
                "LEG-ITEM-1",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
            ]
        )
        + "\n"
    )
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        assert shell.run_commands(["new-item"]) == 0
        rows = db.search("items", "item_inventory_code", "LEG-ITEM-1")
        assert rows


def test_text_browser_new_tag_wizard_creates_tag_or_label(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_tag.sqlite"
    output = io.StringIO()
    input_stream = io.StringIO(
        "\n".join(
            [
                "Space Opera",
                "Retro SF shelf marker",
                "",
            ]
        )
        + "\n"
    )

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        assert shell.run_commands(["add tag"]) == 0

        tag_table, rows = _search_tag_rows(db, "Space Opera")
        assert rows
        row = rows[0]
        if tag_table == "tags":
            assert row["tag"] == "Space Opera"
            assert row["tag_description"] == "Retro SF shelf marker"
        else:
            assert row["label_text"] == "Space Opera"
            assert row["label_description"] == "Retro SF shelf marker"

    rendered = output.getvalue()
    assert "New tag wizard" in rendered
    assert "Tag created:" in rendered


def test_text_browser_new_tag_blank_rejected(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_tag_blank.sqlite"
    output = io.StringIO()
    input_stream = io.StringIO("\n")
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        with pytest.raises(ValueError):
            shell.execute_line("add tag")


def test_text_browser_new_tag_legacy_alias_still_works(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_tag_legacy.sqlite"
    output = io.StringIO()
    input_stream = io.StringIO(
        "\n".join(
            [
                "Legacy Tag",
                "",
                "",
            ]
        )
        + "\n"
    )
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        assert shell.run_commands(["new-tag"]) == 0

        _tag_table, rows = _search_tag_rows(db, "Legacy Tag")
        assert rows


def test_text_browser_new_note_wizard_creates_note(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_note.sqlite"
    output = io.StringIO()
    input_stream = io.StringIO(
        "\n".join(
            [
                "Imported from handwritten card index.",
                "",
            ]
        )
        + "\n"
    )

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        assert shell.run_commands(["add note"]) == 0

        rows = db.search("notes", "note", "Imported from handwritten card index.")
        assert rows

    rendered = output.getvalue()
    assert "New note wizard" in rendered
    assert "Note created:" in rendered


def test_text_browser_new_note_blank_rejected(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_note_blank.sqlite"
    output = io.StringIO()
    input_stream = io.StringIO("\n")
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        with pytest.raises(ValueError):
            shell.execute_line("add note")


def test_text_browser_new_note_legacy_alias_still_works(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_note_legacy.sqlite"
    output = io.StringIO()
    input_stream = io.StringIO(
        "\n".join(
            [
                "Legacy note entry",
                "",
            ]
        )
        + "\n"
    )
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        assert shell.run_commands(["new-note"]) == 0
        rows = db.search("notes", "note", "Legacy note entry")
        assert rows


def test_text_browser_link_links_unlink_note_and_work(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_link_note_work.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        note_row = Row.from_idless_row_dict(
            db,
            row_dict={"note": "Primary source confirmed."},
            table="notes",
        )
        work_row = Row.from_idless_row_dict(
            db,
            row_dict={
                "work_title": "The Left Hand of Darkness",
                "work_canonical_title": "The Left Hand of Darkness",
                "work_sort_title": "The Left Hand of Darkness",
            },
            table="works",
        )

        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line(
            "link note {} work {} --priority 0 --set scratch=manual_link".format(
                note_row["note_id"],
                work_row["work_id"],
            )
        )

        linked_notes = db.get_interlinked_rows(target_row=work_row, secondary_table="notes")
        assert any(int(row["note_id"]) == int(note_row["note_id"]) for row in linked_notes)
        link_row = db.get_interlink_row(primary_row=note_row, secondary_row=work_row)
        assert link_row is not None
        assert link_row["note_work_link_scratch"] == "manual_link"

        assert shell.execute_line("links work {} note".format(work_row["work_id"]))
        assert shell.execute_line("unlink work {} note {}".format(work_row["work_id"], note_row["note_id"]))

        linked_notes_after = db.get_interlinked_rows(target_row=work_row, secondary_table="notes")
        assert not linked_notes_after

    rendered = output.getvalue()
    assert "Link created:" in rendered
    assert "Linked notes rows" in rendered
    assert "#{} | Primary source confirmed.".format(note_row["note_id"]) in rendered
    assert "Unlinked" in rendered


def test_text_browser_link_rejects_unknown_table(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_link_invalid.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        note_row = Row.from_idless_row_dict(
            db,
            row_dict={"note": "Link failure test note"},
            table="notes",
        )
        shell = TextDatabaseBrowser(db, output=output)
        with pytest.raises(ValueError):
            shell.execute_line("link unknown_table 1 note {}".format(note_row["note_id"]))


def test_text_browser_link_rejects_invalid_set_field_with_guidance(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_link_invalid_set.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        note_row = Row.from_idless_row_dict(
            db,
            row_dict={"note": "Link invalid field note"},
            table="notes",
        )
        work_row = Row.from_idless_row_dict(
            db,
            row_dict={
                "work_title": "Invalid Set Field Work",
                "work_canonical_title": "Invalid Set Field Work",
                "work_sort_title": "Invalid Set Field Work",
            },
            table="works",
        )

        shell = TextDatabaseBrowser(db, output=output)
        with pytest.raises(ValueError, match="Valid --set fields"):
            shell.execute_line(
                "link note {} work {} --set not_a_field=1".format(
                    note_row["note_id"],
                    work_row["work_id"],
                )
            )


def test_text_browser_link_supports_to_sugar_syntax(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_link_to_sugar.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        note_row = Row.from_idless_row_dict(
            db,
            row_dict={"note": "Sugar syntax note"},
            table="notes",
        )
        work_row = Row.from_idless_row_dict(
            db,
            row_dict={
                "work_title": "Sugar Syntax Work",
                "work_canonical_title": "Sugar Syntax Work",
                "work_sort_title": "Sugar Syntax Work",
            },
            table="works",
        )

        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line(
            "link note {} to work {} --priority 0 --set scratch=sugar".format(
                note_row["note_id"],
                work_row["work_id"],
            )
        )

        link_row = db.get_interlink_row(primary_row=note_row, secondary_row=work_row)
        assert link_row is not None
        assert link_row["note_work_link_scratch"] == "sugar"


def test_text_browser_link_unlink_links_support_compact_refs(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_link_compact_refs.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        note_row = Row.from_idless_row_dict(
            db,
            row_dict={"note": "Compact ref link note"},
            table="notes",
        )
        work_row = Row.from_idless_row_dict(
            db,
            row_dict={
                "work_title": "Compact Ref Link Work",
                "work_canonical_title": "Compact Ref Link Work",
                "work_sort_title": "Compact Ref Link Work",
            },
            table="works",
        )

        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("link note:{} to work:{}".format(note_row["note_id"], work_row["work_id"]))
        assert shell.execute_line("links work:{} note".format(work_row["work_id"]))
        assert shell.execute_line("unlink work:{} note:{}".format(work_row["work_id"], note_row["note_id"]))

        linked_notes_after = db.get_interlinked_rows(target_row=work_row, secondary_table="notes")
        assert not linked_notes_after

    rendered = output.getvalue()
    assert "Link created:" in rendered
    assert "Linked notes rows" in rendered
    assert "#{} | Compact ref link note".format(note_row["note_id"]) in rendered
    assert "Unlinked" in rendered


def test_text_browser_note_on_creates_and_links_note(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_note_on.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_row = Row.from_idless_row_dict(
            db,
            row_dict={
                "work_title": "Note On Work",
                "work_canonical_title": "Note On Work",
                "work_sort_title": "Note On Work",
            },
            table="works",
        )

        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("note-on works {} \"Curated from archive source\"".format(work_row["work_id"]))

        note_rows = db.search("notes", "note", "Curated from archive source")
        assert note_rows
        note_row = note_rows[0]
        link_row = db.get_interlink_row(primary_row=note_row, secondary_row=work_row)
        assert link_row is not None

    rendered = output.getvalue()
    assert "Note linked:" in rendered


def test_text_browser_note_on_accepts_singular_table_token(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_note_on_singular.sqlite"
    output = io.StringIO()
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_row = Row.from_idless_row_dict(
            db,
            row_dict={
                "work_title": "Note On Singular Work",
                "work_canonical_title": "Note On Singular Work",
                "work_sort_title": "Note On Singular Work",
            },
            table="works",
        )

        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("note-on work {} \"Added via singular table token\"".format(work_row["work_id"]))

        note_rows = db.search("notes", "note", "Added via singular table token")
        assert note_rows
        note_row = note_rows[0]
        link_row = db.get_interlink_row(primary_row=note_row, secondary_row=work_row)
        assert link_row is not None


def test_text_browser_note_on_accepts_compact_table_id(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_note_on_compact.sqlite"
    output = io.StringIO()
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_row = Row.from_idless_row_dict(
            db,
            row_dict={
                "work_title": "Note On Compact Work",
                "work_canonical_title": "Note On Compact Work",
                "work_sort_title": "Note On Compact Work",
            },
            table="works",
        )

        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line(
            "note-on work:{} \"Added via compact table:id token\"".format(work_row["work_id"])
        )

        note_rows = db.search("notes", "note", "Added via compact table:id token")
        assert note_rows
        note_row = note_rows[0]
        link_row = db.get_interlink_row(primary_row=note_row, secondary_row=work_row)
        assert link_row is not None


def test_text_browser_note_on_rejects_bad_id(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_note_on_bad_id.sqlite"
    output = io.StringIO()
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, output=output)
        with pytest.raises(ValueError):
            shell.execute_line("note-on works abc \"invalid id\"")


def test_text_browser_on_note_creates_and_links_note(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_on_note.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_row = Row.from_idless_row_dict(
            db,
            row_dict={
                "work_title": "On Note Work",
                "work_canonical_title": "On Note Work",
                "work_sort_title": "On Note Work",
            },
            table="works",
        )
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("on work {} note \"Curated from archive source\"".format(work_row["work_id"]))

        note_rows = db.search("notes", "note", "Curated from archive source")
        assert note_rows
        note_row = note_rows[0]
        link_row = db.get_interlink_row(primary_row=note_row, secondary_row=work_row)
        assert link_row is not None

    assert "Note linked:" in output.getvalue()


def test_text_browser_on_note_subcommand_style(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_on_note_subcommand.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_row = Row.from_idless_row_dict(
            db,
            row_dict={
                "work_title": "On Note Subcommand Work",
                "work_canonical_title": "On Note Subcommand Work",
                "work_sort_title": "On Note Subcommand Work",
            },
            table="works",
        )
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("on note work:{} \"Subcommand note path\"".format(work_row["work_id"]))

        note_rows = db.search("notes", "note", "Subcommand note path")
        assert note_rows
        note_row = note_rows[0]
        link_row = db.get_interlink_row(primary_row=note_row, secondary_row=work_row)
        assert link_row is not None


def test_text_browser_on_tag_creates_and_links_tag(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_on_tag.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_row = Row.from_idless_row_dict(
            db,
            row_dict={
                "work_title": "On Tag Work",
                "work_canonical_title": "On Tag Work",
                "work_sort_title": "On Tag Work",
            },
            table="works",
        )
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("on works {} tag \"Speculative Fiction\"".format(work_row["work_id"]))

        _tag_table, tag_rows = _search_tag_rows(db, "Speculative Fiction")
        assert tag_rows
        tag_row = tag_rows[0]
        link_row = db.get_interlink_row(primary_row=tag_row, secondary_row=work_row)
        assert link_row is not None

    rendered = output.getvalue()
    assert "Tag linked:" in rendered
    assert "metadata writer" in rendered
    assert "metadata report:" in rendered


def test_text_browser_on_tag_supports_multiple_values(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_on_tag_multiple_values.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_row = Row.from_idless_row_dict(
            db,
            row_dict={
                "work_title": "On Tag Multi Work",
                "work_canonical_title": "On Tag Multi Work",
                "work_sort_title": "On Tag Multi Work",
            },
            table="works",
        )
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("on works {} tag \"testing\" \"fish\" \"chips\"".format(work_row["work_id"]))

        expected = ["testing", "fish", "chips"]
        for tag_text in expected:
            _tag_table, rows = _search_tag_rows(db, tag_text)
            assert rows
            tag_row = rows[0]
            link_row = db.get_interlink_row(primary_row=tag_row, secondary_row=work_row)
            assert link_row is not None


def test_text_browser_on_tag_supports_csv_values(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_on_tag_csv_values.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_row = Row.from_idless_row_dict(
            db,
            row_dict={
                "work_title": "On Tag CSV Work",
                "work_canonical_title": "On Tag CSV Work",
                "work_sort_title": "On Tag CSV Work",
            },
            table="works",
        )
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("on works {} tag \"testing,fish,chips,beans\"".format(work_row["work_id"]))

        expected = ["testing", "fish", "chips", "beans"]
        for tag_text in expected:
            _tag_table, rows = _search_tag_rows(db, tag_text)
            assert rows
            tag_row = rows[0]
            link_row = db.get_interlink_row(primary_row=tag_row, secondary_row=work_row)
            assert link_row is not None


def test_text_browser_on_tag_supports_compact_target_and_unquoted_csv(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_on_tag_compact_and_csv.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_row = Row.from_idless_row_dict(
            db,
            row_dict={
                "work_title": "On Tag Compact CSV Work",
                "work_canonical_title": "On Tag Compact CSV Work",
                "work_sort_title": "On Tag Compact CSV Work",
            },
            table="works",
        )
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("on work:{} tag fish,chips,sauce".format(work_row["work_id"]))

        expected = ["fish", "chips", "sauce"]
        for tag_text in expected:
            _tag_table, rows = _search_tag_rows(db, tag_text)
            assert rows
            tag_row = rows[0]
            link_row = db.get_interlink_row(primary_row=tag_row, secondary_row=work_row)
            assert link_row is not None


def test_text_browser_on_legacy_compact_target_kind_then_csv_values(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_on_legacy_compact_kind_then_csv.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_row = Row.from_idless_row_dict(
            db,
            row_dict={
                "work_title": "On Legacy Compact CSV Work",
                "work_canonical_title": "On Legacy Compact CSV Work",
                "work_sort_title": "On Legacy Compact CSV Work",
            },
            table="works",
        )
        shell = TextDatabaseBrowser(db, output=output)
        # Keep legacy ordering support: on <target> <kind> <csv...>
        assert shell.execute_line("on work:{} tag fish,sausage,pies".format(work_row["work_id"]))

        expected = ["fish", "sausage", "pies"]
        for tag_text in expected:
            _tag_table, rows = _search_tag_rows(db, tag_text)
            assert rows
            tag_row = rows[0]
            link_row = db.get_interlink_row(primary_row=tag_row, secondary_row=work_row)
            assert link_row is not None


def test_text_browser_on_tag_subcommand_style(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_on_tag_subcommand.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_row = Row.from_idless_row_dict(
            db,
            row_dict={
                "work_title": "On Tag Subcommand Work",
                "work_canonical_title": "On Tag Subcommand Work",
                "work_sort_title": "On Tag Subcommand Work",
            },
            table="works",
        )
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("on tag work:{} fish,chips,sauce".format(work_row["work_id"]))

        expected = ["fish", "chips", "sauce"]
        for tag_text in expected:
            _tag_table, rows = _search_tag_rows(db, tag_text)
            assert rows
            tag_row = rows[0]
            link_row = db.get_interlink_row(primary_row=tag_row, secondary_row=work_row)
            assert link_row is not None


def test_text_browser_on_tag_subcommand_supports_target_ranges(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_on_tag_target_ranges.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_rows = [
            Row.from_idless_row_dict(
                db,
                row_dict={
                    "work_title": "On Tag Range Work {}".format(i),
                    "work_canonical_title": "On Tag Range Work {}".format(i),
                    "work_sort_title": "On Tag Range Work {}".format(i),
                },
                table="works",
            )
            for i in range(1, 4)
        ]

        first_id = int(work_rows[0]["work_id"])
        last_id = int(work_rows[-1]["work_id"])
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("on tag work:{}-{} fish".format(first_id, last_id))

        _tag_table, rows = _search_tag_rows(db, "fish")
        assert rows
        tag_row = rows[0]

        for work_row in work_rows:
            link_row = db.get_interlink_row(primary_row=tag_row, secondary_row=work_row)
            assert link_row is not None


def test_text_browser_on_tag_bulk_atomic_rollback_on_error(monkeypatch, driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_on_tag_bulk_atomic_rollback.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_rows = [
            Row.from_idless_row_dict(
                db,
                row_dict={
                    "work_title": "On Atomic Work {}".format(i),
                    "work_canonical_title": "On Atomic Work {}".format(i),
                    "work_sort_title": "On Atomic Work {}".format(i),
                },
                table="works",
            )
            for i in range(1, 3)
        ]
        first_id = int(work_rows[0]["work_id"])
        second_id = int(work_rows[1]["work_id"])
        shell = TextDatabaseBrowser(db, output=output)

        original_link_one = on_commands._link_one_value
        call_count = {"n": 0}

        def _flaky_link(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise RuntimeError("synthetic bulk failure")
            return original_link_one(*args, **kwargs)

        monkeypatch.setattr(on_commands, "_link_one_value", _flaky_link)

        with pytest.raises(ValueError, match="Bulk `on` aborted"):
            shell.execute_line("on tag work:{},{} fish".format(first_id, second_id))

        _tag_table, rows = _search_tag_rows(db, "fish")

        if rows:
            tag_row = rows[0]
            assert db.get_interlink_row(primary_row=tag_row, secondary_row=work_rows[0]) is None
            assert db.get_interlink_row(primary_row=tag_row, secondary_row=work_rows[1]) is None


def test_text_browser_on_tag_bulk_best_effort_keeps_successes(monkeypatch, driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_on_tag_bulk_best_effort.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_rows = [
            Row.from_idless_row_dict(
                db,
                row_dict={
                    "work_title": "On Best Effort Work {}".format(i),
                    "work_canonical_title": "On Best Effort Work {}".format(i),
                    "work_sort_title": "On Best Effort Work {}".format(i),
                },
                table="works",
            )
            for i in range(1, 3)
        ]
        first_id = int(work_rows[0]["work_id"])
        second_id = int(work_rows[1]["work_id"])
        shell = TextDatabaseBrowser(db, output=output)

        original_link_one = on_commands._link_one_value
        call_count = {"n": 0}

        def _flaky_link(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise RuntimeError("synthetic best-effort failure")
            return original_link_one(*args, **kwargs)

        monkeypatch.setattr(on_commands, "_link_one_value", _flaky_link)

        assert shell.execute_line("on tag work:{},{} --best-effort fish".format(first_id, second_id))

        _tag_table, rows = _search_tag_rows(db, "fish")
        assert rows
        tag_row = rows[0]
        assert db.get_interlink_row(primary_row=tag_row, secondary_row=work_rows[0]) is not None
        assert db.get_interlink_row(primary_row=tag_row, secondary_row=work_rows[1]) is None


def test_text_browser_on_tag_selector_target_limit(monkeypatch, driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_on_tag_selector_target_limit.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_rows = [
            Row.from_idless_row_dict(
                db,
                row_dict={
                    "work_title": "Selector Limit Work {}".format(i),
                    "work_canonical_title": "Selector Limit Work {}".format(i),
                    "work_sort_title": "Selector Limit Work {}".format(i),
                },
                table="works",
            )
            for i in range(1, 4)
        ]
        shell = TextDatabaseBrowser(db, output=output)
        monkeypatch.setattr(on_commands, "MAX_SELECTOR_TARGETS", 2)
        first_id = int(work_rows[0]["work_id"])
        second_id = int(work_rows[1]["work_id"])
        third_id = int(work_rows[2]["work_id"])
        with pytest.raises(ValueError, match="too many ids"):
            shell.execute_line("on tag work:{},{},{} fish".format(first_id, second_id, third_id))


def test_text_browser_on_tag_selector_range_limit(monkeypatch, driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_on_tag_selector_range_limit.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, output=output)
        monkeypatch.setattr(on_commands, "MAX_SELECTOR_RANGE_SPAN", 2)
        with pytest.raises(ValueError, match="too large"):
            shell.execute_line("on tag work:1-5 fish")


def test_text_browser_off_tag_subcommand_supports_batch_targets(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_off_tag_batch.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_rows = [
            Row.from_idless_row_dict(
                db,
                row_dict={
                    "work_title": "Off Tag Batch Work {}".format(i),
                    "work_canonical_title": "Off Tag Batch Work {}".format(i),
                    "work_sort_title": "Off Tag Batch Work {}".format(i),
                },
                table="works",
            )
            for i in range(1, 4)
        ]
        first_id = int(work_rows[0]["work_id"])
        last_id = int(work_rows[-1]["work_id"])

        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("on tag work:{}-{} fish".format(first_id, last_id))
        assert shell.execute_line("off tag work:{},{} fish".format(first_id, last_id))

        _tag_table, rows = _search_tag_rows(db, "fish")
        assert rows
        tag_row = rows[0]

        assert db.get_interlink_row(primary_row=tag_row, secondary_row=work_rows[0]) is None
        assert db.get_interlink_row(primary_row=tag_row, secondary_row=work_rows[2]) is None
        assert db.get_interlink_row(primary_row=tag_row, secondary_row=work_rows[1]) is not None


def test_text_browser_off_tag_bulk_atomic_rollback_on_error(monkeypatch, driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_off_tag_bulk_atomic_rollback.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_rows = [
            Row.from_idless_row_dict(
                db,
                row_dict={
                    "work_title": "Off Atomic Work {}".format(i),
                    "work_canonical_title": "Off Atomic Work {}".format(i),
                    "work_sort_title": "Off Atomic Work {}".format(i),
                },
                table="works",
            )
            for i in range(1, 3)
        ]
        first_id = int(work_rows[0]["work_id"])
        second_id = int(work_rows[1]["work_id"])
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("on tag work:{},{} fish".format(first_id, second_id))

        original_unlink_one = off_commands._unlink_one_value
        call_count = {"n": 0}

        def _flaky_unlink(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise RuntimeError("synthetic unlink failure")
            return original_unlink_one(*args, **kwargs)

        monkeypatch.setattr(off_commands, "_unlink_one_value", _flaky_unlink)

        with pytest.raises(ValueError, match="Bulk `off` aborted"):
            shell.execute_line("off tag work:{},{} fish".format(first_id, second_id))

        _tag_table, rows = _search_tag_rows(db, "fish")
        assert rows
        tag_row = rows[0]
        assert db.get_interlink_row(primary_row=tag_row, secondary_row=work_rows[0]) is not None
        assert db.get_interlink_row(primary_row=tag_row, secondary_row=work_rows[1]) is not None


def test_text_browser_off_legacy_compact_target_kind_then_csv_values(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_off_legacy_compact_kind_then_csv.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_row = Row.from_idless_row_dict(
            db,
            row_dict={
                "work_title": "Off Legacy Compact CSV Work",
                "work_canonical_title": "Off Legacy Compact CSV Work",
                "work_sort_title": "Off Legacy Compact CSV Work",
            },
            table="works",
        )
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("on tag work:{} fish,sausage,pies".format(work_row["work_id"]))
        assert shell.execute_line("off work:{} tag fish,sausage,pies".format(work_row["work_id"]))

        for tag_text in ["fish", "sausage", "pies"]:
            _tag_table, rows = _search_tag_rows(db, tag_text)
            assert rows
            tag_row = rows[0]
            link_row = db.get_interlink_row(primary_row=tag_row, secondary_row=work_row)
            assert link_row is None


def test_text_browser_on_language_subcommand_style(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_on_language_subcommand.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_row = Row.from_idless_row_dict(
            db,
            row_dict={
                "work_title": "On Language Subcommand Work",
                "work_canonical_title": "On Language Subcommand Work",
                "work_sort_title": "On Language Subcommand Work",
            },
            table="works",
        )
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("on language work:{} eng".format(work_row["work_id"]))

        language_rows = db.search("languages", "language_code", "eng")
        assert language_rows
        language_row = language_rows[0]
        link_row = db.get_interlink_row(primary_row=language_row, secondary_row=work_row)
        assert link_row is not None

    rendered = output.getvalue()
    assert "Language linked:" in rendered
    assert "metadata report:" not in rendered


def test_text_browser_on_series_subcommand_style(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_on_series_subcommand.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_row = Row.from_idless_row_dict(
            db,
            row_dict={
                "work_title": "On Series Subcommand Work",
                "work_canonical_title": "On Series Subcommand Work",
                "work_sort_title": "On Series Subcommand Work",
            },
            table="works",
        )
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("on series work:{} \"Culture\"".format(work_row["work_id"]))

        series_rows = db.search("series", "series", "Culture")
        assert series_rows
        series_row = series_rows[0]
        link_row = db.get_interlink_row(primary_row=series_row, secondary_row=work_row)
        assert link_row is not None


def test_text_browser_on_genre_creates_and_links_genre(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_on_genre.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_row = Row.from_idless_row_dict(
            db,
            row_dict={
                "work_title": "On Genre Work",
                "work_canonical_title": "On Genre Work",
                "work_sort_title": "On Genre Work",
            },
            table="works",
        )
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("on works {} genre \"Science Fiction\"".format(work_row["work_id"]))

        genre_phash = make_title_search_term(standardize_genre("Science Fiction"))
        genre_rows = db.search("genres", "genre_phash", genre_phash)
        assert genre_rows
        genre_row = genre_rows[0]
        link_row = db.get_interlink_row(primary_row=genre_row, secondary_row=work_row)
        assert link_row is not None

    assert "Genre linked:" in output.getvalue()


def test_text_browser_on_subject_creates_and_links_subject(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_on_subject.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_row = Row.from_idless_row_dict(
            db,
            row_dict={
                "work_title": "On Subject Work",
                "work_canonical_title": "On Subject Work",
                "work_sort_title": "On Subject Work",
            },
            table="works",
        )
        shell = TextDatabaseBrowser(db, output=output)
        assert shell.execute_line("on works {} subject \"Polar exploration\"".format(work_row["work_id"]))

        subject_sort = make_title_search_term("Polar exploration")
        subject_rows = db.search("subjects", "subject_sort", subject_sort)
        assert subject_rows
        subject_row = subject_rows[0]
        link_row = db.get_interlink_row(primary_row=subject_row, secondary_row=work_row)
        assert link_row is not None

    assert "Subject linked:" in output.getvalue()


def test_text_browser_on_rejects_unknown_kind(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_on_unknown_kind.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_row = Row.from_idless_row_dict(
            db,
            row_dict={
                "work_title": "On Unknown Work",
                "work_canonical_title": "On Unknown Work",
                "work_sort_title": "On Unknown Work",
            },
            table="works",
        )
        shell = TextDatabaseBrowser(db, output=output)
        with pytest.raises(ValueError, match="Unsupported `on` kind"):
            shell.execute_line("on works {} publisher \"Ace\"".format(work_row["work_id"]))


def test_text_browser_new_genre_wizard_creates_genre(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_genre.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        parent_row = Row.from_idless_row_dict(
            db,
            row_dict={
                "genre": "Fiction",
                "genre_sort": "Fiction",
                "genre_phash": make_title_search_term("Fiction"),
            },
            table="genres",
        )
        parent_id = int(parent_row["genre_id"])

        input_stream = io.StringIO(
            "\n".join(
                [
                    "Science Fiction",
                    "",
                    "",
                    str(parent_id),
                    "2",
                    "Fiction > Science Fiction",
                    "",
                ]
            )
            + "\n"
        )
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        assert shell.run_commands(["add genre"]) == 0

        rows = db.search("genres", "genre", "Science Fiction")
        assert rows
        row = rows[0]
        assert row["genre_sort"] == standardize_genre("Science Fiction")
        assert row["genre_phash"] == make_title_search_term(standardize_genre("Science Fiction"))
        assert int(row["genre_parent_id"]) == parent_id
        assert int(row["genre_position"]) == 2
        assert row["genre_full"] == "Fiction > Science Fiction"

    rendered = output.getvalue()
    assert "New genre wizard" in rendered
    assert "Genre created:" in rendered


def test_text_browser_new_genre_invalid_parent_id_rejected(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_genre_invalid.sqlite"
    output = io.StringIO()
    input_stream = io.StringIO(
        "\n".join(
            [
                "Genre X",
                "",
                "",
                "abc",
            ]
        )
        + "\n"
    )
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        with pytest.raises(ValueError):
            shell.execute_line("add genre")


def test_text_browser_new_genre_legacy_alias_still_works(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_genre_legacy.sqlite"
    output = io.StringIO()
    input_stream = io.StringIO(
        "\n".join(
            [
                "Legacy Genre",
                "",
                "",
                "",
                "",
                "",
                "",
            ]
        )
        + "\n"
    )
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        assert shell.run_commands(["new-genre"]) == 0
        rows = db.search("genres", "genre", "Legacy Genre")
        assert rows


def test_text_browser_new_subject_wizard_creates_subject(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_subject.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        parent_row = Row.from_idless_row_dict(
            db,
            row_dict={
                "subject": "History",
                "subject_sort": make_title_search_term("History"),
            },
            table="subjects",
        )
        parent_id = int(parent_row["subject_id"])

        input_stream = io.StringIO(
            "\n".join(
                [
                    "Space Exploration",
                    "",
                    str(parent_id),
                    "",
                ]
            )
            + "\n"
        )
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        assert shell.run_commands(["add subject"]) == 0

        rows = db.search("subjects", "subject", "Space Exploration")
        assert rows
        row = rows[0]
        assert row["subject_sort"] == make_title_search_term("Space Exploration")
        assert int(row["subject_parent_id"]) == parent_id

    rendered = output.getvalue()
    assert "New subject wizard" in rendered
    assert "Subject created:" in rendered


def test_text_browser_new_subject_invalid_parent_id_rejected(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_subject_invalid.sqlite"
    output = io.StringIO()
    input_stream = io.StringIO(
        "\n".join(
            [
                "Subject X",
                "",
                "abc",
            ]
        )
        + "\n"
    )
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        with pytest.raises(ValueError):
            shell.execute_line("add subject")


def test_text_browser_new_subject_legacy_alias_still_works(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_subject_legacy.sqlite"
    output = io.StringIO()
    input_stream = io.StringIO(
        "\n".join(
            [
                "Legacy Subject",
                "",
                "",
                "",
            ]
        )
        + "\n"
    )
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        assert shell.run_commands(["new-subject"]) == 0
        rows = db.search("subjects", "subject", "Legacy Subject")
        assert rows


def test_text_browser_new_series_wizard_creates_series(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_series.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        parent_row = Row.from_idless_row_dict(
            db,
            row_dict={"series": "Main Saga", "series_sort": "Main Saga"},
            table="series",
        )
        creator_row = Row.from_idless_row_dict(
            db,
            row_dict={
                "agent_type": "person",
                "agent_canonical_name": "Ursula K. Le Guin",
                "agent_sort_name": "Le Guin, Ursula K.",
            },
            table="agents",
        )

        input_stream = io.StringIO(
            "\n".join(
                [
                    "Hainish Cycle",
                    "",
                    "",
                    str(parent_row["series_id"]),
                    "1",
                    "Main Saga > Hainish Cycle",
                    "y",
                    str(creator_row["agent_id"]),
                    "",
                ]
            )
            + "\n"
        )
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        assert shell.run_commands(["add series"]) == 0

        rows = db.search("series", "series", "Hainish Cycle")
        assert rows
        row = rows[0]
        assert row["series_sort"] == "Hainish Cycle"
        assert int(row["series_parent_id"]) == int(parent_row["series_id"])
        assert int(row["series_parent_position"]) == 1
        assert row["series_full"] == "Main Saga > Hainish Cycle"
        assert int(row["series_over_author"]) == 1

        linked_agents = db.get_interlinked_rows(target_row=row, secondary_table="agents")
        assert any(int(agent["agent_id"]) == int(creator_row["agent_id"]) for agent in linked_agents)

    rendered = output.getvalue()
    assert "New series wizard" in rendered
    assert "Series created:" in rendered


def test_text_browser_new_series_invalid_parent_id_rejected(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_series_invalid.sqlite"
    output = io.StringIO()
    input_stream = io.StringIO(
        "\n".join(
            [
                "Series X",
                "",
                "",
                "abc",
            ]
        )
        + "\n"
    )
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        with pytest.raises(ValueError):
            shell.execute_line("add series")


def test_text_browser_new_series_legacy_alias_still_works(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_series_legacy.sqlite"
    output = io.StringIO()
    input_stream = io.StringIO(
        "\n".join(
            [
                "Legacy Series",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
            ]
        )
        + "\n"
    )
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        assert shell.run_commands(["new-series"]) == 0
        rows = db.search("series", "series", "Legacy Series")
        assert rows


def test_text_browser_new_series_group_alias_works(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_series_group_alias.sqlite"
    output = io.StringIO()
    input_stream = io.StringIO(
        "\n".join(
            [
                "Grouped Alias Series",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
            ]
        )
        + "\n"
    )
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        assert shell.run_commands(["new series"]) == 0
        rows = db.search("series", "series", "Grouped Alias Series")
        assert rows


def test_text_browser_new_organisation_wizard_creates_org_agent(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_organisation.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        parent_agent = Row.from_idless_row_dict(
            db,
            row_dict={
                "agent_type": "organisation",
                "agent_canonical_name": "Parent Org",
                "agent_sort_name": "Parent Org",
            },
            table="agents",
        )
        Row.from_idless_row_dict(
            db,
            row_dict={"org_agent_agent_id": parent_agent["agent_id"], "org_agent_legal_name": "Parent Org Ltd"},
            table="org_agents",
        )

        input_stream = io.StringIO(
            "\n".join(
                [
                    "Orbit House",
                    "",
                    "Orbit,Orbit House",
                    "Imported from partner feed",
                    "Orbit House Ltd",
                    "Orbit",
                    "REG-001",
                    "UK",
                    "1999-01-01",
                    "",
                    "https://orbit.example.com",
                    "contact@orbit.example.com",
                    "Speculative fiction publisher",
                    str(parent_agent["agent_id"]),
                    "",
                    "Imprint relationship",
                    "",
                    "Publishes SF/F titles",
                    "",
                ]
            )
            + "\n"
        )
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        assert shell.run_commands(["add organisation"]) == 0

        agent_rows = db.search("agents", "agent_canonical_name", "Orbit House")
        assert agent_rows
        agent_row = agent_rows[0]
        assert agent_row["agent_type"] == "organisation"

        org_rows = db.search("org_agents", "org_agent_agent_id", agent_row["agent_id"])
        assert len(org_rows) == 1
        org_row = org_rows[0]
        assert org_row["org_agent_legal_name"] == "Orbit House Ltd"
        assert org_row["org_agent_website"] == "https://orbit.example.com"
        assert org_row["org_agent_contact_email"] == "contact@orbit.example.com"

        rel_rows = db.search("org_agent_relations", "org_agent_relation_child_agent_id", agent_row["agent_id"])
        assert rel_rows
        assert int(rel_rows[0]["org_agent_relation_parent_agent_id"]) == int(parent_agent["agent_id"])

        ident_rows = db.search("entity_identifiers", "entity_identifier_entity_id", agent_row["agent_id"])
        assert any(row["entity_identifier_scheme"] == "url" for row in ident_rows)

    rendered = output.getvalue()
    assert "New organisation wizard" in rendered
    assert "Organisation created:" in rendered


def test_text_browser_new_organisation_invalid_parent_id_rejected(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_organisation_invalid.sqlite"
    output = io.StringIO()
    input_stream = io.StringIO(
        "\n".join(
            [
                "Org X",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "abc",
            ]
        )
        + "\n"
    )
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        with pytest.raises(ValueError):
            shell.execute_line("add organisation")


def test_text_browser_new_organisation_legacy_alias_still_works(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_organisation_legacy.sqlite"
    output = io.StringIO()
    input_stream = io.StringIO(
        "\n".join(
            [
                "Legacy Organisation",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
            ]
        )
        + "\n"
    )
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        assert shell.run_commands(["new-organisation"]) == 0
        rows = db.search("agents", "agent_canonical_name", "Legacy Organisation")
        assert rows


def test_text_browser_new_publisher_wizard_creates_publisher(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_publisher.sqlite"
    output = io.StringIO()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        parent_agent = Row.from_idless_row_dict(
            db,
            row_dict={
                "agent_type": "organisation",
                "agent_canonical_name": "Parent Publisher",
                "agent_sort_name": "Parent Publisher",
            },
            table="agents",
        )
        Row.from_idless_row_dict(
            db,
            row_dict={"org_agent_agent_id": parent_agent["agent_id"], "org_agent_legal_name": "Parent Publisher Ltd"},
            table="org_agents",
        )

        input_stream = io.StringIO(
            "\n".join(
                [
                    "Ace Books",
                    "",
                    "acebooks",
                    "Publisher description",
                    "https://en.wikipedia.org/wiki/Ace_Books",
                    "https://ace.example.com",
                    str(parent_agent["agent_id"]),
                    "2",
                    "Parent Publisher > Ace Books",
                    "",
                ]
            )
            + "\n"
        )
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        assert shell.run_commands(["add publisher"]) == 0

        agent_rows = db.search("agents", "agent_canonical_name", "Ace Books")
        assert agent_rows
        agent_row = agent_rows[0]
        assert agent_row["agent_type"] == "organisation"
        assert "publisher_phash:acebooks" in (agent_row["agent_aliases"] or "")

        org_rows = db.search("org_agents", "org_agent_agent_id", agent_row["agent_id"])
        assert len(org_rows) == 1
        org_row = org_rows[0]
        assert org_row["org_agent_website"] == "https://ace.example.com"
        assert org_row["org_agent_description"] == "Publisher description"

        ident_rows = db.search("entity_identifiers", "entity_identifier_entity_id", agent_row["agent_id"])
        schemes = {row["entity_identifier_scheme"] for row in ident_rows}
        assert "wikipedia_url" in schemes
        assert "publisher_phash" in schemes

    rendered = output.getvalue()
    assert "New publisher wizard" in rendered
    assert "Publisher created:" in rendered


def test_text_browser_new_publisher_invalid_parent_id_rejected(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_publisher_invalid.sqlite"
    output = io.StringIO()
    input_stream = io.StringIO(
        "\n".join(
            [
                "Publisher X",
                "",
                "",
                "",
                "",
                "",
                "abc",
            ]
        )
        + "\n"
    )
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        with pytest.raises(ValueError):
            shell.execute_line("add publisher")


def test_text_browser_new_publisher_legacy_alias_still_works(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_publisher_legacy.sqlite"
    output = io.StringIO()
    input_stream = io.StringIO(
        "\n".join(
            [
                "Legacy Publisher",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
            ]
        )
        + "\n"
    )
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        assert shell.run_commands(["new-publisher"]) == 0
        rows = db.search("agents", "agent_canonical_name", "Legacy Publisher")
        assert rows


def test_text_browser_new_title_wizard_creates_wemi_stack(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_title.sqlite"
    output = io.StringIO()
    input_stream = io.StringIO(
        "\n".join(
            [
                "How Much For Just The Planet?",
                "",
                "Roberts, Eric",
                "1987-10-01",
                "1987-10-01",
                "https://en.wikipedia.org/wiki/How_Much_for_Just_the_Planet%3F",
                "novel",
                "novel",
                "132560",
                "terminal_wizard",
                "/tmp/planet_1.epub,/tmp/planet_2.epub",
                "planet_1.epub,planet_2.epub",
                "",
            ]
        )
        + "\n"
    )

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        assert shell.run_commands(["add title"]) == 0

        work_rows = db.search("works", "work_canonical_title", "How Much For Just The Planet?")
        assert work_rows
        work_row = work_rows[0]
        assert work_row["work_type"] == "novel"
        assert work_row["work_creator_sort"] == "Roberts, Eric"

        expression_rows = db.get_interlinked_rows(target_row=work_row, secondary_table="expressions")
        assert len(expression_rows) == 1
        expression_row = expression_rows[0]
        assert expression_row["expression_wordcount"] == 132560
        assert expression_row["expression_fiction_length_category"] == "novel"

        manifestation_rows = db.get_interlinked_rows(target_row=expression_row, secondary_table="manifestations")
        assert len(manifestation_rows) == 1
        manifestation_row = manifestation_rows[0]
        assert manifestation_row["manifestation_format_detail"] == "EPUB"
        assert manifestation_row["manifestation_carrier_type"] == "ebook"

        item_rows = db.search(
            table="items",
            column="item_manifestation_id",
            search_term=manifestation_row["manifestation_id"],
        )
        assert len(item_rows) == 2
        assert sorted(row["item_source_path"] for row in item_rows) == ["/tmp/planet_1.epub", "/tmp/planet_2.epub"]

    rendered = output.getvalue()
    assert "New title wizard" in rendered
    assert "Title created:" in rendered
    assert "items_created=2" in rendered


def test_text_browser_new_title_invalid_wordcount_rejected(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_title_invalid.sqlite"
    output = io.StringIO()
    input_stream = io.StringIO(
        "\n".join(
            [
                "Some Title",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "not-an-int",
            ]
        )
        + "\n"
    )
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        with pytest.raises(ValueError):
            shell.execute_line("add title")


def test_text_browser_new_title_legacy_alias_still_works(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_title_legacy.sqlite"
    output = io.StringIO()
    input_stream = io.StringIO(
        "\n".join(
            [
                "Legacy Title",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "/tmp/legacy.epub",
                "",
                "",
            ]
        )
        + "\n"
    )
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        assert shell.run_commands(["new-title"]) == 0
        work_rows = db.search("works", "work_canonical_title", "Legacy Title")
        assert work_rows


def test_text_browser_new_manifestation_invalid_year_rejected(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_manifestation_invalid.sqlite"
    output = io.StringIO()
    input_stream = io.StringIO(
        "\n".join(
            [
                "Some subtitle",
                "ebook",
                "EPUB",
                "",
                "year-abc",
            ]
        )
        + "\n"
    )
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        with pytest.raises(ValueError):
            shell.execute_line("add manifestation")


def test_text_browser_new_manifestation_legacy_alias_still_works(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "browser_new_manifestation_legacy.sqlite"
    output = io.StringIO()
    input_stream = io.StringIO(
        "\n".join(
            [
                "",
                "ebook",
                "EPUB",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
            ]
        )
        + "\n"
    )
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        shell = TextDatabaseBrowser(db, input=input_stream, output=output)
        assert shell.run_commands(["new-manifestation"]) == 0
        rows = db.search("manifestations", "manifestation_format_detail", "EPUB")
        assert rows


def test_text_browser_main_creates_database_if_missing(driver_spec, tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "new_library" / "created.sqlite"
    assert not db_path.exists()

    rc = browser_main(
        [
            "--database",
            str(db_path),
            "--db-type",
            driver_spec.db_type,
            "--command",
            "tables",
        ]
    )
    assert rc == 0
    assert db_path.exists()

    out = capsys.readouterr().out
    assert "stores" in out


def test_text_browser_main_no_create_if_missing_fails(driver_spec, tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "missing_library" / "missing.sqlite"
    assert not db_path.exists()

    rc = browser_main(
        [
            "--database",
            str(db_path),
            "--db-type",
            driver_spec.db_type,
            "--no-create-if-missing",
            "--command",
            "tables",
        ]
    )
    assert rc == 2
    assert not db_path.exists()
    assert "ERROR:" in capsys.readouterr().err


def test_text_browser_main_create_new_db_wizard(driver_spec, tmp_path: Path, capsys, monkeypatch) -> None:
    db_path = tmp_path / "wizard" / "library.sqlite"
    assert not db_path.exists()

    # path, db_type, create parent, enable storage manager, strict bootstrap, startup-on-add, proceed
    wizard_input = "\n".join(
        [
            "",
            "",
            "y",
            "y",
            "n",
            "n",
            "y",
        ]
    ) + "\n"
    monkeypatch.setattr("sys.stdin", io.StringIO(wizard_input))

    rc = browser_main(
        [
            "--database",
            str(db_path),
            "--db-type",
            driver_spec.db_type,
            "--create-new-db",
            "--command",
            "tables",
        ]
    )
    assert rc == 0
    assert db_path.exists()

    out = capsys.readouterr().out
    assert "Database creation wizard" in out
    assert "Creation summary" in out
    assert "stores" in out


def test_text_browser_main_create_new_db_wizard_cancel(driver_spec, tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "wizard_cancel" / "library.sqlite"
    assert not db_path.exists()

    # path, db_type, create parent, enable storage manager, strict bootstrap, startup-on-add, proceed
    wizard_input = "\n".join(
        [
            "",
            "",
            "y",
            "y",
            "n",
            "n",
            "n",
        ]
    ) + "\n"
    monkeypatch.setattr("sys.stdin", io.StringIO(wizard_input))

    rc = browser_main(
        [
            "--database",
            str(db_path),
            "--db-type",
            driver_spec.db_type,
            "--create-new-db",
            "--command",
            "tables",
        ]
    )
    assert rc == 1
    assert not db_path.exists()
