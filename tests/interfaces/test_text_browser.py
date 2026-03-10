from __future__ import annotations

import builtins
import io

from pathlib import Path

import pytest

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.interfaces.terminal.commands import off as off_commands
from LiuXin_alpha.interfaces.terminal.commands import on as on_commands
from LiuXin_alpha.interfaces.terminal.plugins import TerminalLifecyclePluginAPI
from LiuXin_alpha.interfaces.terminal.text_browser import TextDatabaseBrowser, main as browser_main
from LiuXin_alpha.metadata.standardization import make_tag_search_term, make_title_search_term, standardize_genre
from LiuXin_alpha.storage.store_backend_plugins.rclone_http_readonly import (
    rclone_http_storage_backend as rclone_backend_module,
)


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
    assert "Database summary" in rendered
    assert "largest_tables" in rendered


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
    assert "inserted_files: 2" in rendered


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
    assert "inserted_files: 2" in rendered


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
    assert "files_total: 2" in rendered
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

        tables = set(db.get_tables())
        if "labels" in tables:
            tag_row = Row.from_idless_row_dict(
                db,
                row_dict={
                    "label_text": "Fish",
                    "label_text_norm": make_tag_search_term("Fish"),
                },
                table="labels",
            )
        elif "tags" in tables:
            tag_row = Row.from_idless_row_dict(
                db,
                row_dict={
                    "tag": "Fish",
                    "tag_phash": make_tag_search_term("Fish"),
                },
                table="tags",
            )
        else:
            pytest.fail("Schema has neither labels nor tags table")

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
    assert "store_id={}".format(store_id) in rendered
    assert "row-compact-store" in rendered


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
        assert shell.execute_line("top store 1")

    rendered = output.getvalue()
    assert "Current table: stores" in rendered
    assert "Schema for stores" in rendered
    assert "stores rows:" in rendered
    assert "Browsing stores rows" in rendered
    assert "Search stores.store_name" in rendered
    assert "Top stores rows" in rendered


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

        tables = set(db.get_tables())
        tag_norm = make_tag_search_term("Space Opera")
        if "labels" in tables:
            rows = db.search("labels", "label_text_norm", tag_norm)
            assert rows
            row = rows[0]
            assert row["label_text"] == "Space Opera"
            assert row["label_description"] == "Retro SF shelf marker"
        elif "tags" in tables:
            rows = db.search("tags", "tag_phash", tag_norm)
            assert rows
            row = rows[0]
            assert row["tag"] == "Space Opera"
        else:
            pytest.fail("Schema has neither labels nor tags table")

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

        tables = set(db.get_tables())
        tag_norm = make_tag_search_term("Legacy Tag")
        if "labels" in tables:
            rows = db.search("labels", "label_text_norm", tag_norm)
            assert rows
        elif "tags" in tables:
            rows = db.search("tags", "tag_phash", tag_norm)
            assert rows
        else:
            pytest.fail("Schema has neither labels nor tags table")


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

        tables = set(db.get_tables())
        norm = make_tag_search_term("Speculative Fiction")
        if "labels" in tables:
            tag_rows = db.search("labels", "label_text_norm", norm)
            assert tag_rows
            tag_row = tag_rows[0]
            link_row = db.get_interlink_row(primary_row=tag_row, secondary_row=work_row)
            assert link_row is not None
        elif "tags" in tables:
            tag_rows = db.search("tags", "tag_phash", norm)
            assert tag_rows
            tag_row = tag_rows[0]
            link_row = db.get_interlink_row(primary_row=tag_row, secondary_row=work_row)
            assert link_row is not None
        else:
            pytest.fail("Schema has neither labels nor tags table")

    assert "Tag linked:" in output.getvalue()


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

        tables = set(db.get_tables())
        expected = ["testing", "fish", "chips"]
        for tag_text in expected:
            norm = make_tag_search_term(tag_text)
            if "labels" in tables:
                rows = db.search("labels", "label_text_norm", norm)
            elif "tags" in tables:
                rows = db.search("tags", "tag_phash", norm)
            else:
                pytest.fail("Schema has neither labels nor tags table")
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

        tables = set(db.get_tables())
        expected = ["testing", "fish", "chips", "beans"]
        for tag_text in expected:
            norm = make_tag_search_term(tag_text)
            if "labels" in tables:
                rows = db.search("labels", "label_text_norm", norm)
            elif "tags" in tables:
                rows = db.search("tags", "tag_phash", norm)
            else:
                pytest.fail("Schema has neither labels nor tags table")
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

        tables = set(db.get_tables())
        expected = ["fish", "chips", "sauce"]
        for tag_text in expected:
            norm = make_tag_search_term(tag_text)
            if "labels" in tables:
                rows = db.search("labels", "label_text_norm", norm)
            elif "tags" in tables:
                rows = db.search("tags", "tag_phash", norm)
            else:
                pytest.fail("Schema has neither labels nor tags table")
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

        tables = set(db.get_tables())
        expected = ["fish", "sausage", "pies"]
        for tag_text in expected:
            norm = make_tag_search_term(tag_text)
            if "labels" in tables:
                rows = db.search("labels", "label_text_norm", norm)
            elif "tags" in tables:
                rows = db.search("tags", "tag_phash", norm)
            else:
                pytest.fail("Schema has neither labels nor tags table")
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

        tables = set(db.get_tables())
        expected = ["fish", "chips", "sauce"]
        for tag_text in expected:
            norm = make_tag_search_term(tag_text)
            if "labels" in tables:
                rows = db.search("labels", "label_text_norm", norm)
            elif "tags" in tables:
                rows = db.search("tags", "tag_phash", norm)
            else:
                pytest.fail("Schema has neither labels nor tags table")
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

        tables = set(db.get_tables())
        norm = make_tag_search_term("fish")
        if "labels" in tables:
            rows = db.search("labels", "label_text_norm", norm)
        elif "tags" in tables:
            rows = db.search("tags", "tag_phash", norm)
        else:
            pytest.fail("Schema has neither labels nor tags table")
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

        tables = set(db.get_tables())
        norm = make_tag_search_term("fish")
        if "labels" in tables:
            rows = db.search("labels", "label_text_norm", norm)
        elif "tags" in tables:
            rows = db.search("tags", "tag_phash", norm)
        else:
            pytest.fail("Schema has neither labels nor tags table")

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

        tables = set(db.get_tables())
        norm = make_tag_search_term("fish")
        if "labels" in tables:
            rows = db.search("labels", "label_text_norm", norm)
        elif "tags" in tables:
            rows = db.search("tags", "tag_phash", norm)
        else:
            pytest.fail("Schema has neither labels nor tags table")
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

        tables = set(db.get_tables())
        norm = make_tag_search_term("fish")
        if "labels" in tables:
            rows = db.search("labels", "label_text_norm", norm)
        elif "tags" in tables:
            rows = db.search("tags", "tag_phash", norm)
        else:
            pytest.fail("Schema has neither labels nor tags table")
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

        tables = set(db.get_tables())
        norm = make_tag_search_term("fish")
        if "labels" in tables:
            rows = db.search("labels", "label_text_norm", norm)
        elif "tags" in tables:
            rows = db.search("tags", "tag_phash", norm)
        else:
            pytest.fail("Schema has neither labels nor tags table")
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

        tables = set(db.get_tables())
        for tag_text in ["fish", "sausage", "pies"]:
            norm = make_tag_search_term(tag_text)
            if "labels" in tables:
                rows = db.search("labels", "label_text_norm", norm)
            elif "tags" in tables:
                rows = db.search("tags", "tag_phash", norm)
            else:
                pytest.fail("Schema has neither labels nor tags table")
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
