"""Protect terminal owners, optional curses loading, and extension contracts."""

from __future__ import annotations

import importlib
import io
import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import create_autospec

import pytest

from LiuXin_alpha.core import CoreClientAPI
from LiuXin_alpha.surfaces.terminal import app, browser
from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI
from LiuXin_alpha.surfaces.terminal.plugins.base import TerminalLifecyclePluginAPI

ROOT = Path(__file__).resolve().parents[2]
PREFIX = "LiuXin_alpha.surfaces.terminal"


@pytest.mark.parametrize(
    "owner",
    [
        "",
        ".commands.base",
        ".plugins.base",
        ".presentation",
        ".database_creation",
        ".browser",
    ],
)
def test_cold_owner_imports_do_not_load_startup_or_curses(owner: str) -> None:
    forbidden = {
        f"{PREFIX}.text_browser",
        f"{PREFIX}.app",
        f"{PREFIX}.windowed_ui",
        "curses",
    }
    if owner != ".browser":
        forbidden.add(f"{PREFIX}.browser")
    script = (
        "import importlib, sys\n"
        f"importlib.import_module({(PREFIX + owner)!r})\n"
        f"assert not ({forbidden!r} & set(sys.modules)), {forbidden!r} & set(sys.modules)\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=ROOT,
        env={**os.environ, "PYTHONPATH": str(ROOT / "src")},
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_plain_entry_point_does_not_require_curses() -> None:
    script = f"""
import importlib.abc
import sys
class NoCurses(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'curses' or fullname.startswith('curses.'):
            raise ImportError('curses intentionally unavailable')
sys.meta_path.insert(0, NoCurses())
from {PREFIX}.browser import TextDatabaseBrowser
from {PREFIX}.app import build_parser, main
assert build_parser().parse_args(['--database', 'example.sqlite']).ui_mode == 'plain'
try:
    main(['--help'])
except SystemExit as exc:
    assert exc.code == 0
else:
    raise AssertionError('Help must retain its argparse exit')
assert 'curses' not in sys.modules
assert '{PREFIX}.windowed_ui' not in sys.modules
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=ROOT,
        env={**os.environ, "PYTHONPATH": str(ROOT / "src")},
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "LiuXin text browser" in result.stdout


def test_typed_extensions_receive_the_real_browser_and_shutdown_order() -> None:
    events = []

    class Command(TerminalCommandAPI[browser.TextDatabaseBrowser]):
        name = "stage-seven"

        def execute(self, host: browser.TextDatabaseBrowser, args: list[str]) -> bool:
            events.append(("command", host, tuple(args)))
            host.emit("extension output")
            return True

    class Plugin(TerminalLifecyclePluginAPI[browser.TextDatabaseBrowser]):
        def __init__(self, name: str) -> None:
            self.name = name

        def on_startup(self, host: browser.TextDatabaseBrowser) -> None:
            events.append((self.name, host, "startup"))

        def on_shutdown(
            self, host: browser.TextDatabaseBrowser, *, reason: str
        ) -> None:
            events.append((self.name, host, reason))

    core = create_autospec(CoreClientAPI, instance=True)
    output = io.StringIO()
    shell = browser.TextDatabaseBrowser(
        core,
        input=io.StringIO(),
        output=output,
        lifecycle_plugins=[Plugin("one"), Plugin("two")],
    )
    shell.register_command(Command())
    assert shell.run_commands(["stage-seven argument", "quit"]) == 0
    assert [event[0] for event in events] == ["one", "two", "command", "two", "one"]
    assert all(event[1] is shell for event in events)
    assert events[2][2] == ("argument",)
    assert events[-1][2] == shell._shutdown_reason
    assert "extension output" in output.getvalue()
    shell.shutdown(reason="second-shutdown")
    assert len(events) == 5


def test_extension_base_keeps_required_command_and_optional_lifecycle_hooks() -> None:
    with pytest.raises(TypeError, match="abstract"):
        TerminalCommandAPI()
    plugin = TerminalLifecyclePluginAPI[object]()
    assert plugin.on_startup(object()) is None
    assert plugin.on_shutdown(object(), reason="finished") is None

    class ExternalPlugin:
        pass

    assert not issubclass(ExternalPlugin, TerminalLifecyclePluginAPI)
    assert TerminalLifecyclePluginAPI.register(ExternalPlugin) is ExternalPlugin
    assert isinstance(ExternalPlugin(), TerminalLifecyclePluginAPI)


def test_windowed_composition_binds_the_real_browser_to_its_driver(monkeypatch) -> None:
    ui_module = importlib.import_module(f"{PREFIX}.windowed_ui")
    core = create_autospec(CoreClientAPI, instance=True)
    seen = []

    def run_shell(shell):
        seen.append(shell)
        assert isinstance(shell, browser.TextDatabaseBrowser)
        assert shell._ui_driver.browser is shell
        assert shell.core is core
        assert shell.page_size == 7
        assert shell._ui_driver.config.status_height == 5
        assert shell._ui_driver.config.job_panel_height == 4
        assert shell._ui_driver.config.telemetry_panel_height == 4
        shell.emit("pane output")
        assert list(shell._ui_driver._lines) == ["pane output"]
        return 37

    monkeypatch.setattr(
        ui_module._CursesUiDriver, "run_with_curses", lambda self, runner: runner()
    )
    monkeypatch.setattr(ui_module._WindowedTextDatabaseBrowser, "run", run_shell)
    assert (
        app.run_windowed_text_browser(
            core,
            page_size=7,
            status_height=1,
            job_panel_height=1,
            telemetry_panel_height=1,
        )
        == 37
    )
    assert len(seen) == 1


@pytest.mark.parametrize("entry", [app.main])
def test_entry_points_keep_argument_and_command_mode_contracts(
    entry, monkeypatch, capsys
) -> None:
    with pytest.raises(SystemExit) as exc:
        entry(["--ui-mode", "unsupported"])
    assert exc.value.code == 2
    assert "invalid choice" in capsys.readouterr().err

    def fail_open(*args, **kwargs):
        raise RuntimeError("Core connection failed")

    monkeypatch.setattr(app, "open_surface_core_from_args", fail_open)
    assert (
        entry(["--core-endpoint", "http://example.invalid", "--command", "quit"]) == 2
    )
    assert "ERROR: Core connection failed" in capsys.readouterr().err
