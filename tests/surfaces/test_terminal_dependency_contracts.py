"""
Provide test terminal dependency contracts utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test terminal dependency contracts through a consuming regression::

        python -m pytest -q tests/surfaces/test_terminal_dependency_contracts.py
"""

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
    """
    Perform the test cold owner imports do not load startup or curses operation under explicit file-format and conversion rules.

    Example:
        Exercise test cold owner imports do not load startup or curses through a consuming regression::

            python -m pytest -q tests/surfaces/test_terminal_dependency_contracts.py


    :param owner: Value supplied for owner under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
    """
    Perform the test plain entry point does not require curses operation under explicit file-format and conversion rules.

    Example:
        Exercise test plain entry point does not require curses through a consuming regression::

            python -m pytest -q tests/surfaces/test_terminal_dependency_contracts.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
    """
    Perform the test typed extensions receive the real browser and shutdown order operation under explicit file-format and conversion rules.

    Example:
        Exercise test typed extensions receive the real browser and shutdown order through a consuming regression::

            python -m pytest -q tests/surfaces/test_terminal_dependency_contracts.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    events = []

    class Command(TerminalCommandAPI[browser.TextDatabaseBrowser]):
        """
        Provide the command contract for validated ebook processing.

        Example:
            Exercise test typed extensions receive the real browser and shutdown order.Command through a consuming regression::

                python -m pytest -q tests/surfaces/test_terminal_dependency_contracts.py
        """

        name = "stage-seven"

        def execute(self, host: browser.TextDatabaseBrowser, args: list[str]) -> bool:
            """
            Perform the execute operation under explicit file-format and conversion rules.

            Example:
                Exercise test typed extensions receive the real browser and shutdown order.Command.execute through a consuming regression::

                    python -m pytest -q tests/surfaces/test_terminal_dependency_contracts.py


            :param host: Value supplied for host under the utility contract.
            :param args: Positional values forwarded to the compatibility implementation.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            events.append(("command", host, tuple(args)))
            host.emit("extension output")
            return True

    class Plugin(TerminalLifecyclePluginAPI[browser.TextDatabaseBrowser]):
        """
        Provide the plugin contract for validated ebook processing.

        Example:
            Exercise test typed extensions receive the real browser and shutdown order.Plugin through a consuming regression::

                python -m pytest -q tests/surfaces/test_terminal_dependency_contracts.py
        """

        def __init__(self, name: str) -> None:
            """
            Initialize and validate the plugin state.

            Example:
                Exercise test typed extensions receive the real browser and shutdown order.Plugin.  init   through a consuming regression::

                    python -m pytest -q tests/surfaces/test_terminal_dependency_contracts.py


            :param name: Field, file, function or resource name addressed by the operation.
            :return: None; validated state is stored on the receiving object.
            """
            self.name = name

        def on_startup(self, host: browser.TextDatabaseBrowser) -> None:
            """
            Perform the on startup operation under explicit file-format and conversion rules.

            Example:
                Exercise test typed extensions receive the real browser and shutdown order.Plugin.on startup through a consuming regression::

                    python -m pytest -q tests/surfaces/test_terminal_dependency_contracts.py


            :param host: Value supplied for host under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            events.append((self.name, host, "startup"))

        def on_shutdown(
            self, host: browser.TextDatabaseBrowser, *, reason: str
        ) -> None:
            """
            Perform the on shutdown operation under explicit file-format and conversion rules.

            Example:
                Exercise test typed extensions receive the real browser and shutdown order.Plugin.on shutdown through a consuming regression::

                    python -m pytest -q tests/surfaces/test_terminal_dependency_contracts.py


            :param host: Value supplied for host under the utility contract.
            :param reason: Value supplied for reason under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
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
    """
    Perform the test extension base keeps required command and optional lifecycle hooks operation under explicit file-format and conversion rules.

    Example:
        Exercise test extension base keeps required command and optional lifecycle hooks through a consuming regression::

            python -m pytest -q tests/surfaces/test_terminal_dependency_contracts.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    with pytest.raises(TypeError, match="abstract"):
        TerminalCommandAPI()
    plugin = TerminalLifecyclePluginAPI[object]()
    assert plugin.on_startup(object()) is None
    assert plugin.on_shutdown(object(), reason="finished") is None

    class ExternalPlugin:
        """
        Provide the externalplugin contract for validated ebook processing.

        Example:
            Exercise test extension base keeps required command and optional lifecycle hooks.ExternalPlugin through a consuming regression::

                python -m pytest -q tests/surfaces/test_terminal_dependency_contracts.py
        """

        pass

    assert not issubclass(ExternalPlugin, TerminalLifecyclePluginAPI)
    assert TerminalLifecyclePluginAPI.register(ExternalPlugin) is ExternalPlugin
    assert isinstance(ExternalPlugin(), TerminalLifecyclePluginAPI)


def test_windowed_composition_binds_the_real_browser_to_its_driver(monkeypatch) -> None:
    """
    Perform the test windowed composition binds the real browser to its driver operation under explicit file-format and conversion rules.

    Example:
        Exercise test windowed composition binds the real browser to its driver through a consuming regression::

            python -m pytest -q tests/surfaces/test_terminal_dependency_contracts.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    ui_module = importlib.import_module(f"{PREFIX}.windowed_ui")
    core = create_autospec(CoreClientAPI, instance=True)
    seen = []

    def run_shell(shell):
        """
        Perform the run shell operation under explicit file-format and conversion rules.

        Example:
            Exercise test windowed composition binds the real browser to its driver.run shell through a consuming regression::

                python -m pytest -q tests/surfaces/test_terminal_dependency_contracts.py


        :param shell: Value supplied for shell under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
    """
    Perform the test entry points keep argument and command mode contracts operation under explicit file-format and conversion rules.

    Example:
        Exercise test entry points keep argument and command mode contracts through a consuming regression::

            python -m pytest -q tests/surfaces/test_terminal_dependency_contracts.py


    :param entry: Value supplied for entry under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :param capsys: Value supplied for capsys under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    with pytest.raises(SystemExit) as exc:
        entry(["--ui-mode", "unsupported"])
    assert exc.value.code == 2
    assert "invalid choice" in capsys.readouterr().err

    def fail_open(*args, **kwargs):
        """
        Perform the fail open operation under explicit file-format and conversion rules.

        Example:
            Exercise test entry points keep argument and command mode contracts.fail open through a consuming regression::

                python -m pytest -q tests/surfaces/test_terminal_dependency_contracts.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise RuntimeError("Core connection failed")

    monkeypatch.setattr(app, "open_surface_core_from_args", fail_open)
    assert (
        entry(["--core-endpoint", "http://example.invalid", "--command", "quit"]) == 2
    )
    assert "ERROR: Core connection failed" in capsys.readouterr().err
