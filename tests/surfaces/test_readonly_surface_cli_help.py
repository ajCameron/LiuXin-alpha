"""
Keep cache-startup examples discoverable in all read-only surface entrypoints.

Module-parser checks stay in process. Python and Bash wrapper checks spawn help
commands from the checkout without starting a database or HTTP listener. Help
presence is tested separately from whether a displayed command can serve content.
"""

from __future__ import annotations

import shutil
import subprocess
import sys

from pathlib import Path

import pytest

from LiuXin_alpha.surfaces.api_readonly import build_arg_parser as build_api_arg_parser
from LiuXin_alpha.surfaces.opds_readonly import build_arg_parser as build_opds_arg_parser
from LiuXin_alpha.surfaces.web_calibre_readonly import build_arg_parser as build_calibre_arg_parser
from LiuXin_alpha.surfaces.web_readonly import build_arg_parser as build_web_arg_parser


REPO_ROOT = Path(__file__).resolve().parents[2]


def _assert_cache_startup_help(help_text: str, *, command: str) -> None:
    """
    Require the expected invocation and cache-selection options in rendered help.

    Substring checks intentionally tolerate argparse wrapping and extra prose;
    they do not parse or execute the examples.

    Example:
        >>> _assert_cache_startup_help(build_opds_arg_parser().format_help(), command="python3 -m LiuXin_alpha.surfaces.opds_readonly")


    :param help_text: Complete parser or wrapper standard-output help text.
    :param command: Invocation spelling expected somewhere in that text.
    :return: None when the invocation, three flags, and cache-mode example are present.
    """
    assert command in help_text
    assert "--metadata-read-source" in help_text
    assert "--cache-type" in help_text
    assert "--no-cache-db-fallback" in help_text
    assert "--metadata-read-source cache" in help_text


def test_readonly_surface_module_help_includes_cache_startup_examples() -> None:
    """
    Verify web, Calibre, API, and OPDS parsers advertise module-mode cache startup.

    Example:
        >>> test_readonly_surface_module_help_includes_cache_startup_examples()


    :return: None after all four in-process format_help results satisfy the help contract.
    """
    parser_cases = [
        (build_web_arg_parser(), "PYTHONPATH=src python3 -m LiuXin_alpha.surfaces.web_readonly"),
        (build_calibre_arg_parser(), "PYTHONPATH=src python3 -m LiuXin_alpha.surfaces.web_calibre_readonly"),
        (build_api_arg_parser(), "PYTHONPATH=src python3 -m LiuXin_alpha.surfaces.api_readonly"),
        (build_opds_arg_parser(), "PYTHONPATH=src python3 -m LiuXin_alpha.surfaces.opds_readonly"),
    ]

    for parser, command in parser_cases:
        _assert_cache_startup_help(parser.format_help(), command=command)


def test_readonly_python_wrapper_help_includes_cache_startup_examples() -> None:
    """
    Verify each Python launcher exits successfully and documents cache startup.

    Use the current test interpreter for --help; the launcher's virtualenv check
    and server subprocess are not reached. Capture both streams without a timeout.

    Example:
        >>> test_readonly_python_wrapper_help_includes_cache_startup_examples()  # doctest: +SKIP


    :return: None after all four wrapper subprocesses exit zero and emit the expected help.
    """
    script_cases = [
        "scripts/run_web_readonly.py",
        "scripts/run_web_calibre_readonly.py",
        "scripts/run_api_readonly.py",
        "scripts/run_opds_readonly.py",
    ]

    for script in script_cases:
        completed = subprocess.run(
            [sys.executable, str(REPO_ROOT / script), "--help"],
            cwd=REPO_ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

        assert completed.returncode == 0, completed.stderr
        _assert_cache_startup_help(
            completed.stdout,
            command=script.replace(".sh", ".py"),
        )


def test_readonly_shell_wrapper_help_includes_cache_startup_examples() -> None:
    """
    Verify Bash launchers forward help to their corresponding Python scripts.

    Skip when Bash is unavailable. Each wrapper resolves its own Python command;
    this is not a guarantee that it uses the running pytest interpreter. Capture
    output without a timeout and check examples using the .py invocation spelling.

    Example:
        >>> test_readonly_shell_wrapper_help_includes_cache_startup_examples()  # doctest: +SKIP


    :return: None after all four shell launchers succeed, or pytest's skip outcome without Bash.
    """
    if shutil.which("bash") is None:
        pytest.skip("bash is required for shell wrapper help checks")

    script_cases = [
        "scripts/run_web_readonly.sh",
        "scripts/run_web_calibre_readonly.sh",
        "scripts/run_api_readonly.sh",
        "scripts/run_opds_readonly.sh",
    ]

    for script in script_cases:
        completed = subprocess.run(
            ["bash", str(REPO_ROOT / script), "--help"],
            cwd=REPO_ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

        assert completed.returncode == 0, completed.stderr
        _assert_cache_startup_help(
            completed.stdout,
            command=script.replace(".sh", ".py"),
        )
