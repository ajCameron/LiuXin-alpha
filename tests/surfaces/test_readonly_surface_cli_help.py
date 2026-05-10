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
    assert command in help_text
    assert "--metadata-read-source" in help_text
    assert "--cache-type" in help_text
    assert "--no-cache-db-fallback" in help_text
    assert "--metadata-read-source cache" in help_text


def test_readonly_surface_module_help_includes_cache_startup_examples() -> None:
    parser_cases = [
        (build_web_arg_parser(), "PYTHONPATH=src python3 -m LiuXin_alpha.surfaces.web_readonly"),
        (build_calibre_arg_parser(), "PYTHONPATH=src python3 -m LiuXin_alpha.surfaces.web_calibre_readonly"),
        (build_api_arg_parser(), "PYTHONPATH=src python3 -m LiuXin_alpha.surfaces.api_readonly"),
        (build_opds_arg_parser(), "PYTHONPATH=src python3 -m LiuXin_alpha.surfaces.opds_readonly"),
    ]

    for parser, command in parser_cases:
        _assert_cache_startup_help(parser.format_help(), command=command)


def test_readonly_python_wrapper_help_includes_cache_startup_examples() -> None:
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
        _assert_cache_startup_help(completed.stdout, command=script)


def test_readonly_shell_wrapper_help_includes_cache_startup_examples() -> None:
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
        _assert_cache_startup_help(completed.stdout, command=script)
