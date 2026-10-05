"""
Provide test import time logging smoke utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test import time logging smoke through a consuming regression::

        python -m pytest -q tests/utils/test_import_time_logging_smoke.py
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def test_import_time_modules_do_not_print_to_stdout_or_stderr(tmp_path) -> None:
    """
    Perform the test import time modules do not print to stdout or stderr utility operation under explicit compatibility rules.

    Example:
        Exercise test import time modules do not print to stdout or stderr through a consuming regression::

            python -m pytest -q tests/utils/test_import_time_logging_smoke.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    root = Path(__file__).resolve().parents[2]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root / "src")
    env["CALIBRE_CONFIG_DIRECTORY"] = str(tmp_path / "calibre-config")
    env["PYTHONWARNINGS"] = "ignore::SyntaxWarning"

    code = (
        "import importlib\n"
        "mods = [\n"
        "    'LiuXin_alpha.catalog.field_metadata',\n"
        "    'LiuXin_alpha.utils.text.icu',\n"
        "    'LiuXin_alpha.utils.libraries.liuxin_dateutil.tz',\n"
        "    'LiuXin_alpha.file_formats.conversion.plugins.html_input',\n"
        "]\n"
        "for m in mods:\n"
        "    importlib.import_module(m)\n"
    )

    proc = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env=env,
        check=False,
        cwd=root,
    )

    assert proc.returncode == 0, proc.stderr
    assert proc.stdout == ""
    assert proc.stderr == ""
