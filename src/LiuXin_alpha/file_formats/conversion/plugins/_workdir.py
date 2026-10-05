"""
Own temporary conversion work directories and deterministic cleanup.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise  workdir through a consuming regression::

        python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py
"""
from __future__ import annotations

from pathlib import Path

from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.ptempfiles import PersistentTemporaryDirectory


def _looks_like_project_root(path: Path) -> bool:
    """
    Perform the looks like project root operation under explicit file-format and conversion rules.

    Example:
        Exercise  looks like project root through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return (path / ".git").exists() and (path / "src" / "LiuXin_alpha").is_dir()


def choose_conversion_workdir(prefix: str) -> str:
    """
    Return a writable conversion workdir.

    Example:
        Exercise choose conversion workdir through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py


    :param prefix: Text prepended to the formatted or selected result.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    cwd = Path.cwd()
    if _looks_like_project_root(cwd):
        workdir = PersistentTemporaryDirectory(prefix=prefix)
        default_log.info(
            f"Using temporary conversion workdir instead of project root: {workdir}"
        )
        return workdir
    return str(cwd)

