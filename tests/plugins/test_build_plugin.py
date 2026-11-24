"""
Check that a plugin folder is zipped and installed.

``build_plugin`` imported ``PersistentTemporaryFile`` and ``ZipFile`` from modules
that no longer exist, so it failed before reading the folder.

Example:
    Run this check::

        python -m pytest -q tests/plugins/test_build_plugin.py
"""

from __future__ import annotations

import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from LiuXin_alpha.plugins import application


def test_a_plugin_folder_is_zipped_without_version_control(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    The folder's files go into the zip, its ``.git`` folder does not, and the zip is installed.

    Example:
        >>> test_a_plugin_folder_is_zipped_without_version_control(Path("/tmp"), None)  # doctest: +SKIP


    :param tmp_path: A folder to build in.
    :param monkeypatch: Replaces the installer, which would change the user's plugins.
    :return: None after the zip is checked.
    """
    folder = tmp_path / "myplugin"
    (folder / ".git").mkdir(parents=True)
    (folder / ".git" / "HEAD").write_text("ref: refs/heads/main\n")
    (folder / "__init__.py").write_text("x = 1\n")
    (folder / "plugin-import-name-myplugin.txt").write_text("")
    installed: list[list[str]] = []

    def add_plugin(path: str) -> SimpleNamespace:
        """
        Record what the zip holds instead of installing it.

        Example:
            >>> add_plugin("plugin.zip")  # doctest: +SKIP


        :param path: The zip built from the folder.
        :return: A plugin's name and version, as the installer returns them.
        """
        with zipfile.ZipFile(path) as archive:
            installed.append(sorted(archive.namelist()))
        return SimpleNamespace(name="My plugin", version=(1, 0, 0))

    monkeypatch.setattr(application, "add_plugin", add_plugin)
    application.build_plugin(folder)
    assert installed == [["__init__.py", "plugin-import-name-myplugin.txt"]]
