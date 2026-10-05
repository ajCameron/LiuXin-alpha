"""
Provide test conversion workdir helper utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test conversion workdir helper through a consuming regression::

        python -m pytest -q tests/file_formats/conversion/plugins/test_conversion_workdir_helper.py
"""
from __future__ import annotations

from pathlib import Path


def test_choose_conversion_workdir_redirects_from_project_root(
    monkeypatch,
    project_root: Path,
) -> None:
    """
    Perform the test choose conversion workdir redirects from project root operation under explicit file-format and conversion rules.

    Example:
        Exercise test choose conversion workdir redirects from project root through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/plugins/test_conversion_workdir_helper.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :param project_root: Value supplied for project root under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.conversion.plugins._workdir import (
        choose_conversion_workdir,
    )

    monkeypatch.chdir(project_root)
    workdir = Path(choose_conversion_workdir("_unit_conv_root"))

    assert workdir.is_dir()
    assert workdir.resolve() != project_root.resolve()


def test_choose_conversion_workdir_keeps_normal_cwd(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """
    Perform the test choose conversion workdir keeps normal cwd operation under explicit file-format and conversion rules.

    Example:
        Exercise test choose conversion workdir keeps normal cwd through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/plugins/test_conversion_workdir_helper.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.conversion.plugins._workdir import (
        choose_conversion_workdir,
    )

    monkeypatch.chdir(tmp_path)
    workdir = Path(choose_conversion_workdir("_unit_conv_tmp"))

    assert workdir.resolve() == tmp_path.resolve()
