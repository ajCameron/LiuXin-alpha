from __future__ import annotations

from pathlib import Path


def test_choose_conversion_workdir_redirects_from_project_root(
    monkeypatch,
    project_root: Path,
) -> None:
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
    from LiuXin_alpha.file_formats.conversion.plugins._workdir import (
        choose_conversion_workdir,
    )

    monkeypatch.chdir(tmp_path)
    workdir = Path(choose_conversion_workdir("_unit_conv_tmp"))

    assert workdir.resolve() == tmp_path.resolve()
