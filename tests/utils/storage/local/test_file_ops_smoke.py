from __future__ import annotations

from pathlib import Path

import pytest


def test_standardize_ext() -> None:
    from LiuXin_alpha.utils.storage.local.file_ops import standardize_ext

    assert standardize_ext("txt") == ".txt"
    assert standardize_ext(".txt") == ".txt"
    assert standardize_ext("..txt") == ".txt"
    assert standardize_ext("txt", dotted=False) == "txt"


def test_get_bare_file_name_handles_multiple_dots() -> None:
    from LiuXin_alpha.utils.storage.local.file_ops import get_bare_file_name

    assert get_bare_file_name("/a/b/c.tar.gz") == "c.tar"
    assert get_bare_file_name("noext") == "noext"


def test_get_file_extension_and_rar_helpers() -> None:
    from LiuXin_alpha.utils.storage.local.file_ops import get_file_extension, is_file_extension_rar

    assert get_file_extension("/x/y/z.RAR").lower() == ".rar"
    assert is_file_extension_rar(".rar") is True
    assert is_file_extension_rar(".r00") is True
    assert is_file_extension_rar(".r99") is True
    assert is_file_extension_rar(".zip") is False


def test_make_free_name_increments_until_available() -> None:
    from LiuXin_alpha.utils.storage.local.file_ops import make_free_name

    forbidden = {"a.txt", "a_2.txt", "a_3.txt"}
    assert make_free_name("a.txt", forbidden) == "a_4.txt"


def test_ensure_folder_and_tokenize_path(tmp_path: Path) -> None:
    from LiuXin_alpha.utils.storage.local.file_ops import ensure_folder, tokenize_path, rebuild_file_path

    p = tmp_path / "a" / "b" / "c"
    assert not p.exists()
    ensure_folder(p)
    assert p.exists() and p.is_dir()

    tokens = tokenize_path(str(p))
    assert tokens[-1] == "c"

    rebuilt = rebuild_file_path(tokens)
    assert rebuilt.endswith("c")


def test_compress_dir_raises_if_fs_missing(tmp_path: Path) -> None:
    from LiuXin_alpha.utils.storage.local.file_ops import compress_dir

    (tmp_path / "d").mkdir()
    (tmp_path / "d" / "x.txt").write_text("x")

    with pytest.raises(ImportError):
        compress_dir(str(tmp_path / "d"), str(tmp_path / "out.zip"))
