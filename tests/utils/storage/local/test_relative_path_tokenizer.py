from __future__ import annotations

import pytest


def test_relative_path_tokens_same_dir_returns_dot_and_empty_parts() -> None:
    from LiuXin_alpha.utils.storage.local.relative_path_tokenizer import relative_path_tokens

    rel, parts = relative_path_tokens("a/b", "a/b")
    assert rel.as_posix() == "."
    assert parts == ()


def test_relative_path_tokens_downwards() -> None:
    from LiuXin_alpha.utils.storage.local.relative_path_tokenizer import relative_path_tokens

    rel, parts = relative_path_tokens("a/b", "a/b/c/d")
    assert rel.as_posix() == "c/d"
    assert parts == ("c", "d")


def test_relative_path_tokens_up_and_down() -> None:
    from LiuXin_alpha.utils.storage.local.relative_path_tokenizer import relative_path_tokens

    rel, parts = relative_path_tokens("a/b/c", "a/d/e")
    assert rel.as_posix() == "../../d/e"
    assert parts[:2] == ("..", "..")


def test_relative_path_tokens_base_is_file_uses_parent() -> None:
    from LiuXin_alpha.utils.storage.local.relative_path_tokenizer import relative_path_tokens

    rel, _ = relative_path_tokens("a/b/file.txt", "a/b/target.bin", base_is_file=True)
    assert rel.as_posix() == "target.bin"


def test_relative_path_tokens_mixed_anchored_and_relative_raises() -> None:
    from LiuXin_alpha.utils.storage.local.relative_path_tokenizer import relative_path_tokens

    with pytest.raises(ValueError):
        relative_path_tokens("/abs/base", "rel/target")

    with pytest.raises(ValueError):
        relative_path_tokens("rel/base", "/abs/target")