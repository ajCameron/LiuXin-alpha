from __future__ import annotations

import pathlib

import pytest

from LiuXin_alpha.utils.storage.local.relative_path_tokenizer import relative_path_tokens


class TestRelativePathTokens:
    def test_same_path_returns_dot(self) -> None:
        base = pathlib.Path("a/b")
        target = pathlib.Path("a/b")
        rel, tokens = relative_path_tokens(base, target)
        assert rel == pathlib.Path(".")
        # In pathlib, Path(".").parts is an empty tuple.
        assert tokens == ()

    def test_sibling(self) -> None:
        base = pathlib.Path("a/b")
        target = pathlib.Path("a/c")
        rel, tokens = relative_path_tokens(base, target)
        assert rel == pathlib.Path("..") / "c"
        assert tokens == ("..", "c")

    def test_child(self) -> None:
        base = pathlib.Path("a/b")
        target = pathlib.Path("a/b/c/d")
        rel, tokens = relative_path_tokens(base, target)
        assert rel == pathlib.Path("c") / "d"
        assert tokens == ("c", "d")

    def test_base_is_file(self) -> None:
        base_file = pathlib.Path("a/b/file.txt")
        target = pathlib.Path("a/b/other.txt")
        rel, tokens = relative_path_tokens(base_file, target, base_is_file=True)
        assert rel == pathlib.Path("other.txt")
        assert tokens == ("other.txt",)

    def test_mixed_absolute_relative_raises(self) -> None:
        with pytest.raises(ValueError):
            relative_path_tokens(pathlib.Path("/a/b"), pathlib.Path("c/d"))
