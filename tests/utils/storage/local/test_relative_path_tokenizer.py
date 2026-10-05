"""
Provide test relative path tokenizer utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test relative path tokenizer through a consuming regression::

        python -m pytest -q tests/utils/storage/local/test_relative_path_tokenizer.py
"""
from __future__ import annotations

import pytest


def test_relative_path_tokens_same_dir_returns_dot_and_empty_parts() -> None:
    """
    Perform the test relative path tokens same dir returns dot and empty parts utility operation under explicit compatibility rules.

    Example:
        Exercise test relative path tokens same dir returns dot and empty parts through a consuming regression::

            python -m pytest -q tests/utils/storage/local/test_relative_path_tokenizer.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.storage.local.relative_path_tokenizer import relative_path_tokens

    rel, parts = relative_path_tokens("a/b", "a/b")
    assert rel.as_posix() == "."
    assert parts == ()


def test_relative_path_tokens_downwards() -> None:
    """
    Perform the test relative path tokens downwards utility operation under explicit compatibility rules.

    Example:
        Exercise test relative path tokens downwards through a consuming regression::

            python -m pytest -q tests/utils/storage/local/test_relative_path_tokenizer.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.storage.local.relative_path_tokenizer import relative_path_tokens

    rel, parts = relative_path_tokens("a/b", "a/b/c/d")
    assert rel.as_posix() == "c/d"
    assert parts == ("c", "d")


def test_relative_path_tokens_up_and_down() -> None:
    """
    Perform the test relative path tokens up and down utility operation under explicit compatibility rules.

    Example:
        Exercise test relative path tokens up and down through a consuming regression::

            python -m pytest -q tests/utils/storage/local/test_relative_path_tokenizer.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.storage.local.relative_path_tokenizer import relative_path_tokens

    rel, parts = relative_path_tokens("a/b/c", "a/d/e")
    assert rel.as_posix() == "../../d/e"
    assert parts[:2] == ("..", "..")


def test_relative_path_tokens_base_is_file_uses_parent() -> None:
    """
    Perform the test relative path tokens base is file uses parent utility operation under explicit compatibility rules.

    Example:
        Exercise test relative path tokens base is file uses parent through a consuming regression::

            python -m pytest -q tests/utils/storage/local/test_relative_path_tokenizer.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.storage.local.relative_path_tokenizer import relative_path_tokens

    rel, _ = relative_path_tokens("a/b/file.txt", "a/b/target.bin", base_is_file=True)
    assert rel.as_posix() == "target.bin"


def test_relative_path_tokens_mixed_anchored_and_relative_raises() -> None:
    """
    Perform the test relative path tokens mixed anchored and relative raises utility operation under explicit compatibility rules.

    Example:
        Exercise test relative path tokens mixed anchored and relative raises through a consuming regression::

            python -m pytest -q tests/utils/storage/local/test_relative_path_tokenizer.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.storage.local.relative_path_tokenizer import relative_path_tokens

    with pytest.raises(ValueError):
        relative_path_tokens("/abs/base", "rel/target")

    with pytest.raises(ValueError):
        relative_path_tokens("rel/base", "/abs/target")
