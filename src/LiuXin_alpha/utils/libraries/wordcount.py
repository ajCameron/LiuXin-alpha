#!/usr/bin/env python

"""
Count normalized words in plain or markup-derived text.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise wordcount through a consuming regression::

        python -m pytest -q tests/scripts/test_docstring_migration.py
"""

from __future__ import annotations

IDEOGRAPHIC_SPACE = 0x3000


def is_asian(char: str) -> bool:
    """
    Return ``True`` when ``char`` is an ideographic/asian character.

    Example:
        Exercise is asian through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :param char: Value supplied for char under the utility contract.
    :return: True when the documented condition holds; otherwise False.
    """

    return ord(char) > IDEOGRAPHIC_SPACE


def filter_jchars(char: str) -> str:
    """
    Map asian characters to spaces so non-asian words can be counted.

    Example:
        Exercise filter jchars through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :param char: Value supplied for char under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    return " " if is_asian(char) else char


def nonj_len(text: str) -> int:
    """
    Count non-asian words in ``text``.

    Example:
        Exercise nonj len through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :param text: Text parsed, normalized or rendered.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    return len("".join(filter_jchars(c) for c in text).split())


def get_wordcount(text: str) -> dict[str, int]:
    """
    Return aggregate word/character counts for ``text``.

    Example:
        Exercise get wordcount through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :param text: Text parsed, normalized or rendered.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    characters = len(text)
    chars_no_spaces = sum(1 for c in text if not c.isspace())
    asian_chars = sum(1 for c in text if is_asian(c))
    non_asian_words = nonj_len(text)
    words = non_asian_words + asian_chars
    return {
        "characters": characters,
        "chars_no_spaces": chars_no_spaces,
        "asian_chars": asian_chars,
        "non_asian_words": non_asian_words,
        "words": words,
    }


class _WordCount:
    """
    Provide the WordCount utility contract with explicit state and cleanup behavior.

    Example:
        Exercise  WordCount through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py
    """
    def __init__(self, counts: dict[str, int]):
        """
        Initialize and validate the WordCount state.

        Example:
            Exercise  WordCount.  init   through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param counts: Value supplied for counts under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.__dict__.update(counts)


def get_wordcount_obj(text: str) -> _WordCount:
    """
    Return counts as attribute-access object used by legacy callers.

    Example:
        Exercise get wordcount obj through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :param text: Text parsed, normalized or rendered.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    return _WordCount(get_wordcount(text))
