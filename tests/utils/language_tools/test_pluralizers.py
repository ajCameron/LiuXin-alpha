"""
Provide test pluralizers utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test pluralizers through a consuming regression::

        python -m pytest -q tests/utils/language_tools/test_pluralizers.py
"""
from __future__ import annotations


def test_singular_plural_mapper_basic() -> None:
    """
    Perform the test singular plural mapper basic utility operation under explicit compatibility rules.

    Example:
        Exercise test singular plural mapper basic through a consuming regression::

            python -m pytest -q tests/utils/language_tools/test_pluralizers.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.language_tools.pluralizers import singular_plural_mapper

    assert singular_plural_mapper("cat") == "cats"


def test_plural_singular_mapper_basic() -> None:
    """
    Perform the test plural singular mapper basic utility operation under explicit compatibility rules.

    Example:
        Exercise test plural singular mapper basic through a consuming regression::

            python -m pytest -q tests/utils/language_tools/test_pluralizers.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.language_tools.pluralizers import plural_singular_mapper

    assert plural_singular_mapper("cats") == "cat"


def test_pluralizers_do_not_mutate_input() -> None:
    """
    Perform the test pluralizers do not mutate input utility operation under explicit compatibility rules.

    Example:
        Exercise test pluralizers do not mutate input through a consuming regression::

            python -m pytest -q tests/utils/language_tools/test_pluralizers.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.language_tools.pluralizers import singular_plural_mapper, plural_singular_mapper

    w = "dog"
    _ = singular_plural_mapper(w)
    assert w == "dog"
    w2 = "dogs"
    _ = plural_singular_mapper(w2)
    assert w2 == "dogs"


def test_inflector_pluralize_does_not_raise() -> None:
    """
    Perform the test inflector pluralize does not raise utility operation under explicit compatibility rules.

    Example:
        Exercise test inflector pluralize does not raise through a consuming regression::

            python -m pytest -q tests/utils/language_tools/test_pluralizers.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.libraries.inflector import Inflector

    inf = Inflector()
    assert inf.pluralize("table") == "tables"
    assert inf.pluralize("ox") in ("oxen", "Oxen")  # depending on your casing behavior
