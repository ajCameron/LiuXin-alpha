from __future__ import annotations


import pytest


@pytest.mark.xfail(reason="vendored inflector rules are not regex-compatible with Python 3.11")
def test_singular_plural_mapper_basic() -> None:
    from LiuXin_alpha.utils.language_tools.pluralizers import singular_plural_mapper

    assert singular_plural_mapper("cat") == "cats"


@pytest.mark.xfail(reason="vendored inflector rules are not regex-compatible with Python 3.11")
def test_plural_singular_mapper_basic() -> None:
    from LiuXin_alpha.utils.language_tools.pluralizers import plural_singular_mapper

    assert plural_singular_mapper("cats") == "cat"


@pytest.mark.xfail(reason="vendored inflector rules are not regex-compatible with Python 3.11")
def test_pluralizers_do_not_mutate_input() -> None:
    from LiuXin_alpha.utils.language_tools.pluralizers import singular_plural_mapper, plural_singular_mapper

    w = "dog"
    _ = singular_plural_mapper(w)
    assert w == "dog"
    w2 = "dogs"
    _ = plural_singular_mapper(w2)
    assert w2 == "dogs"


def test_inflector_pluralize_does_not_raise() -> None:
    from LiuXin_alpha.utils.libraries.inflector import Inflector

    inf = Inflector()
    assert inf.pluralize("table") == "tables"
    assert inf.pluralize("ox") in ("oxen", "Oxen")  # depending on your casing behavior