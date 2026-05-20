from __future__ import annotations


def test_singular_plural_mapper_basic() -> None:
    from LiuXin_alpha.utils.language_tools.pluralizers import singular_plural_mapper

    assert singular_plural_mapper("cat") == "cats"


def test_plural_singular_mapper_basic() -> None:
    from LiuXin_alpha.utils.language_tools.pluralizers import plural_singular_mapper

    assert plural_singular_mapper("cats") == "cat"


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
