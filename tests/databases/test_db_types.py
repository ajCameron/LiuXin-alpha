"""Check database-owned compatibility aliases and structural type enums."""

from __future__ import annotations


def test_table_type_enum_members() -> None:
    """Pin the four relationship cardinality values."""
    from LiuXin_alpha.databases.db_types import TableTypesEnum

    assert tuple(member.value for member in TableTypesEnum) == (0, 1, 2, 3)


def test_table_type_module_aliases() -> None:
    """Pin module-level aliases to the corresponding cardinality values."""
    from LiuXin_alpha.databases.db_types import MANY_MANY, MANY_ONE, ONE_MANY, ONE_ONE

    assert (ONE_ONE, MANY_ONE, MANY_MANY, ONE_MANY) == (0, 1, 2, 3)


def test_tri_state_bool_accepts_none() -> None:
    """Demonstrate the nullable branch of the static compatibility alias."""
    from LiuXin_alpha.databases.db_types import TriStateBool

    value: TriStateBool = None
    assert value is None


def test_tri_state_bool_accepts_true() -> None:
    """Demonstrate the true branch of the static compatibility alias."""
    from LiuXin_alpha.databases.db_types import TriStateBool

    value: TriStateBool = True
    assert value is True


def test_tri_state_bool_accepts_false() -> None:
    """Demonstrate the false branch of the static compatibility alias."""
    from LiuXin_alpha.databases.db_types import TriStateBool

    value: TriStateBool = False
    assert value is False


def test_data_type_json_value() -> None:
    """Pin the JSON database datatype enum value."""
    from LiuXin_alpha.databases.db_types import DataTypesEnum

    assert DataTypesEnum.JSON.value == "json"


def test_data_type_text_value() -> None:
    """Pin the text database datatype enum value."""
    from LiuXin_alpha.databases.db_types import DataTypesEnum

    assert DataTypesEnum.TEXT.value == "text"
