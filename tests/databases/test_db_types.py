"""Tests for LiuXin_alpha.databases.db_types.

Covers the enums, StrEnums, type aliases and frozen-set constants that
define the curated vocabulary for identifier schemes, entity types,
MARC relator roles, and table-type flags.
"""
from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# IdentifierEntityType
# ---------------------------------------------------------------------------


class TestIdentifierEntityType:
    def test_all_expected_members(self) -> None:
        from LiuXin_alpha.databases.db_types import IdentifierEntityType

        values = {e.value for e in IdentifierEntityType}
        assert "work" in values
        assert "expression" in values
        assert "manifestation" in values
        assert "item" in values
        assert "agent" in values

    def test_is_str_subclass(self) -> None:
        from LiuXin_alpha.databases.db_types import IdentifierEntityType

        assert isinstance(IdentifierEntityType.WORK, str)
        assert IdentifierEntityType.WORK == "work"

    def test_comparison_with_plain_string(self) -> None:
        from LiuXin_alpha.databases.db_types import IdentifierEntityType

        assert IdentifierEntityType.AGENT == "agent"
        assert IdentifierEntityType.ITEM == "item"


# ---------------------------------------------------------------------------
# IdentifierScheme
# ---------------------------------------------------------------------------


class TestIdentifierScheme:
    def test_known_schemes_present(self) -> None:
        from LiuXin_alpha.databases.db_types import IdentifierScheme

        values = {s.value for s in IdentifierScheme}
        for expected in ("isbn_10", "isbn_13", "asin", "uuid", "doi", "url"):
            assert expected in values, f"{expected!r} missing from IdentifierScheme"

    def test_is_str_subclass(self) -> None:
        from LiuXin_alpha.databases.db_types import IdentifierScheme

        assert isinstance(IdentifierScheme.DOI, str)
        assert IdentifierScheme.DOI == "doi"


# ---------------------------------------------------------------------------
# ALL_IDENTIFIER_ENTITY_TYPES / ALL_IDENTIFIER_SCHEMES
# ---------------------------------------------------------------------------


class TestAllIdentifierConstants:
    def test_all_entity_types_is_tuple_of_strings(self) -> None:
        from LiuXin_alpha.databases.db_types import ALL_IDENTIFIER_ENTITY_TYPES

        assert isinstance(ALL_IDENTIFIER_ENTITY_TYPES, tuple)
        assert all(isinstance(v, str) for v in ALL_IDENTIFIER_ENTITY_TYPES)

    def test_all_entity_types_covers_all_enum_members(self) -> None:
        from LiuXin_alpha.databases.db_types import (
            ALL_IDENTIFIER_ENTITY_TYPES,
            IdentifierEntityType,
        )

        assert set(ALL_IDENTIFIER_ENTITY_TYPES) == {e.value for e in IdentifierEntityType}

    def test_all_schemes_is_tuple_of_strings(self) -> None:
        from LiuXin_alpha.databases.db_types import ALL_IDENTIFIER_SCHEMES

        assert isinstance(ALL_IDENTIFIER_SCHEMES, tuple)
        assert all(isinstance(v, str) for v in ALL_IDENTIFIER_SCHEMES)

    def test_all_schemes_covers_all_enum_members(self) -> None:
        from LiuXin_alpha.databases.db_types import (
            ALL_IDENTIFIER_SCHEMES,
            IdentifierScheme,
        )

        assert set(ALL_IDENTIFIER_SCHEMES) == {s.value for s in IdentifierScheme}


# ---------------------------------------------------------------------------
# Entity-specific identifier scheme frozensets
# ---------------------------------------------------------------------------


class TestEntityIdentifierSchemes:
    def test_work_schemes_are_frozenset(self) -> None:
        from LiuXin_alpha.databases.db_types import WORK_IDENTIFIER_SCHEMES

        assert isinstance(WORK_IDENTIFIER_SCHEMES, frozenset)

    def test_expression_schemes_are_frozenset(self) -> None:
        from LiuXin_alpha.databases.db_types import EXPRESSION_IDENTIFIER_SCHEMES

        assert isinstance(EXPRESSION_IDENTIFIER_SCHEMES, frozenset)

    def test_manifestation_schemes_are_frozenset(self) -> None:
        from LiuXin_alpha.databases.db_types import MANIFESTATION_IDENTIFIER_SCHEMES

        assert isinstance(MANIFESTATION_IDENTIFIER_SCHEMES, frozenset)

    def test_item_schemes_are_frozenset(self) -> None:
        from LiuXin_alpha.databases.db_types import ITEM_IDENTIFIER_SCHEMES

        assert isinstance(ITEM_IDENTIFIER_SCHEMES, frozenset)

    def test_agent_schemes_are_frozenset(self) -> None:
        from LiuXin_alpha.databases.db_types import AGENT_IDENTIFIER_SCHEMES

        assert isinstance(AGENT_IDENTIFIER_SCHEMES, frozenset)

    def test_work_schemes_subset_of_all_schemes(self) -> None:
        from LiuXin_alpha.databases.db_types import (
            IdentifierScheme,
            WORK_IDENTIFIER_SCHEMES,
        )

        all_schemes = set(IdentifierScheme)
        assert WORK_IDENTIFIER_SCHEMES <= all_schemes

    def test_entity_schemes_by_type_has_all_entity_keys(self) -> None:
        from LiuXin_alpha.databases.db_types import (
            ENTITY_IDENTIFIER_SCHEMES_BY_TYPE,
            IdentifierEntityType,
        )

        # Note: AGENT is intentionally absent from ENTITY_IDENTIFIER_SCHEMES_BY_TYPE
        # since agents have AGENT_IDENTIFIER_SCHEMES but are not catalogued entries.
        expected_keys = {
            IdentifierEntityType.WORK,
            IdentifierEntityType.EXPRESSION,
            IdentifierEntityType.MANIFESTATION,
            IdentifierEntityType.ITEM,
        }
        assert expected_keys <= set(ENTITY_IDENTIFIER_SCHEMES_BY_TYPE.keys())

    def test_observed_item_schemes_is_all_schemes(self) -> None:
        from LiuXin_alpha.databases.db_types import (
            IdentifierScheme,
            OBSERVED_ITEM_IDENTIFIER_SCHEMES,
        )

        assert OBSERVED_ITEM_IDENTIFIER_SCHEMES == frozenset(IdentifierScheme)


# ---------------------------------------------------------------------------
# MarcRelatorRole
# ---------------------------------------------------------------------------


class TestMarcRelatorRole:
    def test_is_str_subclass(self) -> None:
        from LiuXin_alpha.databases.db_types import MarcRelatorRole

        assert isinstance(MarcRelatorRole.AUTHOR, str)

    def test_known_roles_present(self) -> None:
        from LiuXin_alpha.databases.db_types import MarcRelatorRole

        values = {r.value for r in MarcRelatorRole}
        for code in ("aut", "edt", "trl", "ill", "pbl"):
            assert code in values

    def test_author_value(self) -> None:
        from LiuXin_alpha.databases.db_types import MarcRelatorRole

        assert MarcRelatorRole.AUTHOR == "aut"

    def test_editor_value(self) -> None:
        from LiuXin_alpha.databases.db_types import MarcRelatorRole

        assert MarcRelatorRole.EDITOR == "edt"


# ---------------------------------------------------------------------------
# ALL_MARC_RELATOR_ROLES
# ---------------------------------------------------------------------------


class TestAllMarcRelatorRoles:
    def test_is_tuple_of_strings(self) -> None:
        from LiuXin_alpha.databases.db_types import ALL_MARC_RELATOR_ROLES

        assert isinstance(ALL_MARC_RELATOR_ROLES, tuple)
        assert all(isinstance(r, str) for r in ALL_MARC_RELATOR_ROLES)

    def test_covers_all_enum_members(self) -> None:
        from LiuXin_alpha.databases.db_types import ALL_MARC_RELATOR_ROLES, MarcRelatorRole

        assert set(ALL_MARC_RELATOR_ROLES) == {r.value for r in MarcRelatorRole}


# ---------------------------------------------------------------------------
# Entity-specific MARC relator role frozensets
# ---------------------------------------------------------------------------


class TestEntityMarcRelatorRoles:
    def test_work_roles_include_author(self) -> None:
        from LiuXin_alpha.databases.db_types import MarcRelatorRole, WORK_MARC_RELATOR_ROLES

        assert MarcRelatorRole.AUTHOR in WORK_MARC_RELATOR_ROLES

    def test_expression_roles_include_translator(self) -> None:
        from LiuXin_alpha.databases.db_types import EXPRESSION_MARC_RELATOR_ROLES, MarcRelatorRole

        assert MarcRelatorRole.TRANSLATOR in EXPRESSION_MARC_RELATOR_ROLES

    def test_manifestation_roles_include_publisher(self) -> None:
        from LiuXin_alpha.databases.db_types import MANIFESTATION_MARC_RELATOR_ROLES, MarcRelatorRole

        assert MarcRelatorRole.PUBLISHER in MANIFESTATION_MARC_RELATOR_ROLES

    def test_item_roles_include_owner(self) -> None:
        from LiuXin_alpha.databases.db_types import ITEM_MARC_RELATOR_ROLES, MarcRelatorRole

        assert MarcRelatorRole.OWNER in ITEM_MARC_RELATOR_ROLES

    def test_entity_roles_by_type_maps_all_wemi(self) -> None:
        from LiuXin_alpha.databases.db_types import (
            ENTITY_MARC_RELATOR_ROLES_BY_TYPE,
            IdentifierEntityType,
        )

        for entity_type in (
            IdentifierEntityType.WORK,
            IdentifierEntityType.EXPRESSION,
            IdentifierEntityType.MANIFESTATION,
            IdentifierEntityType.ITEM,
        ):
            assert entity_type in ENTITY_MARC_RELATOR_ROLES_BY_TYPE
            assert isinstance(ENTITY_MARC_RELATOR_ROLES_BY_TYPE[entity_type], frozenset)


# ---------------------------------------------------------------------------
# TableTypesEnum and module-level aliases
# ---------------------------------------------------------------------------


class TestTableTypesEnum:
    def test_enum_members(self) -> None:
        from LiuXin_alpha.databases.db_types import TableTypesEnum

        assert TableTypesEnum.ONE_ONE.value == 0
        assert TableTypesEnum.MANY_ONE.value == 1
        assert TableTypesEnum.MANY_MANY.value == 2
        assert TableTypesEnum.ONE_MANY.value == 3

    def test_module_level_aliases(self) -> None:
        from LiuXin_alpha.databases.db_types import (
            MANY_MANY,
            MANY_ONE,
            ONE_MANY,
            ONE_ONE,
        )

        assert ONE_ONE == 0
        assert MANY_ONE == 1
        assert MANY_MANY == 2
        assert ONE_MANY == 3


# ---------------------------------------------------------------------------
# TriStateBool type alias and simple type checks
# ---------------------------------------------------------------------------


class TestSimpleTypeAliases:
    def test_tri_state_bool_accepts_none(self) -> None:
        from LiuXin_alpha.databases.db_types import TriStateBool

        val: TriStateBool = None
        assert val is None

    def test_tri_state_bool_accepts_true(self) -> None:
        from LiuXin_alpha.databases.db_types import TriStateBool

        val: TriStateBool = True
        assert val is True

    def test_tri_state_bool_accepts_false(self) -> None:
        from LiuXin_alpha.databases.db_types import TriStateBool

        val: TriStateBool = False
        assert val is False


# ---------------------------------------------------------------------------
# DataTypesEnum
# ---------------------------------------------------------------------------


class TestDataTypesEnum:
    def test_json_value(self) -> None:
        from LiuXin_alpha.databases.db_types import DataTypesEnum

        assert DataTypesEnum.JSON.value == "json"

    def test_text_value(self) -> None:
        from LiuXin_alpha.databases.db_types import DataTypesEnum

        assert DataTypesEnum.TEXT.value == "text"


# ---------------------------------------------------------------------------
# IdentifierScheme string set coverage
# ---------------------------------------------------------------------------


class TestIdentifierSchemeStringSet:
    def test_isbn_variants_present(self) -> None:
        from LiuXin_alpha.databases.db_types import IdentifierScheme

        assert IdentifierScheme.ISBN_10 == "isbn_10"
        assert IdentifierScheme.ISBN_13 == "isbn_13"
        assert IdentifierScheme.ISBN10 == "isbn10"
        assert IdentifierScheme.ISBN13 == "isbn13"

    def test_url_schemes_present(self) -> None:
        from LiuXin_alpha.databases.db_types import IdentifierScheme

        assert IdentifierScheme.URL == "url"
        assert IdentifierScheme.WIKIPEDIA_URL == "wikipedia_url"

    def test_archive_identifiers_present(self) -> None:
        from LiuXin_alpha.databases.db_types import IdentifierScheme

        assert IdentifierScheme.ASSET_ID == "asset-id"
        assert IdentifierScheme.ARCHIVE_ID == "archive-id"
