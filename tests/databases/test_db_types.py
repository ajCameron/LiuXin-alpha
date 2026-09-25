"""
Check database vocabulary enums, registry constants and compatibility type aliases.

Assertions distinguish required-member subsets from complete set equality, and
runtime singleton checks from static type checking. No database or external fixture
is needed.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/test_db_types.py
"""
from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# IdentifierEntityType
# ---------------------------------------------------------------------------


class TestIdentifierEntityType:
    """
    Check WEMI and agent entity names exposed as string enum values.

    Example:
        >>> TestIdentifierEntityType().test_all_expected_members()
    """
    def test_all_expected_members(self) -> None:
        """
        Require work, expression, manifestation, item and agent values without asserting exhaustive membership.

        Example:
            >>> TestIdentifierEntityType().test_all_expected_members()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.db_types import IdentifierEntityType

        values = {e.value for e in IdentifierEntityType}
        assert "work" in values
        assert "expression" in values
        assert "manifestation" in values
        assert "item" in values
        assert "agent" in values

    def test_is_str_subclass(self) -> None:
        """
        Check that WORK is a string equal to work.

        Example:
            >>> TestIdentifierEntityType().test_is_str_subclass()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.db_types import IdentifierEntityType

        assert isinstance(IdentifierEntityType.WORK, str)
        assert IdentifierEntityType.WORK == "work"

    def test_comparison_with_plain_string(self) -> None:
        """
        Compare AGENT and ITEM members directly with their lowercase strings.

        Example:
            >>> TestIdentifierEntityType().test_comparison_with_plain_string()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.db_types import IdentifierEntityType

        assert IdentifierEntityType.AGENT == "agent"
        assert IdentifierEntityType.ITEM == "item"


# ---------------------------------------------------------------------------
# IdentifierScheme
# ---------------------------------------------------------------------------


class TestIdentifierScheme:
    """
    Check representative identifier schemes and string-enum behavior.

    Example:
        >>> TestIdentifierScheme().test_known_schemes_present()
    """
    def test_known_schemes_present(self) -> None:
        """
        Require the listed ISBN, ASIN, UUID, DOI and URL values without excluding other schemes.

        Example:
            >>> TestIdentifierScheme().test_known_schemes_present()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.db_types import IdentifierScheme

        values = {s.value for s in IdentifierScheme}
        for expected in ("isbn_10", "isbn_13", "asin", "uuid", "doi", "url"):
            assert expected in values, f"{expected!r} missing from IdentifierScheme"

    def test_is_str_subclass(self) -> None:
        """
        Check that DOI is a string equal to doi.

        Example:
            >>> TestIdentifierScheme().test_is_str_subclass()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.db_types import IdentifierScheme

        assert isinstance(IdentifierScheme.DOI, str)
        assert IdentifierScheme.DOI == "doi"


# ---------------------------------------------------------------------------
# ALL_IDENTIFIER_ENTITY_TYPES / ALL_IDENTIFIER_SCHEMES
# ---------------------------------------------------------------------------


class TestAllIdentifierConstants:
    """
    Check tuple containers and set-equivalent coverage of identifier enums.

    Example:
        >>> TestAllIdentifierConstants().test_all_entity_types_is_tuple_of_strings()
    """
    def test_all_entity_types_is_tuple_of_strings(self) -> None:
        """
        Require the entity-name constant to be a tuple containing only strings.

        Example:
            >>> TestAllIdentifierConstants().test_all_entity_types_is_tuple_of_strings()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.db_types import ALL_IDENTIFIER_ENTITY_TYPES

        assert isinstance(ALL_IDENTIFIER_ENTITY_TYPES, tuple)
        assert all(isinstance(v, str) for v in ALL_IDENTIFIER_ENTITY_TYPES)

    def test_all_entity_types_covers_all_enum_members(self) -> None:
        """
        Compare the entity-name constant with all entity enum values as sets.

        Order and duplicate entries are not checked.

        Example:
            >>> TestAllIdentifierConstants().test_all_entity_types_covers_all_enum_members()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.db_types import (
            ALL_IDENTIFIER_ENTITY_TYPES,
            IdentifierEntityType,
        )

        assert set(ALL_IDENTIFIER_ENTITY_TYPES) == {e.value for e in IdentifierEntityType}

    def test_all_schemes_is_tuple_of_strings(self) -> None:
        """
        Require the scheme-name constant to be a tuple containing only strings.

        Example:
            >>> TestAllIdentifierConstants().test_all_schemes_is_tuple_of_strings()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.db_types import ALL_IDENTIFIER_SCHEMES

        assert isinstance(ALL_IDENTIFIER_SCHEMES, tuple)
        assert all(isinstance(v, str) for v in ALL_IDENTIFIER_SCHEMES)

    def test_all_schemes_covers_all_enum_members(self) -> None:
        """
        Compare the scheme-name constant with all scheme enum values as sets.

        Order and duplicate entries are not checked.

        Example:
            >>> TestAllIdentifierConstants().test_all_schemes_covers_all_enum_members()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.db_types import (
            ALL_IDENTIFIER_SCHEMES,
            IdentifierScheme,
        )

        assert set(ALL_IDENTIFIER_SCHEMES) == {s.value for s in IdentifierScheme}


# ---------------------------------------------------------------------------
# Entity-specific identifier scheme frozensets
# ---------------------------------------------------------------------------


class TestEntityIdentifierSchemes:
    """
    Check frozen per-entity scheme registries and WEMI lookup coverage.

    Example:
        >>> TestEntityIdentifierSchemes().test_work_schemes_are_frozenset()
    """
    def test_work_schemes_are_frozenset(self) -> None:
        """
        Require the work scheme registry to be a frozenset.

        Example:
            >>> TestEntityIdentifierSchemes().test_work_schemes_are_frozenset()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.db_types import WORK_IDENTIFIER_SCHEMES

        assert isinstance(WORK_IDENTIFIER_SCHEMES, frozenset)

    def test_expression_schemes_are_frozenset(self) -> None:
        """
        Require the expression scheme registry to be a frozenset.

        Example:
            >>> TestEntityIdentifierSchemes().test_expression_schemes_are_frozenset()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.db_types import EXPRESSION_IDENTIFIER_SCHEMES

        assert isinstance(EXPRESSION_IDENTIFIER_SCHEMES, frozenset)

    def test_manifestation_schemes_are_frozenset(self) -> None:
        """
        Require the manifestation scheme registry to be a frozenset.

        Example:
            >>> TestEntityIdentifierSchemes().test_manifestation_schemes_are_frozenset()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.db_types import MANIFESTATION_IDENTIFIER_SCHEMES

        assert isinstance(MANIFESTATION_IDENTIFIER_SCHEMES, frozenset)

    def test_item_schemes_are_frozenset(self) -> None:
        """
        Require the item scheme registry to be a frozenset.

        Example:
            >>> TestEntityIdentifierSchemes().test_item_schemes_are_frozenset()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.db_types import ITEM_IDENTIFIER_SCHEMES

        assert isinstance(ITEM_IDENTIFIER_SCHEMES, frozenset)

    def test_agent_schemes_are_frozenset(self) -> None:
        """
        Require the agent scheme registry to be a frozenset.

        Example:
            >>> TestEntityIdentifierSchemes().test_agent_schemes_are_frozenset()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.db_types import AGENT_IDENTIFIER_SCHEMES

        assert isinstance(AGENT_IDENTIFIER_SCHEMES, frozenset)

    def test_work_schemes_subset_of_all_schemes(self) -> None:
        """
        Require work schemes to belong to the complete identifier scheme enum.

        Example:
            >>> TestEntityIdentifierSchemes().test_work_schemes_subset_of_all_schemes()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.db_types import (
            IdentifierScheme,
            WORK_IDENTIFIER_SCHEMES,
        )

        all_schemes = set(IdentifierScheme)
        assert WORK_IDENTIFIER_SCHEMES <= all_schemes

    def test_entity_schemes_by_type_has_all_entity_keys(self) -> None:
        """
        Require lookup keys for all four WEMI entity types.

        The assertion allows additional keys and does not assert agent absence.

        Example:
            >>> TestEntityIdentifierSchemes().test_entity_schemes_by_type_has_all_entity_keys()


        :return: None; failed expectations raise AssertionError.
        """
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
        """
        Require observed-item schemes to equal the frozenset of every identifier scheme.

        Example:
            >>> TestEntityIdentifierSchemes().test_observed_item_schemes_is_all_schemes()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.db_types import (
            IdentifierScheme,
            OBSERVED_ITEM_IDENTIFIER_SCHEMES,
        )

        assert OBSERVED_ITEM_IDENTIFIER_SCHEMES == frozenset(IdentifierScheme)


# ---------------------------------------------------------------------------
# MarcRelatorRole
# ---------------------------------------------------------------------------


class TestMarcRelatorRole:
    """
    Check string roles and representative MARC relator codes.

    Example:
        >>> TestMarcRelatorRole().test_is_str_subclass()
    """
    def test_is_str_subclass(self) -> None:
        """
        Require the AUTHOR role member to be a string.

        Example:
            >>> TestMarcRelatorRole().test_is_str_subclass()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.db_types import MarcRelatorRole

        assert isinstance(MarcRelatorRole.AUTHOR, str)

    def test_known_roles_present(self) -> None:
        """
        Require author, editor, translator, illustrator and publisher code values.

        Example:
            >>> TestMarcRelatorRole().test_known_roles_present()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.db_types import MarcRelatorRole

        values = {r.value for r in MarcRelatorRole}
        for code in ("aut", "edt", "trl", "ill", "pbl"):
            assert code in values

    def test_author_value(self) -> None:
        """
        Pin the AUTHOR role to aut.

        Example:
            >>> TestMarcRelatorRole().test_author_value()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.db_types import MarcRelatorRole

        assert MarcRelatorRole.AUTHOR == "aut"

    def test_editor_value(self) -> None:
        """
        Pin the EDITOR role to edt.

        Example:
            >>> TestMarcRelatorRole().test_editor_value()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.db_types import MarcRelatorRole

        assert MarcRelatorRole.EDITOR == "edt"


# ---------------------------------------------------------------------------
# ALL_MARC_RELATOR_ROLES
# ---------------------------------------------------------------------------


class TestAllMarcRelatorRoles:
    """
    Check the role-code tuple and its enum coverage.

    Example:
        >>> TestAllMarcRelatorRoles().test_is_tuple_of_strings()
    """
    def test_is_tuple_of_strings(self) -> None:
        """
        Require all-role codes to be a tuple containing only strings.

        Example:
            >>> TestAllMarcRelatorRoles().test_is_tuple_of_strings()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.db_types import ALL_MARC_RELATOR_ROLES

        assert isinstance(ALL_MARC_RELATOR_ROLES, tuple)
        assert all(isinstance(r, str) for r in ALL_MARC_RELATOR_ROLES)

    def test_covers_all_enum_members(self) -> None:
        """
        Compare all-role codes with the enum values as sets, ignoring order and duplicates.

        Example:
            >>> TestAllMarcRelatorRoles().test_covers_all_enum_members()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.db_types import ALL_MARC_RELATOR_ROLES, MarcRelatorRole

        assert set(ALL_MARC_RELATOR_ROLES) == {r.value for r in MarcRelatorRole}


# ---------------------------------------------------------------------------
# Entity-specific MARC relator role frozensets
# ---------------------------------------------------------------------------


class TestEntityMarcRelatorRoles:
    """
    Check representative roles and frozen registries for each WEMI entity.

    Example:
        >>> TestEntityMarcRelatorRoles().test_work_roles_include_author()
    """
    def test_work_roles_include_author(self) -> None:
        """
        Include AUTHOR among work roles.

        Example:
            >>> TestEntityMarcRelatorRoles().test_work_roles_include_author()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.db_types import MarcRelatorRole, WORK_MARC_RELATOR_ROLES

        assert MarcRelatorRole.AUTHOR in WORK_MARC_RELATOR_ROLES

    def test_expression_roles_include_translator(self) -> None:
        """
        Include TRANSLATOR among expression roles.

        Example:
            >>> TestEntityMarcRelatorRoles().test_expression_roles_include_translator()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.db_types import EXPRESSION_MARC_RELATOR_ROLES, MarcRelatorRole

        assert MarcRelatorRole.TRANSLATOR in EXPRESSION_MARC_RELATOR_ROLES

    def test_manifestation_roles_include_publisher(self) -> None:
        """
        Include PUBLISHER among manifestation roles.

        Example:
            >>> TestEntityMarcRelatorRoles().test_manifestation_roles_include_publisher()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.db_types import MANIFESTATION_MARC_RELATOR_ROLES, MarcRelatorRole

        assert MarcRelatorRole.PUBLISHER in MANIFESTATION_MARC_RELATOR_ROLES

    def test_item_roles_include_owner(self) -> None:
        """
        Include OWNER among item roles.

        Example:
            >>> TestEntityMarcRelatorRoles().test_item_roles_include_owner()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.db_types import ITEM_MARC_RELATOR_ROLES, MarcRelatorRole

        assert MarcRelatorRole.OWNER in ITEM_MARC_RELATOR_ROLES

    def test_entity_roles_by_type_maps_all_wemi(self) -> None:
        """
        Require a frozenset role registry for each of the four WEMI keys.

        Example:
            >>> TestEntityMarcRelatorRoles().test_entity_roles_by_type_maps_all_wemi()


        :return: None; failed expectations raise AssertionError.
        """
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
    """
    Pin relationship cardinality flags and their module-level aliases.

    Example:
        >>> TestTableTypesEnum().test_enum_members()
    """
    def test_enum_members(self) -> None:
        """
        Pin one-one, many-one, many-many and one-many enum values to 0, 1, 2 and 3.

        Example:
            >>> TestTableTypesEnum().test_enum_members()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.db_types import TableTypesEnum

        assert TableTypesEnum.ONE_ONE.value == 0
        assert TableTypesEnum.MANY_ONE.value == 1
        assert TableTypesEnum.MANY_MANY.value == 2
        assert TableTypesEnum.ONE_MANY.value == 3

    def test_module_level_aliases(self) -> None:
        """
        Pin the four module-level cardinality aliases to their expected integer values.

        Example:
            >>> TestTableTypesEnum().test_module_level_aliases()


        :return: None; failed expectations raise AssertionError.
        """
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
    """
    Demonstrate the three annotated TriStateBool values without runtime type enforcement.

    Assignments carry the alias annotation, while assertions check the assigned
    singleton. These tests alone do not establish static checker acceptance.

    Example:
        >>> TestSimpleTypeAliases().test_tri_state_bool_accepts_none()
    """
    def test_tri_state_bool_accepts_none(self) -> None:
        """
        Assign None to a TriStateBool-annotated local and check singleton identity.

        Example:
            >>> TestSimpleTypeAliases().test_tri_state_bool_accepts_none()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.db_types import TriStateBool

        val: TriStateBool = None
        assert val is None

    def test_tri_state_bool_accepts_true(self) -> None:
        """
        Assign True to a TriStateBool-annotated local and check singleton identity.

        Example:
            >>> TestSimpleTypeAliases().test_tri_state_bool_accepts_true()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.db_types import TriStateBool

        val: TriStateBool = True
        assert val is True

    def test_tri_state_bool_accepts_false(self) -> None:
        """
        Assign False to a TriStateBool-annotated local and check singleton identity.

        Example:
            >>> TestSimpleTypeAliases().test_tri_state_bool_accepts_false()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.db_types import TriStateBool

        val: TriStateBool = False
        assert val is False


# ---------------------------------------------------------------------------
# DataTypesEnum
# ---------------------------------------------------------------------------


class TestDataTypesEnum:
    """
    Pin the JSON and text datatype enum strings.

    Example:
        >>> TestDataTypesEnum().test_json_value()
    """
    def test_json_value(self) -> None:
        """
        Require the JSON datatype value to be json.

        Example:
            >>> TestDataTypesEnum().test_json_value()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.db_types import DataTypesEnum

        assert DataTypesEnum.JSON.value == "json"

    def test_text_value(self) -> None:
        """
        Require the TEXT datatype value to be text.

        Example:
            >>> TestDataTypesEnum().test_text_value()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.db_types import DataTypesEnum

        assert DataTypesEnum.TEXT.value == "text"


# ---------------------------------------------------------------------------
# IdentifierScheme string set coverage
# ---------------------------------------------------------------------------


class TestIdentifierSchemeStringSet:
    """
    Pin compatibility spellings for ISBN, URL and archive identifier schemes.

    Example:
        >>> TestIdentifierSchemeStringSet().test_isbn_variants_present()
    """
    def test_isbn_variants_present(self) -> None:
        """
        Check both underscored and compact ISBN-10 and ISBN-13 values.

        Example:
            >>> TestIdentifierSchemeStringSet().test_isbn_variants_present()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.db_types import IdentifierScheme

        assert IdentifierScheme.ISBN_10 == "isbn_10"
        assert IdentifierScheme.ISBN_13 == "isbn_13"
        assert IdentifierScheme.ISBN10 == "isbn10"
        assert IdentifierScheme.ISBN13 == "isbn13"

    def test_url_schemes_present(self) -> None:
        """
        Check the URL and Wikipedia URL scheme strings.

        Example:
            >>> TestIdentifierSchemeStringSet().test_url_schemes_present()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.db_types import IdentifierScheme

        assert IdentifierScheme.URL == "url"
        assert IdentifierScheme.WIKIPEDIA_URL == "wikipedia_url"

    def test_archive_identifiers_present(self) -> None:
        """
        Check hyphenated asset-id and archive-id scheme strings.

        Example:
            >>> TestIdentifierSchemeStringSet().test_archive_identifiers_present()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.db_types import IdentifierScheme

        assert IdentifierScheme.ASSET_ID == "asset-id"
        assert IdentifierScheme.ARCHIVE_ID == "archive-id"
