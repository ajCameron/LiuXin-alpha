"""
Check shared database datatype sets, spool size and the bootstrap name sentinel.

Assertions cover immutable containers, required members and sentinel conventions
without treating the datatype member lists as exhaustive. Imports happen inside
tests.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/test_constants.py
"""
from __future__ import annotations


# ---------------------------------------------------------------------------
# constants.py
# ---------------------------------------------------------------------------


class TestValidDataTypes:
    """
    Check the immutable valid-type registry and its None sentinel.

    Example:
        >>> TestValidDataTypes().test_is_frozenset()
    """
    def test_is_frozenset(self) -> None:
        """
        Require VALID_DATA_TYPES to be a frozenset.

        Example:
            >>> TestValidDataTypes().test_is_frozenset()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.constants import VALID_DATA_TYPES

        assert isinstance(VALID_DATA_TYPES, frozenset)

    def test_contains_none_sentinel(self) -> None:
        """
        Require the valid-type registry to contain None.

        Example:
            >>> TestValidDataTypes().test_contains_none_sentinel()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.constants import VALID_DATA_TYPES

        assert None in VALID_DATA_TYPES

    def test_contains_expected_datatypes(self) -> None:
        """
        Require the ten listed datatype names without excluding additional names.

        Example:
            >>> TestValidDataTypes().test_contains_expected_datatypes()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.constants import VALID_DATA_TYPES

        expected = {"rating", "text", "comments", "datetime", "int", "float", "bool", "series", "composite", "enumeration"}
        for dt in expected:
            assert dt in VALID_DATA_TYPES, f"{dt!r} not in VALID_DATA_TYPES"

    def test_is_immutable(self) -> None:
        """
        Reject attempts to add a new valid datatype with AttributeError or TypeError.

        Example:
            >>> TestValidDataTypes().test_is_immutable()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.constants import VALID_DATA_TYPES

        import pytest
        with pytest.raises((AttributeError, TypeError)):
            VALID_DATA_TYPES.add("new_type")  # type: ignore[attr-defined]


class TestCustomDataTypes:
    """
    Check custom datatype membership and immutability.

    Example:
        >>> TestCustomDataTypes().test_is_frozenset()
    """
    def test_is_frozenset(self) -> None:
        """
        Require CUSTOM_DATA_TYPES to be a frozenset.

        Example:
            >>> TestCustomDataTypes().test_is_frozenset()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.constants import CUSTOM_DATA_TYPES

        assert isinstance(CUSTOM_DATA_TYPES, frozenset)

    def test_excludes_none_sentinel(self) -> None:
        """
        Exclude None from the custom datatype registry.

        Example:
            >>> TestCustomDataTypes().test_excludes_none_sentinel()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.constants import CUSTOM_DATA_TYPES

        assert None not in CUSTOM_DATA_TYPES

    def test_is_subset_of_valid_data_types(self) -> None:
        """
        Require every custom datatype to be valid and non-None.

        Example:
            >>> TestCustomDataTypes().test_is_subset_of_valid_data_types()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.constants import CUSTOM_DATA_TYPES, VALID_DATA_TYPES

        assert CUSTOM_DATA_TYPES <= VALID_DATA_TYPES - {None}

    def test_contains_expected_custom_types(self) -> None:
        """
        Require the six listed core custom datatypes without asserting exhaustive membership.

        Example:
            >>> TestCustomDataTypes().test_contains_expected_custom_types()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.constants import CUSTOM_DATA_TYPES

        for dt in ("text", "int", "float", "bool", "datetime", "rating"):
            assert dt in CUSTOM_DATA_TYPES

    def test_is_immutable(self) -> None:
        """
        Reject adding a custom datatype with AttributeError or TypeError.

        Example:
            >>> TestCustomDataTypes().test_is_immutable()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.constants import CUSTOM_DATA_TYPES

        import pytest
        with pytest.raises((AttributeError, TypeError)):
            CUSTOM_DATA_TYPES.add("new_type")  # type: ignore[attr-defined]


class TestSpoolSize:
    """
    Check the spool threshold type and configured byte count.

    Example:
        >>> TestSpoolSize().test_spool_size_is_positive_int()
    """
    def test_spool_size_is_positive_int(self) -> None:
        """
        Require SPOOL_SIZE to be an int greater than zero.

        Example:
            >>> TestSpoolSize().test_spool_size_is_positive_int()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.constants import SPOOL_SIZE

        assert isinstance(SPOOL_SIZE, int)
        assert SPOOL_SIZE > 0

    def test_spool_size_is_reasonable_value(self) -> None:
        """
        Pin SPOOL_SIZE to exactly 30 times 1024 squared bytes.

        Example:
            >>> TestSpoolSize().test_spool_size_is_reasonable_value()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.constants import SPOOL_SIZE

        # 30 MB in bytes
        assert SPOOL_SIZE == 30 * 1024 * 1024


# ---------------------------------------------------------------------------
# bootstrap_constants.py
# ---------------------------------------------------------------------------


class TestBootstrapConstants:
    """
    Check that the null-agent canonical name is a recognizable string sentinel.

    Example:
        >>> TestBootstrapConstants().test_agents_null_canonical_name_is_str()
    """
    def test_agents_null_canonical_name_is_str(self) -> None:
        """
        Require the null-agent canonical name to be a string.

        Example:
            >>> TestBootstrapConstants().test_agents_null_canonical_name_is_str()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.bootstrap_constants import AGENTS_NULL_CANONICAL_NAME

        assert isinstance(AGENTS_NULL_CANONICAL_NAME, str)

    def test_agents_null_canonical_name_is_non_empty(self) -> None:
        """
        Require the null-agent canonical name to contain at least one character.

        Example:
            >>> TestBootstrapConstants().test_agents_null_canonical_name_is_non_empty()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.bootstrap_constants import AGENTS_NULL_CANONICAL_NAME

        assert len(AGENTS_NULL_CANONICAL_NAME) > 0

    def test_agents_null_canonical_name_is_obviously_fake(self) -> None:
        """
        Require NULL or DELIBERATELY in the uppercased sentinel name.

        This tests the marker convention, not whether the value could match any real name.

        Example:
            >>> TestBootstrapConstants().test_agents_null_canonical_name_is_obviously_fake()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.bootstrap_constants import AGENTS_NULL_CANONICAL_NAME

        # Verify it contains the word "NULL" or "DELIBERATELY" to signal it's a sentinel.
        upper = AGENTS_NULL_CANONICAL_NAME.upper()
        assert "NULL" in upper or "DELIBERATELY" in upper
