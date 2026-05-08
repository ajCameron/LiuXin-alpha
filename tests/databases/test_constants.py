"""Tests for LiuXin_alpha.databases.constants and bootstrap_constants.

Covers the module-level constant values that the rest of the databases package
relies on: SPOOL_SIZE, VALID_DATA_TYPES, CUSTOM_DATA_TYPES, and
AGENTS_NULL_CANONICAL_NAME.
"""
from __future__ import annotations


# ---------------------------------------------------------------------------
# constants.py
# ---------------------------------------------------------------------------


class TestValidDataTypes:
    def test_is_frozenset(self) -> None:
        from LiuXin_alpha.databases.constants import VALID_DATA_TYPES

        assert isinstance(VALID_DATA_TYPES, frozenset)

    def test_contains_none_sentinel(self) -> None:
        from LiuXin_alpha.databases.constants import VALID_DATA_TYPES

        assert None in VALID_DATA_TYPES

    def test_contains_expected_datatypes(self) -> None:
        from LiuXin_alpha.databases.constants import VALID_DATA_TYPES

        expected = {"rating", "text", "comments", "datetime", "int", "float", "bool", "series", "composite", "enumeration"}
        for dt in expected:
            assert dt in VALID_DATA_TYPES, f"{dt!r} not in VALID_DATA_TYPES"

    def test_is_immutable(self) -> None:
        from LiuXin_alpha.databases.constants import VALID_DATA_TYPES

        import pytest
        with pytest.raises((AttributeError, TypeError)):
            VALID_DATA_TYPES.add("new_type")  # type: ignore[attr-defined]


class TestCustomDataTypes:
    def test_is_frozenset(self) -> None:
        from LiuXin_alpha.databases.constants import CUSTOM_DATA_TYPES

        assert isinstance(CUSTOM_DATA_TYPES, frozenset)

    def test_excludes_none_sentinel(self) -> None:
        from LiuXin_alpha.databases.constants import CUSTOM_DATA_TYPES

        assert None not in CUSTOM_DATA_TYPES

    def test_is_subset_of_valid_data_types(self) -> None:
        from LiuXin_alpha.databases.constants import CUSTOM_DATA_TYPES, VALID_DATA_TYPES

        assert CUSTOM_DATA_TYPES <= VALID_DATA_TYPES - {None}

    def test_contains_expected_custom_types(self) -> None:
        from LiuXin_alpha.databases.constants import CUSTOM_DATA_TYPES

        for dt in ("text", "int", "float", "bool", "datetime", "rating"):
            assert dt in CUSTOM_DATA_TYPES

    def test_is_immutable(self) -> None:
        from LiuXin_alpha.databases.constants import CUSTOM_DATA_TYPES

        import pytest
        with pytest.raises((AttributeError, TypeError)):
            CUSTOM_DATA_TYPES.add("new_type")  # type: ignore[attr-defined]


class TestSpoolSize:
    def test_spool_size_is_positive_int(self) -> None:
        from LiuXin_alpha.databases.constants import SPOOL_SIZE

        assert isinstance(SPOOL_SIZE, int)
        assert SPOOL_SIZE > 0

    def test_spool_size_is_reasonable_value(self) -> None:
        from LiuXin_alpha.databases.constants import SPOOL_SIZE

        # 30 MB in bytes
        assert SPOOL_SIZE == 30 * 1024 * 1024


# ---------------------------------------------------------------------------
# bootstrap_constants.py
# ---------------------------------------------------------------------------


class TestBootstrapConstants:
    def test_agents_null_canonical_name_is_str(self) -> None:
        from LiuXin_alpha.databases.bootstrap_constants import AGENTS_NULL_CANONICAL_NAME

        assert isinstance(AGENTS_NULL_CANONICAL_NAME, str)

    def test_agents_null_canonical_name_is_non_empty(self) -> None:
        from LiuXin_alpha.databases.bootstrap_constants import AGENTS_NULL_CANONICAL_NAME

        assert len(AGENTS_NULL_CANONICAL_NAME) > 0

    def test_agents_null_canonical_name_is_obviously_fake(self) -> None:
        """The sentinel value should be obviously not a real agent name."""
        from LiuXin_alpha.databases.bootstrap_constants import AGENTS_NULL_CANONICAL_NAME

        # Verify it contains the word "NULL" or "DELIBERATELY" to signal it's a sentinel.
        upper = AGENTS_NULL_CANONICAL_NAME.upper()
        assert "NULL" in upper or "DELIBERATELY" in upper
