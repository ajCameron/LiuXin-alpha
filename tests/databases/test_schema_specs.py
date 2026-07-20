"""Tests for LiuXin_alpha.databases.schema_specs.

Covers the dataclasses (StorageColumnSpec, StorageTableSpec, StorageLinkSpec,
StorageSchemaSpec, LinkCapabilities), the enums (RelationKind,
LinkCardinality, LinkKind), and the build_row_dataclass_for_table factory.
"""
from __future__ import annotations

import pytest

from LiuXin_alpha.databases.schema_specs import (
    LinkCardinality,
    LinkCapabilities,
    LinkKind,
    RelationKind,
    StorageColumnSpec,
    StorageLinkSpec,
    StorageSchemaSpec,
    StorageTableSpec,
    build_row_dataclass_for_table,
)


# ---------------------------------------------------------------------------
# RelationKind enum
# ---------------------------------------------------------------------------


class TestRelationKind:
    def test_table_value(self) -> None:
        assert RelationKind.TABLE == "table"

    def test_view_value(self) -> None:
        assert RelationKind.VIEW == "view"

    def test_is_str_subclass(self) -> None:
        assert isinstance(RelationKind.TABLE, str)

    def test_all_members_present(self) -> None:
        members = {m.value for m in RelationKind}
        assert "table" in members
        assert "view" in members


# ---------------------------------------------------------------------------
# LinkCardinality enum
# ---------------------------------------------------------------------------


class TestLinkCardinality:
    def test_all_cardinal_values(self) -> None:
        values = {m.value for m in LinkCardinality}
        assert "one_to_one" in values
        assert "one_to_many" in values
        assert "many_to_one" in values
        assert "many_to_many" in values
        assert "unknown" in values

    def test_is_str_subclass(self) -> None:
        assert isinstance(LinkCardinality.ONE_TO_ONE, str)


# ---------------------------------------------------------------------------
# StorageColumnSpec
# ---------------------------------------------------------------------------


class TestStorageColumnSpec:
    def test_minimal_construction(self) -> None:
        col = StorageColumnSpec(name="id", ordinal=0)
        assert col.name == "id"
        assert col.ordinal == 0
        assert col.nullable is True
        assert col.has_default is False
        assert col.is_primary_key is False
        assert col.is_unique is False

    def test_full_construction(self) -> None:
        col = StorageColumnSpec(
            name="title",
            ordinal=1,
            declared_type="TEXT",
            affinity="TEXT",
            nullable=False,
            has_default=True,
            default_value="",
            is_primary_key=False,
            is_unique=False,
            references_table=None,
            references_column=None,
        )
        assert col.name == "title"
        assert col.affinity == "TEXT"
        assert col.nullable is False

    def test_is_frozen(self) -> None:
        col = StorageColumnSpec(name="x", ordinal=0)
        with pytest.raises((AttributeError, TypeError)):
            col.name = "y"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# StorageTableSpec
# ---------------------------------------------------------------------------


class TestStorageTableSpec:
    def _make_col(self, name: str, ordinal: int, affinity: str = "TEXT") -> StorageColumnSpec:
        return StorageColumnSpec(name=name, ordinal=ordinal, affinity=affinity)

    def test_minimal_construction(self) -> None:
        col = self._make_col("id", 0, "INTEGER")
        spec = StorageTableSpec(
            name="books",
            relation_kind=RelationKind.TABLE,
            columns=(col,),
        )
        assert spec.name == "books"
        assert spec.relation_kind == RelationKind.TABLE
        assert len(spec.columns) == 1
        assert spec.is_main_table is False
        assert spec.is_link_table is False
        assert spec.linked_tables == ()

    def test_with_flags(self) -> None:
        col = self._make_col("id", 0, "INTEGER")
        spec = StorageTableSpec(
            name="titles",
            relation_kind=RelationKind.TABLE,
            columns=(col,),
            is_main_table=True,
            id_column="id",
        )
        assert spec.is_main_table is True
        assert spec.id_column == "id"

    def test_is_frozen(self) -> None:
        col = self._make_col("id", 0)
        spec = StorageTableSpec(
            name="books", relation_kind=RelationKind.TABLE, columns=(col,)
        )
        with pytest.raises((AttributeError, TypeError)):
            spec.name = "other"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# LinkCapabilities
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("type_column", "priority_column", "expected_kind"),
    (
        (None, None, LinkKind.PLAIN),
        ("demo_link_type", None, LinkKind.TYPED),
        (None, "demo_link_priority", LinkKind.PRIORITY),
        (
            "demo_link_type",
            "demo_link_priority",
            LinkKind.TYPED_PRIORITY,
        ),
    ),
)
def test_link_capabilities_four_way_classification(
    type_column: str | None,
    priority_column: str | None,
    expected_kind: LinkKind,
) -> None:
    capabilities = LinkCapabilities(
        primary_table="left",
        secondary_table="right",
        link_table="left_right_links",
        type_column=type_column,
        priority_column=priority_column,
    )

    assert capabilities.typed is (type_column is not None)
    assert capabilities.priority is (priority_column is not None)
    assert capabilities.ordered is capabilities.priority
    assert capabilities.both is (
        type_column is not None and priority_column is not None
    )
    assert capabilities.kind is expected_kind


# ---------------------------------------------------------------------------
# StorageLinkSpec
# ---------------------------------------------------------------------------


class TestStorageLinkSpec:
    def test_minimal_construction(self) -> None:
        link = StorageLinkSpec(
            primary_table="books",
            secondary_table="agents",
            link_table="agent_book_links",
        )
        assert link.primary_table == "books"
        assert link.secondary_table == "agents"
        assert link.cardinality == LinkCardinality.UNKNOWN
        assert link.ordered is False
        assert link.typed is False
        assert link.symmetric is False

    def test_full_construction(self) -> None:
        link = StorageLinkSpec(
            primary_table="titles",
            secondary_table="agents",
            link_table="agent_title_links",
            cardinality=LinkCardinality.MANY_TO_MANY,
            primary_id_col="id",
            secondary_id_col="id",
            primary_link_col="title_id",
            secondary_link_col="agent_id",
            ordered=True,
            typed=True,
            symmetric=False,
        )
        assert link.cardinality == LinkCardinality.MANY_TO_MANY
        assert link.ordered is True

    def test_is_frozen(self) -> None:
        link = StorageLinkSpec(
            primary_table="a",
            secondary_table="b",
            link_table="a_b_links",
        )
        with pytest.raises((AttributeError, TypeError)):
            link.primary_table = "x"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# StorageSchemaSpec
# ---------------------------------------------------------------------------


class TestStorageSchemaSpec:
    def _minimal_table(self) -> StorageTableSpec:
        col = StorageColumnSpec(name="id", ordinal=0, affinity="INTEGER")
        return StorageTableSpec(
            name="test_table",
            relation_kind=RelationKind.TABLE,
            columns=(col,),
        )

    def test_construction(self) -> None:
        table = self._minimal_table()
        schema = StorageSchemaSpec(
            tables={"test_table": table},
            interlinks=(),
            intralinks=(),
        )
        assert "test_table" in schema.tables
        assert schema.interlinks == ()
        assert schema.intralinks == ()


# ---------------------------------------------------------------------------
# build_row_dataclass_for_table
# ---------------------------------------------------------------------------


class TestBuildRowDataclassForTable:
    def _make_table(
        self, name: str, cols: list[tuple[str, str, bool, bool, object]]
    ) -> StorageTableSpec:
        """cols: list of (col_name, affinity, nullable, has_default, default_value)."""
        columns = tuple(
            StorageColumnSpec(
                name=c[0],
                ordinal=i,
                affinity=c[1],
                nullable=c[2],
                has_default=c[3],
                default_value=c[4],
            )
            for i, c in enumerate(cols)
        )
        return StorageTableSpec(
            name=name,
            relation_kind=RelationKind.TABLE,
            columns=columns,
        )

    def test_class_name_derived_from_table(self) -> None:
        table = self._make_table("my_books", [("id", "INTEGER", False, False, None)])
        cls = build_row_dataclass_for_table(table)
        assert cls.__name__ == "MyBooksRow"

    def test_integer_column_type(self) -> None:
        table = self._make_table("items", [("count", "INTEGER", True, False, None)])
        cls = build_row_dataclass_for_table(table)
        instance = cls(count=5)
        assert instance.count == 5

    def test_real_column_type(self) -> None:
        table = self._make_table("items", [("weight", "REAL", True, False, None)])
        cls = build_row_dataclass_for_table(table)
        instance = cls(weight=3.14)
        assert abs(instance.weight - 3.14) < 1e-9

    def test_text_column_type(self) -> None:
        table = self._make_table("items", [("name", "TEXT", True, False, None)])
        cls = build_row_dataclass_for_table(table)
        instance = cls(name="hello")
        assert instance.name == "hello"

    def test_blob_column_type(self) -> None:
        table = self._make_table("items", [("data", "BLOB", True, False, None)])
        cls = build_row_dataclass_for_table(table)
        instance = cls(data=b"\x00\x01")
        assert instance.data == b"\x00\x01"

    def test_unknown_affinity_uses_any(self) -> None:
        table = self._make_table("items", [("misc", "NUMERIC", True, False, None)])
        cls = build_row_dataclass_for_table(table)
        instance = cls(misc="anything")
        assert instance.misc == "anything"

    def test_column_with_default(self) -> None:
        table = self._make_table("items", [("count", "INTEGER", True, True, 0)])
        cls = build_row_dataclass_for_table(table)
        # Default value is used when no argument is provided.
        instance = cls()
        assert instance.count == 0
        # Callers can still override the default with an explicit value.
        override = cls(count=42)
        assert override.count == 42

    def test_multiple_columns(self) -> None:
        table = self._make_table(
            "books",
            [
                ("id", "INTEGER", False, False, None),
                ("title", "TEXT", True, False, None),
                ("rating", "REAL", True, True, 0.0),
            ],
        )
        cls = build_row_dataclass_for_table(table)
        instance = cls(id=1, title="Dune", rating=4.5)
        assert instance.id == 1
        assert instance.title == "Dune"
        assert abs(instance.rating - 4.5) < 1e-9

    def test_returns_class_type(self) -> None:
        table = self._make_table("empty", [])
        cls = build_row_dataclass_for_table(table)
        assert isinstance(cls, type)

    def test_instance_is_mutable(self) -> None:
        table = self._make_table("items", [("value", "INTEGER", True, False, None)])
        cls = build_row_dataclass_for_table(table)
        obj = cls(value=10)
        obj.value = 20
        assert obj.value == 20
