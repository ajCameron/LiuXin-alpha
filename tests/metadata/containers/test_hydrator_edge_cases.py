from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.metadata.api import (
    ExpressionRelationLink,
    ManifestationRelationLink,
    WorkRelationLink,
)
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api import (
    ItemRelationLink,
)
from LiuXin_alpha.metadata.containers import (
    ExpressionMetadata,
    ExpressionMetadataHydrator,
    ItemMetadata,
    ItemMetadataHydrator,
    LazyLiuXinWEMIMetadata,
    LazyLiuXinWEMIMetadataHydrator,
    LiuXinWEMIMetadataHydrator,
    ManifestationMetadata,
    ManifestationMetadataHydrator,
    WorkMetadata,
    WorkMetadataHydrator,
)
from tests.metadata.containers.test_expression_metadata_hydrator import (
    _build_fake_database as _build_expression_db,
)
from tests.metadata.containers.test_item_metadata_hydrator import (
    _build_fake_database as _build_item_db,
)
from tests.metadata.containers.test_manifestation_metadata_hydrator import (
    _build_fake_database as _build_manifestation_db,
)
from tests.metadata.containers.test_work_metadata_hydrator import (
    _build_fake_database as _build_work_db,
)


class _MinimalDriverWrapper:
    def get_id_column(self, table: str) -> str:
        return f"{str(table).rstrip('s')}_id"


class _SchemaFailureDatabase:
    driver_wrapper = _MinimalDriverWrapper()

    def get_tables(self, force_refresh: bool = False) -> list[str]:
        raise RuntimeError("schema unavailable")

    def get_tables_and_columns(self) -> dict[str, list[str]]:
        raise RuntimeError("columns unavailable")

    def get_row_from_id(self, table: str, row_id: int) -> None:
        return None


class _ObjectTarget:
    expression_id = "44"


class _BrokenIdColumnDriverWrapper(_MinimalDriverWrapper):
    def get_id_column(self, table: str) -> str:
        raise RuntimeError("id column unavailable")


class _BrokenIdColumnDatabase(_SchemaFailureDatabase):
    driver_wrapper = _BrokenIdColumnDriverWrapper()


def _raise_runtime(*args: Any, **kwargs: Any) -> None:
    raise RuntimeError("boom")


@pytest.mark.parametrize(
    ("hydrator_cls", "message"),
    (
        (WorkMetadataHydrator, "WorkMetadataHydrator requires"),
        (ExpressionMetadataHydrator, "ExpressionMetadataHydrator requires"),
        (ManifestationMetadataHydrator, "ManifestationMetadataHydrator requires"),
        (ItemMetadataHydrator, "ItemMetadataHydrator requires"),
        (LiuXinWEMIMetadataHydrator, "LiuXinWEMIMetadataHydrator requires"),
        (LazyLiuXinWEMIMetadataHydrator, "LazyLiuXinWEMIMetadataHydrator requires"),
    ),
)
def test_hydrators_reject_missing_database(hydrator_cls: type, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        hydrator_cls(None)


@pytest.mark.parametrize(
    "hydrator_cls",
    (
        WorkMetadataHydrator,
        ExpressionMetadataHydrator,
        ManifestationMetadataHydrator,
        ItemMetadataHydrator,
        LazyLiuXinWEMIMetadataHydrator,
    ),
)
def test_hydrators_tolerate_schema_snapshot_failures(hydrator_cls: type) -> None:
    hydrator = hydrator_cls(_SchemaFailureDatabase())

    assert hydrator._tables == set()
    assert hydrator._tables_and_columns == {}


def test_level_hydrators_report_missing_ids_and_unresolved_source_rows() -> None:
    cases = (
        (WorkMetadataHydrator(_build_work_db()), "from_work_id", 999),
        (ExpressionMetadataHydrator(_build_expression_db()), "from_expression_id", 999),
        (
            ManifestationMetadataHydrator(_build_manifestation_db()),
            "from_manifestation_id",
            999,
        ),
        (ItemMetadataHydrator(_build_item_db()), "from_item_id", 999),
    )

    for hydrator, method_name, missing_id in cases:
        with pytest.raises(ValueError):
            getattr(hydrator, method_name)(missing_id)
        with pytest.raises(ValueError):
            hydrator.from_source_row({"unrelated": "value"})


def test_level_hydrators_accept_mapping_only_identity_payloads() -> None:
    work = WorkMetadataHydrator(_build_work_db()).from_source_row(
        {"title_id": "300", "work_title": "Mapping Work"}
    )
    expression = ExpressionMetadataHydrator(_build_expression_db()).from_source_row(
        {"book_expression_id": "200", "expression_label": "Mapping Expression"}
    )
    manifestation = ManifestationMetadataHydrator(_build_manifestation_db()).from_source_row(
        {
            "book_manifestation_id": "100",
            "manifestation_format_detail": "Mapping Manifestation",
        }
    )
    item = ItemMetadataHydrator(_build_item_db()).from_source_row(
        {"item_manifestation_id": "10", "item_type": "mapping-item"}
    )

    assert work.work is not None
    assert work.work.work_id == 300
    assert work.work.work_title == "Mapping Work"
    assert expression.expression is not None
    assert expression.expression.expression_id == 200
    assert expression.expression.expression_label == "Mapping Expression"
    assert manifestation.manifestation is not None
    assert manifestation.manifestation.manifestation_id == 100
    assert manifestation.manifestation.manifestation_format_detail == (
        "Mapping Manifestation"
    )
    assert item.item is not None
    assert item.item.item_manifestation_id == "10"
    assert item.item.item_type == "mapping-item"


def test_hydrator_static_helpers_handle_unknown_and_invalid_values() -> None:
    assert WorkMetadataHydrator._mapping_from(object()) == {}
    assert ExpressionMetadataHydrator._mapping_from(object()) == {}
    assert ManifestationMetadataHydrator._mapping_from(object()) == {}
    assert ItemMetadataHydrator._mapping_from(object()) == {}
    assert WorkMetadataHydrator._extract_known_ids({"work_id": object()})["work_id"] is None
    assert ExpressionMetadataHydrator._extract_known_ids(
        {"expression_id": "not-int"}
    )["expression_id"] is None
    assert ManifestationMetadataHydrator._extract_known_ids(
        {"manifestation_id": object()}
    )["manifestation_id"] is None
    assert ItemMetadataHydrator._extract_known_ids({"item_id": object()})["item_id"] is None

    metadata = WorkMetadata()
    assert (
        LiuXinWEMIMetadataHydrator._first_relation_target_id(
            metadata,
            "not-a-relation",
            "expression_id",
        )
        is None
    )
    metadata.set_relation_links(
        "expressions",
        [
            WorkRelationLink(target={"expression_id": ""}, primary=True),
            WorkRelationLink(target=_ObjectTarget()),
        ],
    )
    assert (
        LiuXinWEMIMetadataHydrator._first_relation_target_id(
            metadata,
            "expressions",
            "expression_id",
        )
        == 44
    )
    metadata.set_relation_links(
        "expressions",
        [WorkRelationLink(target={"expression_id": ""})],
    )
    assert (
        LiuXinWEMIMetadataHydrator._first_relation_target_id(
            metadata,
            "expressions",
            "expression_id",
        )
        is None
    )
    assert LiuXinWEMIMetadataHydrator._prefer_id("5", "6") == 5
    assert LiuXinWEMIMetadataHydrator._prefer_id("", "6") == 6
    assert LiuXinWEMIMetadataHydrator._prefer_id("bad", "also-bad") is None


def test_central_hydrator_identity_dispatch_and_error_paths() -> None:
    db = _build_item_db()
    hydrator = LiuXinWEMIMetadataHydrator(db)

    assert hydrator.get_work_identity(30).work_id == 30
    assert hydrator.get_expression_identity(20).expression_id == 20
    assert hydrator.get_manifestation_identity(10).manifestation_id == 10
    assert hydrator.get_item_identity(1).item_id == 1

    assert hydrator.hydrate_metadata("expression", expression_id=20).expression.expression_id == 20
    assert (
        hydrator.hydrate_metadata(
            "manifestation",
            manifestation_id=10,
        ).manifestation.manifestation_id
        == 10
    )
    assert hydrator.get_item_metadata(source_row={"item_id": 1}).item.item_id == 1
    item_row = db.get_row_from_id("items", 1)
    assert item_row is not None
    assert hydrator.get_item_metadata(source_row=item_row).item.item_id == 1
    assert hydrator.get_liuxin_wemi_metadata(source_row={"item_id": 1}).item.item_id == 1
    assert hydrator.hydrate_metadata("work", source_row={"work_id": 30}).work.work_id == 30
    assert (
        hydrator.hydrate_metadata(
            "expression",
            source_row={"expression_id": 20},
        ).expression.expression_id
        == 20
    )
    assert (
        hydrator.hydrate_metadata(
            "manifestation",
            source_row={"manifestation_id": 10},
        ).manifestation.manifestation_id
        == 10
    )
    assert isinstance(hydrator.hydrate_metadata("liuxin_wemi", item_id=1), object)

    with pytest.raises(ValueError, match="Provide either item_id or source_row"):
        hydrator.get_item_metadata()
    with pytest.raises(ValueError, match="Provide either item_id or source_row"):
        hydrator.get_liuxin_wemi_metadata()
    with pytest.raises(ValueError, match="Could not hydrate metadata kind"):
        hydrator.hydrate_metadata("unknown", item_id=1)
    with pytest.raises(ValueError, match="Could not hydrate metadata kind"):
        hydrator.hydrate_metadata("work")


def test_level_hydrators_accept_direct_source_rows() -> None:
    work_db = _build_work_db()
    work_row = work_db.get_row_from_id("works", 30)
    assert work_row is not None
    assert WorkMetadataHydrator(work_db).from_source_row(work_row).work.work_id == 30

    item_db = _build_item_db()
    item_row = item_db.get_row_from_id("items", 1)
    assert item_row is not None
    assert ItemMetadataHydrator(item_db).from_source_row(item_row).item.item_id == 1


def test_level_hydrator_row_helpers_cover_skip_and_duplicate_paths() -> None:
    work_hydrator = WorkMetadataHydrator(_build_work_db())
    work_db = work_hydrator.db
    expression_row = work_db.get_row_from_id("expressions", 20)
    assert expression_row is not None
    empty_work_row = Row(work_db, row_dict={}, read_only=True)

    assert work_hydrator._row_key(object()) is None
    assert work_hydrator._row_key(empty_work_row) is None
    assert work_hydrator._dedupe_rows([expression_row, expression_row, empty_work_row]) == [
        expression_row
    ]

    work_metadata = WorkMetadata()
    work_hydrator._ensure_row_link(
        work_metadata,
        "expressions",
        empty_work_row,
        type_hint="ignored",
        source_entity_type="test",
    )
    assert work_metadata.get_relation_links("expressions") == []
    work_metadata.add_relation_link(
        "expressions",
        WorkRelationLink(target=expression_row, type="existing"),
    )
    work_hydrator._append_links_unique(
        work_metadata,
        "expressions",
        [WorkRelationLink(target=expression_row, type="incoming")],
    )
    assert [link.type for link in work_metadata.get_relation_links("expressions")] == [
        "existing"
    ]

    expression_hydrator = ExpressionMetadataHydrator(_build_expression_db())
    expression_db = expression_hydrator.db
    work_row = expression_db.get_row_from_id("works", 30)
    assert work_row is not None
    empty_expression_row = Row(expression_db, row_dict={}, read_only=True)
    assert expression_hydrator._row_key(object()) is None
    assert expression_hydrator._row_key(empty_expression_row) is None
    assert expression_hydrator._dedupe_rows([work_row, work_row, empty_expression_row]) == [
        work_row
    ]
    expression_metadata = ExpressionMetadata()
    expression_hydrator._ensure_row_link(
        expression_metadata,
        "works",
        empty_expression_row,
        type_hint="ignored",
        source_entity_type="test",
    )
    assert expression_metadata.get_relation_links("works") == []
    expression_metadata.add_relation_link(
        "works",
        ExpressionRelationLink(target=work_row, type="existing"),
    )
    expression_hydrator._append_links_unique(
        expression_metadata,
        "works",
        [ExpressionRelationLink(target=work_row, type="incoming")],
    )
    assert expression_metadata.get_relation_links("works")[0].type == "incoming"

    manifestation_hydrator = ManifestationMetadataHydrator(_build_manifestation_db())
    manifestation_db = manifestation_hydrator.db
    manifestation_expression_row = manifestation_db.get_row_from_id("expressions", 20)
    assert manifestation_expression_row is not None
    empty_manifestation_row = Row(manifestation_db, row_dict={}, read_only=True)
    assert manifestation_hydrator._row_key(object()) is None
    assert manifestation_hydrator._row_key(empty_manifestation_row) is None
    assert manifestation_hydrator._dedupe_rows(
        [
            manifestation_expression_row,
            manifestation_expression_row,
            empty_manifestation_row,
        ]
    ) == [manifestation_expression_row]
    manifestation_metadata = ManifestationMetadata()
    manifestation_hydrator._ensure_row_link(
        manifestation_metadata,
        "expressions",
        empty_manifestation_row,
        type_hint="ignored",
        source_entity_type="test",
    )
    assert manifestation_metadata.get_relation_links("expressions") == []
    manifestation_metadata.add_relation_link(
        "expressions",
        ManifestationRelationLink(target=manifestation_expression_row, type="existing"),
    )
    manifestation_hydrator._append_links_unique(
        manifestation_metadata,
        "expressions",
        [
            ManifestationRelationLink(
                target=manifestation_expression_row,
                type="incoming",
            )
        ],
    )
    assert manifestation_metadata.get_relation_links("expressions")[0].type == "incoming"

    item_hydrator = ItemMetadataHydrator(_build_item_db())
    item_db = item_hydrator.db
    item_manifestation_row = item_db.get_row_from_id("manifestations", 10)
    assert item_manifestation_row is not None
    empty_item_row = Row(item_db, row_dict={}, read_only=True)
    assert item_hydrator._row_key(object()) is None
    assert item_hydrator._row_key(empty_item_row) is None
    assert item_hydrator._dedupe_rows(
        [item_manifestation_row, item_manifestation_row, empty_item_row]
    ) == [item_manifestation_row]
    item_metadata = ItemMetadata()
    item_hydrator._ensure_row_link(
        item_metadata,
        "manifestations",
        empty_item_row,
        type_hint="ignored",
        source_entity_type="test",
    )
    assert item_metadata.get_relation_links("manifestations") == []
    item_metadata.add_relation_link(
        "manifestations",
        ItemRelationLink(target=item_manifestation_row, type="existing"),
    )
    item_hydrator._ensure_row_link(
        item_metadata,
        "manifestations",
        item_manifestation_row,
        type_hint="ignored-duplicate",
        source_entity_type="test",
    )
    assert item_metadata.get_relation_links("manifestations")[0].type == "existing"
    item_hydrator._append_links_unique(
        item_metadata,
        "manifestations",
        [ItemRelationLink(target=item_manifestation_row, type="incoming")],
    )
    assert item_metadata.get_relation_links("manifestations")[0].type == "incoming"


def test_item_hydrator_uses_source_manifestation_id_when_item_mapping_lacks_one() -> None:
    metadata = ItemMetadataHydrator(_build_item_db()).from_source_row(
        {
            "item_id": "999",
            "manifestation_id": "10",
            "item_type": "mapping-only",
        }
    )

    assert metadata.item is not None
    assert metadata.item.item_id == "999"
    assert metadata.item.item_manifestation_id is None
    assert [
        row.row_id
        for row in metadata.get_related("manifestations")
        if isinstance(row, Row)
    ] == [10]


def test_item_hydrator_skips_non_row_digital_assets_during_replica_resolution() -> None:
    db = _build_item_db()
    hydrator = ItemMetadataHydrator(db)
    original_collect = hydrator._collect_interlinks_from_row

    def collect_with_non_row_asset(
        source_row: Row | None,
        *,
        secondary_table: str,
        source_entity_type: str,
    ) -> list[ItemRelationLink]:
        if secondary_table == "digital_assets":
            return [ItemRelationLink(target="not-a-row")]
        return original_collect(
            source_row,
            secondary_table=secondary_table,
            source_entity_type=source_entity_type,
        )

    hydrator._collect_interlinks_from_row = collect_with_non_row_asset

    metadata = hydrator.from_item_id(1)

    assert metadata.get_related("digital_assets") == ["not-a-row"]
    assert metadata.get_relation_links("asset_replicas") == []


def test_item_hydrator_collect_interlink_edge_paths() -> None:
    hydrator = ItemMetadataHydrator(_build_item_db())
    assert hydrator._collect_interlinks_from_row(
        None,
        secondary_table="tags",
        source_entity_type="item",
    ) == []
    assert hydrator._collect_interlinks_from_row(
        hydrator.db.get_row_from_id("items", 1),
        secondary_table="not_a_table",
        source_entity_type="item",
    ) == []

    db = _build_item_db()
    hydrator = ItemMetadataHydrator(db)
    work_row = db.get_row_from_id("works", 30)
    assert work_row is not None
    db.interlinks[("works", 30, "tags")][0]["tag_work_link_custom"] = "kept"
    db.interlinks[("works", 30, "tags")][0]["unrelated"] = "ignored"
    links = hydrator._collect_interlinks_from_row(
        work_row,
        secondary_table="tags",
        source_entity_type="work",
    )
    assert links[0].extra["custom"] == "kept"
    assert "unrelated" not in links[0].extra

    db = _build_item_db()
    hydrator = ItemMetadataHydrator(db)
    work_row = db.get_row_from_id("works", 30)
    assert work_row is not None
    db.get_interlink_rows = _raise_runtime
    assert hydrator._collect_interlinks_from_row(
        work_row,
        secondary_table="tags",
        source_entity_type="work",
    ) == []

    db = _build_item_db()
    hydrator = ItemMetadataHydrator(db)
    work_row = db.get_row_from_id("works", 30)
    assert work_row is not None
    db.driver_wrapper.get_link_column = _raise_runtime
    assert hydrator._collect_interlinks_from_row(
        work_row,
        secondary_table="tags",
        source_entity_type="work",
    ) == []

    db = _build_item_db()
    hydrator = ItemMetadataHydrator(db)
    work_row = db.get_row_from_id("works", 30)
    assert work_row is not None
    db.driver_wrapper.get_link_column = (
        lambda table1, table2, secondary_id_column: "tag_work_link_tag_id"
    )
    db.driver_wrapper.get_link_table_name = _raise_runtime
    links = hydrator._collect_interlinks_from_row(
        work_row,
        secondary_table="tags",
        source_entity_type="work",
    )
    assert links[0].target.row_dict["tag"] == "Space Opera"

    db = _build_item_db()
    hydrator = ItemMetadataHydrator(db)
    work_row = db.get_row_from_id("works", 30)
    assert work_row is not None
    db.get_row_from_id = _raise_runtime
    assert hydrator._collect_interlinks_from_row(
        work_row,
        secondary_table="tags",
        source_entity_type="work",
    ) == []


def test_item_hydrator_direct_fk_and_identifier_exception_paths() -> None:
    hydrator = ItemMetadataHydrator(_build_item_db())
    assert hydrator._collect_direct_fk_rows(
        table="not_a_table",
        fk_column="file_item_id",
        fk_value=1,
        type_hint="item_file",
    ) == []

    db = _build_item_db()
    hydrator = ItemMetadataHydrator(db)
    db.search = _raise_runtime
    assert hydrator._collect_direct_fk_rows(
        table="files",
        fk_column="file_item_id",
        fk_value=1,
        type_hint="item_file",
    ) == []
    assert hydrator._collect_identifier_rows(
        item_id=1,
        work_rows=[],
        expression_rows=[],
        manifestation_rows=[],
    ) == []

    db = _build_item_db()
    hydrator = ItemMetadataHydrator(db)
    db.add_row(
        "entity_identifiers",
        {
            "entity_identifier_id": 999,
            "entity_identifier_entity_type": "work",
            "entity_identifier_entity_id": 1,
            "entity_identifier_scheme": "wrong",
            "entity_identifier_value": "wrong-type",
        },
    )
    links = hydrator._collect_identifier_rows(
        item_id=1,
        work_rows=[],
        expression_rows=[],
        manifestation_rows=[],
    )
    assert all(
        link.target.row_dict.get("entity_identifier_value") != "wrong-type"
        for link in links
        if isinstance(link.target, Row)
    )


def test_item_hydrator_resolves_folders_and_stores_from_files_and_work_links() -> None:
    db = _build_item_db()
    db.tables_and_columns["folders"] = ["folder_id", "folder_store_id", "folder_path"]
    db.driver_wrapper.tables_and_columns["folders"] = [
        "folder_id",
        "folder_store_id",
        "folder_path",
    ]
    folder = db.add_row(
        "folders",
        {
            "folder_id": 61,
            "folder_store_id": 60,
            "folder_path": "Greg Egan/Permutation City",
        },
    )
    file_row = db.add_row(
        "files",
        {
            "file_id": 500,
            "file_item_id": 1,
            "file_folder_id": 61,
            "file_store_id": 60,
            "file_extension": "epub",
        },
    )
    image_row = db.add_row(
        "images",
        {
            "image_id": 501,
            "image_item_id": 1,
            "image_folder_id": 61,
            "image_role": "cover",
        },
    )
    replica_row = db.add_row(
        "asset_replicas",
        {
            "asset_replica_id": 502,
            "asset_replica_digital_asset_id": 52,
            "asset_replica_folder_id": 61,
            "asset_replica_store_id": 60,
        },
    )
    db.interlinks[("works", 30, "folders")] = [
        {"folder_work_link_folder_id": 61}
    ]
    work_row = db.get_row_from_id("works", 30)
    assert work_row is not None

    hydrator = ItemMetadataHydrator(db)
    metadata = ItemMetadata()
    metadata.add_relation_link("files", ItemRelationLink(target="not-a-row"))
    metadata.add_relation_link("files", ItemRelationLink(target=file_row))
    metadata.add_relation_link("images", ItemRelationLink(target=image_row))
    metadata.add_relation_link("asset_replicas", ItemRelationLink(target=replica_row))

    hydrator._hydrate_folders_and_stores(metadata, work_rows=[work_row])

    assert [link.target for link in metadata.get_relation_links("folders")] == [folder]
    assert [
        link.target.row_dict["store_name"]
        for link in metadata.get_relation_links("stores")
        if isinstance(link.target, Row)
    ] == ["Main Store"]


def test_manifestation_hydrator_uses_explicit_work_and_item_ids_from_mapping() -> None:
    metadata = ManifestationMetadataHydrator(_build_manifestation_db()).from_source_row(
        {
            "book_manifestation_id": "999",
            "manifestation_format_detail": "Mapping Manifestation",
            "work_id": "30",
            "item_id": "1",
        }
    )

    assert metadata.manifestation is not None
    assert metadata.manifestation.manifestation_id == 999
    assert [
        row.row_id
        for row in metadata.get_related("works")
        if isinstance(row, Row)
    ] == [30]
    assert [
        row.row_id
        for row in metadata.get_related("items")
        if isinstance(row, Row)
    ] == [1]


def test_manifestation_hydrator_skips_non_row_and_idless_assets() -> None:
    db = _build_manifestation_db()
    hydrator = ManifestationMetadataHydrator(db)
    original_collect = hydrator._collect_interlinks_from_row
    idless_asset = Row(db, row_dict={"digital_asset_id": None}, read_only=True)

    def collect_with_unusable_assets(
        source_row: Row | None,
        *,
        secondary_table: str,
        source_entity_type: str,
    ) -> list[ManifestationRelationLink]:
        if secondary_table == "digital_assets":
            return [
                ManifestationRelationLink(target="not-a-row"),
                ManifestationRelationLink(target=idless_asset),
            ]
        return original_collect(
            source_row,
            secondary_table=secondary_table,
            source_entity_type=source_entity_type,
        )

    hydrator._collect_interlinks_from_row = collect_with_unusable_assets

    metadata = hydrator.from_manifestation_id(10)

    related_assets = metadata.get_related("digital_assets")
    assert related_assets.count("not-a-row") == 2
    assert idless_asset in related_assets
    assert metadata.get_relation_links("asset_replicas") == []


def test_manifestation_hydrator_skips_idless_item_rows_for_direct_assets() -> None:
    db = _build_manifestation_db()
    hydrator = ManifestationMetadataHydrator(db)
    idless_item = Row(db, row_dict={"item_id": None}, read_only=True)
    hydrator._collect_item_rows_from_manifestation = lambda manifestation_row: [
        idless_item
    ]
    hydrator._dedupe_rows = lambda rows: list(rows)

    metadata = hydrator.from_manifestation_id(10)

    assert metadata.get_related("items") == []
    assert metadata.get_relation_links("files") == []


def test_manifestation_hydrator_collect_interlink_edge_paths() -> None:
    hydrator = ManifestationMetadataHydrator(_build_manifestation_db())
    assert hydrator._collect_interlinks_from_row(
        None,
        secondary_table="expressions",
        source_entity_type="manifestation",
    ) == []
    assert hydrator._collect_interlinks_from_row(
        hydrator.db.get_row_from_id("manifestations", 10),
        secondary_table="not_a_table",
        source_entity_type="manifestation",
    ) == []

    db = _build_manifestation_db()
    hydrator = ManifestationMetadataHydrator(db)
    manifestation_row = db.get_row_from_id("manifestations", 10)
    assert manifestation_row is not None
    db.interlinks[("manifestations", 10, "expressions")][0][
        "expression_manifestation_link_custom"
    ] = "kept"
    db.interlinks[("manifestations", 10, "expressions")][0]["unrelated"] = "ignored"
    links = hydrator._collect_interlinks_from_row(
        manifestation_row,
        secondary_table="expressions",
        source_entity_type="manifestation",
    )
    assert links[0].extra["custom"] == "kept"
    assert "unrelated" not in links[0].extra

    db = _build_manifestation_db()
    hydrator = ManifestationMetadataHydrator(db)
    manifestation_row = db.get_row_from_id("manifestations", 10)
    assert manifestation_row is not None
    db.get_interlink_rows = _raise_runtime
    assert hydrator._collect_interlinks_from_row(
        manifestation_row,
        secondary_table="expressions",
        source_entity_type="manifestation",
    ) == []

    db = _build_manifestation_db()
    hydrator = ManifestationMetadataHydrator(db)
    manifestation_row = db.get_row_from_id("manifestations", 10)
    assert manifestation_row is not None
    db.driver_wrapper.get_link_column = _raise_runtime
    assert hydrator._collect_interlinks_from_row(
        manifestation_row,
        secondary_table="expressions",
        source_entity_type="manifestation",
    ) == []

    db = _build_manifestation_db()
    hydrator = ManifestationMetadataHydrator(db)
    manifestation_row = db.get_row_from_id("manifestations", 10)
    assert manifestation_row is not None
    db.driver_wrapper.get_link_column = (
        lambda table1, table2, secondary_id_column: (
            "expression_manifestation_link_expression_id"
        )
    )
    db.driver_wrapper.get_link_table_name = _raise_runtime
    links = hydrator._collect_interlinks_from_row(
        manifestation_row,
        secondary_table="expressions",
        source_entity_type="manifestation",
    )
    assert links[0].target.row_dict["expression_id"] == 20

    db = _build_manifestation_db()
    hydrator = ManifestationMetadataHydrator(db)
    manifestation_row = db.get_row_from_id("manifestations", 10)
    assert manifestation_row is not None
    db.get_row_from_id = _raise_runtime
    assert hydrator._collect_interlinks_from_row(
        manifestation_row,
        secondary_table="expressions",
        source_entity_type="manifestation",
    ) == []


def test_manifestation_hydrator_direct_fk_item_and_identifier_edge_paths() -> None:
    hydrator = ManifestationMetadataHydrator(_build_manifestation_db())
    idless_manifestation = Row(
        hydrator.db,
        row_dict={"manifestation_id": None},
        read_only=True,
    )
    no_schema_hydrator = ManifestationMetadataHydrator(_SchemaFailureDatabase())
    assert hydrator._collect_item_rows_from_manifestation(idless_manifestation) == []
    assert no_schema_hydrator._collect_item_rows_from_manifestation(
        idless_manifestation
    ) == []
    assert hydrator._collect_direct_fk_rows(
        table="not_a_table",
        fk_column="file_item_id",
        fk_value=1,
        type_hint="item_file",
    ) == []

    db = _build_manifestation_db()
    hydrator = ManifestationMetadataHydrator(db)
    manifestation_row = db.get_row_from_id("manifestations", 10)
    assert manifestation_row is not None
    db.search = _raise_runtime
    assert hydrator._collect_item_rows_from_manifestation(manifestation_row) == []
    assert hydrator._collect_direct_fk_rows(
        table="files",
        fk_column="file_item_id",
        fk_value=1,
        type_hint="item_file",
    ) == []

    db.tables_and_columns["item_identifiers"] = [
        "item_identifier_id",
        "item_identifier_item_id",
    ]
    db.driver_wrapper.tables_and_columns["item_identifiers"] = [
        "item_identifier_id",
        "item_identifier_item_id",
    ]
    hydrator._tables.add("item_identifiers")
    hydrator._tables_and_columns["item_identifiers"] = [
        "item_identifier_id",
        "item_identifier_item_id",
    ]
    item_row = db.get_row_from_id("items", 1)
    assert item_row is not None
    idless_item = Row(db, row_dict={"item_id": None}, read_only=True)
    assert hydrator._collect_identifier_rows(
        manifestation_rows=[],
        expression_rows=[],
        work_rows=[],
        item_rows=[idless_item],
    ) == []
    assert hydrator._collect_identifier_rows(
        manifestation_rows=[],
        expression_rows=[],
        work_rows=[],
        item_rows=[item_row],
    ) == []

    db = _build_manifestation_db()
    hydrator = ManifestationMetadataHydrator(db)
    manifestation_row = db.get_row_from_id("manifestations", 10)
    assert manifestation_row is not None
    db.add_row(
        "entity_identifiers",
        {
            "entity_identifier_id": 999,
            "entity_identifier_entity_type": "work",
            "entity_identifier_entity_id": 10,
            "entity_identifier_scheme": "wrong",
            "entity_identifier_value": "wrong-type",
        },
    )
    links = hydrator._collect_identifier_rows(
        manifestation_rows=[manifestation_row],
        expression_rows=[],
        work_rows=[],
        item_rows=[],
    )
    assert all(
        link.target.row_dict.get("entity_identifier_value") != "wrong-type"
        for link in links
        if isinstance(link.target, Row)
    )


def test_expression_hydrator_uses_explicit_manifestation_and_item_ids_from_mapping() -> None:
    metadata = ExpressionMetadataHydrator(_build_expression_db()).from_source_row(
        {
            "book_expression_id": "999",
            "expression_label": "Mapping Expression",
            "manifestation_id": "10",
            "item_id": "1",
        }
    )

    assert metadata.expression is not None
    assert metadata.expression.expression_id == 999
    assert [
        row.row_id
        for row in metadata.get_related("manifestations")
        if isinstance(row, Row)
    ] == [10]
    assert [
        row.row_id
        for row in metadata.get_related("items")
        if isinstance(row, Row)
    ] == [1]


def test_expression_hydrator_collect_interlink_edge_paths() -> None:
    hydrator = ExpressionMetadataHydrator(_build_expression_db())
    assert hydrator._collect_interlinks_from_row(
        None,
        secondary_table="works",
        source_entity_type="expression",
    ) == []
    assert hydrator._collect_interlinks_from_row(
        hydrator.db.get_row_from_id("expressions", 20),
        secondary_table="not_a_table",
        source_entity_type="expression",
    ) == []

    db = _build_expression_db()
    hydrator = ExpressionMetadataHydrator(db)
    expression_row = db.get_row_from_id("expressions", 20)
    assert expression_row is not None
    db.interlinks[("expressions", 20, "works")][0][
        "expression_work_link_custom"
    ] = "kept"
    db.interlinks[("expressions", 20, "works")][0]["unrelated"] = "ignored"
    links = hydrator._collect_interlinks_from_row(
        expression_row,
        secondary_table="works",
        source_entity_type="expression",
    )
    assert links[0].extra["custom"] == "kept"
    assert "unrelated" not in links[0].extra

    db = _build_expression_db()
    hydrator = ExpressionMetadataHydrator(db)
    expression_row = db.get_row_from_id("expressions", 20)
    assert expression_row is not None
    db.get_interlink_rows = _raise_runtime
    assert hydrator._collect_interlinks_from_row(
        expression_row,
        secondary_table="works",
        source_entity_type="expression",
    ) == []

    db = _build_expression_db()
    hydrator = ExpressionMetadataHydrator(db)
    expression_row = db.get_row_from_id("expressions", 20)
    assert expression_row is not None
    db.driver_wrapper.get_link_column = _raise_runtime
    assert hydrator._collect_interlinks_from_row(
        expression_row,
        secondary_table="works",
        source_entity_type="expression",
    ) == []

    db = _build_expression_db()
    hydrator = ExpressionMetadataHydrator(db)
    expression_row = db.get_row_from_id("expressions", 20)
    assert expression_row is not None
    db.driver_wrapper.get_link_column = (
        lambda table1, table2, secondary_id_column: "expression_work_link_work_id"
    )
    db.driver_wrapper.get_link_table_name = _raise_runtime
    links = hydrator._collect_interlinks_from_row(
        expression_row,
        secondary_table="works",
        source_entity_type="expression",
    )
    assert links[0].target.row_dict["work_id"] == 30

    db = _build_expression_db()
    hydrator = ExpressionMetadataHydrator(db)
    expression_row = db.get_row_from_id("expressions", 20)
    assert expression_row is not None
    db.get_row_from_id = _raise_runtime
    assert hydrator._collect_interlinks_from_row(
        expression_row,
        secondary_table="works",
        source_entity_type="expression",
    ) == []


def test_expression_hydrator_item_and_identifier_edge_paths() -> None:
    hydrator = ExpressionMetadataHydrator(_build_expression_db())
    idless_manifestation = Row(
        hydrator.db,
        row_dict={"manifestation_id": None},
        read_only=True,
    )
    no_schema_hydrator = ExpressionMetadataHydrator(_SchemaFailureDatabase())
    assert hydrator._collect_item_rows_from_manifestation(idless_manifestation) == []
    assert no_schema_hydrator._collect_item_rows_from_manifestation(
        idless_manifestation
    ) == []

    db = _build_expression_db()
    hydrator = ExpressionMetadataHydrator(db)
    manifestation_row = db.get_row_from_id("manifestations", 10)
    assert manifestation_row is not None
    db.search = _raise_runtime
    assert hydrator._collect_item_rows_from_manifestation(manifestation_row) == []

    db.tables_and_columns["item_identifiers"] = [
        "item_identifier_id",
        "item_identifier_item_id",
    ]
    db.driver_wrapper.tables_and_columns["item_identifiers"] = [
        "item_identifier_id",
        "item_identifier_item_id",
    ]
    hydrator._tables.add("item_identifiers")
    hydrator._tables_and_columns["item_identifiers"] = [
        "item_identifier_id",
        "item_identifier_item_id",
    ]
    idless_item = Row(db, row_dict={"item_id": None}, read_only=True)
    assert hydrator._collect_identifier_rows(
        expression_rows=[],
        work_rows=[],
        manifestation_rows=[],
        item_rows=[idless_item],
    ) == []
    item_row = _build_expression_db().get_row_from_id("items", 1)
    assert item_row is not None
    assert hydrator._collect_identifier_rows(
        expression_rows=[],
        work_rows=[],
        manifestation_rows=[],
        item_rows=[item_row],
    ) == []

    db = _build_expression_db()
    db.tables_and_columns["item_identifiers"] = [
        "item_identifier_id",
        "item_identifier_item_id",
        "item_identifier_scheme",
        "item_identifier_value",
    ]
    db.driver_wrapper.tables_and_columns["item_identifiers"] = [
        "item_identifier_id",
        "item_identifier_item_id",
        "item_identifier_scheme",
        "item_identifier_value",
    ]
    db.add_row(
        "item_identifiers",
        {
            "item_identifier_id": 700,
            "item_identifier_item_id": 1,
            "item_identifier_scheme": "isbn",
            "item_identifier_value": "9780000000001",
        },
    )
    hydrator = ExpressionMetadataHydrator(db)
    item_row = db.get_row_from_id("items", 1)
    assert item_row is not None
    item_identifier_links = hydrator._collect_identifier_rows(
        expression_rows=[],
        work_rows=[],
        manifestation_rows=[],
        item_rows=[item_row],
    )
    assert item_identifier_links[0].type == "item_identifier"

    db = _build_expression_db()
    hydrator = ExpressionMetadataHydrator(db)
    expression_row = db.get_row_from_id("expressions", 20)
    assert expression_row is not None
    db.search = _raise_runtime
    assert hydrator._collect_identifier_rows(
        expression_rows=[expression_row],
        work_rows=[],
        manifestation_rows=[],
        item_rows=[],
    ) == []

    db = _build_expression_db()
    hydrator = ExpressionMetadataHydrator(db)
    expression_row = db.get_row_from_id("expressions", 20)
    assert expression_row is not None
    db.add_row(
        "entity_identifiers",
        {
            "entity_identifier_id": 999,
            "entity_identifier_entity_type": "work",
            "entity_identifier_entity_id": 20,
            "entity_identifier_scheme": "wrong",
            "entity_identifier_value": "wrong-type",
        },
    )
    links = hydrator._collect_identifier_rows(
        expression_rows=[expression_row],
        work_rows=[],
        manifestation_rows=[],
        item_rows=[],
    )
    assert all(
        link.target.row_dict.get("entity_identifier_value") != "wrong-type"
        for link in links
        if isinstance(link.target, Row)
    )


def test_work_hydrator_skips_idless_item_rows_for_direct_assets() -> None:
    db = _build_work_db()
    hydrator = WorkMetadataHydrator(db)
    idless_item = Row(db, row_dict={"item_id": None}, read_only=True)
    hydrator._collect_item_rows_from_manifestation = lambda manifestation_row: [
        idless_item
    ]
    hydrator._dedupe_rows = lambda rows: list(rows)

    metadata = hydrator.from_work_id(30)

    assert metadata.get_relation_links("files") == []
    assert metadata.get_relation_links("images") == []


def test_work_hydrator_collect_interlink_edge_paths() -> None:
    hydrator = WorkMetadataHydrator(_build_work_db())
    assert hydrator._collect_interlinks_from_row(
        None,
        secondary_table="expressions",
        source_entity_type="work",
    ) == []
    assert hydrator._collect_interlinks_from_row(
        hydrator.db.get_row_from_id("works", 30),
        secondary_table="not_a_table",
        source_entity_type="work",
    ) == []

    db = _build_work_db()
    hydrator = WorkMetadataHydrator(db)
    work_row = db.get_row_from_id("works", 30)
    assert work_row is not None
    db.interlinks[("works", 30, "expressions")][0][
        "expression_work_link_custom"
    ] = "kept"
    db.interlinks[("works", 30, "expressions")][0]["unrelated"] = "ignored"
    links = hydrator._collect_interlinks_from_row(
        work_row,
        secondary_table="expressions",
        source_entity_type="work",
    )
    assert links[0].extra["custom"] == "kept"
    assert "unrelated" not in links[0].extra

    db = _build_work_db()
    hydrator = WorkMetadataHydrator(db)
    work_row = db.get_row_from_id("works", 30)
    assert work_row is not None
    db.get_interlink_rows = _raise_runtime
    assert hydrator._collect_interlinks_from_row(
        work_row,
        secondary_table="expressions",
        source_entity_type="work",
    ) == []

    db = _build_work_db()
    hydrator = WorkMetadataHydrator(db)
    work_row = db.get_row_from_id("works", 30)
    assert work_row is not None
    db.driver_wrapper.get_link_column = _raise_runtime
    assert hydrator._collect_interlinks_from_row(
        work_row,
        secondary_table="expressions",
        source_entity_type="work",
    ) == []

    db = _build_work_db()
    hydrator = WorkMetadataHydrator(db)
    work_row = db.get_row_from_id("works", 30)
    assert work_row is not None
    db.driver_wrapper.get_link_column = (
        lambda table1, table2, secondary_id_column: "expression_work_link_expression_id"
    )
    db.driver_wrapper.get_link_table_name = _raise_runtime
    links = hydrator._collect_interlinks_from_row(
        work_row,
        secondary_table="expressions",
        source_entity_type="work",
    )
    assert links[0].target.row_dict["expression_id"] == 20

    db = _build_work_db()
    hydrator = WorkMetadataHydrator(db)
    work_row = db.get_row_from_id("works", 30)
    assert work_row is not None
    db.get_row_from_id = _raise_runtime
    assert hydrator._collect_interlinks_from_row(
        work_row,
        secondary_table="expressions",
        source_entity_type="work",
    ) == []


def test_work_hydrator_direct_fk_item_and_identifier_edge_paths() -> None:
    hydrator = WorkMetadataHydrator(_build_work_db())
    idless_manifestation = Row(
        hydrator.db,
        row_dict={"manifestation_id": None},
        read_only=True,
    )
    no_schema_hydrator = WorkMetadataHydrator(_SchemaFailureDatabase())
    assert hydrator._collect_item_rows_from_manifestation(idless_manifestation) == []
    assert no_schema_hydrator._collect_item_rows_from_manifestation(
        idless_manifestation
    ) == []
    assert hydrator._collect_direct_fk_rows(
        table="not_a_table",
        fk_column="file_item_id",
        fk_value=1,
        type_hint="item_file",
    ) == []

    db = _build_work_db()
    hydrator = WorkMetadataHydrator(db)
    manifestation_row = db.get_row_from_id("manifestations", 10)
    assert manifestation_row is not None
    db.search = _raise_runtime
    assert hydrator._collect_item_rows_from_manifestation(manifestation_row) == []
    assert hydrator._collect_direct_fk_rows(
        table="files",
        fk_column="file_item_id",
        fk_value=1,
        type_hint="item_file",
    ) == []

    db = _build_work_db()
    db.add_row(
        "entity_identifiers",
        {
            "entity_identifier_id": 999,
            "entity_identifier_entity_type": "expression",
            "entity_identifier_entity_id": 30,
            "entity_identifier_scheme": "wrong",
            "entity_identifier_value": "wrong-type",
        },
    )
    hydrator = WorkMetadataHydrator(db)
    work_row = db.get_row_from_id("works", 30)
    assert work_row is not None
    links = hydrator._collect_identifier_rows(
        work_rows=[work_row],
        expression_rows=[],
        manifestation_rows=[],
        item_rows=[],
    )
    assert all(
        link.target.row_dict.get("entity_identifier_value") != "wrong-type"
        for link in links
        if isinstance(link.target, Row)
    )

    db = _build_work_db()
    hydrator = WorkMetadataHydrator(db)
    work_row = db.get_row_from_id("works", 30)
    assert work_row is not None
    db.search = _raise_runtime
    assert hydrator._collect_identifier_rows(
        work_rows=[work_row],
        expression_rows=[],
        manifestation_rows=[],
        item_rows=[],
    ) == []


def test_work_hydrator_item_identifier_and_folder_resolution_paths() -> None:
    work_singulars = _build_work_db.__globals__["SINGULARS"]
    work_singulars["item_identifiers"] = "item_identifier"
    work_singulars["folders"] = "folder"

    db = _build_work_db()
    db.tables_and_columns["item_identifiers"] = [
        "item_identifier_id",
        "item_identifier_item_id",
        "item_identifier_scheme",
        "item_identifier_value",
    ]
    db.driver_wrapper.tables_and_columns["item_identifiers"] = [
        "item_identifier_id",
        "item_identifier_item_id",
        "item_identifier_scheme",
        "item_identifier_value",
    ]
    db.add_row(
        "item_identifiers",
        {
            "item_identifier_id": 700,
            "item_identifier_item_id": 1,
            "item_identifier_scheme": "isbn",
            "item_identifier_value": "9780000000001",
        },
    )
    hydrator = WorkMetadataHydrator(db)
    item_row = db.get_row_from_id("items", 1)
    assert item_row is not None
    idless_item = Row(db, row_dict={"item_id": None}, read_only=True)

    assert hydrator._collect_identifier_rows(
        work_rows=[],
        expression_rows=[],
        manifestation_rows=[],
        item_rows=[idless_item],
    ) == []
    assert hydrator._collect_identifier_rows(
        work_rows=[],
        expression_rows=[],
        manifestation_rows=[],
        item_rows=[item_row],
    )[0].type == "item_identifier"

    db.search = _raise_runtime
    assert hydrator._collect_identifier_rows(
        work_rows=[],
        expression_rows=[],
        manifestation_rows=[],
        item_rows=[item_row],
    ) == []

    db = _build_work_db()
    db.tables_and_columns["folders"] = ["folder_id", "folder_store_id", "folder_path"]
    db.driver_wrapper.tables_and_columns["folders"] = [
        "folder_id",
        "folder_store_id",
        "folder_path",
    ]
    folder = db.add_row(
        "folders",
        {
            "folder_id": 61,
            "folder_store_id": 60,
            "folder_path": "Greg Egan/Permutation City",
        },
    )
    file_row = db.add_row(
        "files",
        {
            "file_id": 500,
            "file_item_id": 1,
            "file_folder_id": 61,
            "file_extension": "epub",
        },
    )
    db.interlinks[("works", 30, "folders")] = [
        {"folder_work_link_folder_id": 61}
    ]
    work_row = db.get_row_from_id("works", 30)
    assert work_row is not None
    hydrator = WorkMetadataHydrator(db)
    metadata = WorkMetadata()
    metadata.add_relation_link("files", WorkRelationLink(target="not-a-row"))
    metadata.add_relation_link("files", WorkRelationLink(target=file_row))

    hydrator._hydrate_folders(metadata, work_rows=[work_row])

    assert [link.target for link in metadata.get_relation_links("folders")] == [folder]


def test_central_hydrator_identity_getters_reject_empty_metadata() -> None:
    hydrator = LiuXinWEMIMetadataHydrator(_build_item_db())
    hydrator.get_work_metadata = lambda work_id: WorkMetadata()
    hydrator.get_expression_metadata = lambda expression_id: ExpressionMetadata()
    hydrator.get_manifestation_metadata = (
        lambda manifestation_id: ManifestationMetadata()
    )
    hydrator.get_item_metadata = lambda item_id=None, source_row=None: ItemMetadata()

    with pytest.raises(ValueError, match="No work identity"):
        hydrator.get_work_identity(1)
    with pytest.raises(ValueError, match="No expression identity"):
        hydrator.get_expression_identity(1)
    with pytest.raises(ValueError, match="No manifestation identity"):
        hydrator.get_manifestation_identity(1)
    with pytest.raises(ValueError, match="No item identity"):
        hydrator.get_item_identity(1)


def test_central_hydrator_empty_fallbacks_and_target_id_variants() -> None:
    hydrator = LiuXinWEMIMetadataHydrator(_build_item_db())

    assert hydrator._get_work_metadata_or_empty(None, {"unrelated": "value"}).work is None
    assert (
        hydrator._get_expression_metadata_or_empty(
            None,
            {"unrelated": "value"},
        ).expression
        is None
    )
    assert (
        hydrator._get_manifestation_metadata_or_empty(
            None,
            {"unrelated": "value"},
        ).manifestation
        is None
    )
    assert hydrator._target_id({"work_id": "30"}, "work_id") == 30
    assert hydrator._target_id(_ObjectTarget(), "expression_id") == 44
    assert hydrator._target_id(object(), "work_id") is None


def test_lazy_hydrator_aliases_and_source_row_paths() -> None:
    db = _build_item_db()
    hydrator = LazyLiuXinWEMIMetadataHydrator(db)

    with pytest.raises(ValueError, match="Provide either item_id or source_row"):
        hydrator.get_lazy_liuxin_wemi_metadata()

    assert isinstance(hydrator.get_liuxin_wemi_metadata(item_id=1), LazyLiuXinWEMIMetadata)
    assert isinstance(
        hydrator.get_lazy_liuxin_metadata(item_id=1),
        LazyLiuXinWEMIMetadata,
    )
    assert hydrator.get_calibre_metadata(item_id=1).title == "Permutation City"

    detached_item_row = Row(
        db,
        row_dict={"item_id": 999, "item_manifestation_id": 10, "item_type": "detached"},
        read_only=True,
    )
    metadata = hydrator.get_lazy_liuxin_wemi_metadata(source_row=detached_item_row)

    assert metadata.item is not None
    assert metadata.item.item_id == 999
    assert metadata.manifestation is not None
    assert metadata.manifestation.manifestation_id == 10

    detached_manifestation_row = Row(
        db,
        row_dict={
            "manifestation_id": 999,
            "manifestation_expression_id": 20,
            "manifestation_format_detail": "Detached Manifestation",
        },
        read_only=True,
    )
    from_manifestation = hydrator.get_lazy_liuxin_wemi_metadata(
        source_row=detached_manifestation_row,
    )
    assert from_manifestation.manifestation is not None
    assert from_manifestation.manifestation.manifestation_id == 999

    detached_expression_row = Row(
        db,
        row_dict={
            "expression_id": 999,
            "expression_work_id": 30,
            "expression_title_override": "Detached Expression",
        },
        read_only=True,
    )
    from_expression = hydrator.get_lazy_liuxin_wemi_metadata(
        source_row=detached_expression_row,
    )
    assert from_expression.expression is not None
    assert from_expression.expression.expression_id == 999

    detached_work_row = Row(
        db,
        row_dict={"work_id": 999, "work_title": "Detached Work"},
        read_only=True,
    )
    from_work = hydrator.get_lazy_liuxin_wemi_metadata(source_row=detached_work_row)
    assert from_work.work is not None
    assert from_work.work.work_id == 999


def test_lazy_hydrator_helper_branches() -> None:
    hydrator = LazyLiuXinWEMIMetadataHydrator(_SchemaFailureDatabase())
    item_row = _build_item_db().get_row_from_id("items", 1)
    assert item_row is not None

    assert hydrator._resolve_row("items", None) is None
    assert hydrator._resolve_row("items", 1) is None
    real_db = _build_item_db()
    real_hydrator = LazyLiuXinWEMIMetadataHydrator(real_db)
    real_db.get_row_from_id = _raise_runtime
    assert real_hydrator._resolve_row("items", 1) is None
    assert hydrator._work_identity(None, {"work_title": "Mapping Work"}).work_title == (
        "Mapping Work"
    )
    assert hydrator._work_identity(None, {}) is None
    assert hydrator._expression_identity(
        None,
        {"expression_title_override": "Mapping Expression"},
    ).expression_title_override == "Mapping Expression"
    assert hydrator._expression_identity(None, {}) is None
    assert hydrator._manifestation_identity(
        None,
        {"manifestation_format_detail": "Mapping Manifestation"},
    ).manifestation_format_detail == "Mapping Manifestation"
    assert hydrator._manifestation_identity(None, {}) is None
    assert hydrator._item_identity(None, {"item_id": 1}).item_id == 1
    assert hydrator._item_identity(None, {}) is None
    assert hydrator._row_key(object()) is None
    assert hydrator._row_key(Row(_build_item_db(), row_dict={}, read_only=True)) is None
    assert hydrator._direct_fk_spec(level="item", relation="files") == (
        "files",
        "file_item_id",
        "item_file",
    )
    assert hydrator._direct_fk_spec(level="work", relation="files") is None

    assert hydrator._collect_relation_links(
        level="item",
        source_row=None,
        secondary_table="tags",
    ) == []
    assert hydrator._collect_direct_fk_links(
        level="item",
        table="files",
        fk_column="file_item_id",
        fk_value=1,
        type_hint="item_file",
    ) == []
    assert hydrator._collect_identifier_links(
        level="item",
        source_row=item_row,
    ) == []
    assert hydrator._extra_from_link_map(
        source_table="items",
        secondary_table="tags",
        prefix="tag_item_link",
        link_map={
            "tag_item_link_item_id": 1,
            "tag_item_link_tag_id": 2,
            "tag_item_link_priority": 1,
            "tag_item_link_custom": "kept",
            "unrelated": "ignored",
        },
    ) == {"custom": "kept"}


def test_lazy_hydrator_relation_loader_exception_paths() -> None:
    db = _build_item_db()
    hydrator = LazyLiuXinWEMIMetadataHydrator(db)
    item_row = db.get_row_from_id("items", 1)
    assert item_row is not None

    assert (
        hydrator._first_relation_link(
            level="item",
            source_row=item_row,
            secondary_table="not_a_table",
        )
        is None
    )

    db.get_interlink_rows = _raise_runtime
    assert hydrator._collect_relation_links(
        level="item",
        source_row=item_row,
        secondary_table="tags",
    ) == []

    db = _build_item_db()
    hydrator = LazyLiuXinWEMIMetadataHydrator(db)
    work_row = db.get_row_from_id("works", 30)
    assert work_row is not None
    db.driver_wrapper.get_link_column = _raise_runtime
    assert hydrator._collect_relation_links(
        level="work",
        source_row=work_row,
        secondary_table="tags",
    ) == []

    db = _build_item_db()
    hydrator = LazyLiuXinWEMIMetadataHydrator(db)
    work_row = db.get_row_from_id("works", 30)
    assert work_row is not None
    db.driver_wrapper.get_link_column = (
        lambda table1, table2, secondary_id_column: "tag_work_link_tag_id"
    )
    db.driver_wrapper.get_link_table_name = _raise_runtime
    links = hydrator._collect_relation_links(
        level="work",
        source_row=work_row,
        secondary_table="tags",
    )
    assert links
    assert links[0].target.row_dict["tag"] == "Space Opera"


def test_lazy_hydrator_direct_and_identifier_loader_exception_paths() -> None:
    db = _build_item_db()
    hydrator = LazyLiuXinWEMIMetadataHydrator(db)
    item_row = db.get_row_from_id("items", 1)
    assert item_row is not None

    db.search = _raise_runtime
    assert hydrator._collect_direct_fk_links(
        level="item",
        table="files",
        fk_column="file_item_id",
        fk_value=1,
        type_hint="item_file",
    ) == []
    assert hydrator._collect_identifier_links(level="item", source_row=item_row) == []

    db = _build_item_db()
    hydrator = LazyLiuXinWEMIMetadataHydrator(db)
    item_row = db.get_row_from_id("items", 1)
    assert item_row is not None
    db.add_row(
        "entity_identifiers",
        {
            "entity_identifier_id": 999,
            "entity_identifier_entity_type": "work",
            "entity_identifier_entity_id": 1,
            "entity_identifier_scheme": "wrong",
            "entity_identifier_value": "wrong-type",
        },
    )
    links = hydrator._collect_identifier_links(level="item", source_row=item_row)
    assert all(
        link.target.row_dict.get("entity_identifier_value") != "wrong-type"
        for link in links
        if isinstance(link.target, Row)
    )


def test_lazy_hydrator_asset_replica_and_extra_helpers_cover_skips() -> None:
    metadata = LazyLiuXinWEMIMetadata()
    metadata.add_wemi_relation_link(
        "item",
        "digital_assets",
        ItemRelationLink(target="not-a-row"),
    )
    hydrator = LazyLiuXinWEMIMetadataHydrator(_build_item_db())

    assert hydrator._collect_item_asset_replica_links(metadata) == []

    broken = LazyLiuXinWEMIMetadataHydrator(_BrokenIdColumnDatabase())
    assert broken._extra_from_link_map(
        source_table="items",
        secondary_table="tags",
        prefix="tag_item_link",
        link_map={
            "tag_item_link_item_id": 1,
            "tag_item_link_priority": 1,
            "tag_item_link_custom": "kept",
        },
    ) == {"item_id": 1, "custom": "kept"}

    metadata = LazyLiuXinWEMIMetadata()
    broken._install_lazy_loaders(metadata, {"work": None})
    assert "tags" in metadata.lazy_fields()
