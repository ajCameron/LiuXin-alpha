"""Verify attributed, bounded inheritance through real WEMI graphs and snapshots."""

import pytest

from LiuXin_alpha.catalog.api.common import CatalogNotFoundError
from LiuXin_alpha.catalog.inspection import InspectionLimitExceeded
from LiuXin_alpha.catalog.relationships import CatalogRelationships
from tests.catalog.test_write_boundary_parity import (
    _stack,
)
from tests.catalog.test_write_boundary_parity import (
    write_runtime as write_runtime,
)


def test_shared_manifestation_retains_both_works_and_direct_tags(write_runtime):
    runtime, catalog, db = write_runtime
    from LiuXin_alpha.catalog.maintenance import catalog_maintenance

    catalog_maintenance(db).install()
    novel = _stack(catalog)
    intro = _stack(catalog, work={"title": "Introduction"}, items=())
    catalog.mutations.writer.link_wemi(
        parent_level="expression",
        parent_id=intro.expression_id,
        child_level="manifestation",
        child_id=novel.manifestation_id,
    )
    common = catalog.tags.create({"tag": "Shared"})
    expression_tag = catalog.tags.create({"tag": "Translation"})
    item_tag = catalog.tags.create({"tag": "Signed copy"})
    for work in (novel.work_id, intro.work_id):
        catalog.tags.link_to_work(entity_id=common, work_id=work)
    relationships = CatalogRelationships(db)
    relationships.link("expressions", novel.expression_id, "tags", expression_tag)
    relationships.link("items", novel.item_ids[0], "tags", item_tag)
    before = {
        table: db.macros.get_rows(table)
        for table in (
            "works",
            "expressions",
            "manifestations",
            "items",
            "tags",
            "tag_work_links",
            "maintenance_changes",
            "maintenance_obligations",
            "maintenance_projections",
        )
    }
    result = runtime.query(
        "catalog.inherited.tags", {"level": "item", "entity_id": novel.item_ids[0]}
    )
    tags = {tag["tag_id"]: tag for tag in result["tags"]}
    assert set(tags) == {common, expression_tag, item_tag}
    contributions = tags[common]["contributions"]
    assert {c["source_id"] for c in contributions} == {novel.work_id, intro.work_id}
    assert all(c["inherited"] for c in contributions)
    assert all(
        [n["level"] for n in c["path"]]
        == ["item", "manifestation", "expression", "work"]
        for c in contributions
    )
    assert all("parent_link" in n for c in contributions for n in c["path"][:-1])
    assert tags[item_tag]["contributions"][0]["inherited"] is False
    assert tags[expression_tag]["contributions"][0]["source_level"] == "expression"
    assert {table: db.macros.get_rows(table) for table in before} == before
    # No upward propagation; removing the final direct link clears immediately.
    assert [
        t["tag_id"]
        for t in catalog.retrieval.inherited.tags(
            level="work", entity_id=intro.work_id
        )["tags"]
    ] == [common]
    catalog.tags.unlink_from_work(entity_id=common, work_id=intro.work_id)
    assert (
        catalog.retrieval.inherited.tags(level="work", entity_id=intro.work_id)["tags"]
        == []
    )


def test_repeated_work_keeps_each_path_and_refuses_partial_inspection(write_runtime):
    _runtime, catalog, db = write_runtime
    stack = _stack(catalog)
    second = _stack(catalog, work_id=stack.work_id, work={}, items=())
    catalog.mutations.writer.link_wemi(
        parent_level="work",
        parent_id=stack.work_id,
        child_level="expression",
        child_id=stack.expression_id,
    )
    catalog.mutations.writer.link_wemi(
        parent_level="expression",
        parent_id=second.expression_id,
        child_level="manifestation",
        child_id=stack.manifestation_id,
    )
    tag = catalog.tags.create({"tag": "Two paths"})
    catalog.tags.link_to_work(entity_id=tag, work_id=stack.work_id)
    result = catalog.retrieval.inherited.tags(
        level="manifestation", entity_id=stack.manifestation_id
    )
    assert len(result["tags"][0]["contributions"]) == 2
    with pytest.raises(InspectionLimitExceeded):
        catalog.retrieval.inherited.tags(
            level="manifestation", entity_id=stack.manifestation_id, max_rows=2
        )
    with pytest.raises(CatalogNotFoundError):
        catalog.retrieval.inherited.tags(level="work", entity_id=9999999)
    with pytest.raises(ValueError):
        catalog.retrieval.inherited.tags(level="work", entity_id=True)


def test_snapshot_rejects_writes_and_nesting_without_poisoning_connection(
    write_runtime,
):
    _runtime, catalog, db = write_runtime
    work = catalog.works.create({"title": "Before"})
    with pytest.raises(Exception, match="readonly|read.only|read-only|ReadOnly"):
        with db.macros.read_transaction():
            db.macros.update_row("works", work, {"work_title": "Forbidden"})
    assert catalog.works.require(work)["work_title"] == "Before"
    with db.macros.transaction():
        with pytest.raises(
            Exception,
            match="outermost|active|nested|borrow",
        ):
            with db.macros.read_transaction():
                pass
        catalog.works.update(work, {"title": "After"})
    assert catalog.works.require(work)["work_title"] == "After"


def test_postgres_snapshot_is_consistent_across_concurrent_commit(write_runtime):
    _runtime, catalog, db = write_runtime
    if not hasattr(db.driver, "schema"):
        pytest.skip("Concurrent commit during a read depends on SQLite journal mode")
    work = catalog.works.create({"title": "Original"})
    with db.macros.read_transaction():
        assert catalog.works.require(work)["work_title"] == "Original"
        connection = db.driver.get_connection()
        try:
            connection.execute(
                "UPDATE works SET work_title=? WHERE work_id=?", ("Concurrent", work)
            )
            connection.commit()
        finally:
            connection.close()
        assert catalog.works.require(work)["work_title"] == "Original"
    assert catalog.works.require(work)["work_title"] == "Concurrent"
