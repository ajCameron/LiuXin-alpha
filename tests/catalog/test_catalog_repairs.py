"""Exercise explicit repair decisions, complete references and atomic receipts."""

import pytest

from LiuXin_alpha.catalog.api.common import CatalogMutationError
from LiuXin_alpha.catalog.relationships import CatalogRelationships
from tests.catalog.test_write_boundary_parity import write_runtime as write_runtime


def test_typed_directed_work_links_survive_and_symmetric_order_is_repaired(
    write_runtime,
):
    _runtime, catalog, db = write_runtime
    source = catalog.works.create({"title": "Source"})
    other = catalog.works.create({"title": "Other"})
    target = catalog.works.create({"canonical_title": "Retained"})
    relationships = CatalogRelationships(db)
    relationships.link("works", source, "works", other, link_type="inspired_by")
    relationships.link("works", source, "works", other, link_type="fanfiction_of")
    relationships.link(
        "works", source, "works", other, link_type="user_marked_different"
    )
    args = dict(repository="works", source_id=source, target_id=target)
    preview = catalog.mutations.repairs.merge(**args)
    assert not preview["conflicts"] and not preview["blockers"]
    catalog.mutations.repairs.merge(
        **args, dry_run=False, expected_state=preview["state"]
    )
    spec = db.driver_wrapper.get_intralink_spec("works")
    rows = db.macros.get_rows(spec.link_table)
    assert {
        (r[spec.primary_link_col], r[spec.secondary_link_col], r[spec.type_link_col])
        for r in rows
    } == {
        (target, other, "inspired_by"),
        (target, other, "fanfiction_of"),
        (other, target, "user_marked_different"),
    }


def test_prompt_failure_rolls_back_merge_and_its_audit_receipt(
    write_runtime, monkeypatch
):
    from LiuXin_alpha.catalog.maintenance import WorkRevisionPlugin, catalog_maintenance

    _runtime, catalog, db = write_runtime
    maintenance = catalog_maintenance(db)
    maintenance.install()
    source = catalog.works.create({"title": "Source"})
    target = catalog.works.create({"canonical_title": "Target"})
    tag = catalog.tags.create({"tag": "Preserve on failure"})
    catalog.tags.link_to_work(entity_id=tag, work_id=source)
    args = dict(repository="works", source_id=source, target_id=target)
    preview = catalog.mutations.repairs.merge(**args)
    before = {
        t: db.macros.get_rows(t)
        for t in (
            "works",
            "tag_work_links",
            "maintenance_changes",
            "maintenance_obligations",
            "maintenance_projections",
        )
    }
    plugin = maintenance._plugins[WorkRevisionPlugin.name]
    original = plugin.handle_events

    def fail(context, events):
        original(context, events)
        raise RuntimeError("prompt repair rejection")

    monkeypatch.setattr(plugin, "handle_events", fail)
    receipts = []
    record = db.macros.record_catalog_repair

    def capture(receipt):
        identity = record(receipt)
        receipts.append(identity)
        return identity

    monkeypatch.setattr(db.macros, "record_catalog_repair", capture)
    with pytest.raises(RuntimeError, match="prompt repair rejection"):
        catalog.mutations.repairs.merge(
            **args, dry_run=False, expected_state=preview["state"]
        )
    assert {t: db.macros.get_rows(t) for t in before} == before
    # Prompt work is flushed at the nested write boundary, before a success
    # receipt can be recorded. The failed operation publishes neither one.
    assert receipts == []


def test_postgres_cleanup_fences_a_concurrent_raw_reference(write_runtime, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    _runtime, catalog, db = write_runtime
    if not hasattr(db.driver, "schema"):
        pytest.skip("PostgreSQL table locks are the boundary under test")
    tag = catalog.tags.create({"tag": "Selected unused tag"})
    work = catalog.works.create({"title": "Concurrent owner"})
    args = dict(
        repository="tags",
        candidates=[
            {
                "entity_id": tag,
                "decision": "discard",
                "reason": "Explicit test disposal",
            }
        ],
    )
    preview = catalog.mutations.repairs.cleanup(**args)
    spec = db.driver_wrapper.get_link_spec("works", "tags")
    started = Event()

    def new_reference():
        connection = db.driver.get_connection()
        try:
            connection.execute("SET lock_timeout='5s'")
            started.set()
            connection.execute(
                f'INSERT INTO "{spec.link_table}" ("{spec.primary_link_col}","{spec.secondary_link_col}") VALUES (?,?)',
                (work, tag),
            )
            connection.commit()
        finally:
            connection.close()

    record = db.macros.record_catalog_repair
    futures = []
    with ThreadPoolExecutor(max_workers=1) as pool:

        def capture(receipt):
            futures.append(pool.submit(new_reference))
            assert started.wait(3)
            # The new FK cannot commit while the repair owns the source/reference locks.
            assert not futures[0].done()
            return record(receipt)

        monkeypatch.setattr(db.macros, "record_catalog_repair", capture)
        result = catalog.mutations.repairs.cleanup(
            **args, dry_run=False, expected_state=preview["state"]
        )
        with pytest.raises(Exception, match="foreign key|ForeignKey"):
            futures[0].result(timeout=10)
    assert result["deleted"] == [tag]
    assert db.macros.get_link_rows(spec, work) == ()


def test_merge_requires_decisions_and_preserves_tags_notes_and_typed_links(
    write_runtime,
):
    runtime, catalog, db = write_runtime
    source = catalog.works.create({"title": "Earlier title"})
    target = catalog.works.create({"title": "Retained title"})
    tag = catalog.tags.create({"tag": "Transfer me"})
    catalog.tags.link_to_work(entity_id=tag, work_id=source)
    catalog.mutations.writer.attach_metadata(
        level="work", entity_id=source, data={"notes": ["Provenance note"]}
    )
    repairs = catalog.mutations.repairs
    args = dict(repository="works", source_id=source, target_id=target)
    preview = runtime.command("catalog.repair.merge", args)
    assert "cache" not in preview
    assert {c["column"] for c in preview["conflicts"] if c["kind"] == "scalar"} == {
        "work_title"
    }
    with pytest.raises(CatalogMutationError, match="unresolved"):
        repairs.merge(**args, dry_run=False, expected_state=preview["state"])
    args["resolutions"] = {"work_title": {"take": "target"}}
    preview = repairs.merge(**args)
    assert not preview["conflicts"] and not preview["blockers"]
    result = runtime.command(
        "catalog.repair.merge",
        {**args, "dry_run": False, "expected_state": preview["state"]},
    )
    assert result["merged"] and result["receipt_id"]
    assert catalog.works.get(source) is None
    assert catalog.tags.list_for_work(target)[0]["tag_id"] == tag
    assert catalog.notes.list_for_wemi(level="work", entity_id=target)
    assert "cache" in result
    receipt = runtime.query(
        "catalog.repair.receipt", {"receipt_id": result["receipt_id"]}
    )["receipt"]
    assert receipt["snapshot"]["source"]["work_title"] == "Earlier title"
    assert receipt["resolutions"] == args["resolutions"]


def test_merge_refuses_stale_preview_and_rolls_back_late_receipt_failure(
    write_runtime, monkeypatch
):
    _runtime, catalog, db = write_runtime
    source = catalog.works.create({"title": "Source"})
    target = catalog.works.create({"canonical_title": "Target"})
    args = dict(repository="works", source_id=source, target_id=target)
    preview = catalog.mutations.repairs.merge(**args)
    tag = catalog.tags.create({"tag": "Arrived after preview"})
    catalog.tags.link_to_work(entity_id=tag, work_id=source)
    with pytest.raises(CatalogMutationError, match="stale"):
        catalog.mutations.repairs.merge(
            **args, dry_run=False, expected_state=preview["state"]
        )
    before = {t: db.macros.get_rows(t) for t in ("works", "tag_work_links")}
    preview = catalog.mutations.repairs.merge(**args)
    original = db.macros.record_catalog_repair
    receipt_ids = []

    def fail_after_receipt(receipt):
        receipt_ids.append(original(receipt))
        raise RuntimeError("late receipt failure")

    monkeypatch.setattr(db.macros, "record_catalog_repair", fail_after_receipt)
    with pytest.raises(RuntimeError, match="late receipt"):
        catalog.mutations.repairs.merge(
            **args, dry_run=False, expected_state=preview["state"]
        )
    assert {t: db.macros.get_rows(t) for t in before} == before
    assert receipt_ids and db.macros.get_catalog_repair_receipt(receipt_ids[0]) is None


def test_cleanup_preserves_selected_and_placeholder_rows_and_checks_new_references(
    write_runtime,
):
    _runtime, catalog, db = write_runtime
    discard = catalog.tags.create({"tag": "Unwanted"})
    preserve = catalog.tags.create({"tag": "Future use"})
    candidates = [
        {"entity_id": discard, "decision": "discard", "reason": "Mistaken import"},
        {
            "entity_id": preserve,
            "decision": "preserve",
            "reason": "Keep this vocabulary",
        },
    ]
    args = dict(repository="tags", candidates=candidates)
    preview = catalog.mutations.repairs.cleanup(**args)
    assert preview["selected"] == [discard]
    work = catalog.works.create({"title": "Reference"})
    catalog.tags.link_to_work(entity_id=discard, work_id=work)
    with pytest.raises(CatalogMutationError, match="stale"):
        catalog.mutations.repairs.cleanup(
            **args, dry_run=False, expected_state=preview["state"]
        )
    assert (
        catalog.mutations.repairs.cleanup(**args)["conflicts"][0]["kind"]
        == "referenced"
    )
    catalog.tags.unlink_from_work(entity_id=discard, work_id=work)
    preview = catalog.mutations.repairs.cleanup(**args)
    result = catalog.mutations.repairs.cleanup(
        **args, dry_run=False, expected_state=preview["state"]
    )
    assert result["deleted"] == [discard]
    assert catalog.tags.get(discard) is None and catalog.tags.get(preserve)
    placeholder = db.macros.insert_row("tags", {"tag": None})
    preview = catalog.mutations.repairs.cleanup(
        repository="tags",
        candidates=[
            {
                "entity_id": placeholder,
                "decision": "discard",
                "reason": "Cannot infer placeholder lifecycle",
            }
        ],
    )
    assert preview["conflicts"][0]["kind"] == "placeholder"


def test_scoped_genres_and_unknown_owned_references_block_deletion(write_runtime):
    _runtime, catalog, db = write_runtime
    parent1 = catalog.genres.create({"genre": "Parent one"})
    parent2 = catalog.genres.create({"genre": "Parent two"})
    source = catalog.genres.create({"genre": "Same name", "parent_id": parent1})
    target = catalog.genres.create({"genre": "Same name", "parent_id": parent2})
    preview = catalog.mutations.repairs.merge(
        repository="genres",
        source_id=source,
        target_id=target,
        resolutions={"genre_parent_id": {"take": "target"}},
    )
    assert any(c["kind"] == "hierarchy_scope" for c in preview["blockers"])
    source = catalog.works.create({"title": "Source"})
    target = catalog.works.create({"canonical_title": "Target"})
    with db.macros.transaction() as connection:
        connection.execute(
            "CREATE TABLE repair_unknown (repair_unknown_id INTEGER PRIMARY KEY, work_id BIGINT REFERENCES works(work_id) ON DELETE CASCADE)"
        )
        connection.execute("INSERT INTO repair_unknown VALUES (1,?)", (source,))
    preview = catalog.mutations.repairs.merge(
        repository="works", source_id=source, target_id=target
    )
    assert any(c["kind"] == "unsupported_reference" for c in preview["conflicts"])
    with pytest.raises(CatalogMutationError, match="unsupported"):
        catalog.mutations.repairs.merge(
            repository="works",
            source_id=source,
            target_id=target,
            dry_run=False,
            expected_state=preview["state"],
        )
    assert db.macros.get_rows("repair_unknown")[0]["work_id"] == source


def test_duplicate_relationship_payload_requires_explicit_choice(write_runtime):
    _runtime, catalog, db = write_runtime
    source = catalog.works.create({"title": "Source"})
    target = catalog.works.create({"canonical_title": "Target"})
    tag = catalog.tags.create({"tag": "Common"})
    rels = CatalogRelationships(db)
    spec = db.driver_wrapper.get_link_spec("works", "tags")
    rels.link("works", source, "tags", tag, priority=2)
    rels.link("works", target, "tags", tag, priority=1)
    args = dict(repository="works", source_id=source, target_id=target)
    preview = catalog.mutations.repairs.merge(**args)
    conflict = next(
        c for c in preview["conflicts"] if c["kind"] == "relationship_payload"
    )
    args["relationship_resolutions"] = {conflict["row"]: {"take": "target"}}
    resolved = catalog.mutations.repairs.merge(**args)
    assert not resolved["conflicts"]
    catalog.mutations.repairs.merge(
        **args, dry_run=False, expected_state=resolved["state"]
    )
    links = db.macros.get_link_rows(spec, target)
    assert len(links) == 1 and links[0].priority == 1
