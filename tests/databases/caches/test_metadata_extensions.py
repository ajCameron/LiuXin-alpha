"""Exercise application-facing batch operations and metadata administration."""

import pytest

from LiuXin_alpha.catalog.api.common import CatalogNotFoundError
from LiuXin_alpha.catalog.api.metadata_access import MetadataReconciliationError
from LiuXin_alpha.catalog.api.metadata_revisions import MetadataConflictError
from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.databases.custom_columns.native_types import (
    CustomFieldError,
    FieldPatch,
    OwnerRef,
    Predicate,
)
from tests.databases.caches.test_metadata_api import MODES, service
from tests.databases.caches.test_metadata_api import metadata_db as metadata_db
from tests.databases.caches.test_metadata_api import (
    metadata_template as metadata_template,
)


@pytest.mark.parametrize("mode", MODES)
def test_get_many_preserves_order_misses_duplicates_and_detachment(
    metadata_db, mode, monkeypatch
):
    api, cache = service(metadata_db, mode)
    first = api.works.create({"title": "First"}).result
    second = api.works.create({"title": "Second"}).result
    captures = []
    if cache is not None and callable(getattr(cache.storage, "query_view", None)):
        original = cache.storage.query_view

        def capture():
            captures.append(True)
            return original()

        monkeypatch.setattr(cache.storage, "query_view", capture)
    values = api.works.get_many((second, 999999, first, second))
    assert [None if value is None else value.work.work_title for value in values] == [
        "Second",
        None,
        "First",
        "Second",
    ]
    assert len(captures) <= 1
    values[0].work.work_title = "Detached"
    assert values[3].work.work_title == "Second"
    assert api.works.require(second).work.work_title == "Second"
    with pytest.raises(CatalogNotFoundError):
        api.works.get_many((first, 999999), missing="raise")
    assert api.works.get_many(()) == ()
    with pytest.raises((ValueError, TypeError)):
        api.works.get_many((first, True))
    if cache is not None:
        cache.close()


@pytest.mark.parametrize("mode", MODES)
def test_batch_atomicity_one_reconciliation_and_nested_receipts(
    metadata_db, mode, monkeypatch
):
    api, cache = service(metadata_db, mode)
    refreshes = []
    if cache is not None:
        reload = cache.reload_data

        def refresh():
            refreshes.append(True)
            return reload()

        monkeypatch.setattr(cache, "reload_data", refresh)

    def edits(metadata):
        first = metadata.works.create({"title": "First"})
        assert first.persistence == "pending"
        second = metadata.batch(
            lambda nested: nested.works.create({"title": "Second"}).result
        )
        assert second.persistence == "pending"
        metadata.works.update(first.result, {"title": "Updated"})
        return first.result, second.result

    receipt = api.batch(edits)
    assert receipt.changed and receipt.persistence == "committed"
    if cache is not None:
        assert len(refreshes) == 1
    assert [value.work.work_title for value in api.works.get_many(receipt.result)] == [
        "Updated",
        "Second",
    ]
    assert not api.batch(lambda metadata: None).changed

    def fails(metadata):
        metadata.works.create({"title": "Rollback"})
        metadata.works.update(999999, {"title": "Missing"})

    with pytest.raises(CatalogNotFoundError):
        api.batch(fails)
    assert api.search.find("work.title:Rollback") == ()
    if cache is not None:
        cache.close()


def test_batch_commit_refresh_failure_retains_aggregate_result(
    metadata_db, monkeypatch
):
    api, cache = service(metadata_db, "schema_backed")

    def fail():
        raise OSError("refresh failed")

    with monkeypatch.context() as patch:
        patch.setattr(cache, "reload_data", fail)
        with pytest.raises(MetadataReconciliationError) as caught:
            api.batch(
                lambda metadata: (
                    metadata.works.create({"title": "Committed batch"}).result
                )
            )
    receipt = caught.value.receipt
    assert receipt.persistence == "committed" and receipt.cache == "dirty"
    assert api.works.require(receipt.result).work.work_title == "Committed batch"
    cache.close()


@pytest.mark.parametrize("mode", MODES)
def test_caught_nested_rollback_does_not_claim_a_dirty_cache_is_current(
    metadata_db, mode
):
    from LiuXin_alpha.caches.api import CacheState

    api, cache = service(metadata_db, mode)
    identity = api.works.create({"title": "Retained"}).result

    def rolled_back(inner):
        inner.works.update(identity, {"title": "Discarded"})
        raise ValueError("discard this nested edit")

    def outer(metadata):
        with pytest.raises(ValueError, match="discard"):
            metadata.batch(rolled_back)
        return identity

    receipt = api.batch(outer)
    assert not receipt.changed
    assert receipt.persistence == "committed"
    if cache is not None:
        assert receipt.cache == "current"
        assert cache.state is CacheState.READY
    assert api.works.require(identity).work.work_title == "Retained"
    if cache is not None:
        cache.close()


def test_caught_nested_rollback_refresh_failure_retains_unchanged_result(
    metadata_db, monkeypatch
):
    api, cache = service(metadata_db, "schema_backed")
    identity = api.works.create({"title": "Retained"}).result

    def rolled_back(inner):
        inner.works.update(identity, {"title": "Discarded"})
        raise ValueError("discard this nested edit")

    def fail_refresh():
        raise OSError("refresh after rollback failed")

    def outer(metadata):
        with pytest.raises(ValueError, match="discard"):
            metadata.batch(rolled_back)
        patch.setattr(cache.storage, "reload", fail_refresh)
        return identity

    with monkeypatch.context() as patch:
        with pytest.raises(MetadataReconciliationError) as error:
            api.batch(outer)
    receipt = error.value.receipt
    assert receipt.result == identity and not receipt.changed
    assert receipt.persistence == "committed" and receipt.cache == "dirty"
    assert api.works.require(identity).work.work_title == "Retained"
    cache.close()


@pytest.mark.parametrize("mode", MODES)
def test_nested_schema_change_keeps_full_publication(metadata_db, mode, monkeypatch):
    api, cache = service(metadata_db, mode)
    calls = []
    if cache is not None:
        reload = cache.reload

        def publish():
            calls.append(True)
            return reload()

        monkeypatch.setattr(cache, "reload", publish)
    definition = api.batch(
        lambda outer: (
            outer.batch(
                lambda inner: (
                    inner.custom_fields.create_field("works", "batch_field").result
                )
            ).result
        )
    ).result
    assert api.custom_fields.get_field(("works", "batch_field")).id == definition.id
    if cache is not None:
        assert len(calls) == 1
        cache.close()


@pytest.mark.parametrize("mode", ("schema_backed", "numpy_vectorized"))
def test_scalar_batch_repairs_rows_and_projections_without_full_refresh(
    metadata_db, mode, monkeypatch
):
    api, cache = service(metadata_db, mode)
    work = api.works.create({"title": "Before"}).result
    assert api.works.require(work).work.work_title == "Before"

    def forbid():
        raise AssertionError("scalar edit rebuilt the whole cache")

    with monkeypatch.context() as scoped:
        scoped.setattr(cache, "reload", forbid)
        scoped.setattr(cache, "reload_data", forbid)
        receipt = api.batch(
            lambda outer: (
                outer.batch(
                    lambda inner: inner.works.update(work, {"title": "After"}).result
                ).result
            )
        )
        assert receipt.cache == "current"
        assert api.works.require(work).work.work_title == "After"
        assert [ref.entity_id for ref in api.search.find("work.title:After")] == [work]
        assert not api.search.find("work.title:Before")
    cache.close()


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("metadata_db", ("SQLite", "SQLite_apsw"), indirect=True)
def test_optimistic_edits_use_authoritative_metadata_and_detect_relationship_changes(
    metadata_db, mode
):
    api, cache = service(metadata_db, mode)
    entity_id = api.works.create({"title": "Original"}).result
    first = api.works.read_for_edit(entity_id)
    assert first.value.work.work_title == "Original"
    api.works.update(entity_id, {"title": "Current"}, expected_revision=first.revision)
    with pytest.raises(MetadataConflictError):
        api.works.update(
            entity_id, {"title": "Stale"}, expected_revision=first.revision
        )
    current = api.works.read_for_edit(entity_id)
    # Direct Catalog changes can leave a bound snapshot stale. Preconditions must
    # compare the database, and attached metadata changes must invalidate them.
    Catalog(metadata_db).mutations.writer.attach_metadata(
        level="work", entity_id=entity_id, data={"notes": ["External note"]}
    )
    with pytest.raises(MetadataConflictError):
        api.works.delete(entity_id, expected_revision=current.revision)
    latest = api.works.read_for_edit(entity_id)
    assert latest.value.notes and latest.revision != current.revision
    assert (
        api.batch(
            lambda metadata: (
                metadata.works.update(entity_id, {"title": "Batch edit"}).result
            ),
            expected=(latest.revision,),
        ).result
        == entity_id
    )
    with pytest.raises(MetadataConflictError):
        api.batch(
            lambda metadata: metadata.works.create({"title": "Must not run"}),
            expected=(latest.revision,),
        )
    assert not api.search.find("work.title:=Must not run")
    final = api.works.read_for_edit(entity_id)
    api.works.delete(entity_id, expected_revision=final.revision)
    with pytest.raises(MetadataConflictError):
        api.works.delete(entity_id, expected_revision=final.revision)
    if cache is not None:
        cache.close()


@pytest.mark.parametrize("mode", MODES)
def test_shared_metadata_updates_refresh_search_and_preserve_policy(metadata_db, mode):
    api, cache = service(metadata_db, mode)
    work = api.works.create({"title": "Shared identities"}).result
    person = api.agents.create_person({"name": "Original Writer"}).result
    api.mutations.attach_metadata(
        level="work",
        entity_id=work,
        data={"agents": [{"agent_id": person, "role": "aut"}]},
    )
    api.agents.rename(person, "Revised Writer", derive_sort=True)
    assert [ref.entity_id for ref in api.search.find("author:Revised")] == [work]
    assert not api.search.find("author:Original")
    agent = api.agents.require(person)
    agent.fields["name"] = "Detached"
    assert api.agents.require(person).fields["name"] == "Revised Writer"
    assert api.agents.get(999999) is None
    assert api.agents.list(limit=1)[0].entity_id == person
    api.agents.update(person, {"note": "Updated identity"})
    organisation = api.agents.create_organisation({"name": "Temporary Press"}).result
    api.agents.delete(organisation)
    for kind in ("tags", "series"):
        vocabulary = api.vocabulary(kind)
        value = vocabulary.create({"name": "Original shared value"}).result
        assert vocabulary.link_to_work(entity_id=value, work_id=work).changed
        assert not vocabulary.link_to_work(entity_id=value, work_id=work).changed
        vocabulary.update(value, {"name": "Renamed value"})
        assert vocabulary.require(value).fields["name"] == "Renamed value"
        assert vocabulary.unlink_from_work(entity_id=value, work_id=work).changed
        assert not vocabulary.unlink_from_work(entity_id=value, work_id=work).changed
        vocabulary.delete(value)
        assert vocabulary.get(value) is None

    assert not hasattr(api.vocabulary("languages"), "create")
    if cache is not None:
        cache.close()


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("metadata_db", ("SQLite", "SQLite_apsw"), indirect=True)
def test_custom_field_lifecycle_values_queries_and_native_conflicts(metadata_db, mode):
    api, cache = service(metadata_db, mode)
    work = api.works.create({"title": "Custom values"}).result
    owner = OwnerRef("works", work)
    fields = api.custom_fields
    definition = fields.create_field("works", "shelf", mode="set").result
    assert definition in fields.list_fields()
    receipt = fields.set_value(
        owner, definition.id, ["Library"], expected_revision=definition.revision
    )
    assert receipt.persistence == "committed"
    assert fields.get_value(owner, definition.id)[0].value == "Library"
    with pytest.raises(CustomFieldError):
        fields.set_value(
            owner, definition.id, ["Stale"], expected_revision=definition.revision
        )
    assert fields.query("works", [Predicate(definition.id, "eq", "Library")]).items == (
        owner,
    )
    assert (
        fields.get_values([owner], [definition.id])[owner][definition.id][0].value
        == "Library"
    )
    inventory = fields.inventory(definition.id)
    fields.rename_value(definition.id, inventory.items[0]["id"], "Study")
    assert fields.get_value(owner, definition.id)[0].value == "Study"
    fields.delete_value(definition.id, inventory.items[0]["id"])
    assert fields.get_value(owner, definition.id) == ()
    fields.apply_patch([FieldPatch(owner, definition.id, ["Restored"])])
    plan = fields.plan_field_migration(definition.id, {"label": "room", "name": "Room"})
    fields.apply_field_migration(plan)
    assert fields.get_field(("works", "room")).id == definition.id
    assert fields.audit() == ()
    fields.retire_field(definition.id)
    assert not fields.list_fields()
    assert fields.get_field(definition.id, include_retired=True).retired
    fields.apply_purge(fields.plan_purge(definition.id))
    assert fields.list_fields(include_retired=True) == ()
    if cache is not None:
        cache.close()
