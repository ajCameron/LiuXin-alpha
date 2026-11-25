"""Exercise real application consumers and failure boundaries of native metadata."""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from types import SimpleNamespace

import pytest

from LiuXin_alpha.catalog.api.metadata_access import MetadataReconciliationError
from LiuXin_alpha.catalog.api.metadata_revisions import MetadataConflictError
from LiuXin_alpha.core.errors import CoreHandlerError
from LiuXin_alpha.core.runtime import CoreRuntime
from LiuXin_alpha.databases.database import Database
from tests.databases.caches.test_metadata_api import metadata_db as metadata_db
from tests.databases.caches.test_metadata_api import (
    metadata_template as metadata_template,
)
from tests.databases.caches.test_metadata_api import service


@pytest.mark.parametrize("mode", (None, "schema_backed"))
def test_core_native_search_display_edit_conflict_and_recovery(
    metadata_db, mode, monkeypatch
):
    with CoreRuntime(
        library=SimpleNamespace(database=metadata_db), cache_type=mode
    ) as runtime:
        api = runtime.services.metadata
        entity_id = api.works.create(
            {"title": "Workflow", "creator_sort": "Hidden by older containers"}
        ).result
        page = runtime.query(
            "metadata.search",
            {
                "query": "work.title:Workflow",
                "filters": [{"field": "id", "operator": "in", "value": [entity_id]}],
            },
        )
        assert page["total_count"] == 1
        ids = [ref["entity_id"] for ref in page["references"]]
        display = runtime.query("metadata.read_many", {"entity_ids": ids + [999999]})
        assert display["entities"][0]["work"]["work_title"] == "Workflow"
        assert display["entities"][1] is None
        edit = runtime.query("metadata.read_for_edit", {"entity_id": entity_id})
        assert edit["fields"]["creator_sort"] == "Hidden by older containers"
        payload = {
            "entity_id": entity_id,
            "data": {"title": "Edited"},
            "expected_revision": edit["revision"],
        }
        result = runtime.command("metadata.update", payload)
        assert result["persistence"] == "committed"
        with pytest.raises(CoreHandlerError) as conflict:
            runtime.command("metadata.update", payload)
        assert conflict.value.code == "metadata_conflict"
        assert conflict.value.details["expected"] == edit["revision"]
        assert (
            runtime.query("metadata.search", {"query": "work.title:Edited"})[
                "total_count"
            ]
            == 1
        )
        with pytest.raises(CoreHandlerError) as invalid:
            runtime.query(
                "metadata.search",
                {"filters": [{"field": "id", "operator": "gte", "value": True}]},
            )
        assert invalid.value.code == "invalid_payload"
        assert invalid.value.details["parameter"] == "filters[0].value"
        if mode is not None:

            def fail(*args):
                raise OSError("Refresh failed after commit")

            with monkeypatch.context() as patch:
                patch.setattr(runtime.cache.storage, "reload_ids", fail)
                with pytest.raises(CoreHandlerError) as committed:
                    runtime.command(
                        "metadata.update",
                        {"entity_id": entity_id, "data": {"title": "Durable"}},
                    )
            assert committed.value.code == "metadata_reconciliation_failed"
            assert committed.value.details["canonical_write_committed"] is True
            assert committed.value.details["receipt"]["result"] == entity_id
            assert api.works.require(entity_id).work.work_title == "Durable"


@pytest.mark.parametrize("mode", (None, "schema_backed"))
def test_native_publication_failure_keeps_committed_receipt(metadata_db, mode):
    api, cache = service(metadata_db, mode)

    def fail(_receipt):
        raise RuntimeError("Listener failed after commit")

    unsubscribe = api._catalog.custom_fields.subscribe(fail)
    try:
        with pytest.raises(MetadataReconciliationError) as error:
            api.custom_fields.create_field("works", "committed_field")
        receipt = error.value.receipt
        assert receipt.persistence == "committed"
        assert receipt.cache == ("not_configured" if cache is None else "dirty")
        assert api.custom_fields.get_field(receipt.result.id).label == "committed_field"
    finally:
        unsubscribe()
        if cache is not None:
            cache.close()


def test_revisions_cover_scalar_fields_absent_from_old_identity_container(metadata_db):
    api, _ = service(metadata_db, None)
    entity_id = api.works.create(
        {"title": "Scalar test", "creator_sort": "Before"}
    ).result
    snapshot = api.works.read_for_edit(entity_id)
    assert snapshot.fields["creator_sort"] == "Before"
    api.works.update(entity_id, {"creator_sort": "After"})
    with pytest.raises(MetadataConflictError):
        api.works.update(
            entity_id, {"creator_sort": "Stale"}, expected_revision=snapshot.revision
        )


@pytest.mark.parametrize("rollback", (False, True))
def test_custom_schema_batch_outer_completion_and_rollback(metadata_db, rollback):
    api, cache = service(metadata_db, "schema_backed")
    try:
        with metadata_db.macros.transaction():
            result = api.batch(
                lambda metadata: (
                    metadata.custom_fields.create_field(
                        "works", "pending_field"
                    ).result.id
                )
            )
            assert result.persistence == "pending" and result.cache == "deferred"
            if rollback:
                raise ValueError("rollback")
    except ValueError:
        if not rollback:
            raise
    fields = api.custom_fields.list_fields()
    assert bool(fields) is not rollback
    assert api.works.list() == ()
    cache.close()


@pytest.mark.parametrize("metadata_db", ("SQLite", "SQLite_apsw"), indirect=True)
def test_independent_metadata_writers_cannot_both_apply_the_same_token(
    metadata_db, tmp_path
):
    first, _ = service(metadata_db, None)
    entity_id = first.works.create({"title": "Original"}).result
    token = first.works.read_for_edit(entity_id).revision
    with Database(
        metadata={"database_path": str(tmp_path / "metadata.sqlite")},
        backup=False,
        repair_bootstrap_rows=False,
        enable_storage_manager=False,
        enable_maintenance=False,
    ) as other_db:
        second, _ = service(other_db, None)
        barrier = Barrier(2)

        def edit(api, title):
            barrier.wait(timeout=10)
            try:
                api.works.update(entity_id, {"title": title}, expected_revision=token)
            except MetadataConflictError:
                return "conflict"
            return "committed"

        with ThreadPoolExecutor(max_workers=2) as executor:
            attempts = [
                executor.submit(edit, api, name)
                for api, name in ((first, "First writer"), (second, "Second writer"))
            ]
            outcomes = [attempt.result(timeout=30) for attempt in attempts]
        assert sorted(outcomes) == ["committed", "conflict"]
