"""Exercise native core record services, identity scope and coordinated publication."""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from LiuXin_alpha.caches.api import (
    CacheClosedError,
    CacheDirtyError,
    UnsupportedCacheQueryError,
)
from LiuXin_alpha.caches.metadata import MetadataAPI
from LiuXin_alpha.catalog.api.common import (
    CatalogAmbiguousMatchError,
    CatalogMutationError,
    CatalogNotFoundError,
)
from LiuXin_alpha.catalog.api.metadata_access import MetadataReconciliationError
from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.databases.database import Database
from tests.databases.caches.test_metadata_api import MODES, service
from tests.databases.caches.test_metadata_api import metadata_db as metadata_db
from tests.databases.caches.test_metadata_api import (
    metadata_template as metadata_template,
)


@pytest.mark.parametrize("mode", MODES)
def test_named_vocabularies_ensure_exact_identity_and_noop_receipts(metadata_db, mode):
    api, cache = service(metadata_db, mode)
    kinds = ("tags", "labels", "genres", "subjects", "series", "languages", "ratings")
    for kind in kinds:
        assert getattr(api, kind) is api.vocabulary(kind)
    ensures = (
        lambda: api.ensure_tag("Science & Nature"),
        lambda: api.ensure_label("To Read"),
        lambda: api.ensure_genre("Fiction"),
        lambda: api.ensure_subject("History"),
        lambda: api.ensure_series("A sequence"),
        lambda: api.ensure_rating(3.5, out_of=5, source="reader"),
    )
    created = api.batch(lambda _: tuple(ensure() for ensure in ensures))
    assert created.changed
    assert all(
        receipt.changed and receipt.persistence == "pending"
        for receipt in created.result
    )
    generation = None if cache is None else cache.generation
    for first, ensure in zip(created.result, ensures, strict=True):
        reused = ensure()
        assert reused.result == first.result and not reused.changed
        assert reused.generation == generation
    assert (
        api.ensure_tag("  ＳＣＩＥＮＣＥ & nature  ").result == created.result[0].result
    )
    assert api.ensure_tag("Science Nature").result != created.result[0].result
    assert (
        api.tags.require(created.result[0].result).fields["name"] == "Science & Nature"
    )
    for ensure in (
        api.ensure_tag,
        api.ensure_label,
        api.ensure_genre,
        api.ensure_subject,
        api.ensure_series,
    ):
        with pytest.raises(ValueError):
            ensure(" ")
    with pytest.raises(CatalogMutationError):
        api.vocabulary("languages").create({"code": "invented"})
    if cache is not None:
        cache.close()


@pytest.mark.parametrize("mode", MODES)
def test_ensure_respects_parent_scale_source_and_ambiguous_values(metadata_db, mode):
    api, cache = service(metadata_db, mode)

    def populate(metadata):
        parent = metadata.ensure_genre("Parent").result
        root = metadata.ensure_genre("Shared").result
        child = metadata.ensure_genre("Shared", parent_id=parent).result
        assert root != child
        assert metadata.ensure_genre("shared").result == root
        assert metadata.ensure_genre("shared", parent_id=parent).result == child
        scores = {
            metadata.ensure_rating(3, out_of=5, source="one").result,
            metadata.ensure_rating(3, out_of=10, source="one").result,
            metadata.ensure_rating(3, out_of=5, source="two").result,
        }
        assert len(scores) == 3
        metadata.tags.create({"name": "Ambiguous"})
        # Historical/incomplete rows can lack the derived uniqueness key.
        with metadata_db.macros.transaction() as connection:
            connection.execute(
                "INSERT INTO tags(tag, tag_phash) VALUES (?, ?)",
                ("AMBIGUOUS", "historical-inconsistent-key"),
            )

    api.batch(populate)
    with pytest.raises(CatalogAmbiguousMatchError):
        api.ensure_tag("ambiguous")
    with pytest.raises(CatalogNotFoundError):
        api.ensure_subject("Missing parent", parent_id=999999)
    with pytest.raises((ValueError, TypeError)):
        api.ensure_rating(6, out_of=5, source="one")
    with pytest.raises(ValueError):
        api.ensure_rating(3, out_of=5, source="")
    if cache is not None:
        cache.close()


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("metadata_db", ("SQLite", "SQLite_apsw"), indirect=True)
def test_contextual_records_crud_ownership_and_detached_reads(metadata_db, mode):
    api, cache = service(metadata_db, mode)

    def populate(metadata):
        work = metadata.works.create({"title": "Owned metadata"}).result
        other_work = metadata.works.create({"title": "Other owner"}).result
        item = metadata.items.create({"type": "digital"}).result
        other_item = metadata.items.create({"type": "digital"}).result
        agent = metadata.agents.create_person({"name": "Example"}).result
        text_ids = {}
        for kind in ("notes", "comments", "synopses"):
            text = getattr(metadata, kind)
            text_ids[kind] = text.add_for_wemi(
                level="work", entity_id=work, text="Shared wording"
            ).result
            assert (
                text.add_for_wemi(
                    level="work", entity_id=other_work, text="Shared wording"
                ).result
                != text_ids[kind]
            )
        identifier = metadata.identifiers.create(
            {"scheme": "uuid", "value": "7fc20567-8c3b-4fb2-91b2-42d338f7cfd4"}
        ).result
        assigned = metadata.identifiers.link_to_wemi(
            identifier_id=identifier, level="work", entity_id=work
        ).result
        copied = metadata.identifiers.link_to_wemi(
            identifier_id=assigned, level="work", entity_id=other_work
        ).result
        agent_identifier = metadata.identifiers.create(
            {"scheme": "url", "value": "https://example.org/author"}
        ).result
        metadata.identifiers.link_to_agent(
            identifier_id=agent_identifier, agent_id=agent
        )
        assert copied != assigned
        observation = metadata.item_identifiers.create(
            {"item_id": item, "scheme": "barcode", "value": "ABC"}
        ).result
        metadata.item_identifiers.create(
            {"item_id": other_item, "scheme": "barcode", "value": "ABC"}
        )
        annotation = metadata.annotations.create(
            {
                "item_id": item,
                "kind": "bookmark",
                "anchor_type": "percentage",
                "anchor_start": "0.5",
                "user_id": 7,
            }
        ).result
        metadata.annotations.create(
            {
                "item_id": other_item,
                "kind": "bookmark",
                "anchor_type": "percentage",
                "anchor_start": "0.5",
                "user_id": 7,
            }
        )
        return (
            work,
            item,
            agent,
            text_ids,
            assigned,
            copied,
            observation,
            annotation,
            agent_identifier,
        )

    (
        work,
        item,
        agent,
        text_ids,
        assigned,
        copied,
        observation,
        annotation,
        agent_identifier,
    ) = api.batch(populate).result
    for kind, entity_id in text_ids.items():
        text = getattr(api, kind)
        assert [
            r.entity_id for r in text.list_for_wemi(level="work", entity_id=work)
        ] == [entity_id]
        record = text.require(entity_id)
        record.fields["text"] = "Detached"
        assert text.require(entity_id).fields["text"] == "Shared wording"
        assert text.get(999999) is None
    assert [
        r.entity_id for r in api.identifiers.list_for_wemi(level="work", entity_id=work)
    ] == [assigned]
    assert [r.entity_id for r in api.identifiers.list_for_agent(agent)] == [
        agent_identifier
    ]
    assert [r.entity_id for r in api.item_identifiers.list_for_item(item)] == [
        observation
    ]
    assert [
        r.entity_id
        for r in api.annotations.list_for_item(item, user_id=7, kind=" bookmark ")
    ] == [annotation]
    assert api.annotations.list_for_item(item, user_id=8) == ()
    assert api.annotations.list_for_item(item, kind="highlight") == ()

    def edits(metadata):
        metadata.notes.update(text_ids["notes"], {"text": "Edited note"})
        metadata.identifiers.update(assigned, {"source": "manual"})
        metadata.item_identifiers.update(observation, {"source": "scanner"})
        metadata.annotations.update(annotation, {"note_text": "Edited annotation"})

    api.batch(edits)
    assert api.notes.require(text_ids["notes"]).fields["text"] == "Edited note"
    assert api.identifiers.require(assigned).fields["source"] == "manual"
    assert api.item_identifiers.require(observation).fields["source"] == "scanner"
    assert (
        api.annotations.require(annotation).fields["note_text"] == "Edited annotation"
    )
    for records, entity_id in (
        (api.notes, text_ids["notes"]),
        (api.identifiers, assigned),
        (api.item_identifiers, observation),
        (api.annotations, annotation),
    ):
        assert entity_id in [r.entity_id for r in records.list()]
    for read in (
        lambda: api.notes.list_for_wemi(level="work", entity_id=999999),
        lambda: api.identifiers.list_for_agent(999999),
        lambda: api.item_identifiers.list_for_item(999999),
        lambda: api.annotations.list_for_item(999999),
    ):
        with pytest.raises(CatalogNotFoundError):
            read()

    def remove(metadata):
        for kind, entity_id in text_ids.items():
            getattr(metadata, kind).delete(entity_id)
        metadata.identifiers.delete(assigned)
        metadata.item_identifiers.delete(observation)
        metadata.annotations.delete(annotation)

    api.batch(remove)
    assert api.notes.list_for_wemi(level="work", entity_id=work) == ()
    assert api.identifiers.list_for_wemi(level="work", entity_id=work) == ()
    assert api.identifiers.require(copied).entity_id == copied
    assert api.item_identifiers.list_for_item(item) == ()
    assert api.annotations.list_for_item(item) == ()
    if cache is not None:
        cache.close()


@pytest.mark.parametrize("mode", MODES)
def test_new_record_validation_and_batch_rollback(metadata_db, mode):
    api, cache = service(metadata_db, mode)
    item = api.items.create({"type": "digital"}).result
    for records, payload in (
        (api.item_identifiers, {"scheme": "barcode", "value": "X"}),
        (
            api.annotations,
            {"kind": "note", "anchor_type": "page_offset", "anchor_start": "1"},
        ),
    ):
        for owner in ({}, {"item_id": True}, {"item_id": 999999}):
            with pytest.raises((TypeError, CatalogNotFoundError)):
                records.create(payload | owner)
        assert records.list() == ()
    with pytest.raises(CatalogMutationError):
        api.annotations.create({"item_id": item, "kind": "note"})
    with pytest.raises(CatalogMutationError):
        api.item_identifiers.create(
            {"item_id": item, "scheme": "barcode", "value": " "}
        )

    def fail(metadata):
        metadata.ensure_tag("Rolled back")
        metadata.item_identifiers.create(
            {"item_id": item, "scheme": "barcode", "value": "X"}
        )
        raise RuntimeError("abort batch")

    with pytest.raises(RuntimeError, match="abort batch"):
        api.batch(fail)
    assert api.tags.list() == ()
    assert api.item_identifiers.list_for_item(item) == ()
    if cache is not None:
        retained = api.annotations
        cache.close()
        with pytest.raises(CacheClosedError):
            retained.list_for_item(item)


def test_ensure_uses_authority_after_cached_miss_and_serializes_competing_calls(
    metadata_db,
):
    api, cache = service(metadata_db, "schema_backed")
    assert api.tags.list() == ()
    existing = Catalog(metadata_db).tags.create({"name": "External"})
    receipt = api.ensure_tag("external")
    assert not receipt.changed and receipt.result == existing
    # External Catalog writers still owe their own cache invalidation.
    cache.reload_data()
    assert api.tags.require(existing).fields["name"] == "External"
    barrier = Barrier(2)

    def ensure():
        independent = MetadataAPI(Catalog(metadata_db))
        barrier.wait(timeout=10)
        return independent.ensure_tag("Concurrent")

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(ensure) for _ in range(2)]
        receipts = [future.result(timeout=30) for future in futures]
    assert receipts[0].result == receipts[1].result
    assert sum(receipt.changed for receipt in receipts) == 1
    with metadata_db.macros.transaction():
        with pytest.raises(RuntimeError, match="serializ|reserv|upgrade"):
            MetadataAPI(Catalog(metadata_db)).ensure_tag("Unreserved")
    cache.close()


def test_contextual_writes_preserve_pending_and_committed_failure_receipts(
    metadata_db, monkeypatch
):
    api, cache = service(metadata_db, "schema_backed")
    work = api.works.create({"title": "Transactions"}).result
    with metadata_db.macros.transaction():
        receipt = api.notes.add_for_wemi(level="work", entity_id=work, text="Pending")
        assert receipt.persistence == "pending" and receipt.cache == "deferred"
        with pytest.raises(CacheDirtyError):
            api.notes.require(receipt.result)
    assert api.notes.require(receipt.result).fields["text"] == "Pending"

    def fail():
        raise OSError("refresh failed")

    with monkeypatch.context() as patch:
        patch.setattr(cache, "reload_data", fail)
        with pytest.raises(MetadataReconciliationError) as caught:
            api.notes.update(receipt.result, {"text": "Committed"})
    assert caught.value.receipt.persistence == "committed"
    assert caught.value.receipt.result == receipt.result
    assert api.notes.require(receipt.result).fields["text"] == "Committed"
    cache.close()


@pytest.mark.parametrize("metadata_db", ("SQLite", "SQLite_apsw"), indirect=True)
def test_ensure_serializes_independent_database_handles(metadata_db):
    """Exercise the database reservation independently of one facade's mutex."""
    barrier = Barrier(2)

    def write():
        with Database(
            metadata={"database_path": metadata_db.metadata["database_path"]},
            db_type=metadata_db.type,
            backup=False,
            repair_bootstrap_rows=False,
            enable_storage_manager=False,
            enable_maintenance=False,
        ) as independent:
            api = MetadataAPI(Catalog(independent))
            barrier.wait(timeout=30)
            return api.ensure_tag("Across connections")

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(write) for _ in range(2)]
        receipts = [future.result(timeout=60) for future in futures]
    assert receipts[0].result == receipts[1].result
    assert sum(receipt.changed for receipt in receipts) == 1
    assert len(Catalog(metadata_db).tags.list()) == 1


def test_contextual_cache_coverage_is_distinct_from_empty_results(
    metadata_db, monkeypatch
):
    api, cache = service(metadata_db, "schema_backed")
    item = api.items.create({"type": "digital"}).result
    work = api.works.create({"title": "Coverage"}).result
    note = api.notes.add_for_wemi(
        level="work", entity_id=work, text="Cached note"
    ).result

    def forbidden(*args, **kwargs):
        raise AssertionError("Confirmed empty cache read must not fall back")

    with monkeypatch.context() as patch:
        patch.setattr(api._catalog.item_identifiers, "list_for_item", forbidden)
        assert api.item_identifiers.list_for_item(item) == ()

    fallback = MetadataAPI(api._catalog, cache=cache, allow_database_fallback=True)

    def unavailable(*args, **kwargs):
        raise UnsupportedCacheQueryError("Not available")

    with monkeypatch.context() as patch:
        patch.setattr(cache, "query", unavailable)
        with pytest.raises(UnsupportedCacheQueryError):
            api.item_identifiers.list_for_item(item)
        assert fallback.item_identifiers.list_for_item(item) == ()

    def missing_link(*args, **kwargs):
        raise KeyError("Missing cached relation")

    with monkeypatch.context() as patch:
        patch.setattr(cache.storage, "get_link_table", missing_link)
        with pytest.raises(UnsupportedCacheQueryError):
            api.notes.list_for_wemi(level="work", entity_id=work)
        assert [
            r.entity_id
            for r in fallback.notes.list_for_wemi(level="work", entity_id=work)
        ] == [note]
    cache.close()
