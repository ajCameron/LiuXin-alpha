"""Exercise application-shaped metadata queries, edits and relationship workflows."""

from __future__ import annotations

from dataclasses import replace

import pytest

from LiuXin_alpha.catalog.api.common import CatalogNotFoundError
from LiuXin_alpha.catalog.api.metadata_revisions import MetadataConflictError
from LiuXin_alpha.catalog.api.metadata_search import MetadataSearchFilter
from tests.databases.caches.test_metadata_api import MODES, service, stack
from tests.databases.caches.test_metadata_api import metadata_db as metadata_db
from tests.databases.caches.test_metadata_api import (
    metadata_template as metadata_template,
)


@pytest.mark.parametrize("mode", MODES)
def test_shared_picker_bulk_reads_and_runtime_capabilities(metadata_db, mode):
    api, cache = service(metadata_db, mode)
    first = api.ensure_tag("History & Nature").result
    second = api.ensure_tag("History of Art").result
    agent = api.agents.create_person(
        {"name": "Ursula Le Guin", "aliases": ["U. Le Guin"]}
    ).result
    page = api.tags.suggest("ｈｉｓｔｏｒｙ", limit=1)
    assert page.total_count == 2 and page.has_more and len(page.records) == 1
    assert page.source == ("database" if cache is None else "cache")
    assert api.tags.suggest("&", mode="contains").records[0].entity_id == first
    assert api.tags.suggest("History Nature", mode="exact").records == ()
    assert api.tags.suggest("  ").records == ()
    assert api.agents.suggest("u. le", mode="prefix").records[0].entity_id == agent
    assert (
        api.tags.lookup({"text": "  ＨＩＳＴＯＲＹ & nature "}).records[0].entity_id
        == first
    )
    values = api.tags.get_many((second, 999999, second))
    assert values[1] is None and values[0] == values[2] and values[0] is not values[2]
    values[0].fields["name"] = "Locally changed"
    assert values[2].fields["name"] == "History of Art"
    with pytest.raises(CatalogNotFoundError):
        api.tags.get_many((first, 999999), missing="raise")
    for fields in ({}, {"physical_column": "x"}):
        with pytest.raises(ValueError):
            api.tags.lookup(fields)
    with pytest.raises(TypeError):
        api.tags.lookup({"name": 12})
    assert not hasattr(api.languages, "create")
    assert not hasattr(api.languages, "read_for_edit")
    assert not hasattr(api.genres, "link_to_work")
    assert not hasattr(api.ratings, "suggest")
    assert hasattr(api.series, "link_to_work") and hasattr(api.series, "subtree")
    if cache is not None:
        cache.close()


@pytest.mark.parametrize("mode", MODES)
def test_hydrated_search_pages_and_record_edit_preconditions(metadata_db, mode):
    api, cache = service(metadata_db, mode)
    created = stack(api).result
    page = api.works.search(
        "work.title:Garden",
        filters=(MetadataSearchFilter("id", "eq", created.work_id),),
    )
    assert page.total_count == 1 and page.references[0].entity_id == created.work_id
    assert page.records[0].work.work_title == "The Metadata Garden"
    assert api.works.search("work.title:Garden", limit=0).records == ()
    tag_id = api.ensure_tag("Before").result
    observed = api.tags.read_for_edit(tag_id)
    api.tags.update(tag_id, {"name": "After"}, expected_revision=observed.revision)
    with pytest.raises(MetadataConflictError):
        api.tags.delete(tag_id, expected_revision=observed.revision)
    latest = api.tags.read_for_edit(tag_id)
    work = api.works.read_for_edit(created.work_id)
    api.batch(
        lambda current: current.tags.update(tag_id, {"name": "Batched"}),
        expected=(latest.revision, work.revision),
    )
    with pytest.raises(ValueError):
        api.tags.update(
            tag_id,
            {"name": "Wrong"},
            expected_revision=replace(
                latest.revision,
                reference=replace(latest.revision.reference, kind="labels"),
            ),
        )
    genre = api.ensure_genre("Genre").result
    snapshot = api.genres.read_for_edit(genre)
    api.genres.update(
        genre, {"name": "Edited Genre"}, expected_revision=snapshot.revision
    )
    with pytest.raises(MetadataConflictError):
        api.genres.delete(genre, expected_revision=snapshot.revision)
    if cache is not None:
        cache.close()


@pytest.mark.parametrize("mode", MODES)
def test_work_memberships_and_role_specific_credit_edits(metadata_db, mode):
    api, cache = service(metadata_db, mode)
    created = stack(api).result
    work_id = created.work_id
    tag = api.works.add_tag(work_id, text="Atomic tag")
    assert tag.changed and tag.persistence == "committed"
    generation = None if cache is None else cache.generation
    repeated = api.works.add_tag(work_id, text="ATOMIC TAG")
    assert repeated.result == tag.result and not repeated.changed
    assert repeated.generation == generation
    assert api.works.list_tags(work_id)[0].member.entity_id == tag.result
    assert not api.works.set_tags(work_id, (tag.result,)).changed
    with pytest.raises(ValueError):
        api.works.set_tags(work_id, (tag.result, tag.result))
    with pytest.raises(CatalogNotFoundError):
        api.works.add_tag(999999, text="Must roll back")
    assert api.tags.lookup({"name": "Must roll back"}).records == ()
    series_id = api.ensure_series("A Series").result
    api.works.add_series(work_id, series_id)
    series_before = api.works.list_series(work_id)
    assert not api.works.set_series(work_id, (series_id,)).changed
    assert api.works.list_series(work_id) == series_before
    agent_id = api.agents.create_person({"name": "One Agent"}).result
    other_id = api.agents.create_person({"name": "Another Agent"}).result
    for entities, entity_id in (
        (api.works, work_id),
        (api.expressions, created.expression_id),
        (api.manifestations, created.manifestation_id),
        (api.items, created.item_ids[0]),
    ):
        entities.add_credit(entity_id, agent_id=agent_id, role="aut")
        entities.add_credit(entity_id, agent_id=agent_id, role="edt")
        assert not entities.add_credit(entity_id, agent_id=agent_id, role="aut").changed
        entities.set_credits(entity_id, role="aut", agent_ids=(other_id, agent_id))
        credits = entities.list_credits(entity_id)
        assert tuple(c.agent.entity_id for c in credits if c.role == "aut") == (
            other_id,
            agent_id,
        )
        retained = tuple(c for c in credits if c.role == "edt")
        entities.remove_credit(entity_id, agent_id=agent_id, role="aut")
        assert (
            tuple(c for c in entities.list_credits(entity_id) if c.role == "edt")
            == retained
        )
        assert not entities.remove_credit(
            entity_id, agent_id=agent_id, role="aut"
        ).changed
        assert not entities.set_credits(
            entity_id, role="aut", agent_ids=(other_id,)
        ).changed
    snapshot = api.works.read_for_edit(work_id)
    api.works.remove_tag(work_id, tag.result, expected_revision=snapshot.revision)
    with pytest.raises(MetadataConflictError):
        api.works.set_tags(work_id, (), expected_revision=snapshot.revision)
    api.works.set_series(work_id, ())
    assert api.works.list_series(work_id) == ()
    assert api.series.require(series_id)
    if cache is not None:
        cache.close()


@pytest.mark.parametrize("mode", ("database_backed", "numpy_vectorized"))
def test_composite_reads_capture_once_and_fallback_as_a_whole(
    metadata_db, mode, monkeypatch
):
    from LiuXin_alpha.caches.api import UnsupportedCacheQueryError
    from LiuXin_alpha.caches.metadata import CatalogCache

    api, cache = service(metadata_db, mode)
    created = stack(api).result
    calls = []
    original = api._reads

    def capture(*args, **kwargs):
        calls.append(True)
        return original(*args, **kwargs)

    monkeypatch.setattr(api, "_reads", capture)
    assert api.works.search().records
    assert len(calls) == 1
    tag_id = api.ensure_tag("Read once").result
    calls.clear()
    api.tags.get_many((tag_id, tag_id))
    assert len(calls) == 1
    fallback = CatalogCache(api._catalog, cache=cache, allow_database_fallback=True)
    hydrate = fallback._works._hydrate
    attempts = []

    def unavailable(source, entity_id):
        attempts.append("cache" if source.cache is not None else "database")
        if source.cache is not None:
            raise UnsupportedCacheQueryError("Unavailable related table")
        return hydrate(source, entity_id)

    monkeypatch.setattr(fallback._works, "_hydrate", unavailable)
    page = fallback.works.search()
    assert attempts == ["cache", "database"]
    assert page.source == "database" and page.generation is None
    assert page.references[0].entity_id == created.work_id

    def corrupt(source, entity_id):
        raise ValueError("Corrupt source")

    monkeypatch.setattr(fallback._works, "_hydrate", corrupt)
    with pytest.raises(ValueError, match="Corrupt"):
        fallback.works.search()
    cache.close()


@pytest.mark.parametrize("mode", MODES)
def test_lookup_scope_literal_candidates_and_complete_search(metadata_db, mode):
    """Keep candidate discovery separate from identity choice and count each WEMI level."""
    api, cache = service(metadata_db, mode)
    ids = stack(api).result
    for entities, identity in (
        (api.works, ids.work_id),
        (api.expressions, ids.expression_id),
        (api.manifestations, ids.manifestation_id),
        (api.items, ids.item_ids[0]),
    ):
        page = entities.search(filters=(MetadataSearchFilter("id", "eq", identity),))
        assert page.total_count == 1 and len(page.records) == 1
        assert page.references[0].entity_id == identity
        page.records[0].to_mapping()
        assert entities.search(offset=10).records == ()
    root = api.ensure_genre("Fiction").result
    child = api.ensure_genre("History", parent_id=root).result
    peer = api.ensure_genre("History").result
    assert {r.entity_id for r in api.genres.lookup({"name": "History"}).records} == {
        child,
        peer,
    }
    assert (
        api.genres.lookup({"name": "History", "parent_id": None}).records[0].entity_id
        == peer
    )
    assert (
        api.genres.suggest("hist", filters={"parent_id": root}).records[0].entity_id
        == child
    )
    assert api.genres.lookup({"name": "History"}, limit=0).total_count == 2
    assert api.genres.lookup({"name": "History"}, offset=10).records == ()
    for text in ("A.*[B]", "A and B", "Café", "Cafe"):
        api.tags.create({"name": text})
    assert len(api.tags.suggest(".*[", mode="contains").records) == 1
    assert len(api.tags.suggest("Cafe", mode="exact").records) == 1
    assert api.languages.suggest("en").records
    for fields in ({"parent_id": True}, {"parent_id": 1.5}):
        with pytest.raises(TypeError):
            api.genres.lookup(fields)
    with pytest.raises(ValueError):
        api.tags.lookup({"name": "A", "text": "B"})
    if cache is not None:
        cache.close()


@pytest.mark.parametrize("mode", MODES)
def test_shared_tokens_cover_stored_rows_and_relationship_rollback(
    metadata_db, mode, monkeypatch
):
    """Check authoritative conflicts, full rollback, and the deliberate row-only token scope."""
    api, cache = service(metadata_db, mode)
    work = api.works.create({"title": "Owner"}).result
    tag = api.ensure_tag("Retained").result
    snapshot = api.tags.read_for_edit(tag)
    api.works.add_tag(work, tag_id=tag)
    assert api.tags.read_for_edit(tag).revision == snapshot.revision
    api._catalog.tags.update(tag, {"name": "External edit"})
    with pytest.raises(MetadataConflictError):
        api.tags.update(tag, {"name": "Stale"}, expected_revision=snapshot.revision)
    current = api.tags.read_for_edit(tag)
    assert current.fields["name"] == "External edit"
    metadata_db.macros.update_row(
        "tags", tag, {"tag_scratch": "Hidden stored change"}, id_column="tag_id"
    )
    with pytest.raises(MetadataConflictError):
        api.tags.update(
            tag, {"name": "Ignored hidden change"}, expected_revision=current.revision
        )
    current = api.tags.read_for_edit(tag)
    # A direct Catalog write bypasses publication; explicitly repair the bound cache.
    if cache is not None:
        cache.reload()

    def fail_link(**kwargs):
        raise RuntimeError("Late attachment failure")

    with monkeypatch.context() as patch:
        patch.setattr(api._catalog.tags, "link_to_work", fail_link)
        with pytest.raises(RuntimeError, match="Late attachment"):
            api.works.add_tag(work, text="Must disappear")
    assert api.tags.lookup({"name": "Must disappear"}).records == ()
    assert api.works.list_tags(work)[0].member.entity_id == tag

    def fail_batch(current_api):
        current_api.tags.update(tag, {"name": "Rolled back"})
        current_api.works.set_tags(work, ())
        current_api.works.add_tag(work, text="Also rolled back")
        raise RuntimeError("Rollback the complete edit")

    with pytest.raises(RuntimeError, match="complete edit"):
        api.batch(fail_batch, expected=(current.revision,))
    assert api.tags.read_for_edit(tag).revision == current.revision
    assert api.works.list_tags(work)[0].member.entity_id == tag
    assert api.tags.lookup({"name": "Also rolled back"}).records == ()
    agent = api.agents.create_person({"name": "Disposable"}).result
    token = api.agents.read_for_edit(agent).revision
    api.agents.delete(agent)
    with pytest.raises(MetadataConflictError):
        api.agents.rename(agent, "Gone", expected_revision=token)
    if cache is not None:
        cache.close()


@pytest.mark.parametrize("metadata_db", ("SQLite", "SQLite_apsw"), indirect=True)
def test_independent_writers_serialize_atomic_tag_attachment_and_shared_edits(
    metadata_db,
):
    """Race separate database handles so facade locks cannot hide lost updates."""
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    from LiuXin_alpha.caches.metadata import MetadataAPI
    from LiuXin_alpha.catalog.catalog import Catalog
    from LiuXin_alpha.databases.database import Database

    api, _ = service(metadata_db, None)
    work = api.works.create({"title": "Concurrent owner"}).result
    barrier = Barrier(2)

    def run(operation):
        with Database(
            metadata={"database_path": metadata_db.metadata["database_path"]},
            db_type=metadata_db.type,
            backup=False,
            repair_bootstrap_rows=False,
            enable_storage_manager=False,
            enable_maintenance=False,
        ) as independent:
            bound = MetadataAPI(Catalog(independent))
            barrier.wait(timeout=30)
            try:
                return operation(bound)
            except MetadataConflictError as error:
                return error

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(run, lambda bound: bound.works.add_tag(work, text="Shared"))
            for _ in range(2)
        ]
        receipts = [future.result(timeout=60) for future in futures]
        assert receipts[0].result == receipts[1].result
        assert sum(receipt.changed for receipt in receipts) == 1
        identity = receipts[0].result
        token = api.tags.read_for_edit(identity).revision
        futures = [
            pool.submit(
                run,
                lambda bound, index=index: bound.tags.update(
                    identity, {"name": f"Writer {index}"}, expected_revision=token
                ),
            )
            for index in range(2)
        ]
        outcomes = [future.result(timeout=60) for future in futures]
    assert sum(isinstance(value, MetadataConflictError) for value in outcomes) == 1
    assert len(api.tags.list()) == 1
    assert len(api.works.list_tags(work)) == 1


@pytest.mark.parametrize(
    "mode", ("schema_backed", "database_backed", "numpy_vectorized")
)
def test_relationship_commit_receipt_pending_reads_and_retained_service_lifecycle(
    metadata_db, mode, monkeypatch
):
    """Separate committed SQL from failed publication and reject retained closed readers."""
    from concurrent.futures import ThreadPoolExecutor

    from LiuXin_alpha.caches.api import (
        CacheClosedError,
        CacheConsistency,
        CacheDirtyError,
    )
    from LiuXin_alpha.catalog.api.metadata_results import MetadataReconciliationError

    api, cache = service(metadata_db, mode)
    work = api.works.create({"title": "Lifecycle"}).result
    retained = api.tags
    with metadata_db.macros.transaction(serialized=True):
        receipt = api.works.add_tag(work, text="Pending")
        assert receipt.persistence == "pending"

        def reads():
            for read in (
                lambda: api.works.search(),
                lambda: api.tags.get_many((receipt.result,)),
                lambda: api.works.list_tags(work),
            ):
                with pytest.raises(CacheDirtyError):
                    read()

        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(reads).result(timeout=20)
    assert retained.suggest("Pending").records[0].entity_id == receipt.result

    publication = (
        "_advance_generation"
        if cache.capabilities.consistency == CacheConsistency.LIVE
        else "catch_up"
    )
    original_publication = getattr(cache, publication)
    failures = []

    def fail():
        if not failures:
            failures.append(True)
            raise OSError("Refresh unavailable")
        original_publication()

    with monkeypatch.context() as patch:
        patch.setattr(cache, publication, fail)
        with pytest.raises(MetadataReconciliationError) as caught:
            api.works.add_tag(work, text="Committed")
    assert caught.value.receipt.persistence == "committed"
    assert caught.value.receipt.cache == "dirty"
    committed = caught.value.receipt.result
    assert api.tags.read_for_edit(committed).fields["name"] == "Committed"
    assert {m.member.entity_id for m in api.works.list_tags(work)} == {
        committed,
        receipt.result,
    }
    cache.close()
    for operation in (
        lambda: retained.get_many(()),
        lambda: retained.lookup({"name": "Pending"}),
        lambda: api.works.search(),
        lambda: api.works.list_tags(work),
    ):
        with pytest.raises(CacheClosedError):
            operation()


@pytest.mark.parametrize("metadata_db", ("SQLite", "SQLite_apsw"), indirect=True)
def test_credit_priority_allocation_spans_owners_without_rewriting_other_roles(
    metadata_db,
    monkeypatch,
):
    """The same Agent may appear on many owners despite reversed physical priority scope."""
    api, _ = service(metadata_db, None)
    first, second = stack(api).result, stack(api).result
    agent = api.agents.create_person({"name": "Shared Writer"}).result
    peer = api.agents.create_person({"name": "Other Writer"}).result
    for entities, owners in (
        (api.works, (first.work_id, second.work_id)),
        (api.expressions, (first.expression_id, second.expression_id)),
        (api.manifestations, (first.manifestation_id, second.manifestation_id)),
        (api.items, (first.item_ids[0], second.item_ids[0])),
    ):
        for owner in owners:
            entities.add_credit(owner, agent_id=agent, role="aut")
            entities.add_credit(owner, agent_id=agent, role="edt")
        original = entities.list_credits(owners[0])
        other_role = tuple(
            c for c in entities.list_credits(owners[1]) if c.role == "edt"
        )
        for ordered in ((peer, agent), (agent, peer), (peer,)):
            entities.set_credits(owners[1], role="aut", agent_ids=ordered)
            actual = entities.list_credits(owners[1])
            assert (
                tuple(c.agent.entity_id for c in actual if c.role == "aut") == ordered
            )
            assert tuple(c for c in actual if c.role == "edt") == other_role
            assert entities.list_credits(owners[0]) == original

    from LiuXin_alpha.catalog.api.common import CatalogMutationError

    original_spec = api._catalog.agents._link_spec
    before = api.works.list_credits(first.work_id)
    monkeypatch.setattr(
        api._catalog.agents,
        "_link_spec",
        lambda *args: replace(original_spec(*args), type_part_of_identity=False),
    )
    with pytest.raises(CatalogMutationError, match="endpoint-and-role identity"):
        api.works.add_credit(first.work_id, agent_id=agent, role="aut")
    with pytest.raises(CatalogMutationError, match="endpoint-and-role identity"):
        api.works.set_credits(first.work_id, role="aut", agent_ids=(peer,))
    assert api.works.list_credits(first.work_id) == before


def test_shared_bulk_fallback_discards_partial_values_and_closes_capture(
    metadata_db, monkeypatch
):
    """A second missing capability must discard the first cached result too."""
    from types import SimpleNamespace

    from LiuXin_alpha.caches.api import UnsupportedCacheQueryError
    from LiuXin_alpha.caches.metadata import CatalogCache
    from LiuXin_alpha.caches.metadata.reads import MetadataReadSource

    api, cache = service(metadata_db, "schema_backed")
    first = api.ensure_tag("First").result
    second = api.ensure_tag("Second").result
    fallback = CatalogCache(api._catalog, cache=cache, allow_database_fallback=True)
    captures = []

    class PartialSource:
        allow_database_fallback = False

        def __init__(self):
            self.closed = False
            self.cache = SimpleNamespace(close=self.close)
            captures.append(self)

        def close(self):
            self.closed = True

        def get_row_from_id(self, table, identity):
            if identity == second:
                raise UnsupportedCacheQueryError("Second record unavailable")
            row = MetadataReadSource(
                metadata_db, None, allow_database_fallback=False
            ).get_row_from_id(table, identity)
            row.row_dict["tag"] = "Discarded partial cache value"
            return row

    monkeypatch.setattr(fallback, "_reads", PartialSource)
    assert tuple(
        record.fields["name"] for record in fallback.tags.get_many((first, second))
    ) == ("First", "Second")
    assert len(captures) == 1 and captures[0].closed

    def no_capture():
        raise UnsupportedCacheQueryError("Capture unavailable")

    monkeypatch.setattr(fallback, "_reads", no_capture)
    assert fallback.tags.lookup({"name": "First"}).source == "database"
    cache.close()


@pytest.mark.parametrize("mode", (None, "schema_backed"))
def test_public_picker_search_and_mixed_token_edit_workflow(metadata_db, mode):
    """Run the documented application workflow without reaching into Catalog or SQL."""
    api, cache = service(metadata_db, mode)
    work = api.works.create({"title": "The Metadata Garden"}).result
    agent = api.agents.create_person({"name": "Garden Writer"}).result
    series = api.ensure_series("Garden Library").result
    api.works.add_tag(work, text="Gardening")
    api.works.add_series(work, series)
    api.works.add_credit(work, agent_id=agent, role="aut")
    candidates = api.tags.suggest("gard")
    page = api.works.search("work.title:Garden")
    assert page.references[0].entity_id == work
    observed = api.works.read_for_edit(work)
    tag = api.tags.read_for_edit(candidates.records[0].entity_id)

    def edit(current):
        current.tags.update(tag.value.entity_id, {"name": "Practical gardening"})
        current.works.set_tags(work, (tag.value.entity_id,))
        current.works.set_credits(work, role="aut", agent_ids=(agent,))
        return work

    receipt = api.batch(edit, expected=(observed.revision, tag.revision))
    assert receipt.persistence == "committed" and receipt.result == work
    assert api.works.list_tags(work)[0].member.fields["name"] == "Practical gardening"
    if cache is not None:
        cache.close()


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("metadata_db", ("SQLite_apsw",), indirect=True)
def test_apsw_picker_hydrated_search_and_relationship_workflows(metadata_db, mode):
    """Exercise every cache mode through APSW's real transaction and row adapters."""
    test_shared_picker_bulk_reads_and_runtime_capabilities(metadata_db, mode)
    test_hydrated_search_pages_and_record_edit_preconditions(metadata_db, mode)
    test_work_memberships_and_role_specific_credit_edits(metadata_db, mode)


def test_postgres_profile_contains_native_vocabulary_tree_columns():
    """Catch missing physical hierarchy columns before any live backend is available."""
    from LiuXin_alpha.catalog.hierarchy import TREE_SPECS
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.schema import (
        build_schema_statements,
        schema_table_catalog,
    )

    statements = build_schema_statements()
    for level in ("work", "expression", "manifestation", "item"):
        table = f"agent_{level}_links"
        ddl = next(
            sql.lower()
            for sql in statements
            if sql.lower().startswith(f'create table if not exists "{table}" (')
        )
        assert f"agent_{level}_link_unique_pair_type" in ddl
        assert f'agent_{level}_link_unique_pair"' not in ddl
    tables = schema_table_catalog()
    for spec in TREE_SPECS.values():
        assert {spec.identity, spec.parent, spec.position} <= set(tables[spec.table])


@pytest.mark.parametrize("mode", (None, "schema_backed"))
def test_typed_series_memberships_preserve_physical_metadata(metadata_db, mode):
    """Membership convenience methods retain every existing type and provenance field."""
    from LiuXin_alpha.databases.db_types.macro_types import LinkValue

    api, cache = service(metadata_db, mode)
    work = api.works.create({"title": "Typed owner"}).result
    series = api.ensure_series("Typed series").result
    other = api.ensure_series("Another series").result
    spec = metadata_db.driver_wrapper.get_link_spec("series", "works")
    types = tuple(
        metadata_db.driver_wrapper.get_allowed_link_types(spec) or ("member", "related")
    )[:2]
    assert len(types) == 2
    for link_type in types:
        metadata_db.macros.upsert_link(
            spec,
            series,
            LinkValue(
                work,
                link_type=link_type,
                priority=17,
                extra={"series_work_link_source": "Imported"},
            ),
        )
    if cache is not None:
        cache.reload()
    before = tuple(metadata_db.macros.get_link_rows(spec, series))
    assert {member.link_type for member in api.works.list_series(work)} == set(types)
    assert not api.works.add_series(work, series).changed
    api.works.set_series(work, (other, series))
    assert tuple(metadata_db.macros.get_link_rows(spec, series)) == before
    assert {
        member.priority
        for member in api.works.list_series(work)
        if member.member.entity_id == series
    } == {17}
    api.works.remove_series(work, series)
    assert all(
        member.member.entity_id == other for member in api.works.list_series(work)
    )
    assert api.series.require(series)
    if cache is not None:
        cache.close()
