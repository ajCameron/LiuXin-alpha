"""Exercise vocabulary forests through real Catalog and metadata/cache services."""

from concurrent.futures import ThreadPoolExecutor
from sqlite3 import IntegrityError
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
from LiuXin_alpha.catalog.api.metadata_results import MetadataReconciliationError
from LiuXin_alpha.catalog.api.metadata_trees import (
    HierarchyIntegrityError,
    HierarchyInUseError,
    HierarchyLimitError,
    VocabularyTreeInput,
)
from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.catalog.hierarchy import TREE_SPECS, ancestor_rows, subtree_rows
from LiuXin_alpha.databases.database import Database
from tests.databases.caches.test_metadata_api import MODES, service
from tests.databases.caches.test_metadata_api import metadata_db as metadata_db
from tests.databases.caches.test_metadata_api import (
    metadata_template as metadata_template,
)

KINDS = ("genres", "subjects", "series")


def ids(rows):
    return tuple(row.entity_id for row in rows)


def node(name, *children, **fields):
    return VocabularyTreeInput({"name": name, **fields}, children)


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("mode", MODES)
def test_tree_creation_read_order_and_exact_reuse(metadata_db, mode, kind):
    api, cache = service(metadata_db, mode)
    tree = getattr(api, kind)
    try:
        assert tree is api.vocabulary(kind)
        result = tree.create_tree(
            node(
                "Root",
                node("First", node("Leaf")),
                node("Second"),
                full="Imported label",
                tree_id="23",
            )
        )
        root, first, leaf, second = result.result
        assert result.changed and result.persistence == "committed"
        assert ids(tree.subtree(root)) == (root, first, leaf, second)
        assert ids(tree.ancestors(leaf)) == (root, first)
        assert ids(tree.children(root)) == (first, second)
        assert ids(tree.children(root, offset=1, limit=1)) == (second,)
        assert ids(tree.roots()) == (root,)
        generation = result.generation
        reused = tree.ensure_path(("root", "first", "leaf"))
        assert reused.result == (root, first, leaf) and not reused.changed
        assert reused.generation == generation
        assert tree.require(root).fields["full"] == "Imported label"
        record = tree.subtree(root)[0]
        record.fields["name"] = "local edit"
        assert tree.require(root).fields["name"] == "Root"
        assert tree.ancestors(root) == ()
        with pytest.raises(HierarchyLimitError):
            tree.subtree(root, max_nodes=3)
        with pytest.raises(HierarchyLimitError):
            tree.ancestors(leaf, max_nodes=1)
        with pytest.raises(CatalogNotFoundError):
            tree.children(999999)
    finally:
        if cache is not None:
            cache.close()


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("mode", MODES)
def test_moves_ordering_cycles_and_explicit_leaf_deletion(metadata_db, mode, kind):
    api, cache = service(metadata_db, mode)
    tree = getattr(api, kind)
    try:
        root, first, leaf, second = tree.create_tree(
            node("Root", node("First", node("Leaf")), node("Second"))
        ).result
        assert tree.reorder(parent_id=root, entity_ids=(second, first)).changed
        assert ids(tree.children(root)) == (second, first)
        assert not tree.reorder(parent_id=root, entity_ids=(second, first)).changed
        assert not tree.move(first, parent_id=root).changed
        assert tree.move(first, parent_id=root, before_id=second).changed
        assert ids(tree.children(root)) == (first, second)
        for target in (first, leaf):
            with pytest.raises(HierarchyIntegrityError):
                tree.move(first, parent_id=target)
        with pytest.raises(HierarchyIntegrityError):
            tree.update(
                root, {f"{kind[:-1] if kind != 'series' else 'series'}_parent_id": leaf}
            )
        with pytest.raises(ValueError):
            tree.reorder(parent_id=root, entity_ids=(first, first))
        with pytest.raises(ValueError):
            tree.move(first, parent_id=root, before_id=leaf)
        with pytest.raises(HierarchyInUseError):
            tree.delete(first)
        tree.move(leaf, parent_id=None)
        assert ids(tree.roots()) == (root, leaf)
        tree.delete(first)
        assert ids(tree.children(root)) == (second,)
        deleted = tree.delete_subtree(root)
        assert deleted.result == (second, root)
        assert ids(tree.roots()) == (leaf,)
    finally:
        if cache is not None:
            cache.close()


@pytest.mark.parametrize("metadata_db", ("SQLite", "SQLite_apsw"), indirect=True)
@pytest.mark.parametrize("kind", KINDS)
def test_catalog_validation_batch_graph_and_attachment_policy(metadata_db, kind):
    catalog = Catalog(metadata_db)
    repo = getattr(catalog, kind)
    root, child, leaf = repo.create_tree(node("Root", node("Child", node("Leaf"))))
    with pytest.raises(HierarchyIntegrityError):
        repo._update_many({root: {"parent_id": leaf}, leaf: {"parent_id": root}})
    # This inversion is valid as a combined edit and must not transiently cycle.
    repo._update_many({root: {"parent_id": child}, child: {"parent_id": None}})
    assert repo.require(root)[repo.tree_spec.parent] == child
    assert repo.require(child)[repo.tree_spec.parent] is None
    work = catalog.works.create({"title": "Linked work"})
    repo._link("works", work, kind, leaf)
    before = tuple(row[repo.id_column] for row in repo.subtree(child))
    with pytest.raises(HierarchyInUseError):
        repo.delete_subtree(child)
    assert tuple(row[repo.id_column] for row in repo.subtree(child)) == before
    assert repo.delete_subtree(child, detach=True) == tuple(reversed(before))
    assert catalog.works.require(work)


@pytest.mark.parametrize("mode", MODES)
def test_tree_rollback_limits_identity_and_pending_receipts(metadata_db, mode):
    api, cache = service(metadata_db, mode)
    try:
        root = api.genres.create({"name": "Root"}).result
        with pytest.raises(ValueError):
            api.genres.ensure_path("Root/Leaf")
        with pytest.raises(HierarchyLimitError):
            api.genres.create_tree(node("Too", node("Many")), max_nodes=1)
        with pytest.raises(ValueError):
            api.genres.create_tree(node("Bad", parent_id=root))
        with pytest.raises(CatalogNotFoundError):
            api.genres.ensure_path(("Missing parent",), parent_id=999999)
        with pytest.raises(ValueError):
            api.genres.create({"name": "Position", "position": 100})
        with pytest.raises(IntegrityError):
            api.genres.create_tree(
                node("Rollback", node("Duplicate"), node("Duplicate"))
            )
        assert len(api.genres.list()) == 1
        other_root, branch = api.genres.ensure_path(("Other", "Branch")).result
        another = api.genres.ensure_path(("Root", "Branch")).result[-1]
        assert another != branch
        with pytest.raises(IntegrityError):
            api.genres.move(branch, parent_id=root)
        assert ids(api.genres.children(other_root)) == (branch,)
        api.series.ensure_path(("First series", "Shared series"))
        with pytest.raises(CatalogMutationError):
            api.series.ensure_path(("Other series", "Shared series"))
        assert len(api.series.list()) == 2
        with pytest.raises(RuntimeError, match="abort"):
            with metadata_db.macros.transaction(serialized=True):
                pending = api.subjects.ensure_path(("Pending", "Child"))
                assert pending.persistence == "pending"
                if cache is not None:
                    with pytest.raises(CacheDirtyError):
                        api.subjects.roots()
                raise RuntimeError("abort")
        assert api.subjects.roots() == ()
    finally:
        if cache is not None:
            cache.close()


def test_whole_traversal_fallback_and_retained_lifecycle(metadata_db, monkeypatch):
    api, cache = service(metadata_db, "schema_backed")
    root, child = api.genres.create_tree(node("Root", node("Child"))).result
    real_query = cache.query
    calls = 0

    def unavailable(query):
        nonlocal calls
        calls += 1
        if calls >= 2:
            raise UnsupportedCacheQueryError("late unsupported traversal")
        return real_query(query)

    monkeypatch.setattr(cache, "query", unavailable)
    with pytest.raises(UnsupportedCacheQueryError):
        api.genres.subtree(root)
    # A retry must discard the earlier cached child page, including its stale membership.
    fresh = Catalog(metadata_db).genres.create({"name": "Fresh", "parent_id": root})
    calls = 0
    fallback = MetadataAPI(
        Catalog(metadata_db), cache=cache, allow_database_fallback=True
    )
    assert ids(fallback.genres.subtree(root)) == (root, child, fresh)
    retained = fallback.genres
    cache.close()
    with pytest.raises(CacheClosedError):
        retained.roots()


def test_tree_publication_failure_preserves_committed_result(metadata_db, monkeypatch):
    api, cache = service(metadata_db, "schema_backed")
    original = cache.reload_data

    def broken(*args, **kwargs):
        raise RuntimeError("publication failed")

    monkeypatch.setattr(cache, "reload_data", broken)
    with pytest.raises(MetadataReconciliationError) as caught:
        api.genres.create_tree(node("Committed", node("Child")))
    assert caught.value.receipt.persistence == "committed"
    assert len(caught.value.receipt.result) == 2
    monkeypatch.setattr(cache, "reload_data", original)
    cache.reload_data()
    assert len(api.genres.subtree(caught.value.receipt.result[0])) == 2
    cache.close()


@pytest.mark.parametrize("metadata_db", ("SQLite", "SQLite_apsw"), indirect=True)
def test_independent_handles_ensure_one_path(metadata_db):
    barrier = Barrier(2)
    path = metadata_db.metadata["database_path"]

    def create():
        with Database(
            metadata={"database_path": path},
            db_type=metadata_db.type,
            backup=False,
            repair_bootstrap_rows=False,
            enable_storage_manager=False,
            enable_maintenance=False,
        ) as db:
            api = MetadataAPI(Catalog(db))
            barrier.wait(timeout=30)
            return api.genres.ensure_path(("Concurrent", "Path")).result

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(create) for _ in range(2)]
        results = [future.result(timeout=60) for future in futures]
    assert results[0] == results[1]


def test_traversal_rejects_corruption_and_bounds_child_reads():
    spec = TREE_SPECS["genres"]

    class Source:
        def __init__(self):
            self.rows = {
                1: {"genre_id": 1, "genre_parent_id": None},
                2: {"genre_id": 2, "genre_parent_id": 1},
            }
            self.limits = []

        def get(self, identity):
            return self.rows.get(identity)

        def children(self, parent_id, *, limit, offset=0):
            self.limits.append(limit)
            return tuple(
                row for row in self.rows.values() if row["genre_parent_id"] == parent_id
            )[offset : offset + limit]

    source = Source()
    with pytest.raises(HierarchyLimitError):
        subtree_rows(source, spec, 1, max_nodes=1)
    assert source.limits == [1]
    source.rows[1]["genre_parent_id"] = 2
    with pytest.raises(HierarchyIntegrityError):
        ancestor_rows(source, spec, 2)
    with pytest.raises(HierarchyIntegrityError):
        subtree_rows(source, spec, 1)
    source.rows[1]["genre_parent_id"] = 999
    with pytest.raises(HierarchyIntegrityError):
        ancestor_rows(source, spec, 2)


@pytest.mark.parametrize("mode", MODES)
def test_cached_tree_detachment_refreshes_related_metadata(metadata_db, mode):
    api, cache = service(metadata_db, mode)
    catalog = Catalog(metadata_db)

    def prepare(metadata):
        work = metadata.works.create({"title": "Tree attachments"}).result
        roots = []
        for kind in KINDS:
            root, leaf = (
                getattr(metadata, kind)
                .create_tree(node("Root " + kind, node("Leaf " + kind)))
                .result
            )
            getattr(catalog, kind)._link("works", work, kind, leaf)
            roots.append(root)
        return work, tuple(roots)

    try:
        work, roots = api.batch(prepare).result
        for kind in KINDS:
            term = "series" if kind == "series" else kind[:-1]
            assert api.search.find(f'{term}:"Leaf {kind}"')[0].entity_id == work
        for kind, root in zip(KINDS, roots, strict=True):
            with pytest.raises(HierarchyInUseError):
                getattr(api, kind).delete_subtree(root)

        def detach(metadata):
            for kind, root in zip(KINDS, roots, strict=True):
                getattr(metadata, kind).delete_subtree(root, detach=True)

        api.batch(detach)
        assert api.works.require(work)
        for kind in KINDS:
            assert getattr(api, kind).roots() == ()
            term = "series" if kind == "series" else kind[:-1]
            assert api.search.find(f'{term}:"Leaf {kind}"') == ()
    finally:
        if cache is not None:
            cache.close()


def test_unknown_references_block_detachment_and_preserve_branch(metadata_db):
    repo = Catalog(metadata_db).genres
    root, leaf = repo.create_tree(node("Root", node("Leaf")))
    with metadata_db.macros.transaction() as connection:
        connection.execute(
            "CREATE TABLE unknown_owner(owner_id INTEGER PRIMARY KEY, genre_id INTEGER REFERENCES genres(genre_id) ON DELETE CASCADE)"
        )
        connection.execute("INSERT INTO unknown_owner(genre_id) VALUES (?)", (leaf,))
    with pytest.raises(HierarchyInUseError):
        repo.delete_subtree(root, detach=True)
    assert len(repo.subtree(root)) == 2
    assert metadata_db.macros.get_rows_page("unknown_owner")[0]


def test_historical_order_positions_and_equal_update_receipts(metadata_db):
    catalog = Catalog(metadata_db)
    root, first, second, third = catalog.genres.create_tree(
        node("Root", node("First"), node("Second"), node("Third"))
    )
    with metadata_db.macros.transaction() as connection:
        connection.execute(
            "UPDATE genres SET genre_position=NULL WHERE genre_id=?", (first,)
        )
        connection.execute(
            "UPDATE genres SET genre_position=4 WHERE genre_id IN (?,?)",
            (second, third),
        )
    api, cache = service(metadata_db, "schema_backed")
    try:
        assert ids(api.genres.children(root)) == (second, third, first)
        assert not api.genres.reorder(
            parent_id=root, entity_ids=(second, third, first)
        ).changed
        api.genres.reorder(parent_id=root, entity_ids=(first, second, third))
        assert [row.fields["position"] for row in api.genres.children(root)] == [
            0,
            1,
            2,
        ]
        generation = cache.generation
        assert not api.genres.update(
            first, {"name": "First", "parent_id": root, "position": 0}
        ).changed
        assert cache.generation == generation
        api.genres.update(third, {"position": 0})
        assert ids(api.genres.children(root)) == (third, first, second)
        assert api.genres.children(root, limit=0) == ()
    finally:
        cache.close()


def test_iterative_traversal_exceeds_python_recursion_depth():
    """Exercise two thousand levels without relying on a large database fixture."""
    size = 2000

    class Chain:
        def get(self, identity):
            if 1 <= identity <= size:
                return {"genre_id": identity, "genre_parent_id": identity - 1 or None}
            return None

        def children(self, parent_id, *, limit, offset=0):
            row = self.get((parent_id or 0) + 1)
            return () if row is None or not limit or offset else (row,)

    source = Chain()
    spec = TREE_SPECS["genres"]
    assert tuple(
        row["genre_id"] for row in subtree_rows(source, spec, 1, max_nodes=size)
    ) == tuple(range(1, size + 1))
    assert tuple(
        row["genre_id"] for row in ancestor_rows(source, spec, size, max_nodes=size)
    ) == tuple(range(1, size))
    with pytest.raises(HierarchyLimitError):
        subtree_rows(source, spec, 1, max_nodes=size - 1)


def test_ambiguous_path_and_repeated_input_are_not_silently_merged(metadata_db):
    api = MetadataAPI(Catalog(metadata_db))
    with metadata_db.macros.transaction() as connection:
        connection.execute(
            "INSERT INTO genres(genre,genre_phash) VALUES (?,?)",
            ("Ambiguous", "historical-one"),
        )
        connection.execute(
            "INSERT INTO genres(genre,genre_phash) VALUES (?,?)",
            ("AMBIGUOUS", "historical-two"),
        )
    with pytest.raises(CatalogAmbiguousMatchError):
        api.genres.ensure_path(("Ambiguous", "Child"))
    assert len(api.genres.list()) == 2
    payload = {"name": "Fresh"}
    child = node("Shared input")
    children = [child, child]
    tree = VocabularyTreeInput(payload, children)
    payload["name"] = "Caller edit"
    children.clear()
    assert tree.data["name"] == "Fresh" and len(tree.children) == 2
    with pytest.raises(TypeError):
        tree.data["name"] = "Cannot edit mapping"
    with pytest.raises(ValueError, match="repeated"):
        api.genres.create_tree(tree)
    assert len(api.genres.list()) == 2
