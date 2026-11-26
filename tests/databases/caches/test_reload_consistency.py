"""
Keep full cache reloads consistent while another connection commits.

A snapshot reload reads every table separately. A commit landing between two of
those reads used to publish links to works the same reload had not read.
"""

from __future__ import annotations

from shutil import copyfile

import pytest

from LiuXin_alpha.caches.cache import create_cache
from LiuXin_alpha.caches.metadata import CatalogCache, MetadataAPI
from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.databases.database import Database
from tests.databases.caches.test_metadata_api import (
    metadata_template as metadata_template,
)

SNAPSHOT_MODES = ("schema_backed", "numpy_vectorized")
OPTIONS = dict(
    backup=False,
    repair_bootstrap_rows=False,
    enable_storage_manager=False,
    enable_maintenance=False,
)


@pytest.fixture
def two_connections(metadata_template, tmp_path):
    """
    Open the same catalogue twice, as two processes would.

    Example:
        >>> def test_example(two_connections): ...  # doctest: +SKIP


    :param metadata_template: Module-scoped empty catalogue file.
    :param tmp_path: Per-test directory for the copied catalogue.
    :return: Generator yielding the two attached Database objects.
    """
    path = tmp_path / "shared.sqlite"
    copyfile(metadata_template, path)
    with Database(metadata={"database_path": str(path)}, **OPTIONS) as first:
        with Database(metadata={"database_path": str(path)}, **OPTIONS) as second:
            yield first, second


def _stack(api, title):
    """
    Create a minimal WEMI stack and return its work ID.

    Example:
        >>> _stack(api, "Before")  # doctest: +SKIP
        1


    :param api: Metadata service used for the write.
    :param title: Work title.
    :return: The new work's ID.
    """
    return api.mutations.create_wemi_stack(
        work={"title": title},
        expression={"label": "text"},
        manifestation={"edition_statement": "first"},
        items=({"inventory_code": title},),
    ).result.work_id


@pytest.mark.parametrize("mode", SNAPSHOT_MODES)
def test_reload_repeats_when_another_connection_commits_midway(
    two_connections, monkeypatch, mode
):
    """
    Commit a tagged work right after the reload has read ``works``.

    The reload must not publish the tag link without the work; it repeats and
    publishes both. The other writer is never blocked.

    Example:
        >>> test_reload_repeats_when_another_connection_commits_midway(dbs, mp, "schema_backed")  # doctest: +SKIP


    :param two_connections: The same catalogue opened twice.
    :param monkeypatch: Fixture used to observe whole-table reads.
    :param mode: Snapshot cache backend under test.
    :return: ``None`` after checking the reloaded snapshot.
    """
    first, second = two_connections
    other = MetadataAPI(Catalog(second))
    _stack(other, "Before")
    cache = create_cache(first, mode)
    api = CatalogCache(Catalog(first), cache=cache)
    wrapper_type = type(first.driver_wrapper)
    original = wrapper_type.get_all_rows
    reads: list[str] = []
    created: dict[str, int] = {}

    def observed(self, table, *args, **kwargs):
        """
        Commit from the other connection once, just after ``works`` was read.

        Example:
            Installed as the wrapper's ``get_all_rows`` during one reload.


        :param self: Driver wrapper being observed.
        :param table: Physical table being read.
        :param args: Remaining positional arguments, forwarded.
        :param kwargs: Remaining keyword arguments, forwarded.
        :return: The original method's rows.
        """
        rows = original(self, table, *args, **kwargs)
        if self is first.driver_wrapper:
            reads.append(table)
            if table == "works" and not created:
                created["work"] = _stack(other, "During")
                other.works.add_tag(created["work"], text="Midway")
        return rows

    monkeypatch.setattr(wrapper_type, "get_all_rows", observed)
    try:
        cache.reload_data()
        monkeypatch.setattr(wrapper_type, "get_all_rows", original)
        assert reads.count("works") == 2
        assert api.works.get(created["work"]) is not None
        assert [
            membership.member.fields["name"]
            for membership in api.works.list_tags(created["work"])
        ] == ["Midway"]
    finally:
        cache.close()


def test_read_consistently_repeats_then_holds_a_read_transaction(two_connections):
    """
    Repeat SQLite reads while commits land, then finish under a read transaction.

    Example:
        >>> test_read_consistently_repeats_then_holds_a_read_transaction(dbs)  # doctest: +SKIP


    :param two_connections: The same catalogue opened twice.
    :return: ``None`` after checking the passes and their transactions.
    """
    first, second = two_connections
    other = MetadataAPI(Catalog(second))
    passes: list[bool] = []

    def read():
        """
        Commit elsewhere during each optimistic pass; record transaction use.

        Example:
            Passed to ``read_consistently`` as its repeatable read.


        :return: The number of passes so far.
        """
        locked = first.macros.current_transaction_connection() is not None
        passes.append(locked)
        if not locked:
            other.ensure_tag(f"Tag {len(passes)}")
        return len(passes)

    assert first.macros.read_consistently(read, attempts=2) == 3
    assert passes == [False, False, True]
    passes.clear()
    assert first.macros.read_consistently(lambda: passes.append(False)) is None
    assert passes == [False]
    with pytest.raises(ValueError):
        first.macros.read_consistently(read, attempts=0)


def test_read_consistently_borrows_an_open_transaction(two_connections):
    """
    Run once inside the caller's transaction, seeing its pending writes.

    Example:
        >>> test_read_consistently_borrows_an_open_transaction(dbs)  # doctest: +SKIP


    :param two_connections: The same catalogue opened twice.
    :return: ``None`` after checking the borrowed read.
    """
    first, _second = two_connections
    calls: list[int] = []
    with first.macros.transaction():
        first.macros.insert_row("tags", {"tag": "Pending"})

        def read():
            """
            Count calls and read every tag through the driver wrapper.

            Example:
                Passed to ``read_consistently`` inside an open transaction.


            :return: Tag names visible to the transaction.
            """
            calls.append(1)
            return [row["tag"] for row in first.driver_wrapper.get_all_rows("tags")]

        assert "Pending" in first.macros.read_consistently(read)
    assert calls == [1]


@pytest.mark.parametrize("mode", SNAPSHOT_MODES)
def test_exact_reuse_of_another_connections_row_refreshes_the_snapshot(
    two_connections, mode
):
    """
    Make a "current" receipt readable when exact reuse finds an external row.

    Another connection creates a Tag after the snapshot loaded. ``ensure_tag`` and
    ``add_tag`` reuse it (no change), and the returned ID must then be readable.

    Example:
        >>> test_exact_reuse_of_another_connections_row_refreshes_the_snapshot(dbs, "schema_backed")  # doctest: +SKIP


    :param two_connections: The same catalogue opened twice.
    :param mode: Snapshot cache backend under test.
    :return: ``None`` after reading back the reused rows.
    """
    first, second = two_connections
    cache = create_cache(first, mode)
    api = CatalogCache(Catalog(first), cache=cache)
    other = MetadataAPI(Catalog(second))
    try:
        work = _stack(api, "Shared")
        tag = other.ensure_tag("External").result
        receipt = api.ensure_tag("external")
        assert (receipt.result, receipt.changed, receipt.cache) == (
            tag,
            False,
            "current",
        )
        assert api.tags.require(tag).fields["name"] == "External"
        label = other.ensure_label("Elsewhere").result
        assert not api.ensure_label("ELSEWHERE").changed
        assert api.labels.require(label).fields["name"] == "Elsewhere"
        unrelated = cache.generation
        assert not api.ensure_tag("External").changed
        assert cache.generation == unrelated
        other_tag = other.ensure_tag("Also External").result
        assert api.works.add_tag(work, text="also external").result == other_tag
        assert [m.member.entity_id for m in api.works.list_tags(work)] == [other_tag]
    finally:
        cache.close()
