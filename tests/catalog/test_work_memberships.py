"""Exercise public Work memberships on the real schema and both SQLite drivers."""

import re
from collections.abc import Mapping
from pathlib import Path

import pytest

from LiuXin_alpha.catalog.api.common import CatalogNotFoundError, MetadataCandidate
from LiuXin_alpha.catalog.api.repositories import WorkMembershipRepositoryAPI
from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.databases.db_types.macro_types import LinkValue


@pytest.fixture(params=["tags", "series"])
def memberships(db, request):
    catalog = Catalog(db)
    repo = getattr(catalog, request.param)
    works = [catalog.works.create({"title": name}) for name in ("First", "Other")]
    ids = [repo.create({"name": name}) for name in ("One", "Two", "Three")]
    spec = db.driver_wrapper.get_link_spec(repo.table_name, "works")
    return repo, works, ids, spec


def snapshot(db, spec):
    return sorted(db.macros.get_rows(spec.link_table), key=lambda row: repr(row))


def member_ids(repo, work_id):
    return {row[repo.id_column] for row in repo.list_for_work(work_id)}


def test_shared_memberships_and_idempotent_link_unlink(db, memberships):
    repo, (first, other), ids, spec = memberships
    assert isinstance(repo, WorkMembershipRepositoryAPI)
    assert repo.list_for_work(first) == ()
    assert not repo.is_linked_to_work(entity_id=ids[0], work_id=first)

    for work_id in (first, other):
        repo.link_to_work(entity_id=ids[0], work_id=work_id)
    before = snapshot(db, spec)
    repo.link_to_work(entity_id=ids[0], work_id=first)
    assert snapshot(db, spec) == before
    links = db.macros.get_link_rows(spec, ids[0])
    assert len({link.priority for link in links}) == 2
    assert repo.is_linked_to_work(entity_id=ids[0], work_id=first)
    row = repo.list_for_work(first)[0]
    assert row[repo.id_column] == ids[0]
    assert row["_catalog_link"]["primary_id"] == first
    assert row["_catalog_link"]["secondary_id"] == ids[0]

    repo.link_to_work(entity_id=ids[1], work_id=first)
    other_before = repo.list_for_work(other)
    repo.unlink_from_work(entity_id=ids[0], work_id=first)
    assert member_ids(repo, first) == {ids[1]}
    assert repo.list_for_work(other) == other_before
    assert repo.require(ids[0])
    after = snapshot(db, spec)
    repo.unlink_from_work(entity_id=ids[0], work_id=first)
    assert snapshot(db, spec) == after
    repo.replace_for_work(work_id=first, entity_ids=())
    assert repo.list_for_work(first) == ()
    assert repo.require(ids[1])


def test_work_membership_usage_example_executes(db):
    guide = (
        Path(__file__).resolve().parents[2] / "dev-docs/catalog/catalog-api-usage.md"
    ).read_text()
    section = guide.split("## Work tag and Series memberships\n", 1)[1].split(
        "\n## ", 1
    )[0]
    example = re.search(r"```python\n(.*?)```", section, re.DOTALL).group(1)
    exec(example, {"catalog": Catalog(db), "MetadataCandidate": MetadataCandidate})


def test_replacement_preserves_retained_metadata_and_other_owners(db, memberships):
    repo, (first, other), ids, spec = memberships
    repo.replace_for_works({first: ids[:2], other: (ids[0],)})
    id_column = db.driver_wrapper.get_id_column(spec.link_table)
    base = spec.link_table.removesuffix("s")
    kept = next(
        row
        for row in snapshot(db, spec)
        if row[spec.primary_link_col] == ids[0]
        and row[spec.secondary_link_col] == first
    )
    db.macros.update_row(
        spec.link_table,
        kept[id_column],
        {f"{base}_source": "curator", f"{base}_scratch": "preserve me"},
    )
    kept_before = db.macros.get_row(spec.link_table, kept[id_column])
    other_before = repo.list_for_work(other)
    if repo.table_name == "series":
        repo.update(ids[1], {"parent_id": ids[0]})

    # Reversing input does not reinterpret stored priorities as a new ranking.
    repo.replace_for_work(work_id=first, entity_ids=(ids[1], ids[0], ids[2]))
    assert member_ids(repo, first) == set(ids)
    assert db.macros.get_row(spec.link_table, kept[id_column]) == kept_before
    assert repo.list_for_work(other) == other_before
    if repo.table_name == "series":
        assert repo.require(ids[1])["series_parent_id"] == ids[0]


def test_batch_validates_all_ids_and_materializes_inputs_before_mutating(
    db, memberships
):
    repo, (first, other), ids, spec = memberships
    repo.replace_for_work(work_id=first, entity_ids=ids[:1])
    before = snapshot(db, spec)
    for replacement in (
        {first: ids[1:], other: [999999]},
        {first: ids[1:], 999999: []},
    ):
        with pytest.raises(CatalogNotFoundError):
            repo.replace_for_works(replacement)
        assert snapshot(db, spec) == before
    for invalid_ids in ([True], [-1], ["1"], "12", [ids[0], ids[0]]):
        with pytest.raises((TypeError, ValueError)):
            repo.replace_for_works({first: ids[1:], other: invalid_ids})
        assert snapshot(db, spec) == before
    with pytest.raises(TypeError):
        repo.replace_for_works({True: ids})
    assert snapshot(db, spec) == before

    def broken_ids():
        yield ids[0]
        raise RuntimeError("input failed")

    with pytest.raises(RuntimeError, match="input failed"):
        repo.replace_for_works({first: ids[1:], other: broken_ids()})
    assert snapshot(db, spec) == before
    repo.replace_for_works({first: (value for value in ids), other: iter(ids[:1])})
    assert member_ids(repo, first) == set(ids)
    assert member_ids(repo, other) == {ids[0]}


def test_late_database_failure_and_outer_transaction_restore_whole_batch(
    db, memberships, monkeypatch
):
    repo, (first, other), ids, spec = memberships
    repo.replace_for_works({first: ids[:1], other: ids[:1]})
    before = snapshot(db, spec)
    original = db.macros.upsert_link
    calls = 0

    def fail_second(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("late insertion failure")
        return original(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(db.macros, "upsert_link", fail_second)
        with pytest.raises(RuntimeError, match="late insertion failure"):
            repo.replace_for_works({first: ids[1:2], other: ids[2:]})
    assert calls == 2
    assert snapshot(db, spec) == before
    with pytest.raises(RuntimeError, match="outer rollback"):
        with db.macros.transaction():
            repo.replace_for_works({first: ids[1:], other: []})
            repo.unlink_from_work(entity_id=ids[1], work_id=first)
            repo.link_to_work(entity_id=ids[2], work_id=other)
            raise RuntimeError("outer rollback")
    assert snapshot(db, spec) == before


def test_missing_entities_are_errors_not_empty_memberships(db, memberships):
    repo, (first, _), ids, _ = memberships
    with pytest.raises(CatalogNotFoundError):
        repo.list_for_work(999999)
    for operation in (
        repo.link_to_work,
        repo.unlink_from_work,
        repo.is_linked_to_work,
    ):
        for entity_id, work_id in ((999999, first), (ids[0], 999999)):
            with pytest.raises(CatalogNotFoundError):
                operation(entity_id=entity_id, work_id=work_id)
        for entity_id, work_id in ((True, first), (ids[0], True)):
            with pytest.raises((TypeError, ValueError)):
                operation(entity_id=entity_id, work_id=work_id)
    assert repo.list_for_work(first) == ()


def test_series_membership_preserves_all_types_until_pair_is_removed(db):
    catalog = Catalog(db)
    repo = catalog.series
    first = catalog.works.create({"title": "First"})
    other = catalog.works.create({"title": "Other"})
    series_id = repo.create({"name": "Typed collection"})
    spec = db.driver_wrapper.get_link_spec("series", "works")
    allowed_types = db.driver_wrapper.get_allowed_link_types(spec)
    types = tuple(allowed_types or ("member", "related"))[:2]
    assert len(types) == 2
    for work_id, priority in ((first, 10), (other, 20)):
        for link_type in types:
            db.macros.upsert_link(
                spec,
                series_id,
                LinkValue(
                    work_id,
                    link_type=link_type,
                    priority=priority,
                    extra={"series_work_link_source": "imported"},
                ),
            )
    before = snapshot(db, spec)
    repo.link_to_work(entity_id=series_id, work_id=first)
    repo.replace_for_work(work_id=first, entity_ids=[series_id])
    assert snapshot(db, spec) == before
    rows = repo.list_for_work(first)
    assert len(rows) == 2
    assert {row["_catalog_link"]["type"] for row in rows} == set(types)
    assert all(isinstance(row, Mapping) for row in rows)
    other_before = repo.list_for_work(other)
    repo.unlink_from_work(entity_id=series_id, work_id=first)
    assert not repo.is_linked_to_work(entity_id=series_id, work_id=first)
    assert repo.list_for_work(other) == other_before
