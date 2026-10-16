"""Compare stored identities across public and compatibility write entrypoints."""

from __future__ import annotations

from pathlib import Path

import pytest

from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.core.runtime import CoreRuntime
from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.library.library import Library


@pytest.mark.parametrize(("priority", "step"), (("highest", 1), ("lowest", -1)))
def test_row_relationship_priorities_see_uncommitted_links(db, priority, step) -> None:
    """Row adapters retain global bounds and see links in one shared transaction."""
    from LiuXin_alpha.catalog.row_mutations import link_rows, unlink_rows

    catalog = Catalog(db)
    work_id = catalog.works.create({"title": "Relationship transaction"})
    other_work_id = catalog.works.create({"title": "Other relationship owner"})
    first_id = catalog.tags.create({"tag": "First transaction tag"})
    second_id = catalog.tags.create({"tag": "Second transaction tag"})
    work = Row(db, dict(catalog.works.require(work_id)))
    other_work = Row(db, dict(catalog.works.require(other_work_id)))
    first = Row(db, dict(catalog.tags.require(first_id)))
    second = Row(db, dict(catalog.tags.require(second_id)))
    spec = db.driver_wrapper.get_link_spec("works", "tags")
    with db.macros.transaction():
        link_rows(catalog, other_work, first, priority=17)
        first_link = link_rows(catalog, work, first, priority=priority)
        second_link = link_rows(catalog, work, second, priority=priority)
        assert first_link[spec.priority_link_col] == 17 + step
        assert second_link[spec.priority_link_col] == 17 + 2 * step
        unlink_rows(catalog, work, first)
        remaining = db.macros.get_link_rows(spec, work_id)
        assert len(remaining) == 1
        assert remaining[0].secondary_id == second_id
        assert remaining[0].priority == second_link[spec.priority_link_col]


def test_row_relationship_adapter_supports_unordered_links(db) -> None:
    """Legacy default priority does not prevent linking an unordered relation."""
    from LiuXin_alpha.catalog.row_mutations import link_rows

    catalog = Catalog(db)
    manifestation_id = catalog.manifestations.create({"format_detail": "EPUB"})
    language_id = catalog.languages.exact("English").entity_id
    manifestation = Row(db, dict(catalog.manifestations.require(manifestation_id)))
    language = Row(db, dict(catalog.languages.require(language_id)))
    spec = db.driver_wrapper.get_link_spec("manifestations", "languages")
    assert not spec.ordered
    receipt = link_rows(catalog, manifestation, language)
    links = db.macros.get_link_rows(spec, manifestation_id)
    assert receipt.row_id is not None
    assert len(links) == 1
    assert links[0].secondary_id == language_id
    assert links[0].priority is None


def test_fresh_database_writers_agree_on_full_text_identities(
    driver_spec, tmp_path: Path
) -> None:
    """Repository, Core, portable macros and Row writes store the same full key."""
    with Database(
        metadata={"database_path": str(tmp_path / "write-parity.sqlite")},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        enable_maintenance=False,
        enable_storage_manager=False,
    ) as db:
        catalog = Catalog(db)
        runtime = CoreRuntime(library=Library(database=db), catalog=catalog)
        try:
            for table, value_column, key_column in (
                ("tags", "tag", "tag_phash"),
                ("labels", "label_text", "label_text_norm"),
                ("series", "series", "series_name_norm"),
            ):
                repository = catalog.repositories.for_table(table)
                for route in ("repository", "row", "macro", "admin"):
                    values = {
                        value_column: "  Ｓｃｉ-Fi & Fantasy  ",
                        key_column: "wrong",
                    }
                    if route == "repository":
                        row_id = repository.create(values)
                    elif route == "row":
                        row_id = Row.from_idless_row_dict(
                            db, values, table=table
                        ).row_id
                    elif route == "macro":
                        row_id = db.macros.insert_row(table, values)
                    else:
                        row_id = runtime.command(
                            "admin.row.create", {"table": table, "values": values}
                        )["record"]["row_id"]
                    row = repository.require(row_id)
                    assert row[value_column] == values[value_column]
                    assert row[key_column] == "sci-fi & fantasy", (table, route, row)
                    replacement = {value_column: "Sci-Fi & Horror"}
                    if route == "repository":
                        repository.update(row_id, replacement)
                    elif route == "row":
                        stored = db.get_row_from_id(table, row_id)
                        stored[value_column] = replacement[value_column]
                        stored.sync()
                    elif route == "macro":
                        db.macros.update_row(table, row_id, replacement)
                    else:
                        runtime.command(
                            "admin.row.update",
                            {"table": table, "row_id": row_id, "updates": replacement},
                        )
                    assert repository.require(row_id)[key_column] == "sci-fi & horror"
                    repository.update(row_id, {key_column: "another-policy"})
                    assert repository.require(row_id)[key_column] == "sci-fi & horror"
                    repository.update(row_id, {value_column: None})
                    assert repository.require(row_id)[key_column] is None
                    repository.delete(row_id)
        finally:
            runtime.shutdown()


def test_container_and_schema_writer_reuse_repository_tag_identity(
    driver_spec, tmp_path: Path
) -> None:
    """Field and container writes reuse a tag without introducing another key."""
    with Database(
        metadata={"database_path": str(tmp_path / "tag-reuse.sqlite")},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        enable_maintenance=False,
        enable_storage_manager=False,
    ) as db:
        catalog = Catalog(db)
        stack = catalog.mutations.writer.create_wemi_stack(
            work={"title": "Write path reuse"},
            expression={"label": "Primary"},
            manifestation={"format_detail": "EPUB"},
            items=({"inventory_code": "write-path-reuse"},),
        )
        tag_id = catalog.tags.create({"tag": "Sci-Fi & Fantasy"})
        catalog.write("works", "tag", {stack.work_id: (" SCI-FI & FANTASY ",)})
        receipt = catalog.mutations.writer.write_values(
            item_id=stack.item_ids[0],
            values={"tags": ["Sci-Fi & Fantasy"]},
            mark_dirty=False,
        )
        assert not receipt["report"]["errors"]
        assert [(row["tag_id"], row["tag_phash"]) for row in catalog.tags.list()] == [
            (tag_id, "sci-fi & fantasy")
        ]
        assert catalog.ensure.tag("  SCI-FI & FANTASY ").row_id == tag_id
        added = catalog.add.tag("Sci-Fi & Horror", tag_phash="wrong-policy")
        assert added["tag_phash"] == "sci-fi & horror"
        receipt = catalog.mutations.writer.write_values(
            item_id=stack.item_ids[0],
            values={"series": "Sci-Fi & Fantasy"},
            mark_dirty=False,
        )
        assert not receipt["report"]["errors"]
        series = tuple(row for row in catalog.series.list() if row["series_id"] != 0)
        assert len(series) == 1
        assert series[0]["series"] == "Sci-Fi & Fantasy"
        assert series[0]["series_name_norm"] == "sci-fi & fantasy"


def test_identifier_batch_and_container_failures_roll_back(
    driver_spec, tmp_path: Path
) -> None:
    """A later invalid identifier cannot leave earlier column or container writes behind."""
    with Database(
        metadata={"database_path": str(tmp_path / "atomic-writes.sqlite")},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        enable_maintenance=False,
        enable_storage_manager=False,
    ) as db:
        catalog = Catalog(db)
        stack = catalog.mutations.writer.create_wemi_stack(
            work={"title": "Atomic writes"},
            expression={"label": "Primary"},
            manifestation={"format_detail": "EPUB"},
            items=({"inventory_code": "atomic"},),
        )
        owner = {"entity_type": "work", "entity_id": stack.work_id, "is_primary": True}
        doi = catalog.identifiers.create(
            {**owner, "scheme": "doi", "value": "10.1234/original"}
        )
        isbn = catalog.identifiers.create(
            {**owner, "scheme": "isbn", "value": "9780306406157"}
        )
        before = catalog.identifiers.list()
        with pytest.raises(ValueError, match="ISBN"):
            catalog.write(
                "entity_identifiers",
                "entity_identifier_value",
                {
                    doi: "10.1234/changed",
                    isbn: "not-an-isbn",
                },
            )
        assert catalog.identifiers.list() == before

        with pytest.raises(ValueError, match="ISBN"):
            Library(database=db).update_row_fields(
                table="entity_identifiers",
                row_id=isbn,
                updates={"entity_identifier_value": "not-an-isbn"},
            )
        assert catalog.identifiers.list() == before
        with pytest.raises(ValueError, match="ISBN"):
            catalog.mutations.writer.write_values(
                item_id=stack.item_ids[0],
                values={
                    "tags": ["Must roll back"],
                    "identifiers": {"isbn": "not-an-isbn"},
                },
                replace=True,
                mark_dirty=False,
            )
        assert catalog.identifiers.list() == before
        assert catalog.tags.list() == ()
        for _ in range(2):
            catalog.mutations.writer.write_values(
                item_id=stack.item_ids[0],
                values={"identifiers": {"DOI": "10.1234/original"}},
                mark_dirty=False,
            )
        assert catalog.identifiers.list() == before
