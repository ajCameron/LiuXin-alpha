"""Real-database coverage for coordinated Catalog aggregate writes."""

from __future__ import annotations

from collections import OrderedDict
from pathlib import Path
from types import SimpleNamespace

import pytest

from LiuXin_alpha.catalog import Catalog
from LiuXin_alpha.catalog.api.common import CatalogMutationError
from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.errors import DatabaseIntegrityError
from LiuXin_alpha.library.library_metadata import Metadata as LibraryMetadata
from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import (
    CalibreLikeLiuXinBookMetaData,
)


def test_agent_and_wemi_aggregate_mutations_are_atomic(driver_spec, tmp_path: Path) -> None:
    """Create complete aggregates and roll back a conflicting WEMI path."""

    database_path = tmp_path / "catalog_aggregates.sqlite"
    with Database(
        metadata={"database_path": str(database_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        catalog = Catalog(db)

        parent_id = catalog.agents.create_organisation(
            {"name": "Parent Press", "sort_name": "Parent Press"},
            details={"org_agent_legal_name": "Parent Press Ltd"},
        )
        child_id = catalog.agents.create_organisation(
            {
                "name": "Reference Imprint",
                "aliases": ["Ref", "ref", "Reference"],
            },
            details={"org_agent_website": "https://imprint.example"},
            parent_id=parent_id,
            identifiers=(
                {
                    "scheme": "url",
                    "value": "https://imprint.example",
                    "is_primary": True,
                },
            ),
        )
        child = catalog.agents.require(child_id)
        assert child["agent_aliases"] == "Ref(#BREAK#)Reference"
        assert len(catalog.identifiers.list_for_agent(child_id)) == 1
        relations = db.search(
            "org_agent_relations",
            "org_agent_relation_child_agent_id",
            child_id,
        )
        assert len(relations) == 1
        assert int(relations[0]["org_agent_relation_parent_agent_id"]) == parent_id

        created = catalog.mutations.writer.create_wemi_stack(
            work={"title": "Aggregate Work", "canonical_title": "Aggregate Work"},
            expression={"label": "Preferred", "is_preferred": 1},
            manifestation={"format_detail": "EPUB", "carrier_type": "ebook"},
            items=(
                {"inventory_code": "AGG-1", "source": "catalog-test"},
                {"inventory_code": "AGG-2", "source": "catalog-test"},
            ),
            origin="catalog-test",
        )
        expressions = catalog.expressions.list_for_work(created.work_id)
        assert [row["expression_id"] for row in expressions] == [created.expression_id]
        manifestations = catalog.manifestations.list_for_expression(created.expression_id)
        assert [row["manifestation_id"] for row in manifestations] == [
            created.manifestation_id
        ]
        assert {row["item_id"] for row in catalog.items.list_for_manifestation(created.manifestation_id)} == set(
            created.item_ids
        )
        comment_id = catalog.comments.replace_for_wemi(
            level="work",
            entity_id=created.work_id,
            data={"text": "Primary cache comment"},
        )
        assert comment_id is not None
        assert [row["comment_id"] for row in catalog.comments.list_for_wemi(
            level="work",
            entity_id=created.work_id,
        )] == [comment_id]
        assert catalog.comments.replace_for_wemi(
            level="work",
            entity_id=created.work_id,
            data=None,
        ) is None
        assert not catalog.comments.list_for_wemi(
            level="work",
            entity_id=created.work_id,
        )

        db.metadata_sql.set_title_isbn(created.work_id, "978-0-306-40615-7")
        first_isbn_rows = catalog.identifiers.list_for_wemi(
            level="work",
            entity_id=created.work_id,
        )
        first_isbn_id = next(
            row["entity_identifier_id"]
            for row in first_isbn_rows
            if row["entity_identifier_scheme"] == "isbn13"
        )
        db.metadata_sql.set_title_isbn(created.work_id, "978-0-14-032872-1")
        isbn_rows = [
            row
            for row in catalog.identifiers.list_for_wemi(
                level="work",
                entity_id=created.work_id,
            )
            if row["entity_identifier_scheme"] == "isbn13"
        ]
        assert {
            row["entity_identifier_value"]: row["entity_identifier_is_primary"]
            for row in isbn_rows
        } == {
            "978-0-306-40615-7": 0,
            "978-0-14-032872-1": 1,
        }
        current_isbn_id = next(
            row["entity_identifier_id"]
            for row in isbn_rows
            if row["entity_identifier_is_primary"] == 1
        )
        assert current_isbn_id != first_isbn_id
        db.metadata_sql.set_title_isbn(created.work_id, "978-0-14-032872-1")
        assert current_isbn_id in {
            row["entity_identifier_id"]
            for row in catalog.identifiers.list_for_wemi(
                level="work",
                entity_id=created.work_id,
            )
        }
        db.metadata_sql.set_title_isbn(created.work_id, None)
        assert not [
            row
            for row in catalog.identifiers.list_for_wemi(
                level="work",
                entity_id=created.work_id,
            )
            if str(row["entity_identifier_scheme"]).startswith("isbn")
        ]
        with pytest.raises(DatabaseIntegrityError):
            db.metadata_sql.set_title_identifier(
                999_999,
                "doi",
                "10.1000/missing-work",
            )

        imported_metadata = CalibreLikeLiuXinBookMetaData(
            title="Library metadata migration",
            authors=("Ada Example",),
        )
        imported_metadata.direct_add(
            "comments",
            OrderedDict((("Imported comment", None),)),
        )
        imported_metadata.direct_add(
            "genre",
            OrderedDict((("Reference", None),)),
        )
        imported_metadata.direct_add(
            "isbn",
            OrderedDict((("9780306406157", None),)),
        )
        imported_metadata.direct_add("languages", ["eng"])
        imported_metadata.direct_add(
            "languages_available",
            OrderedDict((("fra", None),)),
        )
        imported_metadata.direct_add(
            "notes",
            OrderedDict((("Imported note", None),)),
        )
        imported_metadata.direct_add(
            "publisher",
            OrderedDict((("Migration Press", None),)),
        )
        imported_metadata.direct_add(
            "ratings",
            OrderedDict((("user", 8),)),
        )
        imported_metadata.direct_add(
            "series",
            OrderedDict((("Migration Series", None),)),
        )
        imported_metadata.direct_add(
            "series_index",
            OrderedDict((("Migration Series", 2.5),)),
        )
        imported_metadata.direct_add(
            "subject",
            OrderedDict((("Software migration", None),)),
        )
        imported_metadata.direct_add(
            "synopses",
            OrderedDict((("Imported synopsis", None),)),
        )
        imported_metadata.direct_add(
            "tags",
            OrderedDict((("catalog", None),)),
        )
        library_metadata = LibraryMetadata(
            db,
            SimpleNamespace(fsm=object()),
        )
        imported_title, imported_book = library_metadata.to_title(
            imported_metadata,
            preserve_uuid="12345678-1234-5678-1234-567812345678",
        )
        imported_work_id = int(imported_title["title_id"])
        assert int(imported_book["book_work_id"]) == imported_work_id
        assert catalog.works.require(imported_work_id)["work_title"] == (
            "Library metadata migration"
        )
        assert {
            row["entity_identifier_scheme"]
            for row in catalog.identifiers.list_for_wemi(
                level="work",
                entity_id=imported_work_id,
            )
        } == {"calibre_uuid", "isbn13"}
        assert [
            row["agent_canonical_name"]
            for row in catalog.agents.list_for_wemi(
                level="work",
                entity_id=imported_work_id,
            )
        ] == ["Ada Example", "Migration Press"]
        expected_link_counts = {
            "genre_work_links": 1,
            "language_work_links": 3,
            "rating_work_links": 1,
            "series_work_links": 1,
            "subject_work_links": 1,
            "tag_work_links": 1,
        }
        for link_table, expected_count in expected_link_counts.items():
            prefix = link_table.removesuffix("s")
            assert len(
                db.macros.get_rows(
                    link_table,
                    where={"{}_work_id".format(prefix): imported_work_id},
                )
            ) == expected_count
        assert len(
            catalog.comments.list_for_wemi(
                level="work",
                entity_id=imported_work_id,
            )
        ) == 1
        assert len(
            catalog.synopses.list_for_wemi(
                level="work",
                entity_id=imported_work_id,
            )
        ) == 1

        replacement_metadata = CalibreLikeLiuXinBookMetaData(
            title="Library metadata replacement",
            authors=("Grace Example",),
        )
        replaced_title, replaced_book = library_metadata.to_title(
            replacement_metadata,
            force_book_id=imported_work_id,
        )
        assert int(replaced_title["title_id"]) == imported_work_id
        assert int(replaced_book["book_work_id"]) == imported_work_id
        assert catalog.works.require(imported_work_id)["work_title"] == (
            "Library metadata replacement"
        )
        assert [
            row["agent_canonical_name"]
            for row in catalog.agents.list_for_wemi(
                level="work",
                entity_id=imported_work_id,
            )
        ] == ["Grace Example"]
        assert not catalog.identifiers.list_for_wemi(
            level="work",
            entity_id=imported_work_id,
        )
        assert not catalog.comments.list_for_wemi(
            level="work",
            entity_id=imported_work_id,
        )
        assert not catalog.synopses.list_for_wemi(
            level="work",
            entity_id=imported_work_id,
        )
        assert len(catalog.expressions.list_for_work(imported_work_id)) == 1
        for link_table in expected_link_counts:
            prefix = link_table.removesuffix("s")
            count = len(
                db.macros.get_rows(
                    link_table,
                    where={"{}_work_id".format(prefix): imported_work_id},
                )
            )
            assert count == (1 if link_table == "language_work_links" else 0)

        counts_before = (
            len(catalog.works.list(limit=10_000)),
            len(catalog.expressions.list(limit=10_000)),
            len(catalog.manifestations.list(limit=10_000)),
            len(catalog.items.list(limit=10_000)),
        )
        with pytest.raises(CatalogMutationError):
            catalog.mutations.writer.create_wemi_stack(
                work={"title": "Must Roll Back"},
                expression={"label": "Must Roll Back"},
                manifestation={"format_detail": "PDF"},
                items=(
                    {
                        "manifestation_id": created.manifestation_id,
                        "inventory_code": "CONFLICT",
                    },
                ),
            )
        counts_after = (
            len(catalog.works.list(limit=10_000)),
            len(catalog.expressions.list(limit=10_000)),
            len(catalog.manifestations.list(limit=10_000)),
            len(catalog.items.list(limit=10_000)),
        )
        assert counts_after == counts_before
