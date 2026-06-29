from __future__ import annotations

from LiuXin_alpha.metadata.api import (
    ExpressionRelationLink,
    ItemRelationLink,
    ManifestationRelationLink,
    WorkRelationLink,
)
from LiuXin_alpha.metadata.containers import (
    ExpressionIdentity,
    ExpressionMetadata,
    ItemIdentity,
    ItemMetadata,
    ManifestationIdentity,
    ManifestationMetadata,
    WorkIdentity,
    WorkMetadata,
)
from LiuXin_alpha.storage.api import (
    ExpressionStorageHints,
    ItemStorageHints,
    ManifestationStorageHints,
    WorkStorageHints,
    derive_storage_hints,
)


def test_derive_work_storage_hints_from_metadata_container() -> None:
    container = WorkMetadata(
        work=WorkIdentity(
            work_id=5,
            work_title="Permutation City",
            work_canonical_title="Permutation City",
            work_sort_title="Permutation City",
            work_type="novel",
            work_medium="text",
        )
    )
    container.add_relation_link(
        "agents",
        WorkRelationLink(
            target={"agent_canonical_name": "Greg Egan"},
            priority=1,
            type="author",
        ),
    )
    container.add_relation_link(
        "manifestations",
        WorkRelationLink(
            target={
                "manifestation_id": 12,
                "manifestation_format_detail": "EPUB",
                "manifestation_carrier_type": "ebook",
            },
            priority=1,
            type="edition",
        ),
    )
    container.add_relation_link(
        "files",
        WorkRelationLink(
            target={"file_extension": "epub"},
            type="item_file",
        ),
    )

    hints = derive_storage_hints(WorkMetadata.from_mapping(container.to_mapping()))

    assert isinstance(hints, WorkStorageHints)
    assert hints.work_id == 5
    assert hints.title == "Permutation City"
    assert hints.primary_agents == ("Greg Egan",)
    assert hints.manifestation_types == ("ebook",)
    assert hints.file_formats == ("EPUB",)
    assert hints.preferred_filename_stem == "Permutation City - Greg Egan"


def test_derive_item_storage_hints_from_metadata_container() -> None:
    container = ItemMetadata(
        item=ItemIdentity(
            item_id=44,
            item_manifestation_id=12,
            item_type="digital",
            item_source="fixture",
            item_source_name="permutation-city.epub",
            item_inventory_code="INV-44",
        )
    )
    container.add_relation_link(
        "works",
        ItemRelationLink(
            target={
                "work_id": 5,
                "work_title": "Permutation City",
                "work_canonical_title": "Permutation City",
            },
            primary=True,
        ),
    )
    container.add_relation_link(
        "agents",
        ItemRelationLink(
            target={"agent_canonical_name": "Greg Egan"},
            primary=True,
            type="author",
        ),
    )
    container.add_relation_link(
        "files",
        ItemRelationLink(
            target={
                "file_extension": "epub",
                "file_role": "primary",
                "file_storage_key": "Greg Egan/Permutation City (5)/Permutation City - Greg Egan.epub",
            },
            primary=True,
        ),
    )

    hints = derive_storage_hints(ItemMetadata.from_mapping(container.to_mapping()))

    assert isinstance(hints, ItemStorageHints)
    assert hints.item_id == 44
    assert hints.work_id == 5
    assert hints.title == "Permutation City"
    assert hints.primary_agents == ("Greg Egan",)
    assert hints.file_formats == ("EPUB",)
    assert hints.preferred_filename_stem == "Permutation City - Greg Egan"
    assert hints.preferred_storage_key is not None
    assert hints.preferred_storage_key.endswith("Greg Egan.epub")


def test_derive_expression_storage_hints_from_metadata_container() -> None:
    container = ExpressionMetadata(
        expression=ExpressionIdentity(
            expression_id=20,
            expression_work_id=30,
            expression_type="translation",
            expression_label="English text",
            expression_title_override="Permutation City",
        )
    )
    container.add_relation_link(
        "agents",
        ExpressionRelationLink(target={"agent_canonical_name": "Greg Egan"}, primary=True),
    )
    container.add_relation_link(
        "languages",
        ExpressionRelationLink(target={"language_code": "en", "language_name": "English"}),
    )

    hints = derive_storage_hints(container)

    assert isinstance(hints, ExpressionStorageHints)
    assert hints.expression_id == 20
    assert hints.work_id == 30
    assert hints.title == "Permutation City"
    assert hints.expression_type == "translation"
    assert hints.language_code == "English"
    assert hints.primary_agents == ("Greg Egan",)


def test_derive_manifestation_storage_hints_from_metadata_container() -> None:
    container = ManifestationMetadata(
        manifestation=ManifestationIdentity(
            manifestation_id=10,
            manifestation_expression_id=20,
            manifestation_format_detail="EPUB",
            manifestation_carrier_type="ebook",
            manifestation_edition_statement="First edition",
            manifestation_pub_year=1994,
        )
    )
    container.add_relation_link(
        "titles",
        ManifestationRelationLink(target={"work_title": "Permutation City"}, primary=True),
    )
    container.add_relation_link(
        "files",
        ManifestationRelationLink(target={"file_extension": "epub"}, primary=True),
    )

    hints = derive_storage_hints(container)

    assert isinstance(hints, ManifestationStorageHints)
    assert hints.manifestation_id == 10
    assert hints.expression_id == 20
    assert hints.title == "Permutation City"
    assert hints.format_detail == "EPUB"
    assert hints.carrier_type == "ebook"
    assert hints.publication_year == 1994
    assert hints.file_formats == ("epub",)


def test_derive_storage_hints_accepts_direct_hints_and_ignores_broken_providers() -> None:
    hints = WorkStorageHints(work_id=5, title="Permutation City")

    class BrokenProvider:
        def storage_hints(self) -> WorkStorageHints:
            raise RuntimeError("provider failed")

    assert derive_storage_hints(hints) is hints
    assert derive_storage_hints(BrokenProvider()) is None
