from __future__ import annotations

from LiuXin_alpha.metadata.api import WorkRelationLink
from LiuXin_alpha.metadata.containers import WorkIdentity, WorkMetadata


def test_work_metadata_container_round_trip_and_hints() -> None:
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

    payload = container.to_mapping()
    hydrated = WorkMetadata.from_mapping(payload)
    hints = hydrated.storage_hints()

    assert hints.work_id == 5
    assert hints.title == "Permutation City"
    assert hints.primary_agents == ("Greg Egan",)
    assert hints.manifestation_types == ("ebook",)
    assert hints.file_formats == ("EPUB",)
    assert hints.preferred_filename_stem == "Permutation City - Greg Egan"
