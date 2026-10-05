"""
Verify work metadata values, aliases, relations and projections.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test work metadata container through its owning regression module::

        python -m pytest -q tests/metadata/containers/test_work_metadata_container.py
"""
from __future__ import annotations

from LiuXin_alpha.metadata.api import WorkRelationLink
from LiuXin_alpha.metadata.containers import WorkIdentity, WorkMetadata


def test_work_metadata_container_round_trip() -> None:
    """
    Verify work metadata container round trip.

    Example:
        Exercise test work metadata container round trip through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_work_metadata_container.py


    :return: None; the function records state or raises through its assertions.
    """
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
            primary=True,
            type="edition",
            index=7,
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

    assert hydrated.work is not None
    assert hydrated.work.work_id == 5
    assert hydrated.work.work_canonical_title == "Permutation City"
    assert hydrated.get_relation_links("agents")[0].target == {"agent_canonical_name": "Greg Egan"}
    assert hydrated.get_relation_links("manifestations")[0].target == {
        "manifestation_id": 12,
        "manifestation_format_detail": "EPUB",
        "manifestation_carrier_type": "ebook",
    }
    assert hydrated.get_relation_links("manifestations")[0].primary is True
    assert hydrated.get_relation_links("manifestations")[0].index == 7
    assert hydrated.get_relation_links("files")[0].target == {"file_extension": "epub"}
    assert not hasattr(hydrated, "storage_hints")
