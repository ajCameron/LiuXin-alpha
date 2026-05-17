from __future__ import annotations

from collections.abc import Mapping

from LiuXin_alpha.metadata.api import (
    ItemRelationLink,
    WorkRelationLink,
)
from LiuXin_alpha.metadata.containers import (
    LazyLiuXinWEMIMetadata,
    LiuXinWEMIMetadata,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers import (
    WorkIdentity,
    WorkMetadata,
)
from LiuXin_alpha.metadata.opf_tools import metadata_from_opf, metadata_to_opf_bytes


def _values(raw: object) -> tuple[str, ...]:
    if raw is None:
        return ()
    if isinstance(raw, Mapping):
        return tuple(str(value) for value in raw.keys())
    if isinstance(raw, str):
        return (raw,)
    try:
        return tuple(str(value) for value in raw)  # type: ignore[arg-type]
    except TypeError:
        return (str(raw),)


def _identifier_values(metadata: object, scheme: str) -> tuple[str, ...]:
    identifiers = metadata.get_identifiers()  # type: ignore[attr-defined]
    return tuple(sorted(_values(identifiers.get(scheme, ()))))


def test_direct_wemi_relations_project_to_calibre_without_mutating_legacy_fields() -> None:
    metadata = LiuXinWEMIMetadata(
        title="Legacy Projection Title",
        authors=["Ada Lovelace"],
        work_metadata=WorkMetadata(
            work=WorkIdentity(
                work_id=10,
                work_canonical_title="規格化された作品",
            ),
        ),
    )
    metadata.tags = "legacy-tag"
    metadata.add_wemi_relation_link(
        "work",
        "tags",
        WorkRelationLink(target={"tag": "标签-タグ"}, link_id="work-tag-link"),
    )
    metadata.add_wemi_relation_link(
        "work",
        "subjects",
        WorkRelationLink(target={"subject": "主题-日本語"}),
    )
    metadata.add_wemi_relation_link(
        "work",
        "series",
        WorkRelationLink(target={"series": "系列-シリーズ"}),
    )
    metadata.add_wemi_relation_link(
        "work",
        "languages",
        WorkRelationLink(target={"language_code": "zh-Hans"}),
    )
    metadata.add_wemi_relation_link(
        "work",
        "identifiers",
        WorkRelationLink(target={"scheme": "doi", "value": "10.5555/图書-本"}),
    )

    calibre_metadata = metadata.as_calibre_metadata()

    assert calibre_metadata.title == "Legacy Projection Title"
    assert set(calibre_metadata.tags) >= {
        "legacy-tag",
        "标签-タグ",
        "主题-日本語",
    }
    assert calibre_metadata.series == "系列-シリーズ"
    assert calibre_metadata.languages == ["zh-Hans"]
    assert "10.5555/图書-本" in _identifier_values(calibre_metadata, "doi")

    assert _values(metadata.tags) == ("legacy-tag",)
    assert metadata.get_wemi_relation_link_ids("work", "tags") == ("work-tag-link",)


def test_wemi_opf_round_trip_preserves_supported_flat_values_not_graph_links() -> None:
    metadata = LiuXinWEMIMetadata(
        work_metadata=WorkMetadata(
            work=WorkIdentity(
                work_id=10,
                work_canonical_title="作品の標題",
            ),
        ),
    )
    metadata.add_wemi_relation_link(
        "work",
        "tags",
        WorkRelationLink(target={"tag": "标签-タグ"}, link_id="work-tag-link"),
    )
    metadata.add_wemi_relation_link(
        "work",
        "subjects",
        WorkRelationLink(target={"subject": "主题-日本語"}),
    )
    metadata.add_wemi_relation_link(
        "work",
        "series",
        WorkRelationLink(target={"series": "系列-シリーズ"}),
    )
    metadata.add_wemi_relation_link(
        "item",
        "identifiers",
        ItemRelationLink(target={"scheme": "doi", "value": "10.5555/图書-本"}),
    )

    raw = metadata_to_opf_bytes(metadata)
    round_tripped = metadata_from_opf(raw, kind="wemi", item_id=77)

    assert round_tripped.display_title == "作品の標題"
    assert set(_values(round_tripped.tags)) >= {"标签-タグ", "主题-日本語"}
    assert _values(round_tripped.series) == ("系列-シリーズ",)
    assert "10.5555/图書-本" in _identifier_values(round_tripped, "doi")
    assert round_tripped.get_database_id("item") == 77

    assert round_tripped.get_wemi_relation_link_ids("work", "tags") == ()
    assert round_tripped.get_wemi_relation_link_ids("item", "identifiers") == ()


def test_lazy_wemi_conversion_materializes_projection_dependencies() -> None:
    metadata = LazyLiuXinWEMIMetadata("Lazy Conversion", ["Lazy Author"])
    metadata.install_lazy_value_to_id("tags", lambda: {"legacy-lazy-tag": 5})
    metadata.install_lazy_relation_loader(
        "work",
        "tags",
        lambda: [WorkRelationLink(target={"tag": "graph-lazy-tag"})],
    )
    metadata.install_lazy_relation_loader(
        "item",
        "identifiers",
        lambda: [ItemRelationLink(target={"scheme": "doi", "value": "10.5555/lazy"})],
    )

    calibre_metadata = metadata.as_calibre_metadata()

    assert set(calibre_metadata.tags) >= {"legacy-lazy-tag", "graph-lazy-tag"}
    assert "10.5555/lazy" in _identifier_values(calibre_metadata, "doi")
    assert metadata.values.tags == ("legacy-lazy-tag", "graph-lazy-tag")
