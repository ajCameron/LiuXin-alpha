"""Surface helpers for routing metadata-specific writes through write reports."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.expression_containers.expression_metadata_api import (
    ExpressionRelationLink,
)
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.item_containers.item_metadata_api import (
    ItemRelationLink,
)
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.manifestation_containers.manifestation_metadata_api import (
    ManifestationRelationLink,
)
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.work_containers.work_metadata_api import (
    WorkRelationLink,
)
from LiuXin_alpha.metadata.containers import (
    ExpressionMetadata,
    ItemMetadata,
    ManifestationMetadata,
    WorkMetadata,
)


_RELATION_FIELD_BY_TABLE = {
    "comments": "comments",
    "genres": "genre",
    "labels": "labels",
    "notes": "notes",
    "series": "series",
    "subjects": "subject",
    "synopses": "synopses",
    "tags": "tags",
}


@dataclass(frozen=True)
class WemiMetadataWriteSpec:
    level: str
    metadata_class: Callable[..., Any]
    relation_link_class: Callable[..., Any]


_WEMI_SPECS_BY_TABLE = {
    "works": WemiMetadataWriteSpec(
        level="work",
        metadata_class=WorkMetadata,
        relation_link_class=WorkRelationLink,
    ),
    "expressions": WemiMetadataWriteSpec(
        level="expression",
        metadata_class=ExpressionMetadata,
        relation_link_class=ExpressionRelationLink,
    ),
    "manifestations": WemiMetadataWriteSpec(
        level="manifestation",
        metadata_class=ManifestationMetadata,
        relation_link_class=ManifestationRelationLink,
    ),
    "items": WemiMetadataWriteSpec(
        level="item",
        metadata_class=ItemMetadata,
        relation_link_class=ItemRelationLink,
    ),
}


def metadata_relation_field_for_table(relation_table: str) -> str | None:
    return _RELATION_FIELD_BY_TABLE.get(str(relation_table).strip().lower())


def supports_wemi_metadata_relation_write(target_table: str, relation_table: str) -> bool:
    spec = _WEMI_SPECS_BY_TABLE.get(str(target_table).strip().lower())
    relation = str(relation_table).strip().lower()
    if spec is None or metadata_relation_field_for_table(relation) is None:
        return False
    relation_names = getattr(spec.metadata_class, "relation_names", None)
    if not callable(relation_names):
        return False
    try:
        return relation in set(str(name) for name in relation_names())
    except Exception:
        return False


def write_wemi_metadata_relation_link(
    database: Any,
    *,
    target_row: Any,
    relation_table: str,
    relation_row: Any,
    priority: Any = "highest",
    link_type: Any = None,
    extra_values: dict[str, Any] | None = None,
    mark_dirty: bool = True,
) -> Any | None:
    target_table = str(getattr(target_row, "table", "") or "").strip().lower()
    relation = str(relation_table).strip().lower()
    spec = _WEMI_SPECS_BY_TABLE.get(target_table)
    field_name = metadata_relation_field_for_table(relation)
    if spec is None or field_name is None:
        return None
    if not supports_wemi_metadata_relation_write(target_table, relation):
        return None

    extras = dict(extra_values or {})
    link_kwargs: dict[str, Any] = {
        "target": relation_row,
        "priority": priority,
        "extra": {"source_entity_type": spec.level},
    }
    if link_type not in (None, ""):
        link_kwargs["type"] = link_type
    for attr in ("primary", "origin", "source", "policy", "data", "index"):
        if attr in extras and extras[attr] not in (None, ""):
            link_kwargs[attr] = extras[attr]

    relation_link = spec.relation_link_class(**link_kwargs)
    metadata = spec.metadata_class(relation_links={relation: [relation_link]})
    return metadata.write_to_database(
        database,
        fields=(field_name,),
        target_row=target_row,
        mark_dirty=mark_dirty,
    )


def metadata_write_report_summary(report: Any) -> str:
    rows_added = len(getattr(report, "rows_added", []) or [])
    rows_updated = len(getattr(report, "rows_updated", []) or [])
    rows_removed = len(getattr(report, "rows_removed", []) or [])
    links_added = len(getattr(report, "links_added", []) or [])
    links_removed = len(getattr(report, "links_removed", []) or [])
    skipped = len(getattr(report, "skipped", []) or [])
    errors = len(getattr(report, "errors", []) or [])
    return (
        "metadata report: rows_added={rows_added}, rows_updated={rows_updated}, "
        "rows_removed={rows_removed}, links_added={links_added}, "
        "links_removed={links_removed}, skipped={skipped}, errors={errors}"
    ).format(
        rows_added=rows_added,
        rows_updated=rows_updated,
        rows_removed=rows_removed,
        links_added=links_added,
        links_removed=links_removed,
        skipped=skipped,
        errors=errors,
    )


__all__ = [
    "metadata_relation_field_for_table",
    "metadata_write_report_summary",
    "supports_wemi_metadata_relation_write",
    "write_wemi_metadata_relation_link",
]
