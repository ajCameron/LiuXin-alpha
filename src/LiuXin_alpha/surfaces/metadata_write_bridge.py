"""Surface helpers for routing metadata-specific writes through write reports."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
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

_METADATA_WRITE_FIELD_ALIASES = {
    "tag": "tags",
    "tags": "tags",
    "label": "labels",
    "labels": "labels",
    "genre": "genre",
    "genres": "genre",
    "subject": "subject",
    "subjects": "subject",
    "series": "series",
    "identifier": "identifiers",
    "identifiers": "identifiers",
}

_METADATA_WRITE_KINDS = {
    "wemi": "liuxin_wemi",
    "liuxin-wemi": "liuxin_wemi",
    "liuxin_wemi": "liuxin_wemi",
    "liuxin": "liuxin",
    "calibre": "calibre",
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


def normalize_metadata_write_field(field: str) -> str:
    normalized = str(field).strip().lower().replace("-", "_")
    try:
        return _METADATA_WRITE_FIELD_ALIASES[normalized]
    except KeyError as exc:
        raise ValueError("Unsupported metadata write field: {!r}".format(field)) from exc


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


def write_wemi_metadata_values(
    database: Any,
    *,
    item_id: int,
    values: dict[str, Any],
    fields: list[str] | tuple[str, ...] | None = None,
    kind: str = "liuxin",
    replace: bool = False,
    target_level: str = "work",
    mark_dirty: bool = True,
) -> dict[str, Any]:
    from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadataHydrator

    normalized_kind = str(kind or "liuxin").strip().lower().replace("-", "_")
    metadata_kind = _METADATA_WRITE_KINDS.get(normalized_kind)
    if metadata_kind is None:
        raise ValueError("Unsupported metadata write kind: {!r}".format(kind))

    value_map = dict(values or {})
    if not value_map:
        raise ValueError("Metadata write values cannot be empty.")

    if fields is None:
        requested_fields = tuple(value_map)
    elif isinstance(fields, str):
        requested_fields = (fields,)
    else:
        requested_fields = tuple(fields)
    normalized_fields = tuple(normalize_metadata_write_field(field) for field in requested_fields)

    hydrator = LiuXinWEMIMetadataHydrator(database)
    metadata = hydrator.hydrate_metadata(metadata_kind, item_id=int(item_id))
    for field_name in normalized_fields:
        value = _metadata_write_value(value_map, field_name)
        _apply_metadata_write_value(metadata, field_name, value, replace=bool(replace))

    report = metadata.write_to_database(
        database,
        fields=normalized_fields,
        target_level=str(target_level or "work"),
        item_id=int(item_id),
        replace=bool(replace),
        mark_dirty=bool(mark_dirty),
    )
    report_mapping = report.to_mapping() if hasattr(report, "to_mapping") else {}
    return {
        "item_id": int(item_id),
        "kind": metadata_kind,
        "fields": list(normalized_fields),
        "replace": bool(replace),
        "changed": bool(getattr(report, "changed", False)),
        "summary": metadata_write_report_summary(report),
        "report": report_mapping,
    }


def _metadata_write_value(values: dict[str, Any], field_name: str) -> Any:
    candidate_keys = (field_name, field_name.rstrip("s"), field_name + "s")
    for key in candidate_keys:
        if key in values:
            return values[key]
    raise ValueError("Metadata write values missing field {!r}.".format(field_name))


def _apply_metadata_write_value(metadata: Any, field_name: str, value: Any, *, replace: bool) -> None:
    if replace:
        try:
            metadata.nullify(field_name)
        except KeyError:
            pass

    if field_name == "identifiers":
        setter = getattr(metadata, "set_identifiers", None)
        if not callable(setter):
            raise ValueError("{} cannot write identifiers.".format(metadata.__class__.__name__))
        setter(dict(value or {}), update=not bool(replace))
        return

    if not isinstance(value, (str, bytes, Mapping)) and isinstance(value, Iterable):
        for entry in value:
            if entry not in (None, ""):
                setattr(metadata, field_name, entry)
        return

    setattr(metadata, field_name, value)


__all__ = [
    "metadata_relation_field_for_table",
    "metadata_write_report_summary",
    "normalize_metadata_write_field",
    "supports_wemi_metadata_relation_write",
    "write_wemi_metadata_values",
    "write_wemi_metadata_relation_link",
]
