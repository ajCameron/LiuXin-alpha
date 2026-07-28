"""Workflow-level metadata writes shared by Core and interaction surfaces."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any


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


def normalize_metadata_write_field(field: str) -> str:
    """Return the canonical public name for one writable metadata field."""

    normalized = str(field).strip().lower().replace("-", "_")
    try:
        return _METADATA_WRITE_FIELD_ALIASES[normalized]
    except KeyError as exc:
        raise ValueError(
            "Unsupported metadata write field: {!r}".format(field)
        ) from exc


def metadata_write_report_summary(report: Any) -> str:
    """Return the compact shared human summary for a metadata write report."""

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
    """Apply selected values through the established WEMI metadata writer."""

    from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadataHydrator

    normalized_kind = (
        str(kind or "liuxin").strip().lower().replace("-", "_")
    )
    metadata_kind = _METADATA_WRITE_KINDS.get(normalized_kind)
    if metadata_kind is None:
        raise ValueError(
            "Unsupported metadata write kind: {!r}".format(kind)
        )

    value_map = dict(values or {})
    if not value_map:
        raise ValueError("Metadata write values cannot be empty.")

    if fields is None:
        requested_fields = tuple(value_map)
    elif isinstance(fields, str):
        requested_fields = (fields,)
    else:
        requested_fields = tuple(fields)
    normalized_fields = tuple(
        normalize_metadata_write_field(field)
        for field in requested_fields
    )

    hydrator = LiuXinWEMIMetadataHydrator(database)
    metadata = hydrator.hydrate_metadata(
        metadata_kind,
        item_id=int(item_id),
    )
    for field_name in normalized_fields:
        value = _metadata_write_value(value_map, field_name)
        _apply_metadata_write_value(
            metadata,
            field_name,
            value,
            replace=bool(replace),
        )

    report = metadata.write_to_database(
        database,
        fields=normalized_fields,
        target_level=str(target_level or "work"),
        item_id=int(item_id),
        replace=bool(replace),
        mark_dirty=bool(mark_dirty),
    )
    report_mapping = (
        report.to_mapping()
        if hasattr(report, "to_mapping")
        else {}
    )
    return {
        "item_id": int(item_id),
        "kind": metadata_kind,
        "fields": list(normalized_fields),
        "replace": bool(replace),
        "changed": bool(getattr(report, "changed", False)),
        "summary": metadata_write_report_summary(report),
        "report": report_mapping,
    }


def _metadata_write_value(
    values: dict[str, Any],
    field_name: str,
) -> Any:
    candidate_keys = (
        field_name,
        field_name.rstrip("s"),
        field_name + "s",
    )
    for key in candidate_keys:
        if key in values:
            return values[key]
    raise ValueError(
        "Metadata write values missing field {!r}.".format(field_name)
    )


def _apply_metadata_write_value(
    metadata: Any,
    field_name: str,
    value: Any,
    *,
    replace: bool,
) -> None:
    if replace:
        try:
            metadata.nullify(field_name)
        except KeyError:
            pass

    if field_name == "identifiers":
        setter = getattr(metadata, "set_identifiers", None)
        if not callable(setter):
            raise ValueError(
                "{} cannot write identifiers.".format(
                    metadata.__class__.__name__
                )
            )
        setter(dict(value or {}), update=not bool(replace))
        return

    if (
        not isinstance(value, (str, bytes, Mapping))
        and isinstance(value, Iterable)
    ):
        for entry in value:
            if entry not in (None, ""):
                setattr(metadata, field_name, entry)
        return

    setattr(metadata, field_name, value)


__all__ = [
    "metadata_write_report_summary",
    "normalize_metadata_write_field",
    "write_wemi_metadata_values",
]
