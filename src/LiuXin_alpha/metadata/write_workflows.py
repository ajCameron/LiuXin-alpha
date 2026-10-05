"""
Apply public metadata write requests and summarize their WEMI writer reports.

The workflow normalizes field and kind names, hydrates an item, updates its metadata
containers, and delegates persistence to their write_to_database method.

Example:
    Exercise the owning behavior with pytest::

        python -m pytest -q tests/core/test_core_application_api.py::test_core_catalog_and_cache_api_round_trip_real_database
"""

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
    """
    Resolve a supported write-field alias after trimming, lower-casing, and replacing hyphens.

    Unsupported names raise ValueError; only the aliases in this module are accepted.

    Example:
        >>> normalize_metadata_write_field(' Genres ')
        'genre'


    :param field: Field or alias converted to text before normalization.
    :return: Canonical public field name.
    """

    normalized = str(field).strip().lower().replace("-", "_")
    try:
        return _METADATA_WRITE_FIELD_ALIASES[normalized]
    except KeyError as exc:
        raise ValueError(
            "Unsupported metadata write field: {!r}".format(field)
        ) from exc


def metadata_write_report_summary(report: Any) -> str:
    """
    Summarize row/link additions and removals, skipped operations, and errors.

    Missing or false report attributes count as empty collections.

    Example:
        >>> from types import SimpleNamespace
        >>> metadata_write_report_summary(SimpleNamespace(rows_added=[1])).startswith('metadata report: rows_added=1,')
        True


    :param report: Object exposing sized report collections as attributes.
    :return: One human-readable metadata report line.
    """

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
    """
    Hydrate an item, apply requested relation values, and persist them through the WEMI writer.

    Accept calibre, liuxin, and WEMI kind aliases. Empty values, unsupported
    kinds/fields, or missing requested values raise ValueError. Field names are
    normalized, while value lookup tries only canonical, singular, and plural keys. The
    database and persistence transaction policy belong to the caller and writer; this
    facade does not close the database.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/core/test_core_application_api.py::test_core_catalog_and_cache_api_round_trip_real_database


    :param database: Caller-owned database used by the metadata writer for persistence;
        this method does not close it.
    :param item_id: Item identifier converted to int for hydration and writing.
    :param values: Field-to-value mapping copied before lookup; must be nonempty.
    :param fields: Requested field names; None uses the supplied value keys and a string
        requests one field.
    :param kind: Metadata family selector; false values default to liuxin.
    :param replace: Whether to clear each selected container and request replacement on
        write.
    :param target_level: WEMI target level forwarded to the writer; false values become
        work.
    :param mark_dirty: Whether the writer should mark changed metadata dirty.
    :return: Receipt dictionary containing item_id, normalized kind and fields, replace,
        changed, summary, and a report mapping.
    """

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
    """
    Read a value using canonical, stripped-plural, then appended-plural field keys.

    The first present key wins, including a value of None. Raise ValueError when none is
    present; this lookup does not perform general alias normalization.

    Example:
        >>> _metadata_write_value({'tag': ['history']}, 'tags')
        ['history']


    :param values: Mapping of caller-supplied field keys to values.
    :param field_name: Canonical field name used to generate the three candidates.
    :return: Value stored under the first matching key.
    """
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
    """
    Apply one field value to an already hydrated metadata object.

    Replacement first calls nullify, ignoring KeyError only. Identifiers require a
    callable set_identifiers and receive a dictionary plus the inverse replacement flag
    as update. Other non-string, non-mapping iterables are assigned entry by entry,
    skipping None and empty strings; scalar values are assigned once.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/core/test_core_application_api.py::test_core_catalog_and_cache_api_round_trip_real_database


    :param metadata: Hydrated metadata object mutated through its public setters.
    :param field_name: Canonical field name being updated.
    :param value: Scalar, iterable entries, or identifier mapping for this field.
    :param replace: Whether to nullify the old value and replace identifier contents.
    :return: None.
    """
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
