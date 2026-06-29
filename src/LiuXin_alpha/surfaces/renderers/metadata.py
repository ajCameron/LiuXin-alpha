"""String/markup renderers for metadata-like objects."""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Mapping
from copy import deepcopy
from typing import Any

from LiuXin_alpha.metadata.standardize import (
    standardize_creator_category,
    standardize_id_name,
    standardize_internal_id_name,
)
from LiuXin_alpha.metadata.utils import fmt_sidx
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iterkeys as iterkeys
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode
from LiuXin_alpha.utils.localization import trans as _


def metadata_to_html(metadata: object) -> str:
    """Render a LiuXin metadata-like object as a simple HTML table."""

    data = _metadata_mapping(metadata)
    rows: list[tuple[str, object]] = []

    def append_creator_rows(field_dict: Mapping[object, object], creator_role: str) -> None:
        rows.append((_("Creator_Role"), six_unicode(creator_role)))
        for person in field_dict:
            rows.append((_("Creator"), six_unicode(person)))

    def append_identifier_rows(identifier_values: set[object], identifier_name: str) -> None:
        rows.append((_("Identifier Type"), six_unicode(identifier_name)))
        for identifier in identifier_values:
            rows.append(("", identifier))

    for field, value in data.items():
        creator_field = standardize_creator_category(field, logging=False)
        id_field = standardize_id_name(field, logging=False)
        internal_id_field = standardize_internal_id_name(field, logging=False)

        if creator_field is not None and isinstance(value, Mapping):
            append_creator_rows(value, creator_field)
        elif id_field is not None and isinstance(value, set):
            append_identifier_rows(value, id_field)
        elif internal_id_field is not None and isinstance(value, set):
            append_identifier_rows(value, internal_id_field)
        elif isinstance(value, OrderedDict):
            field_values = [six_unicode(one_value) for one_value in deepcopy([key for key in iterkeys(value)])]
            rows.append((_(field), " , ".join(field_values) if value.keys() is not None else "None"))
        elif isinstance(value, set):
            field_values = [six_unicode(one_value) for one_value in deepcopy(value)]
            rows.append((_(field), field_values if field_values else "None"))
        else:
            rows.append((_(field), value))

    rendered_rows = [
        "<tr><td><b>{}</b></td><td>{}</td></tr>".format(label, value)
        for label, value in rows
    ]
    return "<table>{}</table>".format("\n".join(rendered_rows))


def series_index_to_text(
    value: str | int | float | None = None,
    *,
    metadata: object | None = None,
) -> str:
    """Render a series index value in human-readable form."""

    if value is None and metadata is not None:
        value = _metadata_series_index(metadata)

    try:
        index = float(1 if value is None else value)
    except (ValueError, TypeError):
        index = 1
    return fmt_sidx(index)


def _metadata_mapping(metadata: object) -> Mapping[str, Any]:
    if isinstance(metadata, Mapping):
        return metadata

    try:
        data = object.__getattribute__(metadata, "_data")
    except AttributeError:
        to_mapping = getattr(metadata, "to_mapping", None)
        if callable(to_mapping):
            mapped = to_mapping()
            if isinstance(mapped, Mapping):
                return mapped
        raise TypeError(
            "metadata_to_html() expects a mapping or metadata object with _data/to_mapping()."
        ) from None

    if not isinstance(data, Mapping):
        raise TypeError("metadata _data must be a mapping.")
    return data


def _metadata_series_index(metadata: object) -> object:
    series_index = getattr(metadata, "series_index", None)
    series = getattr(metadata, "series", None)

    if isinstance(series_index, Mapping) and isinstance(series, Mapping):
        first_series = next(iter(series), None)
        if first_series is not None:
            return series_index.get(first_series, 1)
    return 1


__all__ = [
    "metadata_to_html",
    "series_index_to_text",
]
