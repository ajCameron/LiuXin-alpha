"""Enrich the selected Calibre book projection using Catalog-owned field meaning.

Only the selected Work, Expression, Manifestation and its Items participate.
This read owner neither evaluates templates nor manages a derived cache.
"""

from datetime import UTC, datetime, timedelta
from pathlib import PurePosixPath


def metadata_values(owner, reference, work, expression, manifestation):
    """Return ordinary metadata for exactly the displayed WEMI context."""
    repository = owner.repositories.expressions
    db = owner.db
    selected = [
        (table, row)
        for table, row in (
            ("works", work),
            ("expressions", expression),
            ("manifestations", manifestation),
        )
        if row
    ]

    def links(table, row, target):
        if db.driver_wrapper.get_link_spec(table, target) is None:
            return ()
        return repository._linked_rows(
            table, row[db.driver_wrapper.get_id_column(table)], target
        )

    def attachments(target, *, by_role=False):
        result = {}
        for table, row in selected:
            for record in links(table, row, target):
                identity = record[db.driver_wrapper.get_id_column(target)]
                if by_role:
                    identity = (identity, record.get("_catalog_link", {}).get("type"))
                result.setdefault(identity, record)
        return tuple(result.values())

    agents = attachments("agents", by_role=True)
    authors = tuple(
        r["agent_canonical_name"]
        for r in agents
        if r.get("_catalog_link", {}).get("type") == "aut"
    )
    publishers = tuple(
        r["agent_canonical_name"]
        for r in agents
        if r.get("_catalog_link", {}).get("type") == "pbl"
    )
    identifiers = {}
    for table, row in selected:
        level = table[:-1]
        for record in owner.repositories.identifiers.list_for_wemi(
            level=level, entity_id=row[f"{level}_id"]
        ):
            identifiers[record["entity_identifier_scheme"]] = record[
                "entity_identifier_value"
            ]
    series = attachments("series")
    items = (
        ()
        if not manifestation
        else owner.repositories.items.list_for_manifestation(
            manifestation["manifestation_id"]
        )
    )
    for item in items:
        selected.append(("items", item))
    files = attachments("files")
    formats = tuple(
        dict.fromkeys(
            PurePosixPath(str(row.get("file_name") or ""))
            .suffix.removeprefix(".")
            .upper()
            for row in files
            if row.get("file_name")
        )
    )
    language_id = expression.get("expression_language_id")
    language = (
        {} if language_id is None else owner.repositories.languages.require(language_id)
    )
    languages = (
        ()
        if not language
        else (
            language.get("language_iso639_2_t")
            or language.get("language_code")
            or language.get("language"),
        )
    )

    def date(value):
        if isinstance(value, int):
            return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=value)
        if isinstance(value, str) and value:
            from LiuXin_alpha.utils.date import parse_date

            return parse_date(value)
        return value

    result = {
        "id": reference.expression_id,
        "authors": authors,
        "languages": languages,
        "publisher": ", ".join(publishers) or None,
        "tags": tuple(r["tag"] for r in attachments("tags")),
        "genre": tuple(r["genre"] for r in attachments("genres")),
        "subjects": tuple(r["subject"] for r in attachments("subjects")),
        "identifiers": identifiers,
        "uuid": identifiers.get("uuid"),
        "series": series[0]["series"] if series else None,
        "series_index": None
        if not series
        else series[0]
        .get("_catalog_link", {})
        .get("extra", {})
        .get("series_index", 1.0),
        "formats": tuple(v for v in formats if v),
        "size": max((r.get("file_size_bytes") or 0 for r in files), default=0),
        "cover": any(r.get("_catalog_link", {}).get("type") == "cover" for r in files),
        "timestamp": date(expression.get("expression_created_timestamp_ep_k")),
        "pubdate": date(manifestation.get("manifestation_pub_date")),
        "path": next(
            (r.get("item_source_path") for r in items if r.get("item_source_path")),
            None,
        ),
    }
    from LiuXin_alpha.databases.custom_columns.native_service import CustomFieldService
    from LiuXin_alpha.databases.custom_columns.native_types import (
        OwnerRef,
        SeriesValue,
        ValueEntry,
    )

    service = CustomFieldService(db)
    descriptors = {}
    labels = {}
    for table, row in selected:
        owner_ref = OwnerRef(table, row[db.driver_wrapper.get_id_column(table)])
        for definition in service.list_fields(attachment=table):
            key = definition.key
            descriptors[key] = {
                "datatype": definition.datatype,
                "display": dict(definition.display),
                "is_multiple": definition.mode != "scalar",
                "search_terms": [key],
            }
            labels.setdefault(definition.label, set()).add(key)
            if definition.datatype == "composite":
                continue  # Evaluation requires the search caller's one-use permission.
            if key in result:
                continue  # A scalar Item projection uses the first selected copy.
            value = service.get_value(owner_ref, definition.id)
            if isinstance(value, (SeriesValue, ValueEntry)):
                result[key] = value.value
                result[key + "_index"] = value.position
            elif isinstance(value, tuple):
                result[key] = tuple(
                    item.value if isinstance(item, (SeriesValue, ValueEntry)) else item
                    for item in value
                )
            else:
                result[key] = value
    for label, keys in labels.items():
        if len(keys) == 1:
            descriptors[next(iter(keys))]["search_terms"].append("#" + label)
    result["_search_fields"] = descriptors
    return result
