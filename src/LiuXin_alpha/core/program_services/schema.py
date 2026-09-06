"""Core-owned schema operations and wire translation."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from LiuXin_alpha.core.errors import CoreDispatchError
from LiuXin_alpha.core.program_services.payloads import (
    _callable,
    _database_callable,
    _mapping,
    _optional_int,
    _payload,
    _required_int,
    _required_text,
    plain,
)

if TYPE_CHECKING:
    from LiuXin_alpha.core.commands import CoreCommand
    from LiuXin_alpha.core.queries import CoreQuery
    from LiuXin_alpha.core.runtime import CoreRuntime


def schema_column(runtime: CoreRuntime, query: CoreQuery) -> Any:
    payload = _payload(query)
    table = _required_text(payload, "table")
    column = _required_text(payload, "column")
    method = _callable(
        runtime.database,
        "get_column_metadata",
        area="database schema",
    )
    return plain(method(table, column))


def schema_link(runtime: CoreRuntime, query: CoreQuery) -> dict[str, Any]:
    payload = _payload(query)
    table = _required_text(payload, "table")
    related = _required_text(payload, "related_table")
    method = _callable(
        runtime.database,
        "get_link_capabilities",
        area="database schema",
    )
    capabilities = method(table, related)
    return {
        "table": table,
        "related_table": related,
        "capabilities": plain(capabilities),
    }


def schema_column_update(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    payload = _payload(command)
    table = _required_text(payload, "table")
    column = _required_text(payload, "column")
    policy = _mapping(payload, "policy")
    from LiuXin_alpha.databases.column_metadata import (
        ColumnEmptyValuePolicy,
        ColumnMergePolicy,
        ColumnMetadata,
        ColumnNormalizationProfile,
        ColumnSemanticRole,
        ColumnValidationProfile,
    )

    get_metadata = _callable(
        runtime.database,
        "get_column_metadata",
        area="database schema",
    )
    current = get_metadata(table, column)
    metadata = ColumnMetadata(
        table=table,
        column=column,
        case_sensitive=bool(
            policy.get(
                "case_sensitive",
                current.case_sensitive,
            )
        ),
        semantic_role=ColumnSemanticRole(
            policy.get(
                "semantic_role",
                current.semantic_role,
            )
        ),
        normalization_profile=ColumnNormalizationProfile(
            policy.get(
                "normalization_profile",
                current.normalization_profile,
            )
        ),
        comparison_column=policy.get(
            "comparison_column",
            current.comparison_column,
        ),
        empty_value_policy=ColumnEmptyValuePolicy(
            policy.get(
                "empty_value_policy",
                current.empty_value_policy,
            )
        ),
        merge_policy=ColumnMergePolicy(
            policy.get(
                "merge_policy",
                current.merge_policy,
            )
        ),
        validation_profile=ColumnValidationProfile(
            policy.get(
                "validation_profile",
                current.validation_profile,
            )
        ),
        formatting_options=policy.get(
            "formatting_options",
            current.formatting_options,
        ),
        display_options=policy.get(
            "display_options",
            current.display_options,
        ),
    )
    _callable(
        runtime.database,
        "set_column_metadata",
        area="database schema",
    )(metadata)
    return runtime.services.reconcile(
        {
            "updated": True,
            "table": table,
            "column": column,
            "policy": plain(metadata),
        }
    )


def custom_fields_list(
    runtime: CoreRuntime,
    query: CoreQuery,
) -> dict[str, Any]:
    del query
    db = runtime.database
    try:
        rows = [
            plain(item)
            for item in db.get_all_rows(
                "custom_columns",
                iterator_return=False,
                sort_column="custom_column_id",
            )
        ]
    except Exception:
        by_label = getattr(db, "custom_column_label_map", None)
        if isinstance(by_label, Mapping):
            rows = [
                plain(item)
                for _label, item in sorted(
                    by_label.items(),
                    key=lambda pair: str(pair[0]),
                )
            ]
        else:
            rows = []
    fields: list[dict[str, Any]] = []
    for raw in rows:
        if not isinstance(raw, Mapping):
            continue
        values = {
            (
                str(key)[len("custom_column_") :]
                if str(key).startswith("custom_column_")
                else str(key)
            ): value
            for key, value in raw.items()
        }
        values["num"] = values.pop("id", values.get("num"))
        display = values.get("display")
        if isinstance(display, str):
            try:
                values["display"] = __import__("json").loads(display)
            except Exception:
                pass
        fields.append(values)
    return {"fields": fields, "count": len(fields)}


def custom_fields_create(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    payload = _payload(command)
    method = _database_callable(
        runtime,
        "create_custom_column",
        area="custom fields",
    )
    display = payload.get("display")
    if display is not None and not isinstance(display, (str, Mapping)):
        raise CoreDispatchError("`display` must be a string, object, or null.")
    num = method(
        name=_required_text(payload, "name"),
        datatype=str(payload.get("datatype") or "text"),
        is_multiple=bool(payload.get("is_multiple", False)),
        label=(str(payload["label"]) if payload.get("label") is not None else None),
        editable=bool(payload.get("editable", True)),
        display=(
            str(display)
            if isinstance(display, str)
            else (
                None
                if display is None
                else __import__("json").dumps(dict(display), sort_keys=True)
            )
        ),
        table=str(payload.get("table") or "books"),
        make_category=(
            bool(payload["make_category"])
            if payload.get("make_category") is not None
            else None
        ),
    )
    runtime.services.refresh_field_metadata()
    return runtime.services.reconcile(
        {"created": True, "num": int(num), "schema_changed": True}
    )


def custom_fields_update(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    payload = _payload(command)
    num = _required_int(payload, "num")
    changes = _mapping(payload, "changes")
    allowed = {
        "name",
        "label",
        "is_editable",
        "display",
        "in_table",
        "notify",
        "update_last_modified",
    }
    unknown = sorted(set(changes) - allowed)
    if unknown:
        raise CoreDispatchError(
            "Unknown custom-field changes: {}.".format(", ".join(unknown))
        )
    method = _database_callable(
        runtime,
        "set_custom_column_metadata",
        area="custom fields",
    )
    changed = method(num=num, **changes)
    runtime.services.refresh_field_metadata()
    return runtime.services.reconcile(
        {
            "updated": True,
            "num": num,
            "changed_row_ids": plain(changed),
            "schema_changed": True,
        }
    )


def custom_fields_delete(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    payload = _payload(command)
    num = _optional_int(payload, "num", minimum=0)
    label_raw = payload.get("label")
    label = str(label_raw).strip() if label_raw is not None else None
    if num is None and not label:
        raise CoreDispatchError("Provide `num` or `label`.")
    method = _database_callable(
        runtime,
        "delete_custom_column",
        area="custom fields",
    )
    method(label=label, num=num)
    runtime.services.refresh_field_metadata()
    return runtime.services.reconcile(
        {
            "deleted": True,
            "num": num,
            "label": label,
            "schema_changed": True,
        }
    )
