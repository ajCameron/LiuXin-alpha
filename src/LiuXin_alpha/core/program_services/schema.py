"""
Adapt Core column-policy, relationship-capability, and custom-field requests to database schema services.

Schema/backends own semantic validation and mutation policy. Metadata refresh
and read-side reconciliation follow writes without an enclosing adapter transaction.
Custom-field listing has a legacy-map fallback that can hide failed canonical reads;
it is not a general schema-health check or an authorization boundary.
"""

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
    """
    Read one column's policy through the database-level metadata capability and project the result.

    This operation does not fall back to the driver wrapper or validate column
    existence separately; missing/invalid-column behavior belongs to the database.

    Example:
        >>> policy = schema_column(runtime, query)  # doctest: +SKIP


    :param runtime: Runtime exposing database.get_column_metadata.
    :param query: Query with required stripped table and column text.
    :return: Plain-projected column metadata value; backend failures propagate.
    """
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
    """
    Describe database-declared relationship capabilities for a requested pair of tables.

    This reads schema capabilities rather than linked row data and does not create
    an interlink. Table-pair validation belongs to get_link_capabilities.

    Example:
        >>> relation = schema_link(runtime, query)  # doctest: +SKIP


    :param runtime: Runtime whose database exposes get_link_capabilities.
    :param query: Query with required table and related_table text.
    :return: Requested table labels and projected relationship capabilities.
    """
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
    """
    Build a replacement column policy from current metadata and supplied known fields, then persist and reconcile.

    Missing known fields inherit current values; unknown policy keys are ignored.
    case_sensitive is truth-tested, enum-valued fields use their constructors, and
    comparison/format/display values pass to ColumnMetadata validation. An empty
    policy still invokes the setter. updated=True reports returned delegation,
    with reconciliation failure possible after persistence and no separate readback.

    Example:
        >>> receipt = schema_column_update(runtime, command)  # doctest: +SKIP


    :param runtime: Runtime exposing database column-metadata get/set and service reconciliation.
    :param command: Command with table, column, and Mapping policy containing optional known ColumnMetadata fields.
    :return: Reconciled table/column receipt with updated=True and the projected constructed policy.
    """
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
    """
    List projected custom-column definitions, falling back to a label-sorted legacy map after canonical read failure.

    Canonical reading, iteration, and projection share the fallback catch; fallback
    errors propagate, and a successful empty canonical result does not trigger it.
    Non-Mapping projections are skipped. Keys lose a leading custom_column_ prefix,
    with collisions resolved by later entries; id becomes num even when its value
    is None. Display JSON accepts any parsed value, retaining text on parse failure.

    Example:
        >>> fields = custom_fields_list(runtime, query)  # doctest: +SKIP


    :param runtime: Runtime providing database custom_columns rows and optional custom_column_label_map.
    :param query: Ignored query envelope; the complete discovered set is returned without paging.
    :return: Normalized field mappings and count; an empty result does not prove canonical-read availability.
    """
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
    """
    Create a custom field through database-first capability lookup, then refresh field metadata and reconcile.

    Name is required/stripped; falsey datatype/table select text/books. Optional
    label remains unstripped, booleans are truth-tested, and make_category preserves
    None. Display accepts verbatim text or a Mapping serialized as sorted-key JSON;
    backend policy owns its meaning. Refresh, ID conversion, or reconciliation can
    fail after creation without adapter rollback.

    Example:
        >>> receipt = custom_fields_create(runtime, command)  # doctest: +SKIP


    :param runtime: Runtime supplying create_custom_column, field metadata refresh, and reconciliation.
    :param command: Command with name and optional datatype, is_multiple, label, editable, display, table, and make_category.
    :return: Reconciled created=True/schema_changed=True receipt and integer field num.
    :raises CoreDispatchError: If required text/display validation fails or creation capability is unavailable.
    """
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
    """
    Forward allowlisted custom-field metadata changes, refresh the field registry, and reconcile.

    Allowed keys are name, label, is_editable, display, in_table, notify, and
    update_last_modified. Values are passed unchanged and an empty changes Mapping
    is allowed. updated/schema_changed flags do not depend on the returned changed
    row collection being nonempty. Later refresh/report failures can follow a write.

    Example:
        >>> receipt = custom_fields_update(runtime, command)  # doctest: +SKIP


    :param runtime: Runtime supplying database-first set_custom_column_metadata and read-side refresh/reconciliation.
    :param command: Command with required integer-convertible num and Mapping changes.
    :return: Reconciled field num, optimistic update/schema flags, and projected changed_row_ids result.
    :raises CoreDispatchError: If required fields are invalid, unknown keys are supplied, or capability lookup fails.
    """
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
    """
    Delegate custom-field deletion by optional numeric ID and/or label, then refresh metadata and reconcile.

    num must be nonnegative when supplied; label is stripped text. At least one
    selector must be usable, but both may be passed, with precedence owned by the
    backend. No confirmation field is checked here, and the deletion result is
    ignored before reporting deleted=True. Refresh/reconciliation can fail afterwards.

    Example:
        >>> receipt = custom_fields_delete(runtime, command)  # doctest: +SKIP


    :param runtime: Runtime providing database-first deletion capability and metadata refresh/reconciliation.
    :param command: Command with optional num and label selecting the custom field.
    :return: Reconciled selectors and deleted=True/schema_changed=True after delegation and refresh.
    :raises CoreDispatchError: If selectors are invalid/absent or deletion capability is unavailable.
    """
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
