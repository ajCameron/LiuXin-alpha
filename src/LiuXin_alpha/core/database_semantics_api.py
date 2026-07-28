"""Stable Core operations for database identity and tree semantics."""

# pyright: reportImportCycles=false

from __future__ import annotations

import dataclasses

from collections.abc import Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, cast

from LiuXin_alpha.core.description import CorePayloadFieldDescription
from LiuXin_alpha.core.errors import CoreDispatchError

if TYPE_CHECKING:
    from LiuXin_alpha.core.commands import CoreCommand
    from LiuXin_alpha.core.queries import CoreQuery
    from LiuXin_alpha.core.runtime import CoreRuntime


def _field(
    name: str,
    *,
    required: bool = False,
    field_type: str | None = None,
) -> CorePayloadFieldDescription:
    return CorePayloadFieldDescription(
        name=name,
        required=required,
        field_type=field_type,
    )


def _payload(envelope: Any) -> dict[str, Any]:
    raw = getattr(envelope, "payload", None)
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        raise CoreDispatchError("Core payload must be an object.")
    return dict(raw)


def _required_text(payload: Mapping[str, Any], name: str) -> str:
    value = str(payload.get(name, "")).strip()
    if not value:
        raise CoreDispatchError("`{}` is required.".format(name))
    return value


def _required_int(payload: Mapping[str, Any], name: str) -> int:
    value = payload.get(name)
    if value is None or isinstance(value, bool):
        raise CoreDispatchError("`{}` must be an integer.".format(name))
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise CoreDispatchError(
            "`{}` must be an integer.".format(name)
        ) from exc


def _plain(value: Any) -> Any:
    if value is None or isinstance(value, (str, bytes, bool, int, float)):
        return value
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: _plain(getattr(value, field.name))
            for field in dataclasses.fields(value)
        }
    row_dict = getattr(value, "row_dict", None)
    if isinstance(row_dict, Mapping):
        return _plain(row_dict)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return [_plain(item) for item in value]
    if isinstance(value, Iterable):
        return [_plain(item) for item in value]
    return str(value)


def _method(runtime: "CoreRuntime", name: str, *, area: str) -> Any:
    value = getattr(runtime.database, name, None)
    if not callable(value):
        raise CoreDispatchError(
            "{} does not support `{}`.".format(area, name),
            code="capability_unavailable",
            details={"area": area, "operation": name},
        )
    return value


def _macros(runtime: "CoreRuntime") -> Any:
    value = getattr(runtime.database, "macros", None)
    required = ("get_row", "get_rows", "update_row", "delete_row")
    if value is None or any(
        not callable(getattr(value, name, None))
        for name in required
    ):
        raise CoreDispatchError(
            "The local database does not provide portable tree persistence.",
            code="capability_unavailable",
            details={"area": "tree"},
        )
    return value


def _tree_columns(
    runtime: "CoreRuntime",
    table: str,
) -> tuple[str, str]:
    headings_method = getattr(runtime.database, "get_column_headings", None)
    if not callable(headings_method):
        wrapper = getattr(runtime.database, "driver_wrapper", None)
        headings_method = getattr(wrapper, "get_column_headings", None)
    if not callable(headings_method):
        raise CoreDispatchError(
            "Tree operations require schema column introspection.",
            code="capability_unavailable",
            details={"area": "tree", "table": table},
        )
    headings = {
        str(value)
        for value in cast(Iterable[object], headings_method(table))
    }
    wrapper = getattr(runtime.database, "driver_wrapper", None)
    id_method = getattr(wrapper, "get_id_column", None)
    if callable(id_method):
        id_column = str(id_method(table))
    else:
        id_candidates = sorted(
            (
                value
                for value in headings
                if value == "id" or value.endswith("_id")
            ),
            key=len,
        )
        if not id_candidates:
            raise CoreDispatchError("`{}` has no row ID column.".format(table))
        id_column = id_candidates[0]
    stem = table[:-1] if table.endswith("s") else table
    parent_candidates = (
        "{}_parent_id".format(stem),
        "{}_parent_id".format(table),
        "parent_id",
    )
    parent_column = next(
        (value for value in parent_candidates if value in headings),
        None,
    )
    if parent_column is None:
        parent_column = next(
            (value for value in headings if value.endswith("_parent_id")),
            None,
        )
    if parent_column is None:
        raise CoreDispatchError(
            "`{}` is not a declared parent-linked tree table.".format(table),
            code="not_a_tree_table",
            details={"table": table},
        )
    return id_column, parent_column


def _tree_get(
    runtime: "CoreRuntime",
    table: str,
    row_id: int,
) -> dict[str, Any]:
    id_column, _parent_column = _tree_columns(runtime, table)
    value = _macros(runtime).get_row(
        table,
        row_id,
        id_column=id_column,
    )
    if value is None:
        raise CoreDispatchError(
            "Unknown {} row {}.".format(table, row_id),
            code="row_not_found",
            details={"table": table, "row_id": row_id},
        )
    return dict(value)


def _tree_record(
    *,
    table: str,
    id_column: str,
    row: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "table": table,
        "row_id": row.get(id_column),
        "values": dict(row),
    }


def _tree_children_rows(
    runtime: "CoreRuntime",
    table: str,
    row_id: int,
) -> list[dict[str, Any]]:
    id_column, parent_column = _tree_columns(runtime, table)
    rows = _macros(runtime).get_rows(
        table,
        where={parent_column: row_id},
        order_by=(id_column,),
    )
    return [dict(value) for value in rows]


def _tree_lineage_rows(
    runtime: "CoreRuntime",
    table: str,
    row_id: int,
) -> list[dict[str, Any]]:
    id_column, parent_column = _tree_columns(runtime, table)
    lineage: list[dict[str, Any]] = []
    seen: set[int] = set()
    current_id: int | None = row_id
    while current_id is not None:
        if current_id in seen:
            raise CoreDispatchError(
                "Cycle detected in `{}` tree at row {}.".format(
                    table,
                    current_id,
                ),
                code="tree_cycle",
            )
        seen.add(current_id)
        current = _tree_get(runtime, table, current_id)
        lineage.append(current)
        parent_id = current.get(parent_column)
        current_id = None if parent_id is None else int(parent_id)
    lineage.reverse()
    if not lineage or lineage[-1].get(id_column) != row_id:
        raise CoreDispatchError("Unable to resolve tree lineage.")
    return lineage


def _tree_walk_rows(
    runtime: "CoreRuntime",
    table: str,
    row_id: int,
) -> list[dict[str, Any]]:
    id_column, _parent_column = _tree_columns(runtime, table)
    root = _tree_get(runtime, table, row_id)
    found: list[dict[str, Any]] = []
    pending = [root]
    seen: set[int] = set()
    while pending:
        current = pending.pop(0)
        current_id = int(current[id_column])
        if current_id in seen:
            raise CoreDispatchError(
                "Cycle detected in `{}` tree at row {}.".format(
                    table,
                    current_id,
                ),
                code="tree_cycle",
            )
        seen.add(current_id)
        found.append(current)
        pending.extend(_tree_children_rows(runtime, table, current_id))
    return found


class CoreDatabaseSemanticsAPI:
    """Install normalized-identity and tree operations."""

    def install(self, runtime: "CoreRuntime") -> None:
        query = runtime.register_query_handler
        command = runtime.register_command_handler

        query(
            "schema.identities.list",
            self.identities_list,
            summary="List normalized row-identity declarations.",
            tags=("schema", "identity", "read"),
        )
        query(
            "schema.identity.get",
            self.identity_get,
            summary="Return one normalized row-identity declaration.",
            payload_fields=(
                _field("table", required=True, field_type="string"),
                _field("value_column", required=True, field_type="string"),
            ),
            tags=("schema", "identity", "read"),
        )
        query(
            "schema.identity.derive",
            self.identity_derive,
            summary="Derive a normalized identity value using schema policy.",
            payload_fields=(
                _field("table", required=True, field_type="string"),
                _field("value_column", required=True, field_type="string"),
                _field("value", required=True),
            ),
            tags=("schema", "identity", "read"),
        )
        query(
            "schema.identity.resolve",
            self.identity_resolve,
            summary="Resolve a display value to its canonical stored identity.",
            payload_fields=(
                _field("table", required=True, field_type="string"),
                _field("value_column", required=True, field_type="string"),
                _field("value", required=True),
                _field("scope_values", field_type="object"),
                _field("id_column", field_type="string|null"),
            ),
            tags=("schema", "identity", "read"),
        )
        query(
            "schema.identities.audit",
            self.identities_audit,
            summary="Audit normalized identities without changing the database.",
            tags=("schema", "identity", "maintenance", "read"),
        )
        command(
            "schema.identities.migrate",
            self.identities_migrate,
            summary="Install, backfill, and index normalized identities.",
            tags=("schema", "identity", "maintenance", "write"),
        )

        query(
            "tree.root",
            self.tree_root,
            summary="Return the root of the tree containing one row.",
            payload_fields=(
                _field("table", required=True, field_type="string"),
                _field("row_id", required=True, field_type="integer"),
            ),
            tags=("database", "tree", "read"),
        )
        query(
            "tree.children",
            self.tree_children,
            summary="Return immediate children of one tree row.",
            payload_fields=(
                _field("table", required=True, field_type="string"),
                _field("row_id", required=True, field_type="integer"),
            ),
            tags=("database", "tree", "read"),
        )
        query(
            "tree.lineage",
            self.tree_lineage,
            summary="Return the root-to-row lineage for one tree row.",
            payload_fields=(
                _field("table", required=True, field_type="string"),
                _field("row_id", required=True, field_type="integer"),
            ),
            tags=("database", "tree", "read"),
        )
        query(
            "tree.walk",
            self.tree_walk,
            summary="Walk every descendant rooted at one tree row.",
            payload_fields=(
                _field("table", required=True, field_type="string"),
                _field("row_id", required=True, field_type="integer"),
            ),
            tags=("database", "tree", "read"),
        )
        query(
            "tree.search",
            self.tree_search,
            summary="Return requested row IDs found beneath one tree row.",
            payload_fields=(
                _field("table", required=True, field_type="string"),
                _field("row_id", required=True, field_type="integer"),
                _field("row_ids", required=True, field_type="array"),
            ),
            tags=("database", "tree", "read"),
        )
        command(
            "tree.nest",
            self.tree_nest,
            summary="Move rows beneath one parent in a declared tree.",
            payload_fields=(
                _field("table", required=True, field_type="string"),
                _field("parent_id", required=True, field_type="integer"),
                _field("child_ids", required=True, field_type="array"),
            ),
            tags=("database", "tree", "write"),
        )
        command(
            "tree.delete",
            self.tree_delete,
            summary="Delete one tree and all descendants after explicit confirmation.",
            payload_fields=(
                _field("table", required=True, field_type="string"),
                _field("row_id", required=True, field_type="integer"),
                _field("confirm", required=True, field_type="boolean"),
            ),
            tags=("database", "tree", "write", "destructive"),
        )

    @staticmethod
    def identities_list(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        del query
        values = _method(
            runtime,
            "iter_normalized_identity_specs",
            area="normalized identities",
        )()
        return {"identities": [_plain(value) for value in values]}

    @staticmethod
    def identity_get(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        value = _method(
            runtime,
            "get_normalized_identity_spec",
            area="normalized identities",
        )(
            _required_text(payload, "table"),
            _required_text(payload, "value_column"),
        )
        return {"identity": _plain(value)}

    @staticmethod
    def identity_derive(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        if "value" not in payload:
            raise CoreDispatchError("`value` is required.")
        value = _method(
            runtime,
            "derive_identity_value",
            area="normalized identities",
        )(
            _required_text(payload, "table"),
            _required_text(payload, "value_column"),
            payload["value"],
        )
        return {"identity_value": _plain(value)}

    @staticmethod
    def identity_resolve(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        if "value" not in payload:
            raise CoreDispatchError("`value` is required.")
        scope = payload.get("scope_values")
        if scope is not None and not isinstance(scope, Mapping):
            raise CoreDispatchError("`scope_values` must be an object or null.")
        value = _method(
            runtime,
            "get_canonical_identity",
            area="normalized identities",
        )(
            _required_text(payload, "table"),
            _required_text(payload, "value_column"),
            payload["value"],
            scope_values=None if scope is None else dict(scope),
            id_column=(
                None
                if payload.get("id_column") is None
                else str(payload["id_column"])
            ),
        )
        return {"identity": _plain(value)}

    @staticmethod
    def identities_audit(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        del query
        report = _method(
            runtime,
            "audit_normalized_identities",
            area="normalized identities",
        )()
        return {"report": _plain(report)}

    @staticmethod
    def identities_migrate(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        del command
        report = _method(
            runtime,
            "migrate_normalized_identities",
            area="normalized identities",
        )()
        return runtime.services.reconcile(
            {"migrated": True, "report": _plain(report)}
        )

    @staticmethod
    def tree_root(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        table = _required_text(payload, "table")
        row_id = _required_int(payload, "row_id")
        id_column, _parent_column = _tree_columns(runtime, table)
        lineage = _tree_lineage_rows(runtime, table, row_id)
        return {
            "root": _tree_record(
                table=table,
                id_column=id_column,
                row=lineage[0],
            )
        }

    @staticmethod
    def tree_children(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        table = _required_text(payload, "table")
        row_id = _required_int(payload, "row_id")
        _tree_get(runtime, table, row_id)
        id_column, _parent_column = _tree_columns(runtime, table)
        return {
            "records": [
                _tree_record(
                    table=table,
                    id_column=id_column,
                    row=value,
                )
                for value in _tree_children_rows(
                    runtime,
                    table,
                    row_id,
                )
            ]
        }

    @staticmethod
    def tree_lineage(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        table = _required_text(payload, "table")
        id_column, _parent_column = _tree_columns(runtime, table)
        values = _tree_lineage_rows(
            runtime,
            table,
            _required_int(payload, "row_id"),
        )
        return {
            "records": [
                _tree_record(
                    table=table,
                    id_column=id_column,
                    row=value,
                )
                for value in values
            ]
        }

    @staticmethod
    def tree_walk(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        table = _required_text(payload, "table")
        id_column, _parent_column = _tree_columns(runtime, table)
        values = _tree_walk_rows(
            runtime,
            table,
            _required_int(payload, "row_id"),
        )
        return {
            "records": [
                _tree_record(
                    table=table,
                    id_column=id_column,
                    row=value,
                )
                for value in values
            ]
        }

    @staticmethod
    def tree_search(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        raw_ids = payload.get("row_ids")
        if not isinstance(raw_ids, Sequence) or isinstance(raw_ids, (str, bytes)):
            raise CoreDispatchError("`row_ids` must be an array.")
        ids = []
        for value in raw_ids:
            if isinstance(value, bool):
                raise CoreDispatchError("Every `row_ids` value must be an integer.")
            ids.append(int(str(value)))
        table = _required_text(payload, "table")
        found = {
            int(value[_tree_columns(runtime, table)[0]])
            for value in _tree_walk_rows(
                runtime,
                table,
                _required_int(payload, "row_id"),
            )
        }
        return {"row_ids": sorted(found.intersection(ids))}

    @staticmethod
    def tree_nest(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        table = _required_text(payload, "table")
        parent_id = _required_int(payload, "parent_id")
        _tree_get(runtime, table, parent_id)
        raw_ids = payload.get("child_ids")
        if not isinstance(raw_ids, Sequence) or isinstance(raw_ids, (str, bytes)):
            raise CoreDispatchError("`child_ids` must be an array.")
        child_ids: list[int] = []
        for value in raw_ids:
            if isinstance(value, bool):
                raise CoreDispatchError(
                    "Every `child_ids` value must be an integer."
                )
            child_id = int(str(value))
            _tree_get(runtime, table, child_id)
            if child_id == parent_id:
                raise CoreDispatchError(
                    "A tree row cannot be nested beneath itself.",
                    code="tree_cycle",
                )
            descendant_ids = {
                int(row[_tree_columns(runtime, table)[0]])
                for row in _tree_walk_rows(runtime, table, child_id)
            }
            if parent_id in descendant_ids:
                raise CoreDispatchError(
                    "Tree nesting would create a cycle.",
                    code="tree_cycle",
                    details={
                        "table": table,
                        "parent_id": parent_id,
                        "child_id": child_id,
                    },
                )
            child_ids.append(child_id)
        id_column, parent_column = _tree_columns(runtime, table)
        macros = _macros(runtime)
        for child_id in child_ids:
            macros.update_row(
                table,
                child_id,
                {parent_column: parent_id},
                id_column=id_column,
            )
        return runtime.services.reconcile(
            {
                "table": table,
                "parent_id": parent_id,
                "child_ids": child_ids,
                "nested": True,
            }
        )

    @staticmethod
    def tree_delete(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        if payload.get("confirm") is not True:
            raise CoreDispatchError(
                "`confirm` must be true to delete a tree.",
                code="confirmation_required",
            )
        table = _required_text(payload, "table")
        row_id = _required_int(payload, "row_id")
        id_column, _parent_column = _tree_columns(runtime, table)
        rows = _tree_walk_rows(runtime, table, row_id)
        record = _tree_record(
            table=table,
            id_column=id_column,
            row=rows[0],
        )
        macros = _macros(runtime)
        for value in reversed(rows):
            macros.delete_row(
                table,
                value[id_column],
                id_column=id_column,
            )
        return runtime.services.reconcile(
            {
                "deleted": True,
                "root": record,
                "deleted_count": len(rows),
            }
        )


def install_database_semantics_api(
    runtime: "CoreRuntime",
) -> CoreDatabaseSemanticsAPI:
    """Register normalized identity and tree operations."""

    api = CoreDatabaseSemanticsAPI()
    api.install(runtime)
    return api


__all__ = ["CoreDatabaseSemanticsAPI", "install_database_semantics_api"]
