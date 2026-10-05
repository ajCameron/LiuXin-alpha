"""
Expose normalized database identities and parent-linked tree operations through named Core routes.

Identity normalization delegates to database policy. Tree helpers use schema
headings and portable macros, with root-to-row lineage and breadth-first subtree
reads. Metadata declares request shapes but is not a validator or authorization
layer. Writes perform sequential macro calls followed by cache reconciliation;
this adapter adds no transaction or rollback if a later write or report fails.
"""

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
    """
    Build a payload-field declaration for endpoint introspection without validating request data.

    Example:
        >>> _field("row_id", required=True, field_type="integer").required
        True


    :param name: Public request key described by the declaration.
    :param required: Whether introspection advertises the key as mandatory.
    :param field_type: Optional transport-facing type label, not an executable validator.
    :return: New field-description record containing the supplied values.
    """
    return CorePayloadFieldDescription(
        name=name,
        required=required,
        field_type=field_type,
    )


def _payload(envelope: Any) -> dict[str, Any]:
    """
    Shallow-copy an envelope's Mapping payload, treating a missing or None payload as empty.

    Attribute access and Mapping iteration errors propagate without translation.

    Example:
        >>> from types import SimpleNamespace
        >>> _payload(SimpleNamespace(payload={"table": "series"}))
        {'table': 'series'}


    :param envelope: Object whose optional payload attribute supplies request data.
    :return: New dictionary preserving keys and references to nested values.
    :raises CoreDispatchError: If a non-None payload is not a Mapping.
    """
    raw = getattr(envelope, "payload", None)
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        raise CoreDispatchError("Core payload must be an object.")
    return dict(raw)


def _required_text(payload: Mapping[str, Any], name: str) -> str:
    """
    Stringify and strip a required payload value, rejecting missing or whitespace-only text.

    Explicit None becomes "None" and other non-string values are accepted through str.

    Example:
        >>> _required_text({"table": " series "}, "table")
        'series'


    :param payload: Request mapping containing the requested key.
    :param name: Exact key to look up and name in a missing-value error.
    :return: Nonempty stripped text, without table/column existence validation.
    :raises CoreDispatchError: If text conversion produces an empty string.
    """
    value = str(payload.get(name, "")).strip()
    if not value:
        raise CoreDispatchError("`{}` is required.".format(name))
    return value


def _required_int(payload: Mapping[str, Any], name: str) -> int:
    """
    Convert a required non-None, non-boolean value with int, without imposing a numeric range.

    Fractional numbers may truncate. TypeError/ValueError are wrapped, while
    OverflowError or other custom conversion failures propagate unchanged.

    Example:
        >>> _required_int({"row_id": "12"}, "row_id")
        12


    :param payload: Request mapping supplying an integer-convertible value.
    :param name: Exact required key, also used in the validation message.
    :return: Converted integer, including zero or negative values.
    :raises CoreDispatchError: If the value is missing, None, bool, or fails conversion with TypeError/ValueError.
    """
    value = payload.get(name)
    if value is None or isinstance(value, bool):
        raise CoreDispatchError("`{}` must be an integer.".format(name))
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise CoreDispatchError("`{}` must be an integer.".format(name)) from exc


def _plain(value: Any) -> Any:
    """
    Recursively project identity reports into basic containers, stringifying otherwise unsupported objects.

    Scalars and bytes pass through. Mappings stringify keys, followed by dataclass
    fields, row_dict mappings, sequences, then other iterables. Key collisions after
    conversion overwrite earlier values. Iterables are fully consumed; no cycle,
    depth, or size guard exists. Bytes still need outer Core wire encoding.

    Example:
        >>> _plain({1: ("tag", 2), "raw": b"x"})
        {'1': ['tag', 2], 'raw': b'x'}


    :param value: Scalar, mapping, row-like value, dataclass instance, iterable, or fallback object.
    :return: Recursively projected value, with unsupported leaves converted using str.
    """
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
    """
    Require a callable directly on runtime.database without a driver-wrapper fallback.

    Lookup may execute a descriptor; such errors propagate. Callability does not
    establish signature compatibility or authorize the eventual operation.

    Example:
        >>> from types import SimpleNamespace
        >>> _method(SimpleNamespace(database={}), "get", area="mapping")("missing", 3)
        3


    :param runtime: Runtime exposing the database object to inspect.
    :param name: Exact database attribute name required by the handler.
    :param area: Human-readable capability area included in missing-operation error details.
    :return: The bound callable itself, without invoking it.
    :raises CoreDispatchError: With capability_unavailable if the attribute is absent or not callable.
    """
    value = getattr(runtime.database, name, None)
    if not callable(value):
        raise CoreDispatchError(
            "{} does not support `{}`.".format(area, name),
            code="capability_unavailable",
            details={"area": area, "operation": name},
        )
    return value


def _macros(runtime: "CoreRuntime") -> Any:
    """
    Require the complete portable tree macro surface, including mutation methods even for a read.

    The four required callables are get_row, get_rows, update_row, and delete_row.
    No method is invoked and signature compatibility is not checked here.

    Example:
        >>> from types import SimpleNamespace
        >>> from unittest.mock import Mock
        >>> macros = Mock()
        >>> _macros(SimpleNamespace(database=SimpleNamespace(macros=macros))) is macros
        True


    :param runtime: Runtime whose database may expose portable macros.
    :return: Existing macro owner after all four callable checks pass.
    :raises CoreDispatchError: With capability_unavailable if macros or any required callable is unavailable.
    """
    value = getattr(runtime.database, "macros", None)
    required = ("get_row", "get_rows", "update_row", "delete_row")
    if value is None or any(
        not callable(getattr(value, name, None)) for name in required
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
    """
    Resolve ID and parent-column names using available schema methods and naming heuristics.

    Headings come from the database first, then its wrapper only if the direct
    method is not callable. A wrapper get_id_column result is trusted; otherwise
    the shortest id/*_id heading wins, with set-order ties. Parent lookup prefers
    singularized-table, full-table, then plain parent_id, followed by the first
    *_parent_id heading in set order. Ambiguous fallbacks are not schema guarantees.

    Example:
        >>> from types import SimpleNamespace
        >>> from unittest.mock import Mock
        >>> database = SimpleNamespace(get_column_headings=Mock(return_value=["id", "parent_id"]))
        >>> _tree_columns(SimpleNamespace(database=database), "series")
        ('id', 'parent_id')


    :param runtime: Runtime with database or driver-wrapper heading introspection.
    :param table: Table name forwarded to introspection and used for parent-column heuristics.
    :return: ID-column and parent-column text; neither relationship nor primary-key semantics are independently verified.
    :raises CoreDispatchError: If headings capability, an ID candidate, or a parent column is unavailable.
    """
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
    headings = {str(value) for value in cast(Iterable[object], headings_method(table))}
    wrapper = getattr(runtime.database, "driver_wrapper", None)
    id_method = getattr(wrapper, "get_id_column", None)
    if callable(id_method):
        id_column = str(id_method(table))
    else:
        id_candidates = sorted(
            (value for value in headings if value == "id" or value.endswith("_id")),
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
    """
    Read one row from a parent-linked table through portable macros, requiring it to exist.

    Tree schema and the full macro surface are checked even though this call only
    reads one row. The returned row's ID is not independently compared with row_id.

    Example:
        >>> row = _tree_get(runtime, "series", 7)  # doctest: +SKIP


    :param runtime: Runtime providing tree introspection and portable row access.
    :param table: Parent-linked table whose row should be fetched.
    :param row_id: Row identity passed directly to get_row without further conversion.
    :return: Shallow dictionary copy of the fetched row.
    :raises CoreDispatchError: With row_not_found if get_row returns None; capability/schema failures also propagate.
    """
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
    """
    Wrap a tree row with table/identity labels while retaining every raw column in a shallow values copy.

    Example:
        >>> _tree_record(table="series", id_column="id", row={"id": 7, "parent_id": None})
        {'table': 'series', 'row_id': 7, 'values': {'id': 7, 'parent_id': None}}


    :param table: Table label placed in the record without validation.
    :param id_column: Row key supplying the exposed identity; an absent key produces row_id=None in the result.
    :param row: Mapping containing raw row fields, including the identity column.
    :return: New table/row_id/values dictionary; nested values are not serialized or copied deeply.
    """
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
    """
    Read immediate children ordered by the resolved ID column, without checking that the parent row exists.

    Example:
        >>> rows = _tree_children_rows(runtime, "series", 7)  # doctest: +SKIP


    :param runtime: Runtime providing tree-column introspection and portable get_rows.
    :param table: Parent-linked table to query.
    :param row_id: Parent identity used in the parent-column equality filter.
    :return: Shallow row dictionaries in the macro's requested ID ordering, fully materialized.
    """
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
    """
    Follow parent links to None and return the root-to-requested-row path, rejecting repeated identities.

    Each step reads fresh schema/row state; there is no snapshot, depth limit, or
    special root sentinel other than None. Non-None parent values are int-converted.
    The final row's raw ID must equal the original row_id after reversing the path.

    Example:
        >>> rows = _tree_lineage_rows(runtime, "series", 7)  # doctest: +SKIP


    :param runtime: Runtime providing schema and row access for every parent hop.
    :param table: Parent-linked table traversed without crossing to another table.
    :param row_id: Starting row identity, included as the final returned row.
    :return: Fully materialized path in root-first order, including both endpoints.
    :raises CoreDispatchError: For cycles, missing rows/capabilities, or a final identity mismatch.
    """
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
    """
    Materialize a breadth-first subtree including its root, rejecting any repeated integer row identity.

    Children are queued in the macro's ID order. No row/depth budget or snapshot
    isolation is added; repeats are reported as tree_cycle even if caused by
    duplicate input rows or concurrent changes rather than a literal parent cycle.

    Example:
        >>> rows = _tree_walk_rows(runtime, "series", 7)  # doctest: +SKIP


    :param runtime: Runtime providing tree introspection and root/child reads.
    :param table: Parent-linked table to traverse.
    :param row_id: Required root identity whose descendants are included.
    :return: Root-first breadth-first list of shallow row dictionaries.
    :raises CoreDispatchError: For repeated identities or unavailable rows/schema/macros.
    """
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
    """
    Register normalized-identity and parent-linked tree handlers without owning database state.

    Direct handler calls bypass the runtime's dispatch lock, envelope conversion,
    and lifecycle events. Tree mutation validation precedes sequential writes,
    but this adapter does not make those reads/writes atomic.

    Example:
        >>> adapter = CoreDatabaseSemanticsAPI()
        >>> adapter.install(runtime)  # doctest: +SKIP
    """

    def install(self, runtime: "CoreRuntime") -> None:
        """
        Register ten identity/tree queries and three mutation commands with explicit metadata.

        Identity routes are registered first, including migration, followed by
        tree routes. A later failure leaves earlier registrations in place;
        duplicate-name policy belongs to the supplied runtime.

        Example:
            >>> CoreDatabaseSemanticsAPI().install(runtime)  # doctest: +SKIP


        :param runtime: Runtime receiving handlers, summaries, payload-field descriptions, and tags.
        :return: None after all registrations finish; no identity migration or tree operation runs here.
        """
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
        """
        Enumerate the database's normalized-identity declarations and project them into basic containers.

        Example:
            >>> result = CoreDatabaseSemanticsAPI.identities_list(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime whose database provides iter_normalized_identity_specs.
        :param query: Query envelope, ignored; no payload fields are consumed or validated.
        :return: identities list in database iteration order, with each declaration passed through _plain.
        :raises CoreDispatchError: If the database lacks the required callable.
        """
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
        """
        Look up one normalized-identity declaration by text-normalized table and value-column names.

        Missing-result behavior belongs to the database: None is projected as None,
        while raised lookup errors propagate to the outer dispatch boundary.

        Example:
            >>> result = CoreDatabaseSemanticsAPI.identity_get(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime whose database provides get_normalized_identity_spec.
        :param query: Query payload containing required table and value_column values converted to stripped text.
        :return: Dictionary with identity containing the projected database result.
        """
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
        """
        Derive an identity using the database's policy for the requested table/value column.

        The value key must be present but may be None; no normalization policy or
        row lookup is implemented independently by this handler.

        Example:
            >>> result = CoreDatabaseSemanticsAPI.identity_derive(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime whose database provides derive_identity_value.
        :param query: Query with required table, value_column, and value; the value is forwarded unchanged.
        :return: identity_value containing the projected derivation result, without storing a row.
        :raises CoreDispatchError: If value is absent or payload/names/capability validation fails.
        """
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
        """
        Resolve a display value through the database's canonical-identity lookup with optional scope and ID column.

        Scope is shallow-copied when present. id_column is stringified but not
        stripped, unlike the required table/value_column names. Canonical matching,
        ambiguity, and missing-result semantics remain database-owned.

        Example:
            >>> result = CoreDatabaseSemanticsAPI.identity_resolve(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime whose database provides get_canonical_identity.
        :param query: Query with table, value_column, value, optional Mapping/null scope_values, and optional id_column.
        :return: identity containing the projected canonical-lookup result.
        :raises CoreDispatchError: If value is absent, scope is not Mapping/null, or required names/capability are unavailable.
        """
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
                None if payload.get("id_column") is None else str(payload["id_column"])
            ),
        )
        return {"identity": _plain(value)}

    @staticmethod
    def identities_audit(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        """
        Request the database's normalized-identity audit and project its report without invoking migration.

        Example:
            >>> report = CoreDatabaseSemanticsAPI.identities_audit(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime whose database supplies audit_normalized_identities.
        :param query: Query envelope, ignored; auditing options are not forwarded.
        :return: report containing the projected audit result; audit scope and diagnostics belong to the database.
        """
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
        """
        Invoke normalized-identity migration and reconcile the service cache after projecting its report.

        The migrated flag means the migration call returned, not that every report
        item succeeded or data changed. Projection/reconciliation can fail after
        database effects; this handler supplies no rollback or extra confirmation.

        Example:
            >>> receipt = CoreDatabaseSemanticsAPI.identities_migrate(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime providing migrate_normalized_identities and service reconciliation.
        :param command: Command envelope, ignored; no payload options or confirmation flag are read.
        :return: Reconciled migrated=True receipt with the database's projected report.
        """
        del command
        report = _method(
            runtime,
            "migrate_normalized_identities",
            area="normalized identities",
        )()
        return runtime.services.reconcile({"migrated": True, "report": _plain(report)})

    @staticmethod
    def tree_root(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        """
        Follow a row's full parent lineage and return the root as a labeled tree record.

        Example:
            >>> result = CoreDatabaseSemanticsAPI.tree_root(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime providing parent-linked schema and portable tree macros.
        :param query: Query with required table text and integer-convertible row_id, excluding None/bool.
        :return: root record containing table, raw row identity, and a shallow values dictionary.
        :raises CoreDispatchError: If the row/ancestors are missing, lineage cycles, or tree capability is unavailable.
        """
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
        """
        Require the parent row to exist, then return its immediate children in ID order.

        This handler does not inspect deeper descendants or validate the parent's
        own lineage for cycles.

        Example:
            >>> result = CoreDatabaseSemanticsAPI.tree_children(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime providing parent-linked schema and portable tree macros.
        :param query: Query with required table and integer-convertible row_id identifying the parent.
        :return: records list of immediate child wrappers; an existing leaf returns an empty list.
        :raises CoreDispatchError: If the parent is missing or request/tree capability validation fails.
        """
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
        """
        Return labeled rows on the complete root-to-requested-row path, including both endpoints.

        Example:
            >>> result = CoreDatabaseSemanticsAPI.tree_lineage(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime providing parent-linked schema and portable tree macros.
        :param query: Query with required table and integer-convertible row_id whose ancestors are followed.
        :return: Root-first records list, each with table, row_id, and raw values.
        :raises CoreDispatchError: For missing rows, cycles, identity mismatch, or unavailable tree support.
        """
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
        """
        Return the root and all descendants in breadth-first order, without a result-size or depth limit.

        Siblings follow macro ID ordering. Reads are not enclosed in a snapshot
        transaction, and repeated integer identities are reported as cycles.

        Example:
            >>> result = CoreDatabaseSemanticsAPI.tree_walk(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime providing tree-schema introspection and portable root/child reads.
        :param query: Query with required table and integer-convertible root row_id.
        :return: Fully materialized records list including the requested root first.
        :raises CoreDispatchError: If traversal finds a repeated identity or required row/schema/capability is absent.
        """
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
        """
        Walk the entire subtree and return sorted unique requested IDs present in it, including the root.

        row_ids must be a non-string Sequence; sets/generators are rejected. Each
        non-boolean element is converted by int(str(value)), so float 1.0 is not
        accepted as it would be by the separate root-ID parser. Conversion failures
        propagate, and even an empty requested sequence still triggers traversal.

        Example:
            >>> result = CoreDatabaseSemanticsAPI.tree_search(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime providing tree introspection and complete subtree traversal.
        :param query: Query with table, root row_id, and required row_ids Sequence of integer-text-convertible values.
        :return: row_ids list sorted numerically and deduplicated, not preserving request order.
        """
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
        """
        Validate requested parent/children and cycle safety, then set every listed child's parent and reconcile.

        All validation precedes writes, but no lock/transaction spans those reads
        and macro updates here. IDs are converted via int(str(value)); booleans are
        rejected and duplicates retained. An empty sequence still yields a nested
        receipt and reconciliation. Later update/report failures do not roll back
        earlier updates, and update return values are ignored.

        Example:
            >>> receipt = CoreDatabaseSemanticsAPI.tree_nest(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime providing tree reads, update_row macros, and service reconciliation.
        :param command: Command with table, required parent_id, and non-string Sequence child_ids.
        :return: Reconciled receipt with table, parent_id, normalized child_ids in input order, and nested=True.
        :raises CoreDispatchError: For missing rows, malformed payloads, self-parenting, or parenting beneath a descendant.
        """
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
                raise CoreDispatchError("Every `child_ids` value must be an integer.")
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
        """
        Require literal True confirmation, read the subtree, then delete it in reverse breadth-first order and reconcile.

        Descendants precede ancestors; peers are reversed relative to the walk.
        deleted_count counts planned row deletions, not checked macro return values.
        No transaction or retry compensation is added, so a later failure can leave
        prior rows deleted and newly concurrent descendants outside the read snapshot.

        Example:
            >>> receipt = CoreDatabaseSemanticsAPI.tree_delete(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime providing tree traversal, delete_row macros, and service reconciliation.
        :param command: Command with confirm=True, required table, and integer-convertible root row_id.
        :return: Reconciled deleted=True receipt with the former root record and number of planned row deletions.
        :raises CoreDispatchError: If confirmation is not the boolean True or tree/request validation fails.
        """
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
    """
    Construct a stateless database-semantics adapter, register its routes, and return it.

    Earlier registrations remain if installation fails; no database/resource
    ownership is transferred to the adapter.

    Example:
        >>> from unittest.mock import Mock
        >>> runtime = Mock()
        >>> isinstance(install_database_semantics_api(runtime), CoreDatabaseSemanticsAPI)
        True
        >>> runtime.register_query_handler.call_count, runtime.register_command_handler.call_count
        (10, 3)


    :param runtime: Runtime registrar receiving normalized-identity and tree handlers.
    :return: Newly constructed adapter after all registrations succeed.
    """

    api = CoreDatabaseSemanticsAPI()
    api.install(runtime)
    return api


__all__ = ["CoreDatabaseSemanticsAPI", "install_database_semantics_api"]
