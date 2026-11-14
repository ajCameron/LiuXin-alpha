"""Native custom-field operations with managed transactions and typed collections."""

from __future__ import annotations

import json
import unicodedata
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import replace
from typing import Any, cast

from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI
from LiuXin_alpha.databases.custom_columns.native_dependencies import (
    dependencies,
    validate_dependencies,
)
from LiuXin_alpha.databases.custom_columns.native_migrations import NativeMigrations
from LiuXin_alpha.databases.custom_columns.native_queries import NativeQueries
from LiuXin_alpha.databases.custom_columns.native_repository import (
    CustomFieldRepository,
    names,
    stem,
)
from LiuXin_alpha.databases.custom_columns.native_types import (
    CollectionMode,
    CustomFieldError,
    FieldDefinition,
    FieldPatch,
    FieldRef,
    OwnerRef,
    SeriesValue,
    UnsupportedStorage,
    ValueEntry,
    WriteReceipt,
)
from LiuXin_alpha.databases.custom_columns.native_values import (
    adapt,
    encode,
    entries,
    identity,
    validate_definition,
)
from LiuXin_alpha.databases.db_types.driver_types import SQLConnection
from LiuXin_alpha.databases.schema_readers import quote_identifier as q
from LiuXin_alpha.databases.schema_readers import rows
from LiuXin_alpha.databases.transaction_participants import after_commit
from LiuXin_alpha.databases.transactions import lock_postgres_writer


class CustomFieldService(NativeQueries, NativeMigrations):
    """Own LiuXin custom-field semantics over one database's managed SQL units.

    Definitions, typed values, revisions and schema plans belong here. Metadata
    projections, computed-field evaluation and Calibre calling conventions have
    separate owners. Construction does not inspect or mutate storage; each read
    uses a snapshot, and each write joins the caller's managed transaction or
    owns one. Instances carry receipt subscriptions, not a result cache."""

    def __init__(self, db: DatabaseAPI):
        """Bind the database and repository without opening a connection or creating tables.

        The database remains externally owned. Subscriptions belong to this service
        instance and do not observe writes made through another instance."""
        self.db = db
        self.repository = CustomFieldRepository(db)
        self._listeners: list[Callable[[WriteReceipt], None]] = []

    @contextmanager
    def read(self) -> Iterator[SQLConnection]:
        """Reuse an active macro unit or own a read-only, consistent snapshot.

        Nested reads see the caller's uncommitted values and never complete its
        transaction. An independently opened read handle is closed by macros on exit."""
        active = self.db.macros.current_transaction_connection()
        if active is not None:
            yield cast(SQLConnection, active)
        else:
            with self.db.macros.read_transaction() as connection:
                yield connection

    @contextmanager
    def write(self) -> Iterator[SQLConnection]:
        """Own or join a write unit without weakening native conflict checks.

        Standalone writes use a serialized outer transaction. Nested writes keep
        the caller's transaction and savepoint ownership, acquiring PostgreSQL's
        same cooperating writer lock before any native read. The lock lasts until
        the outer commit, or rollback of the savepoint that acquired it. Ordinary
        SQLite composition can fail with a busy/snapshot conflict on promotion;
        retry that outer transaction, never a write using the stale snapshot.

        The connection belongs to macros. Receipts publish after the outer commit,
        while a nested rollback discards only its writes and pending receipts.
        """
        nested = self.db.macros.current_transaction_connection() is not None
        with self.db.macros.transaction(serialized=not nested) as connection:
            if nested and self.repository.postgres:
                schema = getattr(self.db.driver, "schema", None)
                if not isinstance(schema, str):
                    raise CustomFieldError(
                        "PostgreSQL native writes require a configured schema"
                    )
                lock_postgres_writer(connection, schema)
            yield connection

    def subscribe(self, listener: Callable[[WriteReceipt], None]) -> Callable[[], None]:
        """Register an after-commit receipt listener and return its unsubscribe function.

        Listeners run in registration order for this service's writes. A listener
        failure is reported after SQL commits; retrying that write would duplicate its
        effects. Removing the same subscription twice raises ValueError."""
        self._listeners.append(listener)
        return lambda: self._listeners.remove(listener)

    def _publish(self, receipt: WriteReceipt) -> None:
        """Queue a native receipt on the current managed transaction's completion frame.

        Capture listeners now, defer delivery until the outer commit, and discard
        delivery when the containing savepoint rolls back. Listener failures cannot
        undo committed SQL and are reported by the completion owner."""
        listeners = tuple(self._listeners)

        def complete() -> None:
            """Deliver this receipt in order; propagate a listener failure after commit."""
            for listener in listeners:
                listener(receipt)

        after_commit(self.db.macros, complete)

    def list_fields(
        self, *, attachment: str | None = None, include_retired: bool = False
    ) -> tuple[FieldDefinition, ...]:
        """Return detached definitions, optionally scoped to one attachment table.

        Retired definitions are hidden unless explicitly included. Unsupported storage
        raises without installing or converting anything."""
        with self.read() as conn:
            return tuple(
                d
                for d in self.repository.definitions(conn, retired=include_retired)
                if attachment is None or d.attachment == attachment
            )

    def get_field(
        self, field: FieldRef, *, include_retired: bool = False
    ) -> FieldDefinition:
        """Resolve an integer field ID or (attachment, label) pair in one snapshot.

        Unknown and, by default, retired fields raise CustomFieldError. Returned
        definitions are snapshots; use migration plans to change persisted metadata."""
        with self.read() as conn:
            return self.repository.resolve(conn, field, retired=include_retired)

    def create_field(
        self,
        attachment: str,
        label: str,
        *,
        name: str | None = None,
        datatype: str = "text",
        mode: CollectionMode = "scalar",
        editable: bool = True,
        display: Mapping[str, Any] | None = None,
    ) -> FieldDefinition:
        """Validate and atomically create a definition and its native physical storage.

        Attachment is an explicit owner table, labels/names are scoped to it, and mode
        is scalar, ordered set or list. Composite fields are forced read-only; their
        evaluation belongs to Catalog. Return the definition and queue a receipt for
        the outer commit. This does not refresh metadata caches or install Calibre views."""
        if datatype == "composite":
            editable = False
        definition = FieldDefinition(
            0,
            attachment,
            label,
            name or label,
            datatype,
            mode,
            editable,
            dict(display or {}),
        )
        validate_definition(definition)
        with self.write() as conn:
            if not self.repository.ready(conn):
                raise UnsupportedStorage(
                    "Unsupported custom-field storage; v1 format support has been removed"
                )
            self.repository.owner_column(conn, attachment)
            self._validate_keys(conn, definition)
            validate_dependencies(definition, self.repository.definitions(conn))
            values = {
                "custom_column_in_table": attachment,
                "custom_column_label": label,
                "custom_column_label_norm": unicodedata.normalize(
                    "NFC", label
                ).casefold(),
                "custom_column_name": definition.name,
                "custom_column_name_norm": unicodedata.normalize("NFC", definition.name)
                .strip()
                .casefold(),
                "custom_column_datatype": datatype,
                "custom_column_is_multiple": int(mode != "scalar"),
                "custom_column_normalized": int(definition.normalized),
                "custom_column_editable": int(editable),
                "custom_column_display": json.dumps(definition.display),
                "custom_column_ordered": int(mode != "scalar"),
                "custom_column_collection_mode": mode,
                "custom_column_storage_version": 2,
            }
            definition = replace(
                definition, id=self.repository.insert(conn, "custom_columns", values)
            )
            self.repository.create_storage(conn, definition)
            self._publish(
                WriteReceipt((), (definition.id,), {definition.id: 0}),
            )
        return definition

    def _validate_keys(self, conn: SQLConnection, definition: FieldDefinition) -> None:
        """Reject an exact attachment/label collision, including retired definitions.

        Physical normalized-key constraints also enforce canonical label/name
        uniqueness. This check does not reserve Calibre synthetic companion names."""
        for other in self.repository.definitions(conn, retired=True):
            if (
                other.id != definition.id
                and other.attachment == definition.attachment
                and other.label == definition.label
            ):
                raise CustomFieldError(
                    "Field label is already registered on this attachment"
                )

    def get_values(
        self, owners: Iterable[OwnerRef], fields: Iterable[FieldRef]
    ) -> dict[OwnerRef, dict[int, Any]]:
        """Read a typed owner/field matrix from one snapshot, batching owner IDs by 400.

        Validate every supplied owner and field. Fields on other attachments are omitted
        from that owner's mapping; applicable unset values are None or an empty tuple.
        Composite values must be requested from Catalog's evaluator instead."""
        owners, fields = tuple(dict.fromkeys(owners)), tuple(fields)
        result: dict[OwnerRef, dict[int, Any]] = {owner: {} for owner in owners}
        with self.read() as conn:
            self.repository.require_owners(conn, owners)
            for reference in fields:
                definition = self.repository.resolve(conn, reference)
                if definition.datatype == "composite":
                    raise CustomFieldError(
                        "Composite evaluation belongs to the Catalog custom-field evaluator"
                    )
                applicable = [o for o in owners if o.table == definition.attachment]
                # Chunk bindings for SQLite and driver parameter limits; query count is per field/chunk.
                for start in range(0, len(applicable), 400):
                    group = applicable[start : start + 400]
                    found = self.repository.read_entries(
                        conn, definition, [o.id for o in group]
                    )
                    for owner in group:
                        values = found.get(owner.id, [])
                        result[owner][definition.id] = self._value(definition, values)
        return result

    @staticmethod
    def _value(definition: FieldDefinition, values: list[ValueEntry]) -> Any:
        """Shape stored entries as a scalar, SeriesValue or immutable entry tuple.

        Unset scalars return None. Multiple rows for a scalar are an integrity error,
        not an invitation to silently choose one; collection order is preserved."""
        if definition.mode != "scalar":
            return tuple(values)
        if not values:
            return None
        if len(values) != 1:
            raise CustomFieldError(
                "Scalar field has multiple stored entries; run audit"
            )
        return (
            SeriesValue(str(values[0].value), values[0].position or 0.0)
            if definition.datatype == "series"
            else values[0].value
        )

    def get_value(self, owner: OwnerRef, field: FieldRef) -> Any:
        """Read one value, rejecting an owner whose table differs from the field attachment."""
        with self.read():
            definition = self.get_field(field)
            if definition.attachment != owner.table:
                raise CustomFieldError("Field does not belong to this owner table")
            return self.get_values((owner,), (definition.id,))[owner][definition.id]

    def get_owner_values(self, owner: OwnerRef) -> dict[int, Any]:
        """Read every active stored field for an owner; omit Catalog-computed composites."""
        with self.read():
            fields = [
                d.id
                for d in self.list_fields(attachment=owner.table)
                if d.datatype != "composite"
            ]
            return self.get_values((owner,), fields)[owner]

    def apply_patch(self, patches: Iterable[FieldPatch]) -> WriteReceipt:
        """Validate then apply a mixed owner/field patch atomically.

        Reject missing owners, wrong attachments, read-only fields, stale expected
        revisions and duplicate owner/field targets before the first value mutation.
        Advance each affected field revision once, even for several owners, and return
        a receipt whose listeners run only after the owning transaction commits."""
        patches = tuple(patches)
        with self.write() as conn:
            prepared = []
            targets = set()
            for patch in patches:
                definition = self.repository.resolve(conn, patch.field)
                if definition.attachment != patch.owner.table:
                    raise CustomFieldError("Field does not belong to this owner table")
                self.repository.require_owner(conn, patch.owner)
                self.repository.require_native(conn, definition)
                if not definition.editable:
                    raise CustomFieldError("Field is read-only")
                if (
                    patch.expected_revision is not None
                    and patch.expected_revision != definition.revision
                ):
                    raise CustomFieldError("Field revision conflict")
                target = (patch.owner, definition.id)
                if target in targets:
                    raise CustomFieldError(
                        "A patch may target each owner/field only once"
                    )
                targets.add(target)
                prepared.append(
                    (patch.owner, definition, entries(patch.value, definition))
                )
            for owner, definition, values in prepared:
                self.repository.write_entries(conn, definition, owner.id, values)
            ids = tuple(sorted({d.id for _, d, _ in prepared}))
            revisions = {}
            for field_id in ids:
                conn.execute(
                    "UPDATE custom_columns SET custom_column_revision=custom_column_revision+1 WHERE custom_column_id=?",
                    (field_id,),
                ).close()
                revisions[field_id] = self.repository.resolve(conn, field_id).revision
            receipt = WriteReceipt(
                tuple(sorted({p.owner for p in patches})), ids, revisions
            )
            self._publish(receipt)
        return receipt

    def set_value(
        self,
        owner: OwnerRef,
        field: FieldRef,
        value: Any,
        *,
        expected_revision: int | None = None,
    ) -> WriteReceipt:
        """Replace one owner's value or collection, optionally checking the field revision.

        None clears the value. Raw replacement entries get new identities; supplied
        ValueEntry objects can retain their owned IDs. Revisions cover the entire field,
        so an edit to another owner can make expected_revision stale too."""
        return self.apply_patch((FieldPatch(owner, field, value, expected_revision),))

    def edit_entries(
        self,
        owner: OwnerRef,
        field: FieldRef,
        *,
        add: Iterable[Any] = (),
        update: Mapping[int, Any] | None = None,
        remove: Iterable[int] = (),
        order: Iterable[int] | None = None,
    ) -> WriteReceipt:
        """Edit an ordered collection while preserving the identities of retained entries.

        Updates/removals must name existing, disjoint entry IDs. An explicit order must
        list every retained ID exactly once; additions follow those retained entries.
        Set identity deduplication still applies. Read and replacement share one write
        reservation so an intervening native write cannot invalidate the edit."""
        with self.write() as conn:
            definition = self.repository.resolve(conn, field)
            if definition.mode == "scalar":
                raise CustomFieldError("Entry editing requires a collection")
            original = self.repository.read_entries(conn, definition, [owner.id]).get(
                owner.id, []
            )
            by_id = {entry.id: entry for entry in original}
            updates, removed = dict(update or {}), set(remove)
            if not (set(updates) | removed) <= by_id.keys() or set(updates) & removed:
                raise CustomFieldError(
                    "Entry edits conflict or reference missing entries"
                )
            retained = []
            for item in original:
                if item.id in removed:
                    continue
                if item.id in updates:
                    value = updates[item.id]
                    if isinstance(value, SeriesValue):
                        item = replace(item, value=value.value, position=value.position)
                    else:
                        item = replace(item, value=value)
                retained.append(item)
            if order is not None:
                requested = tuple(order)
                if len(requested) != len(retained) or set(requested) != {
                    e.id for e in retained
                }:
                    raise CustomFieldError(
                        "Reordering requires every retained entry exactly once"
                    )
                lookup = {e.id: e for e in retained}
                retained = [lookup[i] for i in requested]
            return self.set_value(owner, definition.id, [*retained, *add])

    def retire_field(self, field: FieldRef) -> WriteReceipt:
        """Retire a native field without discarding its stored values."""
        with self.write() as conn:
            definition = self.repository.resolve(conn, field)
            self.repository.require_native(conn, definition)
            self._require_no_dependents(conn, definition)
            conn.execute(
                "UPDATE custom_columns SET custom_column_mark_for_delete=1, custom_column_revision=custom_column_revision+1 WHERE custom_column_id=?",
                (definition.id,),
            ).close()
            receipt = WriteReceipt(
                (), (definition.id,), {definition.id: definition.revision + 1}
            )
            self._publish(receipt)
            return receipt

    def _require_no_dependents(
        self, conn: SQLConnection, definition: FieldDefinition
    ) -> None:
        """Reject a definition change still referenced by an active composite field."""
        for other in self.repository.definitions(conn):
            if other.id != definition.id and definition.id in dependencies(
                other, self.repository.definitions(conn)
            ):
                raise CustomFieldError(
                    f"Composite {other.key} depends on this field; update its definition first"
                )

    def ensure_value(self, field: FieldRef, value: Any) -> int:
        """Ensure an inventory identity without assigning it to an owner."""
        with self.write() as conn:
            definition = self.repository.resolve(conn, field)
            if not definition.normalized or not definition.editable:
                raise CustomFieldError(
                    "Inventory values require an editable normalized field"
                )
            value = adapt(value, definition)
            if value is None:
                raise CustomFieldError("Inventory values cannot be unset")
            key = identity(value, definition)
            table, _ = names(definition)
            base = stem(table)
            found = rows(
                conn,
                f"SELECT {q(base + '_id')} FROM {q(table)} WHERE {q(base + '_key')}=?",
                (key,),
            )
            if found:
                return int(found[0][0])
            value_id = self.repository.insert(
                conn, table, {base + "_value": encode(value), base + "_key": key}
            )
            self._update_definition(
                conn, replace(definition, revision=definition.revision + 1)
            )
            self._publish(
                WriteReceipt(
                    (), (definition.id,), {definition.id: definition.revision + 1}
                ),
            )
            return value_id

    def rename_value(
        self, field: FieldRef, value_id: int, new_value: Any
    ) -> WriteReceipt:
        """Rename a shared value, or merge memberships into an existing identity."""
        with self.write() as conn:
            definition = self.repository.resolve(conn, field)
            self.repository.require_native(conn, definition)
            if not definition.normalized or not definition.editable:
                raise CustomFieldError(
                    "Shared renaming requires an editable normalized field"
                )
            table, _ = names(definition)
            ts = stem(table)
            old = rows(
                conn,
                f"SELECT {q(ts + '_value')} FROM {q(table)} WHERE {q(ts + '_id')}=?",
                (value_id,),
            )
            if not old:
                raise CustomFieldError("Unknown inventory value")
            stored = self.repository.read_entries(conn, definition)
            if definition.datatype == "enumeration":
                display = dict(definition.display)
                choices = [v for v in display["enum_values"] if v != old[0][0]]
                if new_value not in choices:
                    choices.append(new_value)
                display["enum_values"] = choices
                definition = replace(definition, display=display)
                validate_definition(definition)
            new_value = adapt(new_value, definition)
            key = identity(new_value, definition)
            target = rows(
                conn,
                f"SELECT {q(ts + '_id')} FROM {q(table)} WHERE {q(ts + '_key')}=?",
                (key,),
            )
            owners = tuple(
                OwnerRef(definition.attachment, owner)
                for owner, values in stored.items()
                if any(v.value_id == value_id for v in values)
            )
            if target and target[0][0] != value_id:
                for owner in owners:
                    revised = [
                        replace(v, value=new_value) if v.value_id == value_id else v
                        for v in stored[owner.id]
                    ]
                    prepared = entries(
                        revised if definition.mode != "scalar" else revised[0],
                        definition,
                    )
                    self.repository.write_entries(conn, definition, owner.id, prepared)
                conn.execute(
                    f"DELETE FROM {q(table)} WHERE {q(ts + '_id')}=?", (value_id,)
                ).close()
            else:
                conn.execute(
                    f"UPDATE {q(table)} SET {q(ts + '_value')}=?, {q(ts + '_key')}=? WHERE {q(ts + '_id')}=?",
                    (encode(new_value), key, value_id),
                ).close()
            definition = replace(definition, revision=definition.revision + 1)
            self._update_definition(conn, definition)
            receipt = WriteReceipt(
                owners, (definition.id,), {definition.id: definition.revision}
            )
            self._publish(receipt)
            return receipt

    def delete_value(self, field: FieldRef, value_id: int) -> WriteReceipt:
        """Remove a shared value and its memberships in one managed transaction."""
        with self.write() as conn:
            definition = self.repository.resolve(conn, field)
            self.repository.require_native(conn, definition)
            if not definition.normalized or not definition.editable:
                raise CustomFieldError(
                    "Shared deletion requires an editable normalized field"
                )
            table, _ = names(definition)
            if not rows(
                conn,
                f"SELECT 1 FROM {q(table)} WHERE {q(stem(table) + '_id')}=?",
                (value_id,),
            ):
                raise CustomFieldError("Unknown inventory value")
            owners = []
            for owner, values in self.repository.read_entries(conn, definition).items():
                retained = [v for v in values if v.value_id != value_id]
                if len(retained) != len(values):
                    owners.append(OwnerRef(definition.attachment, owner))
                    self.repository.write_entries(
                        conn,
                        definition,
                        owner,
                        [(v.value, v.position, v.id) for v in retained],
                    )
            conn.execute(
                f"DELETE FROM {q(table)} WHERE {q(stem(table) + '_id')}=?", (value_id,)
            ).close()
            definition = replace(definition, revision=definition.revision + 1)
            self._update_definition(conn, definition)
            receipt = WriteReceipt(
                tuple(owners), (definition.id,), {definition.id: definition.revision}
            )
            self._publish(receipt)
            return receipt
