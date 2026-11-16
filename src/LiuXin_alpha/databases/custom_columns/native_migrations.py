"""Explicit fingerprinted field migrations and integrity repair plans."""

from __future__ import annotations

import json
import unicodedata
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import replace
from typing import TYPE_CHECKING, Any

from LiuXin_alpha.databases.custom_columns.native_dependencies import (
    validate_dependencies,
)
from LiuXin_alpha.databases.custom_columns.native_repository import (
    names,
    stem,
)
from LiuXin_alpha.databases.custom_columns.native_types import (
    ChangePlan,
    CustomFieldError,
    FieldDefinition,
    FieldRef,
    Issue,
    OwnerRef,
    StalePlan,
    ValueEntry,
    WriteReceipt,
)
from LiuXin_alpha.databases.custom_columns.native_values import (
    adapt,
    entries,
    identity,
    validate_definition,
)
from LiuXin_alpha.databases.schema_readers import quote_identifier as q
from LiuXin_alpha.databases.schema_readers import rows

if TYPE_CHECKING:
    from LiuXin_alpha.databases.custom_columns.native_api import CustomFieldHost
    from LiuXin_alpha.databases.db_types.driver_types import SQLConnection


class NativeMigrations:
    """Preview, validate and apply explicit field/schema changes and integrity repairs."""

    def audit(self: CustomFieldHost) -> tuple[Issue, ...]:
        """Inspect active and retired fields without repairing or rewriting storage.

        Return located metadata, value, cardinality, identity, ownership, ordering and
        SQLite-trigger issues. Expected malformed records become Issue objects;
        unexpected connection/SQL failures still propagate. This can scan full fields."""
        issues: list[Issue] = []
        with self.read() as conn:
            for definition in self.repository.definitions(
                conn, retired=True, issues=issues
            ):
                try:
                    validate_definition(definition)
                    self.repository.owner_column(conn, definition.attachment)
                except (CustomFieldError, ValueError, TypeError) as error:
                    issues.append(
                        Issue("invalid_definition", definition.id, str(error))
                    )
                    continue
                table, link = names(definition)
                storage = link if definition.normalized else table
                ss = stem(storage)
                if not all(
                    self.repository.table_exists(conn, name)
                    for name in ([table, link] if definition.normalized else [table])
                ):
                    issues.append(
                        Issue(
                            "missing_storage",
                            definition.id,
                            "Backing storage is missing",
                        )
                    )
                    continue
                required = {ss + suffix for suffix in ("_id", "_book", "_value")}
                required.add(ss + "_ordinal")
                if definition.datatype == "series":
                    required.add(ss + "_extra")
                if not required <= self.repository.columns(conn, storage):
                    issues.append(
                        Issue(
                            "missing_columns",
                            definition.id,
                            "Backing storage has missing columns",
                        )
                    )
                    continue
                if definition.normalized:
                    ts = stem(table)
                    if not {ts + "_id", ts + "_value"} <= self.repository.columns(
                        conn, table
                    ):
                        issues.append(
                            Issue(
                                "missing_columns",
                                definition.id,
                                "Vocabulary has missing columns",
                            )
                        )
                        continue
                    seen = set()
                    for value_id, value in rows(
                        conn,
                        f"SELECT {q(ts + '_id')},{q(ts + '_value')} FROM {q(table)}",
                    ):
                        try:
                            from LiuXin_alpha.databases.custom_columns.native_values import (
                                decode,
                            )

                            key = identity(decode(value, definition), definition)
                            if key in seen:
                                issues.append(
                                    Issue(
                                        "duplicate_identity",
                                        definition.id,
                                        f"Vocabulary value {value_id} collides after normalization",
                                    )
                                )
                            seen.add(key)
                        except (ValueError, TypeError) as error:
                            issues.append(
                                Issue("invalid_value", definition.id, str(error))
                            )
                from LiuXin_alpha.databases.custom_columns.native_values import (
                    decode,
                )

                ts = stem(table)
                required_keys = {ts + "_key"}
                if not required_keys <= self.repository.columns(conn, table):
                    issues.append(
                        Issue(
                            "missing_columns",
                            definition.id,
                            "Value identity key is missing",
                        )
                    )
                    continue
                for value_id, value, stored_key in rows(
                    conn,
                    f"SELECT {q(ts + '_id')},{q(ts + '_value')},{q(ts + '_key')} FROM {q(table)}",
                ):
                    try:
                        if stored_key != identity(
                            decode(value, definition), definition
                        ):
                            issues.append(
                                Issue(
                                    "identity_key",
                                    definition.id,
                                    f"Value {value_id} has a stale identity key",
                                )
                            )
                    except (ValueError, TypeError) as error:
                        issues.append(Issue("invalid_value", definition.id, str(error)))
                owner_id = self.repository.owner_column(conn, definition.attachment)
                for row in rows(
                    conn,
                    f"SELECT {q(ss + '_id')}, {q(ss + '_book')} FROM {q(storage)} e WHERE NOT EXISTS (SELECT 1 FROM {q(definition.attachment)} o WHERE o.{q(owner_id)}=e.{q(ss + '_book')})",
                ):
                    issues.append(
                        Issue(
                            "orphan_owner",
                            definition.id,
                            "Entry references a missing owner",
                            row[1],
                            row[0],
                        )
                    )
                if definition.normalized:
                    ts = stem(table)
                    for row in rows(
                        conn,
                        f"SELECT {q(ss + '_id')}, {q(ss + '_book')} FROM {q(storage)} e WHERE NOT EXISTS (SELECT 1 FROM {q(table)} v WHERE v.{q(ts + '_id')}=e.{q(ss + '_value')})",
                    ):
                        issues.append(
                            Issue(
                                "orphan_value",
                                definition.id,
                                "Entry references a missing value",
                                row[1],
                                row[0],
                            )
                        )
                try:
                    stored = self.repository.read_entries(conn, definition)
                    for owner, values in stored.items():
                        if definition.datatype == "series":
                            entries(
                                values if definition.mode != "scalar" else values[0],
                                definition,
                            )
                        if definition.mode == "scalar" and len(values) > 1:
                            issues.append(
                                Issue(
                                    "cardinality",
                                    definition.id,
                                    "Scalar has several entries",
                                    owner,
                                )
                            )
                        keys = [identity(entry.value, definition) for entry in values]
                        if definition.mode == "set" and len(set(keys)) != len(keys):
                            issues.append(
                                Issue(
                                    "duplicate_identity",
                                    definition.id,
                                    "Set contains duplicate identities",
                                    owner,
                                )
                            )
                        if [v.ordinal for v in values] != list(range(len(values))):
                            issues.append(
                                Issue(
                                    "ordering",
                                    definition.id,
                                    "Entry ordering is not contiguous",
                                    owner,
                                )
                            )
                except (ValueError, TypeError) as error:
                    issues.append(Issue("invalid_value", definition.id, str(error)))
                if not self.repository.postgres:
                    for name, expected in self.repository.cleanup_triggers(
                        conn, definition
                    ).items():
                        installed = rows(
                            conn,
                            "SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?",
                            (name,),
                        )
                        if not installed or installed[0][0] != expected:
                            issues.append(
                                Issue(
                                    "cleanup_trigger",
                                    definition.id,
                                    f"Cleanup/reference trigger {name} is missing or stale",
                                )
                            )
        return tuple(issues)

    def _check_plan(
        self: CustomFieldHost, conn: SQLConnection, plan: ChangePlan, kind: str
    ) -> None:
        """Reject a wrong plan kind, changed source fingerprint or unresolved blockers."""
        if plan.kind != kind or self.repository.fingerprint(conn) != plan.fingerprint:
            raise StalePlan("Plan kind or source fingerprint no longer matches")
        if plan.issues:
            raise CustomFieldError(
                "Resolve plan issues before application: "
                + ", ".join(i.code for i in plan.issues)
            )

    def _rebuild(
        self: CustomFieldHost,
        conn: SQLConnection,
        old: FieldDefinition,
        new: FieldDefinition,
        stored: dict[int, list[ValueEntry]],
    ) -> None:
        """Recreate physical storage while retaining membership and allocation history.

        Surviving entry IDs are restored explicitly; compatible vocabulary IDs are
        retained too. Capture allocator bounds before DROP so deleted IDs and
        unused allocations cannot be recycled. New identities begin above that
        bound even when a conversion removes an identity space temporarily.
        All definition, DDL and value changes belong to the supplied transaction.
        """
        self._validate_keys(conn, new)
        high_water = self.repository.allocation_high_water(conn, old)
        preserved = []
        if old.normalized and new.normalized and old.datatype == new.datatype:
            table, _ = names(old)
            ts = stem(table)
            preserved = rows(
                conn,
                f"SELECT {q(ts + '_id')},{q(ts + '_value')} FROM {q(table)} ORDER BY {q(ts + '_id')}",
            )
        self.repository.drop_storage(conn, old)
        self._update_definition(conn, new)
        self.repository.create_storage(conn, new)
        table, _ = names(new)
        ts = stem(table)
        for value_id, value in preserved:
            # Removed enum choices must not survive as an unused invalid inventory.
            if new.datatype == "enumeration" and value not in new.display.get(
                "enum_values", []
            ):
                continue
            self.repository.insert(
                conn,
                table,
                {
                    ts + "_id": value_id,
                    ts + "_value": value,
                    ts + "_key": identity(value, new),
                },
            )
        self.repository.reserve_identities(conn, new, high_water)
        for owner, values in stored.items():
            converted = entries(
                values if new.mode != "scalar" else values[0] if values else None, new
            )
            self.repository.write_entries(
                conn, new, owner, converted, preserve_ids=True
            )

    def _update_definition(
        self: CustomFieldHost, conn: SQLConnection, d: FieldDefinition
    ) -> None:
        """Persist a validated definition and revision on the supplied write connection.

        Recompute normalized keys and native collection flags together. This changes
        neither backing tables nor any metadata projection and never commits."""
        conn.execute(
            "UPDATE custom_columns SET custom_column_in_table=?, custom_column_label=?, custom_column_label_norm=?, custom_column_name=?, custom_column_name_norm=?, custom_column_datatype=?, custom_column_is_multiple=?, custom_column_ordered=?, custom_column_normalized=?, custom_column_editable=?, custom_column_display=?, custom_column_collection_mode=?, custom_column_storage_version=2, custom_column_revision=? WHERE custom_column_id=?",
            (
                d.attachment,
                d.label,
                unicodedata.normalize("NFC", d.label).casefold(),
                d.name,
                unicodedata.normalize("NFC", d.name).strip().casefold(),
                d.datatype,
                int(d.mode != "scalar"),
                int(d.mode != "scalar"),
                int(d.normalized),
                int(d.editable),
                json.dumps(d.display),
                d.mode,
                d.revision,
                d.id,
            ),
        ).close()

    def plan_field_migration(
        self: CustomFieldHost,
        field: FieldRef,
        changes: Mapping[str, Any],
        *,
        owner_mapping: Mapping[int, int] | None = None,
        value_mapping: Mapping[Any, Any] | None = None,
        reduce: str | None = None,
        collapse: bool = False,
    ) -> ChangePlan:
        """Preview an explicit definition change against one read snapshot.

        Validate the new definition/dependencies and materialize conversions to report
        blockers before writing. Attachment changes need mappings for populated owners;
        multi-to-scalar reduction and set-identity collapse require explicit policies.
        Value mappings can remove memberships with None. The returned intent/fingerprint
        must be revalidated under a writer reservation when applied."""
        allowed = {
            "attachment",
            "label",
            "name",
            "datatype",
            "mode",
            "editable",
            "display",
        }
        if set(changes) - allowed or reduce not in {None, "first", "last"}:
            raise CustomFieldError("Unknown definition change or reduction policy")
        with self.read() as conn:
            old = self.repository.resolve(conn, field)
            new = replace(old, **dict(changes), revision=old.revision + 1)
            validate_definition(new)
            self._validate_keys(conn, new)
            validate_dependencies(new, self.repository.definitions(conn))
            self.repository.owner_column(conn, new.attachment)
            if any(
                getattr(old, k) != getattr(new, k)
                for k in ("attachment", "label", "datatype", "mode")
            ):
                self._require_no_dependents(conn, old)
            payload = {
                "field": old.id,
                "changes": dict(changes),
                "owners": dict(owner_mapping or {}),
                "values": list((value_mapping or {}).items()),
                "reduce": reduce,
                "collapse": collapse,
            }
            issues = []
            try:
                converted = self._convert(conn, old, new, payload)
                count = sum(len(v) for v in converted.values())
            except (ValueError, TypeError) as error:
                count = 0
                issues.append(Issue("conversion", old.id, str(error)))
            action = (
                "Update metadata for"
                if set(changes) <= {"label", "name", "editable"} and not value_mapping
                else "Rebuild"
            )
            return ChangePlan(
                "migration",
                self.repository.fingerprint(conn),
                payload,
                tuple(issues),
                (f"{action} field {old.id}; resulting entries: {count}",),
            )

    def _convert(
        self: CustomFieldHost,
        conn: SQLConnection,
        old: FieldDefinition,
        new: FieldDefinition,
        payload: dict[str, Any],
    ) -> dict[int, list[ValueEntry]]:
        """Compute target memberships without writing or inventing a lossy conversion.

        Apply explicit owner/value mappings, retain surviving entry IDs and Series
        positions, and enforce reduction/collapse policies after owners are combined.
        Referenced target owners must exist. Callers decide whether to rebuild storage."""
        source = self.repository.read_entries(conn, old)
        result: dict[int, list[ValueEntry]] = defaultdict(list)
        mapping = {int(k): int(v) for k, v in payload["owners"].items()}
        if old.attachment != new.attachment and not set(source) <= mapping.keys():
            raise CustomFieldError(
                "Attachment migration requires a target for every populated owner"
            )
        for owner, values in source.items():
            target = (
                mapping.get(owner, owner) if old.attachment != new.attachment else owner
            )
            self.repository.require_owner(conn, OwnerRef(new.attachment, target))
            result.setdefault(target, [])
            for entry in values:
                value = entry.value
                replacement = next(
                    (
                        new_value
                        for old_value, new_value in payload["values"]
                        if old_value == value
                    ),
                    value,
                )
                if replacement is None:
                    continue  # An explicit value mapping to null removes this membership.
                if (
                    not any(old_value == value for old_value, _ in payload["values"])
                    and new.datatype == "int"
                    and old.datatype == "float"
                    and isinstance(value, float)
                    and value.is_integer()
                ):
                    replacement = int(value)
                adapted = adapt(replacement, new)
                if (
                    old.datatype == "int"
                    and new.datatype == "float"
                    and isinstance(adapted, float)
                    and int(adapted) != value
                ):
                    raise CustomFieldError(
                        "Integer-to-float conversion would lose precision"
                    )
                result[target].append(
                    replace(
                        entry,
                        value=adapted,
                        position=(1.0 if entry.position is None else entry.position)
                        if new.datatype == "series"
                        else None,
                    )
                )
        for owner, values in result.items():
            if new.mode == "scalar" and len(values) > 1:
                if payload["reduce"] is None:
                    raise CustomFieldError(
                        "Multiple-to-scalar conversion needs an explicit reduction"
                    )
                result[owner] = (
                    values[:1] if payload["reduce"] == "first" else values[-1:]
                )
            elif new.mode == "set":
                unique: dict[str, ValueEntry] = {}
                for entry in values:
                    key = identity(entry.value, new)
                    if key in unique and not payload["collapse"]:
                        raise CustomFieldError(
                            "Conversion collapses identities; choose collapse explicitly"
                        )
                    unique.setdefault(key, entry)
                result[owner] = list(unique.values())
        return dict(result)

    def apply_field_migration(self: CustomFieldHost, plan: ChangePlan) -> WriteReceipt:
        """Revalidate and apply a migration within one native writer reservation.

        Simple label/name/editability changes update metadata in place. Structural or
        value changes rebuild storage while preserving historical allocation bounds.
        Return entry/vocabulary identity mappings and publish only after the outer commit.
        A stale source or invalid intent raises before mutation; later failures roll back
        the entire migration, including DDL on supported backends."""
        with self.write() as conn:
            self._check_plan(conn, plan, "migration")
            verified = self.plan_field_migration(
                plan.payload["field"],
                plan.payload["changes"],
                owner_mapping=plan.payload["owners"],
                value_mapping=dict(plan.payload["values"]),
                reduce=plan.payload["reduce"],
                collapse=plan.payload["collapse"],
            )
            self._check_plan(conn, verified, "migration")
            old = self.repository.resolve(conn, plan.payload["field"])
            new = replace(old, **plan.payload["changes"], revision=old.revision + 1)
            before = self.repository.read_entries(conn, old)
            converted = self._convert(conn, old, new, plan.payload)
            if (
                set(plan.payload["changes"]) <= {"label", "name", "editable"}
                and not plan.payload["values"]
            ):
                # Descriptive metadata does not require replacing any table or ID.
                self._update_definition(conn, new)
            else:
                self._rebuild(conn, old, new, converted)
            after = {
                v.id: v
                for values in self.repository.read_entries(conn, new).values()
                for v in values
            }
            entry_ids = {
                v.id: v.id if v.id in after else None
                for values in before.values()
                for v in values
            }
            value_ids: dict[int, int | None] = {
                v.value_id: None
                for values in before.values()
                for v in values
                if v.value_id is not None
            }
            for values in before.values():
                for entry in values:
                    if entry.value_id is not None and entry.id in after:
                        value_ids[entry.value_id] = after[entry.id].value_id
            receipt = WriteReceipt(
                tuple(OwnerRef(new.attachment, i) for i in sorted(converted)),
                (new.id,),
                {new.id: new.revision},
                entry_ids,
                value_ids,
            )
            self._publish(receipt)
            return receipt

    def plan_purge(self: CustomFieldHost, field: FieldRef) -> ChangePlan:
        """Preview permanent removal of an already retired field and all its storage.

        Reject active fields and remaining composite dependents. No deletion occurs
        until the fingerprinted plan is explicitly applied."""
        with self.read() as conn:
            d = self.repository.resolve(conn, field, retired=True)
            if not d.retired:
                raise CustomFieldError("Retire the field before planning a purge")
            self._require_no_dependents(conn, d)
            total = sum(len(v) for v in self.repository.read_entries(conn, d).values())
            return ChangePlan(
                "purge",
                self.repository.fingerprint(conn),
                {"field": d.id},
                effects=(f"Delete definition {d.id} and {total} entries permanently",),
            )

    def apply_purge(self: CustomFieldHost, plan: ChangePlan) -> None:
        """Revalidate a retired-field purge, then delete its storage and definition atomically.

        Queue a receipt for the outer commit. Purging is permanent after commit; ordinary
        retirement leaves values intact and is a separate operation."""
        with self.write() as conn:
            self._check_plan(conn, plan, "purge")
            d = self.repository.resolve(conn, plan.payload["field"], retired=True)
            if not d.retired:
                raise CustomFieldError("Field is no longer retired")
            self._require_no_dependents(conn, d)
            self.repository.drop_storage(conn, d)
            conn.execute(
                "DELETE FROM custom_columns WHERE custom_column_id=?", (d.id,)
            ).close()
            self._publish(WriteReceipt((), (d.id,), {}))

    def plan_repair(
        self: CustomFieldHost,
        *,
        actions: tuple[str, ...] = ("cleanup_trigger", "ordering"),
    ) -> ChangePlan:
        """Preview only explicitly requested, supported integrity repairs.

        Audit without mutation and retain unsupported issues as blockers. The default
        repairs cleanup triggers and ordering; removing orphans/unused values or rewriting
        identity keys requires naming those actions. Repair never guesses new values."""
        supported = {
            "cleanup_trigger",
            "ordering",
            "orphan_owner",
            "orphan_value",
            "unused_values",
            "identity_key",
        }
        if set(actions) - supported:
            raise CustomFieldError(
                "Repair actions must be explicit supported operations"
            )
        with self.read() as conn:
            found = self.audit()
            blockers = tuple(i for i in found if i.code not in supported)
            return ChangePlan(
                "repair",
                self.repository.fingerprint(conn),
                {"actions": list(actions)},
                blockers,
                tuple(i.detail for i in found if i.code in actions),
            )

    def apply_repair(self: CustomFieldHost, plan: ChangePlan) -> None:
        """Re-audit a fingerprinted repair under the writer lock and apply its named actions.

        All repairs share one transaction. Update field revisions and queue a receipt
        after success; neither planning nor ordinary reads perform these mutations."""
        with self.write() as conn:
            self._check_plan(conn, plan, "repair")
            verified = self.plan_repair(actions=tuple(plan.payload["actions"]))
            self._check_plan(conn, verified, "repair")
            actions = set(plan.payload["actions"])
            definitions = self.repository.definitions(conn, retired=True)
            for d in definitions:
                table, link = names(d)
                storage = link if d.normalized else table
                ss, ts = stem(storage), stem(table)
                if "orphan_owner" in actions:
                    key = self.repository.owner_column(conn, d.attachment)
                    conn.execute(
                        f"DELETE FROM {q(storage)} WHERE NOT EXISTS (SELECT 1 FROM {q(d.attachment)} o WHERE o.{q(key)}={q(storage)}.{q(ss + '_book')})"
                    ).close()
                if d.normalized and "orphan_value" in actions:
                    conn.execute(
                        f"DELETE FROM {q(link)} WHERE NOT EXISTS (SELECT 1 FROM {q(table)} v WHERE v.{q(ts + '_id')}={q(link)}.{q(ss + '_value')})"
                    ).close()
                if d.normalized and "unused_values" in actions:
                    conn.execute(
                        f"DELETE FROM {q(table)} WHERE NOT EXISTS (SELECT 1 FROM {q(link)} e WHERE e.{q(ss + '_value')}={q(table)}.{q(ts + '_id')})"
                    ).close()
                if "identity_key" in actions:
                    from uuid import uuid4

                    from LiuXin_alpha.databases.custom_columns.native_values import (
                        decode,
                    )

                    inventory_rows = rows(
                        conn,
                        f"SELECT {q(ts + '_id')},{q(ts + '_value')} FROM {q(table)}",
                    )
                    prefix = "repair-" + uuid4().hex
                    for value_id, _ in inventory_rows:
                        conn.execute(
                            f"UPDATE {q(table)} SET {q(ts + '_key')}=? WHERE {q(ts + '_id')}=?",
                            (f"{prefix}-{value_id}", value_id),
                        ).close()
                    for value_id, value in inventory_rows:
                        conn.execute(
                            f"UPDATE {q(table)} SET {q(ts + '_key')}=? WHERE {q(ts + '_id')}=?",
                            (identity(decode(value, d), d), value_id),
                        ).close()
                if "ordering" in actions:
                    for owner, values in self.repository.read_entries(conn, d).items():
                        self.repository.write_entries(
                            conn,
                            d,
                            owner,
                            [(v.value, v.position, v.id) for v in values],
                        )
                if "cleanup_trigger" in actions:
                    self.repository.install_cleanup(conn, d)
            revisions = {}
            for d in definitions:
                self._update_definition(conn, replace(d, revision=d.revision + 1))
                revisions[d.id] = d.revision + 1
            self._publish(WriteReceipt((), tuple(d.id for d in definitions), revisions))
