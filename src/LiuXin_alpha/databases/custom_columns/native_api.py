"""Structural native custom-field contract shared by service components."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from contextlib import AbstractContextManager
from typing import Any, Protocol

from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI
from LiuXin_alpha.databases.custom_columns.native_repository import (
    CustomFieldRepository,
)
from LiuXin_alpha.databases.custom_columns.native_types import (
    ChangePlan,
    CollectionMode,
    FieldDefinition,
    FieldPatch,
    FieldRef,
    Issue,
    OwnerRef,
    Page,
    Predicate,
    ValueEntry,
    WriteReceipt,
)
from LiuXin_alpha.databases.db_types.driver_types import SQLConnection


class CustomFieldHost(Protocol):
    """Structural storage and transaction contract used by native service mixins."""

    db: DatabaseAPI
    repository: CustomFieldRepository

    def read(self) -> AbstractContextManager[SQLConnection]:
        """Reuse an active macro unit or own a read-only, consistent snapshot.

        Nested reads see the caller's uncommitted values and never complete its
        transaction. An independently opened read handle is closed by macros on exit."""
        ...

    def write(self) -> AbstractContextManager[SQLConnection]:
        """Own or join a write unit without weakening native conflict checks.

        Standalone writes use a serialized outer transaction. Nested writes keep
        the caller's transaction and savepoint ownership, acquiring PostgreSQL's
        same cooperating writer lock before any native read. The lock lasts until
        the outer commit, or rollback of the savepoint that acquired it. Ordinary
        SQLite composition can fail with a busy/snapshot conflict on promotion;
        retry that outer transaction, never a write using the stale snapshot.

        The connection belongs to macros. Receipts publish after the outer commit,
        while a nested rollback discards only its writes and pending receipts."""
        ...

    def subscribe(self, listener: Callable[[WriteReceipt], None]) -> Callable[[], None]:
        """Register an after-commit receipt listener and return its unsubscribe function.

        Listeners run in registration order for this service's writes. A listener
        failure is reported after SQL commits; retrying that write would duplicate its
        effects. Removing the same subscription twice raises ValueError."""
        ...

    def _publish(self, receipt: WriteReceipt) -> None:
        """Queue a native receipt on the current managed transaction's completion frame.

        Capture listeners now, defer delivery until the outer commit, and discard
        delivery when the containing savepoint rolls back. Listener failures cannot
        undo committed SQL and are reported by the completion owner."""
        ...

    def list_fields(
        self, *, attachment: str | None = None, include_retired: bool = False
    ) -> tuple[FieldDefinition, ...]:
        """Return detached definitions, optionally scoped to one attachment table.

        Retired definitions are hidden unless explicitly included. Unsupported storage
        raises without installing or converting anything."""
        ...

    def get_field(
        self, field: FieldRef, *, include_retired: bool = False
    ) -> FieldDefinition:
        """Resolve an integer field ID or (attachment, label) pair in one snapshot.

        Unknown and, by default, retired fields raise CustomFieldError. Returned
        definitions are snapshots; use migration plans to change persisted metadata."""
        ...

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
        ...

    def _validate_keys(self, conn: SQLConnection, definition: FieldDefinition) -> None:
        """Reject an exact attachment/label collision, including retired definitions.

        Physical normalized-key constraints also enforce canonical label/name
        uniqueness. This check does not reserve Calibre synthetic companion names."""
        ...

    def get_values(
        self, owners: Iterable[OwnerRef], fields: Iterable[FieldRef]
    ) -> dict[OwnerRef, dict[int, Any]]:
        """Read a typed owner/field matrix from one snapshot, batching owner IDs by 400.

        Validate every supplied owner and field. Fields on other attachments are omitted
        from that owner's mapping; applicable unset values are None or an empty tuple.
        Composite values must be requested from Catalog's evaluator instead."""
        ...

    @staticmethod
    def _value(definition: FieldDefinition, values: list[ValueEntry]) -> Any:
        """Shape stored entries as a scalar, SeriesValue or immutable entry tuple.

        Unset scalars return None. Multiple rows for a scalar are an integrity error,
        not an invitation to silently choose one; collection order is preserved."""
        ...

    def get_value(self, owner: OwnerRef, field: FieldRef) -> Any:
        """Read one value, rejecting an owner whose table differs from the field attachment."""
        ...

    def get_owner_values(self, owner: OwnerRef) -> dict[int, Any]:
        """Read every active stored field for an owner; omit Catalog-computed composites."""
        ...

    def apply_patch(self, patches: Iterable[FieldPatch]) -> WriteReceipt:
        """Validate then apply a mixed owner/field patch atomically.

        Reject missing owners, wrong attachments, read-only fields, stale expected
        revisions and duplicate owner/field targets before the first value mutation.
        Advance each affected field revision once, even for several owners, and return
        a receipt whose listeners run only after the owning transaction commits."""
        ...

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
        ...

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
        ...

    def retire_field(self, field: FieldRef) -> WriteReceipt:
        """Retire a native field without discarding its stored values."""
        ...

    def _require_no_dependents(
        self, conn: SQLConnection, definition: FieldDefinition
    ) -> None:
        """Reject a definition change still referenced by an active composite field."""
        ...

    def audit(self) -> tuple[Issue, ...]:
        """Inspect active and retired fields without repairing or rewriting storage.

        Return located metadata, value, cardinality, identity, ownership, ordering and
        SQLite-trigger issues. Expected malformed records become Issue objects;
        unexpected connection/SQL failures still propagate. This can scan full fields."""
        ...

    def _check_plan(self, conn: SQLConnection, plan: ChangePlan, kind: str) -> None:
        """Reject a wrong plan kind, changed source fingerprint or unresolved blockers."""
        ...

    def _rebuild(
        self,
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
        All definition, DDL and value changes belong to the supplied transaction."""
        ...

    def _update_definition(self, conn: SQLConnection, d: FieldDefinition) -> None:
        """Persist a validated definition and revision on the supplied write connection.

        Recompute normalized keys and native collection flags together. This changes
        neither backing tables nor any metadata projection and never commits."""
        ...

    def plan_field_migration(
        self,
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
        ...

    def _convert(
        self,
        conn: SQLConnection,
        old: FieldDefinition,
        new: FieldDefinition,
        payload: dict[str, Any],
    ) -> dict[int, list[ValueEntry]]:
        """Compute target memberships without writing or inventing a lossy conversion.

        Apply explicit owner/value mappings, retain surviving entry IDs and Series
        positions, and enforce reduction/collapse policies after owners are combined.
        Referenced target owners must exist. Callers decide whether to rebuild storage."""
        ...

    def apply_field_migration(self, plan: ChangePlan) -> WriteReceipt:
        """Revalidate and apply a migration within one native writer reservation.

        Simple label/name/editability changes update metadata in place. Structural or
        value changes rebuild storage while preserving historical allocation bounds.
        Return entry/vocabulary identity mappings and publish only after the outer commit.
        A stale source or invalid intent raises before mutation; later failures roll back
        the entire migration, including DDL on supported backends."""
        ...

    def plan_purge(self, field: FieldRef) -> ChangePlan:
        """Preview permanent removal of an already retired field and all its storage.

        Reject active fields and remaining composite dependents. No deletion occurs
        until the fingerprinted plan is explicitly applied."""
        ...

    def apply_purge(self, plan: ChangePlan) -> None:
        """Revalidate a retired-field purge, then delete its storage and definition atomically.

        Queue a receipt for the outer commit. Purging is permanent after commit; ordinary
        retirement leaves values intact and is a separate operation."""
        ...

    def plan_repair(
        self, *, actions: tuple[str, ...] = ("cleanup_trigger", "ordering")
    ) -> ChangePlan:
        """Preview only explicitly requested, supported integrity repairs.

        Audit without mutation and retain unsupported issues as blockers. The default
        repairs cleanup triggers and ordering; removing orphans/unused values or rewriting
        identity keys requires naming those actions. Repair never guesses new values."""
        ...

    def apply_repair(self, plan: ChangePlan) -> None:
        """Re-audit a fingerprinted repair under the writer lock and apply its named actions.

        All repairs share one transaction. Update field revisions and queue a receipt
        after success; neither planning nor ordinary reads perform these mutations."""
        ...

    def find_owners(
        self,
        attachment: str,
        predicates: Iterable[Predicate] = (),
        *,
        limit: int = 100,
        after_id: int = 0,
    ) -> Page:
        """Return ascending owner IDs matching every supplied stored-field predicate.

        Equality/membership use canonical identities; substring/prefix checks are literal
        and case-sensitive. Collection all is non-vacuous: empty values do not match it.
        Page limits are 1..1000 with an exclusive owner-ID cursor. Hold an outer read()
        context when several pages must use one snapshot. Composites require Catalog."""
        ...

    def inventory(
        self,
        field: FieldRef,
        *,
        limit: int = 100,
        after_id: int = 0,
        unused: bool = False,
    ) -> Page:
        """Page distinct values with their distinct-owner counts, including retired fields.

        Normalized fields use stable vocabulary IDs and can restrict results to unused
        values. Inline fields group by canonical identity and use the smallest current
        entry ID as the page key; unused=True is empty for those fields. A cursor is
        exclusive and remains snapshot-relative when values change between pages."""
        ...

    def usage_count(self, field: FieldRef, value: Any) -> int:
        """Count distinct owners using a canonical value, including a retired field's data."""
        ...

    def is_shared(self, field: FieldRef, value: Any) -> bool:
        """Report whether a canonical value is assigned to more than one owner."""
        ...

    def next_series_position(self, field: FieldRef, series: str) -> float:
        """Return the greatest position for a Series identity plus one, or one if absent.

        This reads a suggestion, not a reservation; group dependent writes in a native
        write context when the selection must remain stable until assignment."""
        ...

    def rename_value(
        self, field: FieldRef, value_id: int, new_value: Any
    ) -> WriteReceipt:
        """Rename a shared value, or merge memberships into an existing identity."""
        ...

    def delete_value(self, field: FieldRef, value_id: int) -> WriteReceipt:
        """Remove a shared value and its memberships in one managed transaction."""
        ...
