"""
Bind native custom-field administration to metadata cache reconciliation.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

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
    WriteReceipt,
)

if TYPE_CHECKING:
    from LiuXin_alpha.caches.metadata.service import MetadataAPI
    from LiuXin_alpha.catalog.api.metadata_access import MetadataWriteReceipt


class MetadataCustomFields:
    """
    Retain native validation, plans and revisions; coordinate cache publication.

    Example:
        ``metadata.custom_fields`` coordinates native field writes with the bound cache.
    """

    def __init__(self, owner: MetadataAPI) -> None:
        """
        Borrow the metadata coordinator without creating another field service.

        Example:
            The field service shares the same cache binding as metadata.works.


        :param owner: Metadata service owning Catalog, cache and transaction policy.
        :return: None; retains the owner.
        """
        self._owner = owner

    def _read[ResultT](self, operation: Callable[[], ResultT]) -> ResultT:
        """
        Check the metadata binding and perform an authoritative native-service read.

        Example:
            Listing fields does not derive definitions from a possibly stale cached registry.


        :param operation: Read operation whose native service supplies snapshot
            semantics.
        :return: Native read result; attachment, validation and storage errors
            propagate.
        """
        with self._owner._lock():
            self._owner._assert_attached()
            return operation()

    def list_fields(
        self, *, attachment: str | None = None, include_retired: bool = False
    ) -> tuple[FieldDefinition, ...]:
        """
        Read detached definitions from authoritative native storage.

        Example:
            ``metadata.custom_fields.list_fields(attachment="works")`` lists Work fields.


        :param attachment: Restrict to one attachment table, or None for all tables.
        :param include_retired: Include retained definitions that are no longer active.
        :return: Tuple of detached native field definitions.
        """
        return self._read(
            lambda: self._owner._catalog.custom_fields.list_fields(
                attachment=attachment, include_retired=include_retired
            )
        )

    def get_field(
        self, field: FieldRef, *, include_retired: bool = False
    ) -> FieldDefinition:
        """
        Resolve an existing field by ID or scoped label.

        Example:
            Keep the returned revision when preparing a conditional value edit.


        :param field: Native field ID or attachment-qualified label resolved by the
            service.
        :param include_retired: Allow a retired definition to be returned.
        :return: Resolved native definition; an unavailable field raises a native field
            error.
        """
        return self._read(
            lambda: self._owner._catalog.custom_fields.get_field(
                field, include_retired=include_retired
            )
        )

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
    ) -> MetadataWriteReceipt[FieldDefinition]:
        """
        Create a native definition and storage, then reconcile schema and metadata.

        Example:
            ``metadata.custom_fields.create_field("works", "shelf", mode="set")`` creates a field.


        :param attachment: Owner table for the new native field.
        :param label: Unique native label within that attachment.
        :param name: Optional display name; the service supplies its default when
            omitted.
        :param datatype: Native datatype, validated by the field service.
        :param mode: Scalar, set or ordered collection storage mode.
        :param editable: Whether ordinary native value writes are permitted.
        :param display: Native display metadata; omitted values use service defaults.
        :return: Outer metadata receipt containing the new FieldDefinition.
        """
        return self._owner._write(
            lambda: (
                self._owner._catalog.custom_fields.create_field(
                    attachment,
                    label,
                    name=name,
                    datatype=datatype,
                    mode=mode,
                    editable=editable,
                    display=display,
                ),
                True,
            ),
            schema_changed=True,
        )

    def get_value(self, owner: OwnerRef, field: FieldRef) -> Any:
        """
        Read one stored value; composite evaluation remains Catalog-owned.

        Example:
            Reading a composite definition does not evaluate its template here.


        :param owner: Native OwnerRef identifying the attachment table and owner row.
        :param field: Native field ID or attachment-qualified label resolved by the
            service.
        :return: Native scalar, collection entries or missing-value representation.
        """
        return self._read(
            lambda: self._owner._catalog.custom_fields.get_value(owner, field)
        )

    def get_values(
        self, owners: Sequence[OwnerRef], fields: Sequence[FieldRef]
    ) -> dict[OwnerRef, dict[int, Any]]:
        """
        Read a typed owner/field matrix in one native snapshot.

        Example:
            A display page can load several owners and fields in one native read.


        :param owners: Owner references whose values are requested.
        :param fields: Field references to resolve in the same native snapshot.
        :return: Owner-to-field-ID matrix of native stored values.
        """
        return self._read(
            lambda: self._owner._catalog.custom_fields.get_values(owners, fields)
        )

    def set_value(
        self,
        owner: OwnerRef,
        field: FieldRef,
        value: Any,
        *,
        expected_revision: int | None = None,
    ) -> MetadataWriteReceipt[WriteReceipt]:
        """
        Replace or clear a value, optionally checking its field-wide revision.

        Example:
            A revision from get_field can guard a subsequent set_value call.


        :param owner: Native OwnerRef identifying the attachment table and owner row.
        :param field: Native field ID or attachment-qualified label resolved by the
            service.
        :param value: Native replacement value; None requests clearing under datatype
            policy.
        :param expected_revision: Optional field-wide revision; a stale value rejects
            the write.
        :return: Outer metadata receipt containing the native WriteReceipt.
        """
        return self._owner._write(
            lambda: (
                self._owner._catalog.custom_fields.set_value(
                    owner, field, value, expected_revision=expected_revision
                ),
                True,
            ),
            schema_changed=False,
        )

    def apply_patch(
        self, patches: Sequence[FieldPatch]
    ) -> MetadataWriteReceipt[WriteReceipt]:
        """
        Apply a mixed owner/field patch atomically, retaining native conflict checks.

        Example:
            Patches across multiple owners roll back together if one fails.


        :param patches: Ordered native patches with their own value and revision
            semantics.
        :return: Outer metadata receipt containing the atomic native patch receipt.
        """
        return self._owner._write(
            lambda: (
                self._owner._catalog.custom_fields.apply_patch(patches),
                bool(patches),
            ),
            schema_changed=False,
        )

    def retire_field(self, field: FieldRef) -> MetadataWriteReceipt[WriteReceipt]:
        """
        Hide a field while retaining its values; refresh definition projections.

        Example:
            Retirement removes a field from ordinary discovery while retaining stored values.


        :param field: Native field ID or attachment-qualified label resolved by the
            service.
        :return: Outer metadata receipt containing the native retirement receipt.
        """
        return self._owner._write(
            lambda: (self._owner._catalog.custom_fields.retire_field(field), True),
            schema_changed=True,
        )

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
        """
        Preview definition changes and explicit conversions without changing storage.

        Example:
            Inspect a migration plan before passing it to apply_field_migration.


        :param field: Native field ID or attachment-qualified label resolved by the
            service.
        :param changes: Proposed definition changes accepted by the native planner.
        :param owner_mapping: Explicit old-to-new owner mapping when moving attachment.
        :param value_mapping: Explicit value conversions for incompatible datatype
            changes.
        :param reduce: Native reduction policy when collapsing a collection to a scalar.
        :param collapse: Allow the explicit collapse policy when the native planner
            requires it.
        :return: Native migration plan; no definition or value is changed by planning.
        """
        return self._read(
            lambda: self._owner._catalog.custom_fields.plan_field_migration(
                field,
                changes,
                owner_mapping=owner_mapping,
                value_mapping=value_mapping,
                reduce=reduce,
                collapse=collapse,
            )
        )

    def apply_field_migration(
        self, plan: ChangePlan
    ) -> MetadataWriteReceipt[WriteReceipt]:
        """
        Revalidate and atomically apply a prepared migration, then reload schema.

        Example:
            A stale plan fails revalidation before its proposed conversion is applied.


        :param plan: Native migration plan to revalidate against current definitions and
            values.
        :return: Outer metadata receipt containing the native migration receipt.
        """
        return self._owner._write(
            lambda: (
                self._owner._catalog.custom_fields.apply_field_migration(plan),
                True,
            ),
            schema_changed=True,
        )

    def plan_purge(self, field: FieldRef) -> ChangePlan:
        """
        Preview permanent removal of a retired field and its retained values.

        Example:
            Planning a purge keeps both definition and values available.


        :param field: Native field ID or attachment-qualified label resolved by the
            service.
        :return: Native purge plan describing the retired field's permanent removal.
        """
        return self._read(lambda: self._owner._catalog.custom_fields.plan_purge(field))

    def apply_purge(self, plan: ChangePlan) -> MetadataWriteReceipt[None]:
        """
        Apply a revalidated purge and reconcile the removed schema.

        Example:
            Only applying the explicit purge removes retained field storage permanently.


        :param plan: Native purge plan revalidated before deletion.
        :return: Outer metadata receipt whose result is None after the native purge
            succeeds.
        """
        return self._owner._write(
            lambda: (self._owner._catalog.custom_fields.apply_purge(plan), True),
            schema_changed=True,
        )

    def query(
        self,
        attachment: str,
        predicates: Sequence[Predicate],
        *,
        limit: int = 100,
        after_id: int = 0,
    ) -> Page:
        """
        Query native stored fields with an exclusive owner-ID cursor.

        Example:
            Use the page's continuation identity as after_id for the next page.


        :param attachment: Owner table to query.
        :param predicates: Native stored-field predicates combined by the field service.
        :param limit: Maximum owner IDs returned by the native page.
        :param after_id: Exclusive owner-ID cursor; zero starts at the beginning.
        :return: Native owner-ID page, including its continuation state.
        """
        return self._read(
            lambda: self._owner._catalog.custom_fields.find_owners(
                attachment, predicates, limit=limit, after_id=after_id
            )
        )

    def inventory(
        self,
        field: FieldRef,
        *,
        limit: int = 100,
        after_id: int = 0,
        unused: bool = False,
    ) -> Page:
        """
        Page shared field values and distinct-owner usage counts.

        Example:
            Inventory counts owners even when a collection can contain repeated entries.


        :param field: Native field ID or attachment-qualified label resolved by the
            service.
        :param limit: Maximum shared values in the page.
        :param after_id: Exclusive shared-value-ID cursor.
        :param unused: Include only unused values when true, according to native
            inventory policy.
        :return: Native page of shared values and distinct-owner usage counts.
        """
        return self._read(
            lambda: self._owner._catalog.custom_fields.inventory(
                field, limit=limit, after_id=after_id, unused=unused
            )
        )

    def rename_value(
        self, field: FieldRef, value_id: int, new_value: Any
    ) -> MetadataWriteReceipt[WriteReceipt]:
        """
        Rename or merge a native shared value and reconcile affected owners.

        Example:
            Renaming to an existing normalized value follows the native merge policy.


        :param field: Native field ID or attachment-qualified label resolved by the
            service.
        :param value_id: Existing native shared-value identity.
        :param new_value: Replacement value, normalized by the native datatype.
        :return: Outer metadata receipt containing the native rename/merge receipt.
        """
        return self._owner._write(
            lambda: (
                self._owner._catalog.custom_fields.rename_value(
                    field, value_id, new_value
                ),
                True,
            ),
            schema_changed=False,
        )

    def delete_value(
        self, field: FieldRef, value_id: int
    ) -> MetadataWriteReceipt[WriteReceipt]:
        """
        Remove a native shared value and its memberships atomically.

        Example:
            Deleting a shared value also removes its memberships from owners.


        :param field: Native field ID or attachment-qualified label resolved by the
            service.
        :param value_id: Existing native shared-value identity to remove.
        :return: Outer metadata receipt containing the native deletion receipt.
        """
        return self._owner._write(
            lambda: (
                self._owner._catalog.custom_fields.delete_value(field, value_id),
                True,
            ),
            schema_changed=False,
        )

    def audit(self) -> tuple[Issue, ...]:
        """
        Inspect native definitions and values without modifying storage.

        Example:
            An empty issue tuple indicates that the native audit found no current issue.


        :return: Tuple of native definition/storage consistency issues.
        """
        return self._read(lambda: self._owner._catalog.custom_fields.audit())
