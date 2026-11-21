"""Metadata-facing native field administration with reconciliation-aware writes.

Native identities, plans and field-wide revisions are preserved. These explicit
database reads use the native service's transaction snapshots, not a cache replica.
All writes return an outer metadata receipt retaining the native result.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Protocol

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
    from LiuXin_alpha.catalog.api.metadata_results import MetadataWriteReceipt


class MetadataCustomFieldsInterface(Protocol):
    """Expose native field administration without leaking uncoordinated writes."""

    def list_fields(
        self, *, attachment: str | None = None, include_retired: bool = False
    ) -> tuple[FieldDefinition, ...]:
        """Read detached definitions from authoritative native storage."""
        ...

    def get_field(
        self, field: FieldRef, *, include_retired: bool = False
    ) -> FieldDefinition:
        """Resolve an existing field by ID or scoped label."""
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
    ) -> MetadataWriteReceipt[FieldDefinition]:
        """Create a native definition and storage, then reconcile schema and metadata."""
        ...

    def get_value(self, owner: OwnerRef, field: FieldRef) -> Any:
        """Read one stored value; composite evaluation remains Catalog-owned."""
        ...

    def get_values(
        self, owners: Sequence[OwnerRef], fields: Sequence[FieldRef]
    ) -> dict[OwnerRef, dict[int, Any]]:
        """Read a typed owner/field matrix in one native snapshot."""
        ...

    def set_value(
        self,
        owner: OwnerRef,
        field: FieldRef,
        value: Any,
        *,
        expected_revision: int | None = None,
    ) -> MetadataWriteReceipt[WriteReceipt]:
        """Replace or clear a value, optionally checking its field-wide revision."""
        ...

    def apply_patch(
        self, patches: Sequence[FieldPatch]
    ) -> MetadataWriteReceipt[WriteReceipt]:
        """Apply a mixed owner/field patch atomically, retaining native conflict checks."""
        ...

    def retire_field(self, field: FieldRef) -> MetadataWriteReceipt[WriteReceipt]:
        """Hide a field while retaining its values; refresh definition projections."""
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
        """Preview definition changes and explicit conversions without changing storage."""
        ...

    def apply_field_migration(
        self, plan: ChangePlan
    ) -> MetadataWriteReceipt[WriteReceipt]:
        """Revalidate and atomically apply a prepared migration, then reload schema."""
        ...

    def plan_purge(self, field: FieldRef) -> ChangePlan:
        """Preview permanent removal of a retired field and its retained values."""
        ...

    def apply_purge(self, plan: ChangePlan) -> MetadataWriteReceipt[None]:
        """Apply a revalidated purge and reconcile the removed schema."""
        ...

    def query(
        self,
        attachment: str,
        predicates: Sequence[Predicate],
        *,
        limit: int = 100,
        after_id: int = 0,
    ) -> Page:
        """Query native stored fields with an exclusive owner-ID cursor."""
        ...

    def inventory(
        self,
        field: FieldRef,
        *,
        limit: int = 100,
        after_id: int = 0,
        unused: bool = False,
    ) -> Page:
        """Page shared field values and distinct-owner usage counts."""
        ...

    def rename_value(
        self, field: FieldRef, value_id: int, new_value: Any
    ) -> MetadataWriteReceipt[WriteReceipt]:
        """Rename or merge a native shared value and reconcile affected owners."""
        ...

    def delete_value(
        self, field: FieldRef, value_id: int
    ) -> MetadataWriteReceipt[WriteReceipt]:
        """Remove a native shared value and its memberships atomically."""
        ...

    def audit(self) -> tuple[Issue, ...]:
        """Inspect native definitions and values without modifying storage."""
        ...
