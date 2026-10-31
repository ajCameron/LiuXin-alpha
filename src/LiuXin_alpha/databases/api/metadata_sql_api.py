"""Declare table-scoped SQL helpers over the current physical schema.

Catalog owns WEMI interpretation and compatibility projections. This contract
addresses physical rows, declared relationships and comparison keys, using
shared normalization and portable transactions. It does not authorize physical
file changes or imply metadata validation beyond the database primitives.
"""

from __future__ import annotations

import abc
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

from LiuXin_alpha.databases.db_types.macro_types import (
    LINK_TYPE_UNSET,
    LinkedTableRow,
    NormalizedIdentityMigrationReport,
)

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


class MetadataSQLAPI(abc.ABC):
    """Specify physical-table operations without a book/title facade.

    Concrete implementations must preserve the caller's portable transaction and
    use the existing canonical-identity service for vocabulary lookup/insertion.

    Example:
        >>> import inspect
        >>> inspect.isabstract(MetadataSQLAPI)
        True
    """

    db: DatabaseAPI

    @abc.abstractmethod
    def __init__(self, db: DatabaseAPI) -> None:
        """Bind the database that owns execution, schema and transactions."""

    @abc.abstractmethod
    def add_feed(self, title: str, script: str) -> None:
        """Insert a feed title and recipe text, leaving commit to the transaction.

        This retained call shape returns None. Call ``macros.insert_row`` when
        the new row ID is needed.
        """

    @abc.abstractmethod
    def delete_feed(
        self, feed_id: int | Iterable[int | tuple[int] | list[int]]
    ) -> None:
        """Delete one feed ID or an atomic batch of IDs/one-cell bindings.

        Validate all batch bindings before deletion. Missing IDs are no-ops;
        an enclosing portable transaction retains ownership of commit/rollback.
        """

    @abc.abstractmethod
    def update_feed(self, feed_id: int, script: str, title: str) -> None:
        """Update a feed's recipe text and display title together by row ID."""

    @abc.abstractmethod
    def set_feeds(self, feeds: Iterable[tuple[str, str]]) -> None:
        """Atomically replace feeds with title/script pairs; IDs are not retained.

        Materialize input before deletion. A late insertion failure rolls back
        the replacement, and an outer transaction can roll back the whole call.
        """

    @abc.abstractmethod
    def set_file_name(self, file_id: int, new_fname: str) -> None:
        """Update ``files.file_name`` for a file row; do not rename any bytes."""

    @abc.abstractmethod
    def set_file_size(self, file_id: int, size: int) -> None:
        """Record ``file_size_bytes`` without measuring files or updating assets."""

    @abc.abstractmethod
    def set_file_size_and_name(self, file_id: int, size: int, fname: str) -> None:
        """Update a file row's recorded name and byte count in one statement."""

    @abc.abstractmethod
    def delete_file_by_id(self, file_id: int) -> None:
        """Delete a file row by its ID; schema cascades apply, bytes are untouched."""

    @abc.abstractmethod
    def delete_files_by_id(
        self, file_ids: Iterable[int | tuple[int] | list[int]]
    ) -> None:
        """Atomically delete file rows from IDs or one-cell bindings.

        Materialize and validate the entire batch before deletion. Missing IDs
        are no-ops. Schema cascades apply; physical files are never removed.
        """

    @abc.abstractmethod
    def get_series_id_from_value(self, series: str) -> int | None:
        """Return the canonical series row ID, or None when no identity matches.

        This reads the physical series table. It does not choose memberships,
        a primary Series, a sequence position or a WEMI owner.
        """

    @abc.abstractmethod
    def delete_tag_by_value(self, tag: str) -> None:
        """Resolve and delete one tag row in a shared portable transaction.

        An absent identity is a no-op. Schema cascades apply to links; this is
        row deletion, not removal of a particular Work's tag membership.
        """

    @abc.abstractmethod
    def get_tag_id_from_value(self, tag: str) -> int | None:
        """Return the canonical tags row ID, or None when no identity matches."""

    @abc.abstractmethod
    def add_tag(self, tag_value: str) -> int:
        """Ensure a canonical tags row and return its ID, without linking owners."""

    @abc.abstractmethod
    def update_files_bulk(
        self, updates: Mapping[int, Mapping[str, str | int | None]]
    ) -> None:
        """Atomically update recorded file names/sizes by physical file ID.

        Payloads may contain file_name and/or file_size_bytes. Empty payloads
        and missing IDs are no-ops. Schema constraints apply; bytes are untouched.
        """

    @abc.abstractmethod
    def get_tag_ids_from_values(self, values: Iterable[str]) -> dict[str, int | None]:
        """Map original Tag spellings to stored IDs or None using batch lookup."""

    @abc.abstractmethod
    def ensure_tags(self, values: Iterable[str]) -> dict[str, int]:
        """Atomically ensure Tag rows, preserving existing/first input spellings."""

    @abc.abstractmethod
    def delete_tags_by_id(self, tag_ids: Iterable[int]) -> None:
        """Atomically delete Tag rows by ID; schema cascades remove their links."""

    @abc.abstractmethod
    def get_series_ids_from_values(
        self, values: Iterable[str]
    ) -> dict[str, int | None]:
        """Map original Series spellings to stored IDs or None using batch lookup."""

    @abc.abstractmethod
    def ensure_series(self, values: Iterable[str]) -> dict[str, int]:
        """Atomically ensure Series rows without choosing memberships or positions."""

    @abc.abstractmethod
    def delete_series_by_id(self, series_ids: Iterable[int]) -> None:
        """Atomically delete Series rows by ID, subject to hierarchy/link constraints."""

    @abc.abstractmethod
    def audit_normalized_keys(
        self,
        table: str,
        *,
        value_columns: Iterable[str] | None = None,
        row_ids: Iterable[int] | None = None,
    ) -> NormalizedIdentityMigrationReport:
        """Report selected stale keys and collisions without changing data/schema.

        Select by declared display-column names and physical row IDs. Collision
        detection scans the table, comparing planned keys with unchanged stored
        keys on other rows. A column suffix alone does not declare an identity.
        """

    @abc.abstractmethod
    def rebuild_normalized_keys(
        self,
        table: str,
        *,
        value_columns: Iterable[str] | None = None,
        row_ids: Iterable[int] | None = None,
    ) -> NormalizedIdentityMigrationReport:
        """Repair selected keys atomically, preserving display values and schema.

        Recheck collisions in the write transaction before effects. Schema
        constraints still apply. Empty selections are no-ops; None selects all
        declared identities/rows. An enclosing transaction retains ownership.
        """

    @abc.abstractmethod
    def get_works_for_expression(
        self, expression_id: int, *, link_type: object = LINK_TYPE_UNSET
    ) -> tuple[LinkedTableRow, ...]:
        """Read Expression-to-Work links and batch their physical Work rows.

        Preserve link order and metadata. An absent destination has row=None;
        source existence and interpretation remain the caller's responsibility.
        The omitted filter includes all types. Supplied filters, including None,
        require a typed relation; the current core WEMI relation is untyped.
        """

    @abc.abstractmethod
    def get_expressions_for_work(
        self, work_id: int, *, link_type: object = LINK_TYPE_UNSET
    ) -> tuple[LinkedTableRow, ...]:
        """Read Work-to-Expression links and batch their destination rows.

        Preserve link order and metadata. An absent destination has row=None;
        no preferred Expression is selected. Supplied type filters require a
        typed relation; the current core WEMI relation is untyped.
        """

    @abc.abstractmethod
    def get_expressions_for_manifestation(
        self, manifestation_id: int, *, link_type: object = LINK_TYPE_UNSET
    ) -> tuple[LinkedTableRow, ...]:
        """Read Manifestation-to-Expression links and batch destination rows.

        Preserve all embodied Expressions in link order, including repeated
        destinations. Missing rows remain None; no singular book is projected.
        """

    @abc.abstractmethod
    def get_manifestations_for_expression(
        self, expression_id: int, *, link_type: object = LINK_TYPE_UNSET
    ) -> tuple[LinkedTableRow, ...]:
        """Read Expression-to-Manifestation links and batch destination rows.

        Keep link metadata and order; an absent destination has row=None. This
        read uses the current connection without opening a snapshot transaction.
        """

    @abc.abstractmethod
    def get_agents_linked_from(
        self,
        source_table: str,
        source_id: int,
        *,
        link_type: object = LINK_TYPE_UNSET,
    ) -> tuple[LinkedTableRow, ...]:
        """Batch physical agents destinations for one declared relationship.

        Resolve the explicit source table through schema introspection. Preserve
        link order, duplicate destinations and all link metadata; missing rows
        remain None. Callers validate source existence and choose relationships.
        Supplied type filters require a typed relation. No new transaction opens.
        """

    @abc.abstractmethod
    def get_comments_linked_from(
        self,
        source_table: str,
        source_id: int,
        *,
        link_type: object = LINK_TYPE_UNSET,
    ) -> tuple[LinkedTableRow, ...]:
        """Batch physical comments destinations for one declared relationship.

        Resolve the explicit source table through schema introspection. Preserve
        link order, duplicate destinations and all link metadata; missing rows
        remain None. Callers validate source existence and choose relationships.
        Supplied type filters require a typed relation. No new transaction opens.
        """

    @abc.abstractmethod
    def get_notes_linked_from(
        self,
        source_table: str,
        source_id: int,
        *,
        link_type: object = LINK_TYPE_UNSET,
    ) -> tuple[LinkedTableRow, ...]:
        """Batch physical notes destinations for one declared relationship.

        Resolve the explicit source table through schema introspection. Preserve
        link order, duplicate destinations and all link metadata; missing rows
        remain None. Callers validate source existence and choose relationships.
        Supplied type filters require a typed relation. No new transaction opens.
        """

    @abc.abstractmethod
    def get_synopses_linked_from(
        self,
        source_table: str,
        source_id: int,
        *,
        link_type: object = LINK_TYPE_UNSET,
    ) -> tuple[LinkedTableRow, ...]:
        """Batch physical synopses destinations for one declared relationship.

        Resolve the explicit source table through schema introspection. Preserve
        link order, duplicate destinations and all link metadata; missing rows
        remain None. Callers validate source existence and choose relationships.
        Supplied type filters require a typed relation. No new transaction opens.
        """

    @abc.abstractmethod
    def get_genres_linked_from(
        self,
        source_table: str,
        source_id: int,
        *,
        link_type: object = LINK_TYPE_UNSET,
    ) -> tuple[LinkedTableRow, ...]:
        """Batch physical genres destinations for one declared relationship.

        Resolve the explicit source table through schema introspection. Preserve
        link order, duplicate destinations and all link metadata; missing rows
        remain None. Callers validate source existence and choose relationships.
        Supplied type filters require a typed relation. No new transaction opens.
        """

    @abc.abstractmethod
    def get_subjects_linked_from(
        self,
        source_table: str,
        source_id: int,
        *,
        link_type: object = LINK_TYPE_UNSET,
    ) -> tuple[LinkedTableRow, ...]:
        """Batch physical subjects destinations for one declared relationship.

        Resolve the explicit source table through schema introspection. Preserve
        link order, duplicate destinations and all link metadata; missing rows
        remain None. Callers validate source existence and choose relationships.
        Supplied type filters require a typed relation. No new transaction opens.
        """

    @abc.abstractmethod
    def get_labels_linked_from(
        self,
        source_table: str,
        source_id: int,
        *,
        link_type: object = LINK_TYPE_UNSET,
    ) -> tuple[LinkedTableRow, ...]:
        """Batch physical labels destinations for one declared relationship.

        Resolve the explicit source table through schema introspection. Preserve
        link order, duplicate destinations and all link metadata; missing rows
        remain None. Callers validate source existence and choose relationships.
        Supplied type filters require a typed relation. No new transaction opens.
        """

    @abc.abstractmethod
    def get_languages_linked_from(
        self,
        source_table: str,
        source_id: int,
        *,
        link_type: object = LINK_TYPE_UNSET,
    ) -> tuple[LinkedTableRow, ...]:
        """Batch physical languages destinations for one declared relationship.

        Resolve the explicit source table through schema introspection. Preserve
        link order, duplicate destinations and all link metadata; missing rows
        remain None. Callers validate source existence and choose relationships.
        Supplied type filters require a typed relation. No new transaction opens.
        """

    @abc.abstractmethod
    def get_ratings_linked_from(
        self,
        source_table: str,
        source_id: int,
        *,
        link_type: object = LINK_TYPE_UNSET,
    ) -> tuple[LinkedTableRow, ...]:
        """Batch physical ratings destinations for one declared relationship.

        Resolve the explicit source table through schema introspection. Preserve
        link order, duplicate destinations and all link metadata; missing rows
        remain None. Callers validate source existence and choose relationships.
        Supplied type filters require a typed relation. No new transaction opens.
        """

    @abc.abstractmethod
    def get_images_linked_from(
        self,
        source_table: str,
        source_id: int,
        *,
        link_type: object = LINK_TYPE_UNSET,
    ) -> tuple[LinkedTableRow, ...]:
        """Batch physical images destinations for one declared relationship.

        Resolve the explicit source table through schema introspection. Preserve
        link order, duplicate destinations and all link metadata; missing rows
        remain None. Callers validate source existence and choose relationships.
        Supplied type filters require a typed relation. No new transaction opens.
        """

    @abc.abstractmethod
    def get_folders_linked_from(
        self,
        source_table: str,
        source_id: int,
        *,
        link_type: object = LINK_TYPE_UNSET,
    ) -> tuple[LinkedTableRow, ...]:
        """Batch physical folders destinations for one declared relationship.

        Resolve the explicit source table through schema introspection. Preserve
        link order, duplicate destinations and all link metadata; missing rows
        remain None. Callers validate source existence and choose relationships.
        Supplied type filters require a typed relation. No new transaction opens.
        """

    @abc.abstractmethod
    def get_devices_linked_from(
        self,
        source_table: str,
        source_id: int,
        *,
        link_type: object = LINK_TYPE_UNSET,
    ) -> tuple[LinkedTableRow, ...]:
        """Batch physical devices destinations for one declared relationship.

        Resolve the explicit source table through schema introspection. Preserve
        link order, duplicate destinations and all link metadata; missing rows
        remain None. Callers validate source existence and choose relationships.
        Supplied type filters require a typed relation. No new transaction opens.
        """

    @abc.abstractmethod
    def get_stores_linked_from(
        self,
        source_table: str,
        source_id: int,
        *,
        link_type: object = LINK_TYPE_UNSET,
    ) -> tuple[LinkedTableRow, ...]:
        """Batch physical stores destinations for one declared relationship.

        Resolve the explicit source table through schema introspection. Preserve
        link order, duplicate destinations and all link metadata; missing rows
        remain None. Callers validate source existence and choose relationships.
        Supplied type filters require a typed relation. No new transaction opens.
        """

    @abc.abstractmethod
    def get_digital_assets_linked_from(
        self,
        source_table: str,
        source_id: int,
        *,
        link_type: object = LINK_TYPE_UNSET,
    ) -> tuple[LinkedTableRow, ...]:
        """Batch physical digital_assets destinations for one declared relationship.

        Resolve the explicit source table through schema introspection. Preserve
        link order, duplicate destinations and all link metadata; missing rows
        remain None. Callers validate source existence and choose relationships.
        Supplied type filters require a typed relation. No new transaction opens.
        """

    @abc.abstractmethod
    def get_composite_digital_assets_linked_from(
        self,
        source_table: str,
        source_id: int,
        *,
        link_type: object = LINK_TYPE_UNSET,
    ) -> tuple[LinkedTableRow, ...]:
        """Batch physical composite_digital_assets destinations for one declared relationship.

        Resolve the explicit source table through schema introspection. Preserve
        link order, duplicate destinations and all link metadata; missing rows
        remain None. Callers validate source existence and choose relationships.
        Supplied type filters require a typed relation. No new transaction opens.
        """

    @abc.abstractmethod
    def get_tags_linked_from(
        self,
        source_table: str,
        source_id: int,
        *,
        link_type: object = LINK_TYPE_UNSET,
    ) -> tuple[LinkedTableRow, ...]:
        """Batch physical tags destinations for one declared relationship.

        Resolve the explicit source table through schema introspection. Preserve
        link order, duplicate destinations and all link metadata; missing rows
        remain None. Callers validate source existence and choose relationships.
        Supplied type filters require a typed relation. No new transaction opens.
        """

    @abc.abstractmethod
    def get_series_linked_from(
        self,
        source_table: str,
        source_id: int,
        *,
        link_type: object = LINK_TYPE_UNSET,
    ) -> tuple[LinkedTableRow, ...]:
        """Batch physical series destinations for one declared relationship.

        Resolve the explicit source table through schema introspection. Preserve
        link order, duplicate destinations and all link metadata; missing rows
        remain None. Callers validate source existence and choose relationships.
        Supplied type filters require a typed relation. No new transaction opens.
        """

    @abc.abstractmethod
    def get_files_linked_from(
        self,
        source_table: str,
        source_id: int,
        *,
        link_type: object = LINK_TYPE_UNSET,
    ) -> tuple[LinkedTableRow, ...]:
        """Batch physical files destinations for one declared relationship.

        Resolve the explicit source table through schema introspection. Preserve
        link order, duplicate destinations and all link metadata; missing rows
        remain None. Callers validate source existence and choose relationships.
        Supplied type filters require a typed relation. No new transaction opens.
        """

    @abc.abstractmethod
    def get_items_for_manifestations(
        self,
        manifestation_ids: Iterable[int],
    ) -> dict[int, tuple[Mapping[str, Any], ...]]:
        """Batch items rows by their stored item_manifestation_id references.

        Keep an ID-ordered tuple for every requested owner, including empty
        groups. No owner selection, validation or value normalization is applied.
        Optional filters use exact stored values; None leaves them unfiltered.
        Read the current connection without committing or creating a snapshot.
        """

    @abc.abstractmethod
    def get_asset_replicas_for_assets(
        self,
        asset_ids: Iterable[int],
    ) -> dict[int, tuple[Mapping[str, Any], ...]]:
        """Batch rows by asset_replica_digital_asset_id, retaining empty owner groups.

        Order each group by physical row ID. Do not require owners to exist or
        interpret stored metadata. Reads share the caller's current transaction.
        """

    @abc.abstractmethod
    def get_asset_replicas_for_stores(
        self,
        store_ids: Iterable[int],
    ) -> dict[int, tuple[Mapping[str, Any], ...]]:
        """Batch rows by asset_replica_store_id, retaining empty owner groups.

        Order each group by physical row ID. Do not require owners to exist or
        interpret stored metadata. Reads share the caller's current transaction.
        """

    @abc.abstractmethod
    def get_file_derivations_for_parents(
        self,
        file_ids: Iterable[int],
    ) -> dict[int, tuple[Mapping[str, Any], ...]]:
        """Batch rows by file_derivation_parent_file_id, retaining empty owner groups.

        Order each group by physical row ID. Do not require owners to exist or
        interpret stored metadata. Reads share the caller's current transaction.
        """

    @abc.abstractmethod
    def get_file_derivations_for_children(
        self,
        file_ids: Iterable[int],
    ) -> dict[int, tuple[Mapping[str, Any], ...]]:
        """Batch rows by file_derivation_child_file_id, retaining empty owner groups.

        Order each group by physical row ID. Do not require owners to exist or
        interpret stored metadata. Reads share the caller's current transaction.
        """

    @abc.abstractmethod
    def get_org_agent_relations_for_parents(
        self,
        agent_ids: Iterable[int],
    ) -> dict[int, tuple[Mapping[str, Any], ...]]:
        """Batch rows by org_agent_relation_parent_agent_id, retaining empty owner groups.

        Order each group by physical row ID. Do not require owners to exist or
        interpret stored metadata. Reads share the caller's current transaction.
        """

    @abc.abstractmethod
    def get_org_agent_relations_for_children(
        self,
        agent_ids: Iterable[int],
    ) -> dict[int, tuple[Mapping[str, Any], ...]]:
        """Batch rows by org_agent_relation_child_agent_id, retaining empty owner groups.

        Order each group by physical row ID. Do not require owners to exist or
        interpret stored metadata. Reads share the caller's current transaction.
        """

    @abc.abstractmethod
    def get_backup_workflow_sources_for_workflows(
        self,
        workflow_ids: Iterable[int],
    ) -> dict[int, tuple[Mapping[str, Any], ...]]:
        """Batch rows by backup_workflow_source_workflow_id, retaining empty owner groups.

        Order each group by physical row ID. Do not require owners to exist or
        interpret stored metadata. Reads share the caller's current transaction.
        """

    @abc.abstractmethod
    def get_backup_workflow_outputs_for_workflows(
        self,
        workflow_ids: Iterable[int],
    ) -> dict[int, tuple[Mapping[str, Any], ...]]:
        """Batch rows by backup_workflow_output_workflow_id, retaining empty owner groups.

        Order each group by physical row ID. Do not require owners to exist or
        interpret stored metadata. Reads share the caller's current transaction.
        """

    @abc.abstractmethod
    def get_backup_workflow_state_for_workflows(
        self,
        workflow_ids: Iterable[int],
    ) -> dict[int, tuple[Mapping[str, Any], ...]]:
        """Batch rows by backup_workflow_state_workflow_id, retaining empty owner groups.

        Order each group by physical row ID. Do not require owners to exist or
        interpret stored metadata. Reads share the caller's current transaction.
        """

    @abc.abstractmethod
    def get_backup_policies_by_ids(
        self,
        policy_ids: Iterable[int],
    ) -> dict[int, Mapping[str, Any]]:
        """Batch explicit policy references without resolving inheritance or defaults.

        Collapse repeated requests, omit missing rows and preserve request order.
        Bound ID lookups in shared portable SQL; no policy evaluation occurs here.
        """

    @abc.abstractmethod
    def get_replication_policies_by_ids(
        self,
        policy_ids: Iterable[int],
    ) -> dict[int, Mapping[str, Any]]:
        """Batch explicit policy references without resolving inheritance or defaults.

        Collapse repeated requests, omit missing rows and preserve request order.
        Bound ID lookups in shared portable SQL; no policy evaluation occurs here.
        """

    @abc.abstractmethod
    def get_backup_workflows_by_status(
        self,
        *,
        status: str | None = None,
    ) -> tuple[Mapping[str, Any], ...]:
        """Read workflow rows in ID order with an optional exact stored status.

        None selects all workflows. Lifecycle validation and declaration decoding
        belong to the backup owner; this query neither normalizes nor transitions.
        """

    @abc.abstractmethod
    def get_annotations_for_items(
        self,
        item_ids: Iterable[int],
        *,
        user_id: int | None = None,
        kind: str | None = None,
    ) -> dict[int, tuple[Mapping[str, Any], ...]]:
        """Batch annotations rows by their stored annotation_item_id references.

        Keep an ID-ordered tuple for every requested owner, including empty
        groups. No owner selection, validation or value normalization is applied.
        Optional filters use exact stored values; None leaves them unfiltered.
        Read the current connection without committing or creating a snapshot.
        """

    @abc.abstractmethod
    def get_entity_identifiers_for_owners(
        self,
        owner_ids: Iterable[int],
        *,
        entity_type: str,
    ) -> dict[int, tuple[Mapping[str, Any], ...]]:
        """Batch entity_identifiers rows by their stored entity_identifier_entity_id references.

        Keep an ID-ordered tuple for every requested owner, including empty
        groups. No owner selection, validation or value normalization is applied.
        Optional filters use exact stored values; None leaves them unfiltered.
        Read the current connection without committing or creating a snapshot.
        """

    @abc.abstractmethod
    def get_item_identifiers_for_items(
        self,
        item_ids: Iterable[int],
    ) -> dict[int, tuple[Mapping[str, Any], ...]]:
        """Batch item_identifiers rows by their stored item_identifier_item_id references.

        Keep an ID-ordered tuple for every requested owner, including empty
        groups. No owner selection, validation or value normalization is applied.
        Optional filters use exact stored values; None leaves them unfiltered.
        Read the current connection without committing or creating a snapshot.
        """

    @abc.abstractmethod
    def get_human_agents_for_agents(
        self,
        agent_ids: Iterable[int],
    ) -> dict[int, tuple[Mapping[str, Any], ...]]:
        """Batch human_agents rows by their stored human_agent_agent_id references.

        Keep an ID-ordered tuple for every requested owner, including empty
        groups. No owner selection, validation or value normalization is applied.
        Optional filters use exact stored values; None leaves them unfiltered.
        Read the current connection without committing or creating a snapshot.
        """

    @abc.abstractmethod
    def get_org_agents_for_agents(
        self,
        agent_ids: Iterable[int],
    ) -> dict[int, tuple[Mapping[str, Any], ...]]:
        """Batch org_agents rows by their stored org_agent_agent_id references.

        Keep an ID-ordered tuple for every requested owner, including empty
        groups. No owner selection, validation or value normalization is applied.
        Optional filters use exact stored values; None leaves them unfiltered.
        Read the current connection without committing or creating a snapshot.
        """
