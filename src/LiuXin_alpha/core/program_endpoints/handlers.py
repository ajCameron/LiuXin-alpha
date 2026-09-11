"""
Specify the named handler signatures consumed by each Core program endpoint provider.

Family protocols distinguish query and command envelopes while the aggregate
protocol checks the complete CoreProgramAPI surface statically at installation.
These protocols are not runtime-checkable validators, executable handlers, or a
promise that arbitrary implementations obey the documented service semantics.
Method descriptions point to the maintained service owner for validation, side
effects, and error details; field metadata is declared separately by providers.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Protocol

from LiuXin_alpha.core.commands import CoreCommand
from LiuXin_alpha.core.queries import CoreQuery

if TYPE_CHECKING:
    from LiuXin_alpha.core.runtime import CoreRuntime


class BackupMaintenanceHandlers(Protocol):
    """
    Require the backup/checkpoint, SquashFS submission, and maintenance methods consumed by their endpoint provider.

    Planning and workflow reads are queries; saving, job submission, cleaning,
    and merging retain distinct command envelopes.

    Example:
        >>> from LiuXin_alpha.core.program_endpoints import backup_maintenance
        >>> backup_maintenance.install_queries(implementation, runtime)  # doctest: +SKIP
    """

    def backup_plan(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        Ask the Store backup planner to group source files into destination-bound artifact plans.

        See :func:`LiuXin_alpha.core.program_services.backup.backup_plan`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.backup_plan(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime supplying live Store resolution and the storage manager used by the planner.
        :param query: Query with source_store, destination_store, target_pack_size_bytes, and optional workflow_name_prefix, output_key_prefix, max_files_per_pack, and allowed_extensions.
        :return: Projected packs and their count; the planner result must be sized as well as iterable.
        """
        ...

    def backup_squashfs_publish_files_start(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Submit selected file IDs for SquashFS publication after validating a nonempty ID sequence and archive text.

        See :func:`LiuXin_alpha.core.program_services.backup.backup_squashfs_publish_files_start`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.backup_squashfs_publish_files_start(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime providing database path/type and job submission.
        :param command: Command with file_ids, archive, optional store_name/compression, build switches, and shared job fields.
        :return: run_publish_squashfs_files_job submission receipt, not published-Store verification.
        :raises CoreDispatchError: For an absent/empty/non-sequence file list, invalid archive, or unavailable database path.
        """
        ...

    def backup_squashfs_publish_store_start(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Submit publication of an open SquashFS Store with caller-selected archive and build policy.

        See :func:`LiuXin_alpha.core.program_services.backup.backup_squashfs_publish_store_start`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.backup_squashfs_publish_store_start(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime supplying path-backed database identity and job submission.
        :param command: Command with store_id, optional output_archive/compression, publication switches, and shared job fields.
        :return: run_publish_open_squashfs_store_job submission receipt with publish SquashFS store as fallback label.
        """
        ...

    def backup_squashfs_start(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Submit a SquashFS backup workflow mapping with verification and staging options.

        See :func:`LiuXin_alpha.core.program_services.backup.backup_squashfs_start`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.backup_squashfs_start(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime providing a path-backed database identity and job submission.
        :param command: Command with workflow_spec, optional verify_after_build, cleanup_staging_after_success, staging_root, and shared job fields.
        :return: run_squashfs_backup_job submission receipt, not an archive/build outcome.
        """
        ...

    def backup_workflow_get(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        Load one durable backup checkpoint and expose its declaration alongside the full projected state.

        See :func:`LiuXin_alpha.core.program_services.backup.backup_workflow_get`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.backup_workflow_get(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime providing the checkpoint repository's database.
        :param query: Query with required integer-convertible workflow_id, excluding bool.
        :return: workflow_id, projected declaration under spec, and projected checkpoint under state.
        """
        ...

    def backup_workflow_save(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Parse a workflow declaration and persist it as DRAFT, creating or targeting a supplied workflow ID.

        See :func:`LiuXin_alpha.core.program_services.backup.backup_workflow_save`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.backup_workflow_save(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime whose database stores workflow declarations.
        :param command: Command with Mapping workflow_spec and optional positive workflow_id.
        :return: Integer saved workflow_id, input-derived created flag, and projected declaration.
        """
        ...

    def backup_workflow_start(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Validate that a durable workflow declaration loads, then submit a worker to execute it by ID.

        See :func:`LiuXin_alpha.core.program_services.backup.backup_workflow_start`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.backup_workflow_start(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime providing the declaration repository, database path/type, and job manager.
        :param command: Command with workflow_id and optional shared job timeout/backend/output/label fields.
        :return: run_persisted_backup_job submission receipt with backup workflow <id> as fallback label.
        """
        ...

    def backup_workflows_list(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        Enumerate durable workflows by ID and load checkpoints only for the selected page.

        See :func:`LiuXin_alpha.core.program_services.backup.backup_workflows_list`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.backup_workflows_list(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime whose database contains backup_workflows and checkpoint state.
        :param query: Query carrying optional limit/offset page bounds.
        :return: Ordered checkpoint-summary records, full workflow total, and requested bounds.
        """
        ...

    def maintenance_clean(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Clean explicitly selected rows through the maintenance service, then reconcile read-side state.

        See :func:`LiuXin_alpha.core.program_services.maintenance.maintenance_clean`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.maintenance_clean(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime supplying maintenance.clean and service reconciliation.
        :param command: Command with required table and row_ids array; no confirmation field is checked here.
        :return: Reconciled cleaned=True receipt with table and converted row_ids.
        :raises CoreDispatchError: If the request shape or clean capability is unavailable.
        """
        ...

    def maintenance_duplicates_find(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        Delegate duplicate-value discovery for one table/column to the legacy maintenance helper.

        See :func:`LiuXin_alpha.core.program_services.maintenance.maintenance_duplicates_find`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.maintenance_duplicates_find(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime whose database is inspected by find_duplicates.
        :param query: Query with required table/column text and optional comparison mode.
        :return: Selected table, column, comparison, and plain-projected duplicate groups.
        """
        ...

    def maintenance_merge(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Merge two distinct row IDs through the maintenance service and reconcile after delegation.

        See :func:`LiuXin_alpha.core.program_services.maintenance.maintenance_merge`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.maintenance_merge(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime providing maintenance.merge and post-write reconciliation.
        :param command: Command with table, retained_id, and a distinct merged_id.
        :return: Reconciled table/ID receipt with merged=True; reconciliation may fail after the merge.
        :raises CoreDispatchError: If request validation fails, IDs are equal, or merge is unavailable.
        """
        ...

    def maintenance_run(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Run one maintenance-service pass with a positive event budget and project its plugin results.

        See :func:`LiuXin_alpha.core.program_services.maintenance.maintenance_run`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.maintenance_run(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime whose maintenance service provides run_once.
        :param command: Command with optional positive integer-convertible max_events, excluding bool.
        :return: Requested event budget and plain-projected run_once result under plugins.
        """
        ...

    def maintenance_status(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        Describe the maintenance service's optional plugins, database dirty count, and queue sizes.

        See :func:`LiuXin_alpha.core.program_services.maintenance.maintenance_status`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.maintenance_status(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime supplying the maintenance service and database dirty-count capability.
        :param query: Ignored query envelope; status accepts no payload options here.
        :return: Service type, plugin name/type records, dirty_count, and optional queue sizes.
        """
        ...


class CatalogSearchHandlers(Protocol):
    """
    Require field discovery, WEMI/Agent/identifier traversal and mutation, and global-search handlers.

    Catalog inspection and search are queries. Identifier replacement and
    Agent linking are explicit command methods rather than generic mutation hooks.

    Example:
        >>> from LiuXin_alpha.core.program_endpoints import catalog_search
        >>> catalog_search.install_queries(implementation, runtime)  # doctest: +SKIP
    """

    def catalog_agent_link(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Link an Agent to a WEMI entity using normalized role and optional priority, then reconcile.

        See :func:`LiuXin_alpha.core.program_services.catalog.catalog_agent_link`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.catalog_agent_link(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime supplying Agents.link_to_wemi and service reconciliation.
        :param command: Command with agent_id, level, entity_id, and optional role/priority.
        :return: Reconciled Agent/entity IDs, normalized level, projected link result, and linked=True.
        """
        ...

    def catalog_agents_list(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        List WEMI-linked Agents and optionally retain only projected links matching a normalized role.

        See :func:`LiuXin_alpha.core.program_services.catalog.catalog_agents_list`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.catalog_agents_list(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime providing the Agents repository's list_for_wemi capability.
        :param query: Query with level, entity_id, and optional role name/code.
        :return: Normalized level/ID, original role text or None, filtered projected Agents, and count.
        """
        ...

    def catalog_fields_get(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        Look up one required field key, distinguishing None from falsey-but-present metadata.

        See :func:`LiuXin_alpha.core.program_services.catalog.catalog_fields_get`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.catalog_fields_get(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime supplying field_metadata.get.
        :param query: Query containing a required stripped key.
        :return: Key, exists based solely on non-None metadata, and its plain projection.
        """
        ...

    def catalog_fields_list(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        Enumerate one field-metadata family and project each key's metadata in provider order.

        See :func:`LiuXin_alpha.core.program_services.catalog.catalog_fields_list`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.catalog_fields_list(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime exposing the shared field_metadata service.
        :param query: Query with kind (all, sortable, displayable, standard, custom, searchable) and optional include_composites=True.
        :return: Normalized kind, ordered key/projected-metadata records, and their count.
        :raises CoreDispatchError: If the normalized kind is not supported.
        """
        ...

    def catalog_hierarchy_list(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        Retrieve parent or child WEMI adjacency and project the returned entities without paging.

        See :func:`LiuXin_alpha.core.program_services.catalog.catalog_hierarchy_list`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.catalog_hierarchy_list(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime whose Catalog supplies hierarchy.parents and hierarchy.children.
        :param query: Query with level, entity_id, and optional children/parents direction.
        :return: Adjacency labels, related result_level, and projected entities in retriever order.
        :raises CoreDispatchError: For invalid request fields/direction or a ValueError during adjacency retrieval.
        """
        ...

    def catalog_identifiers_list(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        List identifiers for an Agent or WEMI entity through the corresponding repository capability.

        See :func:`LiuXin_alpha.core.program_services.catalog.catalog_identifiers_list`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.catalog_identifiers_list(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime exposing Catalog identifier repository capabilities.
        :param query: Query containing level and required integer-convertible entity_id.
        :return: Normalized requested level/ID, projected identifier list, and list count.
        """
        ...

    def catalog_identifiers_primary_values(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        Retrieve repository-selected primary identifier values for one WEMI entity.

        See :func:`LiuXin_alpha.core.program_services.catalog.catalog_identifiers_primary_values`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.catalog_identifiers_primary_values(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime whose identifier repository implements primary_values_for_wemi.
        :param query: Query containing required level text and integer-convertible entity_id.
        :return: Lowercased level/ID, identifier dictionary, and length of the original result.
        """
        ...

    def catalog_identifiers_replace(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Replace WEMI identifiers using the repository's own input policy, then project and reconcile.

        See :func:`LiuXin_alpha.core.program_services.catalog.catalog_identifiers_replace`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.catalog_identifiers_replace(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime providing replace_for_wemi and read-side reconciliation.
        :param command: Command with level, entity_id, and required identifiers value.
        :return: Reconciled lowercased level/ID, projected repository result, and updated=True.
        :raises CoreDispatchError: If the required payload/capability is unavailable.
        """
        ...

    def search_global(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        Scan selected tables for a casefolded text substring, then page all accumulated matches.

        See :func:`LiuXin_alpha.core.program_services.catalog.search_global`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.search_global(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime whose selected read_source enumerates tables and rows.
        :param query: Query with required text and optional tables, offset, and limit.
        :return: Text, successfully opened table names, page items, observed total, bounds, and has_more; no completeness guarantee for skipped tables.
        """
        ...


class ContentWorkflowsHandlers(Protocol):
    """
    Require ingestion, conversion, metadata-file, and online-metadata handlers at the content workflow boundary.

    Capability/metadata inspection uses queries. File writes and managed-job
    submission use commands; a returned submission receipt is not job completion.

    Example:
        >>> from LiuXin_alpha.core.program_endpoints import content_workflows
        >>> content_workflows.install_queries(implementation, runtime)  # doctest: +SKIP
    """

    def conversion_formats(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        List conversion input/output formats advertised by the current customization registry.

        See :func:`LiuXin_alpha.core.program_services.conversion.conversion_formats`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.conversion_formats(runtime, query)  # doctest: +SKIP


        :param runtime: Unused runtime; discovery uses the process-wide customization registry.
        :param query: Unused query envelope; no payload filters are consumed.
        :return: input and output format-name lists; discovery/import failures propagate.
        """
        ...

    def conversion_options(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        Construct a path-selected Plumber and describe its input, output, and pipeline option recommendations.

        See :func:`LiuXin_alpha.core.program_services.conversion.conversion_options`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.conversion_options(runtime, query)  # doctest: +SKIP


        :param runtime: Unused runtime; Plumber is constructed with the shared default logger.
        :param query: Query with required stripped input_path and output_path text selecting conversion formats.
        :return: Plumber-reported input/output formats and ordered unique option descriptions.
        """
        ...

    def conversion_start(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Submit a named conversion workflow job with copied options and required input/output paths.

        See :func:`LiuXin_alpha.core.program_services.conversion.conversion_start`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.conversion_start(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime whose job manager accepts the run_conversion_job request.
        :param command: Command with input_path, output_path, optional Mapping options, and shared job submission fields.
        :return: Job ID and submission options, using convert as the fallback label; not a conversion outcome.
        """
        ...

    def ingest_disk_start(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Submit an unmanaged-disk ingestion job carrying the current path-backed database identity.

        See :func:`LiuXin_alpha.core.program_services.ingest.ingest_disk_start`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.ingest_disk_start(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime supplying database path/type and job submission.
        :param command: Command with disk_path and optional store_name, ebook_extensions, source_label, compute_hash, follow_symlinks, attach_store_links, refresh_storage_manager, and shared job fields.
        :return: Submission receipt for run_ingest_disk_job, with ingest disk as the fallback label.
        """
        ...

    def ingest_formats(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        Report ebook extensions and metadata-file types known to the imported registries.

        See :func:`LiuXin_alpha.core.program_services.ingest.ingest_formats`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.ingest_formats(runtime, query)  # doctest: +SKIP


        :param runtime: Unused runtime; discovery reads module-level format registries.
        :param query: Unused query envelope; no input file is opened for detection.
        :return: ebook_extensions and metadata_extensions lists.
        """
        ...

    def ingest_remote_html_start(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Submit a native-HTML or wget-HTML ingestion job after normalizing its provider name.

        See :func:`LiuXin_alpha.core.program_services.ingest.ingest_remote_html_start`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.ingest_remote_html_start(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime supplying a path-backed database identity and job manager.
        :param command: Command with kind, required Mapping options, and shared job submission fields.
        :return: run_ingest_remote_html_job submission receipt with the normalized kind in the fallback label.
        :raises CoreDispatchError: If kind/options validation fails or a database path is unavailable.
        """
        ...

    def metadata_covers_start(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Submit online cover lookup with normalized author/identifier hints and a worker timeout.

        See :func:`LiuXin_alpha.core.program_services.metadata.metadata_covers_start`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.metadata_covers_start(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime whose job manager accepts run_metadata_cover_job.
        :param command: Command with title/authors/identifiers, optional timeout_s, and shared job submission fields.
        :return: Job submission receipt with metadata covers as fallback label, not cover bytes or a success result.
        :raises CoreDispatchError: For invalid Mapping/list hints or no usable search hint.
        """
        ...

    def metadata_file_formats(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        List registered readable metadata types and the subset with an enabled metadata writer.

        See :func:`LiuXin_alpha.core.program_services.metadata.metadata_file_formats`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.metadata_file_formats(runtime, query)  # doctest: +SKIP


        :param runtime: Unused runtime; discovery uses process-wide metadata/customization registries.
        :param query: Ignored query envelope; no file-type filter is consumed.
        :return: Sorted readable types and writer-enabled writable subset.
        """
        ...

    def metadata_file_inspect(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        Read metadata from exactly one nonblank path or strict-base64 input and project it for Core.

        See :func:`LiuXin_alpha.core.program_services.metadata.metadata_file_inspect`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.metadata_file_inspect(runtime, query)  # doctest: +SKIP


        :param runtime: Unused runtime; metadata is read through the file-source registry.
        :param query: Query containing exactly one of path/base64 and optional file_type, required for bytes.
        :return: Type hint/suffix and plain-projected metadata; reader/projection failures propagate.
        :raises CoreDispatchError: For ambiguous/missing input, invalid base64, or byte input without a type.
        """
        ...

    def metadata_file_write(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Rewrite metadata in an existing path or decoded byte stream using an enabled format writer.

        See :func:`LiuXin_alpha.core.program_services.metadata.metadata_file_write`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.metadata_file_write(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime supplying optional Item metadata hydration; arbitrary supplied paths are not confined here.
        :param command: Command with path or base64, optional/inferred file_type, and item_id or metadata for the writer.
        :return: Type, path-or-None, rewritten bytes for byte input or content=None for path input, byte size, and updated=True.
        :raises CoreDispatchError: For input/type/base64/metadata validation, unavailable writers, or collected writer failures.
        """
        ...

    def metadata_identify_start(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Submit online metadata identification using at least one title, author, or identifier hint.

        See :func:`LiuXin_alpha.core.program_services.metadata.metadata_identify_start`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.metadata_identify_start(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime whose job manager accepts run_metadata_identify_job.
        :param command: Command with title/authors/identifiers hints, optional allowed_plugins and timeout_s, plus shared job fields.
        :return: Submission receipt with metadata identify as fallback label; no search results are fetched here.
        :raises CoreDispatchError: For invalid Mapping/list hints or an entirely empty search request.
        """
        ...

    def metadata_online_sources(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        Describe identify/cover plugin names, first-seen versions, merged capabilities, and configuration checks.

        See :func:`LiuXin_alpha.core.program_services.metadata.metadata_online_sources`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.metadata_online_sources(runtime, query)  # doctest: +SKIP


        :param runtime: Unused runtime; discovery consults the global plugin registry.
        :param query: Ignored query envelope; no credentials or plugin filter is consumed.
        :return: sources list with merged name/version/capabilities/configured descriptions, not availability guarantees.
        """
        ...


class DatabaseSchemaHandlers(Protocol):
    """
    Require database administration, column/link policy, preference, and custom-field handler entry points.

    Inspection and migration planning remain queries. Backup, vacuum, migration
    application, policy edits, preferences, and custom-field writes are commands.

    Example:
        >>> from LiuXin_alpha.core.program_endpoints import database_schema
        >>> database_schema.install_queries(implementation, runtime)  # doctest: +SKIP
    """

    def custom_fields_create(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Create a custom field through database-first capability lookup, then refresh field metadata and reconcile.

        See :func:`LiuXin_alpha.core.program_services.schema.custom_fields_create`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.custom_fields_create(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime supplying create_custom_column, field metadata refresh, and reconciliation.
        :param command: Command with name and optional datatype, is_multiple, label, editable, display, table, and make_category.
        :return: Reconciled created=True/schema_changed=True receipt and integer field num.
        :raises CoreDispatchError: If required text/display validation fails or creation capability is unavailable.
        """
        ...

    def custom_fields_delete(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Delegate custom-field deletion by optional numeric ID and/or label, then refresh metadata and reconcile.

        See :func:`LiuXin_alpha.core.program_services.schema.custom_fields_delete`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.custom_fields_delete(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime providing database-first deletion capability and metadata refresh/reconciliation.
        :param command: Command with optional num and label selecting the custom field.
        :return: Reconciled selectors and deleted=True/schema_changed=True after delegation and refresh.
        :raises CoreDispatchError: If selectors are invalid/absent or deletion capability is unavailable.
        """
        ...

    def custom_fields_list(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        List projected custom-column definitions, falling back to a label-sorted legacy map after canonical read failure.

        See :func:`LiuXin_alpha.core.program_services.schema.custom_fields_list`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.custom_fields_list(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime providing database custom_columns rows and optional custom_column_label_map.
        :param query: Ignored query envelope; the complete discovered set is returned without paging.
        :return: Normalized field mappings and count; an empty result does not prove canonical-read availability.
        """
        ...

    def custom_fields_update(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Forward allowlisted custom-field metadata changes, refresh the field registry, and reconcile.

        See :func:`LiuXin_alpha.core.program_services.schema.custom_fields_update`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.custom_fields_update(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime supplying database-first set_custom_column_metadata and read-side refresh/reconciliation.
        :param command: Command with required integer-convertible num and Mapping changes.
        :return: Reconciled field num, optimistic update/schema flags, and projected changed_row_ids result.
        :raises CoreDispatchError: If required fields are invalid, unknown keys are supplied, or capability lookup fails.
        """
        ...

    def database_backup(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Invoke backend backup and optionally open the reported file read-only for SQLite quick_check.

        See :func:`LiuXin_alpha.core.program_services.database.database_backup`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.database_backup(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime providing the selected database and its optional driver backup capability.
        :param command: Command with optional output_path and truth-tested verify=False.
        :return: backed_up=True, stringified backup_path or None, and optional quick_check outcome/messages.
        :raises CoreDispatchError: For unsupported explicit paths, unavailable verification paths, SQLite verification errors, or a non-ok check.
        """
        ...

    def database_info(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        Describe database identity and shallow metadata, suppressing ordinary identity-attribute access failures.

        See :func:`LiuXin_alpha.core.program_services.database.database_info`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.database_info(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime exposing the database object to inspect.
        :param query: Ignored query envelope; no fields are consumed.
        :return: Raw identity/type values, optional exists flag, and copied Mapping metadata or an empty dictionary.
        """
        ...

    def database_migrations_apply(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Apply storage-schema migration, then identity migration, refresh field metadata, and reconcile in sequence.

        See :func:`LiuXin_alpha.core.program_services.database.database_migrations_apply`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.database_migrations_apply(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime supplying the database, identity migrator, field metadata refresh, and reconciliation.
        :param command: Ignored command envelope; both migrations are always attempted in order.
        :return: Reconciled migrated=True receipt with projected storage and identity reports after all preceding steps succeed.
        """
        ...

    def database_migrations_plan(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        Derive suggested additive migrations and collision count from a fresh status report without applying changes.

        See :func:`LiuXin_alpha.core.program_services.database.database_migrations_plan`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.database_migrations_plan(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime used for the underlying migration-status inspection.
        :param query: Query forwarded to database_migrations_status without additional options.
        :return: Status, ordered storage/identity action suggestions, collision count, and a message based only on action presence.
        """
        ...

    def database_migrations_status(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        Compare the storage migration ledger with two required IDs and audit normalized identities.

        See :func:`LiuXin_alpha.core.program_services.database.database_migrations_status`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.database_migrations_status(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime providing the database ledger and database-first identity audit capability.
        :param query: Ignored query envelope; the required storage schema version is fixed at two.
        :return: Pending storage IDs, identity report/clean/update state, and ok only when all three checks pass.
        """
        ...

    def database_summary(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        Count advertised tables and optionally group them by category, retaining failed row counts as None.

        See :func:`LiuXin_alpha.core.program_services.database.database_summary`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.database_summary(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime supplying database table enumeration, row counts, and optional categorization.
        :param query: Ignored query envelope; no table filter or refresh option is consumed.
        :return: Advertised table_count, partial known row_total, counts, and optional category groups.
        """
        ...

    def database_telemetry(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        Read optional write telemetry and dirty counters as independent database observations.

        See :func:`LiuXin_alpha.core.program_services.database.database_telemetry`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.database_telemetry(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime exposing database telemetry and optional dirty-count methods.
        :param query: Ignored query envelope; counters are not reset by this adapter.
        :return: Projected write snapshot plus optional dirty_count and persisted_dirty_count integers.
        """
        ...

    def database_vacuum(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Invoke database vacuum, falling back to backend vacuum only when the database callable is unavailable.

        See :func:`LiuXin_alpha.core.program_services.database.database_vacuum`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.database_vacuum(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime exposing database and its optional backend vacuum operation.
        :param command: Ignored command envelope; no compaction options or confirmation fields are consumed.
        :return: Dictionary containing vacuumed=True after the selected callable returns.
        :raises CoreDispatchError: If neither object supplies a callable vacuum method.
        """
        ...

    def preferences_delete(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Delete an existing preference by item deletion, treating failed membership checks as absence.

        See :func:`LiuXin_alpha.core.program_services.preferences.preferences_delete`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.preferences_delete(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime providing the selected preference store with membership and deletion support.
        :param command: Command with text-normalized key and optional scope defaulting to library.
        :return: Scope, key, and deleted flag reflecting the successful pre-deletion membership check.
        :raises CoreDispatchError: If payload/scope is invalid or key is absent/blank after text conversion.
        """
        ...

    def preferences_get(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        Read one key with an optional default and report a separately determined membership flag.

        See :func:`LiuXin_alpha.core.program_services.preferences.preferences_get`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.preferences_get(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime providing the selected preference store.
        :param query: Query containing key, optional scope, and optional default (None when omitted).
        :return: Scope, normalized key, membership estimate, and value or caller default.
        :raises CoreDispatchError: If payload/scope is invalid or key is missing or blank after text conversion.
        """
        ...

    def preferences_list(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        Snapshot preference items from a Mapping or an object with a callable items method.

        See :func:`LiuXin_alpha.core.program_services.preferences.preferences_list`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.preferences_list(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime providing the requested preference store through its services.
        :param query: Query with optional scope, defaulting to library for missing or falsey values.
        :return: Normalized input scope token and a shallow dictionary of the store's current items.
        :raises CoreDispatchError: If payload/scope is invalid or the store has neither Mapping support nor callable items.
        """
        ...

    def preferences_set(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Set one preference using callable store.set when available, otherwise item assignment.

        See :func:`LiuXin_alpha.core.program_services.preferences.preferences_set`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.preferences_set(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime providing the selected writable preference store.
        :param command: Command with text-normalized key, required value, and optional scope defaulting to library.
        :return: Scope, key, original supplied value, and updated=True after the write call returns.
        :raises CoreDispatchError: If payload/scope is invalid, key is absent/blank, or value is absent.
        """
        ...

    def schema_column(self, runtime: CoreRuntime, query: CoreQuery) -> object:
        """
        Read one column's policy through the database-level metadata capability and project the result.

        See :func:`LiuXin_alpha.core.program_services.schema.schema_column`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.schema_column(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime exposing database.get_column_metadata.
        :param query: Query with required stripped table and column text.
        :return: Plain-projected column metadata value; backend failures propagate.
        """
        ...

    def schema_column_update(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Build a replacement column policy from current metadata and supplied known fields, then persist and reconcile.

        See :func:`LiuXin_alpha.core.program_services.schema.schema_column_update`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.schema_column_update(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime exposing database column-metadata get/set and service reconciliation.
        :param command: Command with table, column, and Mapping policy containing optional known ColumnMetadata fields.
        :return: Reconciled table/column receipt with updated=True and the projected constructed policy.
        """
        ...

    def schema_link(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        Describe database-declared relationship capabilities for a requested pair of tables.

        See :func:`LiuXin_alpha.core.program_services.schema.schema_link`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.schema_link(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime whose database exposes get_link_capabilities.
        :param query: Query with required table and related_table text.
        :return: Requested table labels and projected relationship capabilities.
        """
        ...


class StorageHandlers(Protocol):
    """
    Require Store administration, storage health/planning, verification/repair, evacuation, and ingest-recovery handlers.

    This provider contract supplements, rather than replaces, the separate
    Core storage graph and base file APIs. Planning and applying are distinct methods.

    Example:
        >>> from LiuXin_alpha.core.program_endpoints import storage
        >>> storage.install_queries(implementation, runtime)  # doctest: +SKIP
    """

    def storage_asset_verify(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Verify an Asset using optional replica selection and report readability as the top-level healthy flag.

        See :func:`LiuXin_alpha.core.program_services.storage_integrity.storage_asset_verify`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.storage_asset_verify(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime providing storage.verify_digital_asset and its replica-selection policy.
        :param command: Command with asset_id and optional replica_ids/all_replicas.
        :return: Asset ID, healthy derived from report.readable, and the projected detailed report.
        :raises CoreDispatchError: If Asset ID or replica-selection validation fails.
        """
        ...

    def storage_audit(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Materialize non-deleted replicas, sort by numeric ID, and verify one bounded page with per-call error receipts.

        See :func:`LiuXin_alpha.core.program_services.storage_integrity.storage_audit`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.storage_audit(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime whose storage manager enumerates and verifies replicas.
        :param command: Command with optional limit, offset, and truth-tested calculate_digests=True.
        :return: Page results, selected health/error counts, full non-deleted count, effective bounds, and has_more.
        """
        ...

    def storage_backends_list(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        Describe default-registry backend kinds, declared capabilities, characteristics, and limitations.

        See :func:`LiuXin_alpha.core.program_services.stores.storage_backends_list`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.storage_backends_list(runtime, query)  # doctest: +SKIP


        :param runtime: Unused runtime; discovery uses DEFAULT_BACKEND_REGISTRY.
        :param query: Query with optional truth-tested include_internal, defaulting to False.
        :return: Backend descriptor list, count, and non-secret configuration guidance.
        """
        ...

    def storage_default_get(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        Resolve the manager's default Store, treating ordinary default/Store lookup errors as unconfigured.

        See :func:`LiuXin_alpha.core.program_services.stores.storage_default_get`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.storage_default_get(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime whose storage manager supplies the default reference and Store.
        :param query: Ignored query envelope; no payload fields are consumed.
        :return: configured=False/store=None on lookup failure, otherwise UUID/name/root and projected configuration.
        """
        ...

    def storage_default_set(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Resolve a live Store and ask the manager to select it as default, then read its name for the receipt.

        See :func:`LiuXin_alpha.core.program_services.stores.storage_default_set`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.storage_default_set(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime providing live Store resolution and default-store selection.
        :param command: Command containing a required nonempty store reference.
        :return: selected=True and the selected Store's reference and configuration name.
        """
        ...

    def storage_file_copy(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Read an entire managed Asset, add its bytes through library policy, and locate the returned Asset.

        See :func:`LiuXin_alpha.core.program_services.stores.storage_file_copy`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.storage_file_copy(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime supplying library read/add/locate operations and optional Store selection.
        :param command: Command with required asset_id, optional Mapping metadata, and optional target store.
        :return: Source ID, projected returned Asset and Location, and source content length in size.
        """
        ...

    def storage_location_stat(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        Construct a UUID/key Location and delegate stat without converting absence or errors into a negative result.

        See :func:`LiuXin_alpha.core.program_services.stores.storage_location_stat`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.storage_location_stat(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime supplying storage.stat for the constructed Location.
        :param query: Query with required stripped store_uuid and key text.
        :return: Projected location, exists=True, and projected stat result after successful delegation.
        """
        ...

    def storage_reconcile_apply(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Always attempt one Store reload, then verify a bounded set of candidate replicas and report later operational health.

        See :func:`LiuXin_alpha.core.program_services.storage_integrity.storage_reconcile_apply`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.storage_reconcile_apply(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime supplying manager reload, replica enumeration/verification, and status snapshots.
        :param command: Command with optional max_actions and truth-tested include_offline=False for reload.
        :return: Before/after status, action receipts, truncation flag, later-health ok, and the fixed deferred-scope explanation.
        """
        ...

    def storage_reconcile_plan(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        Partition current recovery suggestions into reload/verification actions and explicitly deferred operations.

        See :func:`LiuXin_alpha.core.program_services.storage_integrity.storage_reconcile_plan`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.storage_reconcile_plan(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime whose storage manager supplies operational status and recovery actions.
        :param query: Query with optional refresh_stores=False.
        :return: Status health/projection and automatic/deferred action lists, with the fixed apply-scope explanation.
        """
        ...

    def storage_recovery_list(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        Materialize ingest operations, optionally filter their state, and return a bounded page.

        See :func:`LiuXin_alpha.core.program_services.storage_recovery.storage_recovery_list`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.storage_recovery_list(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime whose storage manager lists durable ingest-operation mappings.
        :param query: Query with optional state filter and limit/offset page bounds.
        :return: Plain-projected operations, filtered total, effective bounds, and end-of-list completeness.
        """
        ...

    def storage_recovery_recover_pending(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Recover one UUID-selected pending ingest, or all pending ingests when no selector is supplied.

        See :func:`LiuXin_alpha.core.program_services.storage_recovery.storage_recovery_recover_pending`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.storage_recovery_recover_pending(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime providing synchronous storage recovery and operation listing.
        :param command: Command with optional operation_id; no separate confirmation is consumed.
        :return: ok, parsed UUID-or-None selector, materialized issues, and all projected operations.
        :raises CoreDispatchError: If UUID construction raises ValueError for the supplied selector.
        """
        ...

    def storage_recovery_retry_ingest(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Retry one required UUID-selected ingest synchronously and project the manager's result.

        See :func:`LiuXin_alpha.core.program_services.storage_recovery.storage_recovery_retry_ingest`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.storage_recovery_retry_ingest(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime whose storage manager implements retry_ingest_operation.
        :param command: Command with required operation_id text parseable as a UUID.
        :return: ok=True, parsed operation UUID, and plain-projected retry result.
        :raises CoreDispatchError: If operation_id is missing/blank or UUID parsing raises ValueError.
        """
        ...

    def storage_repair_apply(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Plan, attempt bounded verification/copy actions, and replan the same selected scope without removing replicas.

        See :func:`LiuXin_alpha.core.program_services.storage_repair.storage_repair_apply`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.storage_repair_apply(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime providing repair planning, replica verification, and Asset replication.
        :param command: Command with optional asset_id, max_assets, max_actions, and max_transfer_bytes; no stored plan or confirmation is consumed.
        :return: Before/after plans, attempt receipts/counts, estimated successful transfer bytes, budgets, truncation, and selected-scope ok.
        """
        ...

    def storage_repair_plan(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        Build a fresh non-deleting repair plan for one Asset or a capped prefix of the Asset registry.

        See :func:`LiuXin_alpha.core.program_services.storage_repair.storage_repair_plan`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.storage_repair_plan(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime whose storage manager supplies the repair-planning APIs.
        :param query: Query with optional asset_id and max_assets.
        :return: Fresh plan with selected-scope completeness, actions, deferrals, estimates, and warnings.
        """
        ...

    def storage_replica_verify(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Verify one replica through the manager and expose the report's health flag.

        See :func:`LiuXin_alpha.core.program_services.storage_integrity.storage_replica_verify`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.storage_replica_verify(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime supplying storage.verify_replica.
        :param command: Command with required integer-convertible replica_id and optional calculate_digests.
        :return: Replica ID, truth-tested report.healthy, and the projected verification report.
        """
        ...

    def storage_source_register(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Invoke an allowlisted library source-registration method with caller options and reconcile afterwards.

        See :func:`LiuXin_alpha.core.program_services.stores.storage_source_register`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.storage_source_register(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime whose library provides the selected registration method and reconciliation.
        :param command: Command with kind and required Mapping options for one advertised source family.
        :return: Reconciled normalized kind, registered=True, and projected library report; not an independent registration probe.
        :raises CoreDispatchError: If kind/options validation fails or the required library callable is unavailable.
        """
        ...

    def storage_sources_supported(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        Advertise the five fixed source-registration families without probing runtime capabilities or installed tools.

        See :func:`LiuXin_alpha.core.program_services.stores.storage_sources_supported`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.storage_sources_supported(runtime, query)  # doctest: +SKIP


        :param runtime: Unused runtime; returned families are static registration metadata.
        :param query: Unused query envelope; no filtering is supported.
        :return: Ordered kind/method/remote declarations for disk, rclone HTTP, wget HTML, native HTML, and open SquashFS.
        """
        ...

    def storage_status(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        Assemble an unpaged storage overview from manager status, durable configuration, Assets, and live replica claims.

        See :func:`LiuXin_alpha.core.program_services.storage_status.storage_status`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.storage_status(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime providing storage observations/registries and canonical Store rows.
        :param query: Query with optional truth-tested refresh_stores=False; no paging/filter fields are consumed.
        :return: Overall health/time, accounting summary, sorted Store records, invalid-row diagnostics, overview issues, and manager status projection.
        """
        ...

    def storage_store_delete(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Unregister a Store and optionally forget configuration and delete matching canonical Store rows.

        See :func:`LiuXin_alpha.core.program_services.stores.storage_store_delete`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.storage_store_delete(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime providing Store configuration lookup, manager removal, and database search/delete.
        :param command: Command with required store and optional delete_from_database switch.
        :return: Original selector, raw removal outcome as unregistered, and requested-and-removed database-deletion flag.
        """
        ...

    def storage_store_evacuate_apply(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Replan, execute bounded evacuation, and replan again to produce a synchronous attempt receipt.

        See :func:`LiuXin_alpha.core.program_services.storage_evacuation.storage_store_evacuate_apply`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.storage_store_evacuate_apply(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime providing Store resolution and the storage manager used for the entire attempt.
        :param command: Command with store, optional destination_store, max_assets, max_actions, max_transfer_bytes, and keep_source_bytes.
        :return: Before/after plans and execution receipts; actions_applied counts all receipts, and ok requires no failures/truncation plus zero after-plan source claims.
        :raises CoreDispatchError: If required source, non-null bounds, or Store/planning constraints are invalid.
        :raises AssertionError: If any explicit bound is None while assertions are enabled.
        """
        ...

    def storage_store_evacuate_plan(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        Validate a plan query's source and Asset bound, then return a fresh evacuation plan receipt.

        See :func:`LiuXin_alpha.core.program_services.storage_evacuation.storage_store_evacuate_plan`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.storage_store_evacuate_plan(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime with library storage and Store-reference lookup services.
        :param query: Query carrying store, optional destination_store, and optional max_assets.
        :return: Wire plan snapshot; complete/blocked fields describe selection and shortfall, not execution success.
        :raises CoreDispatchError: If store is absent/empty, a non-null bound is invalid, or Store resolution/planning rejects it.
        :raises AssertionError: If max_assets is explicitly None while assertions are enabled.
        """
        ...

    def storage_store_get(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        Describe a live Store, falling back to durable configuration only on CoreDispatchError during lookup.

        See :func:`LiuXin_alpha.core.program_services.stores.storage_store_get`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.storage_store_get(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime supplying live Store and durable configuration resolution.
        :param query: Query with required store reference accepted by the shared Store resolvers.
        :return: Projected configuration/status, UUID/name/root labels, and loaded flag.
        :raises CoreDispatchError: If the selector is absent/empty or both resolution paths fail.
        """
        ...

    def storage_store_probe(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Probe one live Store and then separately read its current status.

        See :func:`LiuXin_alpha.core.program_services.stores.storage_store_probe`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.storage_store_probe(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime supplying live Store resolution.
        :param command: Command with a required nonempty store reference.
        :return: Original selector, projected probe result as status, and projected later live_status.
        """
        ...

    def storage_store_update(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]:
        """
        Normalize allowlisted durable Store edits, reject unsafe endpoint changes, and report the updated live status.

        See :func:`LiuXin_alpha.core.program_services.stores.storage_store_update`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.storage_store_update(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime providing configuration lookup, replica enumeration, Store update, and live status.
        :param command: Command with required store reference and Mapping changes using the public keys above.
        :return: updated=True, string Store UUID, projected manager result, and live status after successful readback.
        :raises CoreDispatchError: For invalid fields/values, no replacements, or endpoint changes with remaining live claims.
        """
        ...


class SystemJobsHandlers(Protocol):
    """
    Require capability discovery, job-result retrieval, and log-reading queries without adding job-control commands.

    Job-result waiting and byte-oriented log access remain separate operations;
    base lifecycle, cancellation, and retry handlers belong to other Core owners.

    Example:
        >>> from LiuXin_alpha.core.program_endpoints import system_jobs
        >>> system_jobs.install_queries(implementation, runtime)  # doctest: +SKIP
    """

    def capabilities_list(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        Compare declared program-family names with the runtime's registered command/query union.

        See :func:`LiuXin_alpha.core.program_services.discovery.capabilities_list`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.capabilities_list(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime providing target-free API descriptions and api_version.
        :param query: Ignored query envelope; no dependency checks or family filters are requested.
        :return: API version, declared families/operations, registration completeness, and static dependency notes.
        """
        ...

    def jobs_log_read(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        Read a byte-bounded slice from the job's trusted log path and decode it with UTF-8 replacement errors.

        See :func:`LiuXin_alpha.core.program_services.discovery.jobs_log_read`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.jobs_log_read(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime whose job manager supplies the job and its optional log_path.
        :param query: Query with job_id, optional offset=0, and max_bytes=65536.
        :return: Decoded text, requested/current next byte offsets, read-time eof, and path-availability flag.
        :raises CoreDispatchError: For invalid request bounds/ID or KeyError from job lookup.
        """
        ...

    def jobs_result(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]:
        """
        Obtain a job execution result using the manager's timeout semantics without re-raising ordinary job failure.

        See :func:`LiuXin_alpha.core.program_services.discovery.jobs_result`
        for the maintained implementation's validation and failure boundaries.

        Example:
            >>> result = handler.jobs_result(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime whose job manager resolves the execution result.
        :param query: Query with required job_id text and optional timeout_s.
        :return: Stripped job_id and plain-projected execution value, including failure reports.
        :raises CoreDispatchError: If job_id is invalid or manager.result raises KeyError.
        """
        ...


class ProgramEndpointHandlers(
    BackupMaintenanceHandlers,
    CatalogSearchHandlers,
    ContentWorkflowsHandlers,
    DatabaseSchemaHandlers,
    StorageHandlers,
    SystemJobsHandlers,
    Protocol,
):
    """
    Combine all six endpoint-family handler requirements for type-checked program installation.

    A complete CoreProgramAPI satisfies this structural contract. Inheritance
    collects the named methods without adding registration, request validation,
    workflow state, or runtime protocol checks.

    Example:
        >>> from LiuXin_alpha.core.program_endpoints import install_program_endpoints
        >>> install_program_endpoints(implementation, runtime)  # doctest: +SKIP
    """
