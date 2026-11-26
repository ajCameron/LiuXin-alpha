
"""
Define immutable helper-table names and capabilities allowed to be absent.

Database schema refresh uses HELPER_TABLES for classification and requires all names
outside OPTIONAL_HELPER_TABLES. Capability readers may infer defaults for optional
tables; this classification never bypasses the base schema-revision admission check.
Storage bootstrap owns its journal/ledger migration.
"""


from __future__ import unicode_literals

# Todo: MOOOVEEEE
HELPER_TABLES = frozenset(
    {
        "conversion_options",
        "compressed_files",
        "column_metadata",
        "custom_columns",
        "database_metadata",
        "database_version",
        "schema_revision",
        "search_index_metadata",
        "feeds",
        "hashes",
        "last_read_positions",
        "library_id",
        "metadata_dirtied_books",
        "new_books",
        "normalized_identities",
        "maintenance_metadata",
        "maintenance_plugins",
        "maintenance_subscriptions",
        "maintenance_changes",
        "maintenance_obligations",
        "maintenance_projections",
        # Catalogue change log (databases/change_log.py)
        "catalog_changes",
        "catalog_changes_metadata",
        "preferences",
        # FRBR plugin data
        "works_plugin_data",
        "expressions_plugin_data",
        "manifestations_plugin_data",
        "items_plugin_data",
        # Workflow tables
        "digital_asset_derivations",
        "digital_asset_workflow",
        "digital_asset_workflow_events",
        "item_workflow",
        "item_workflow_events",
        "storage_ingest_operations",
        "storage_schema_migrations",
        "transform_runs",
        "transform_run_inputs",
        "transform_run_outputs",
        "workflow_states",
        "workflow_steps",
    }
)

# Optional capability readers may supply inferred defaults when their physical
# catalogue is absent. Unsupported base formats still fail the revision check.
OPTIONAL_HELPER_TABLES = frozenset(
    {
        # PostgreSQL has no SQLite expression indexes.
        "search_index_metadata",
        "maintenance_metadata",
        "maintenance_plugins",
        "maintenance_subscriptions",
        "maintenance_changes",
        "maintenance_obligations",
        "maintenance_projections",
        # Catalogues created before the change log add it explicitly.
        "catalog_changes",
        "catalog_changes_metadata",
        "column_metadata",
        "normalized_identities",
        # Storage bootstrapping owns the additive migration from catalogues
        # created before its durable ingest journal and migration ledger.
        "storage_ingest_operations",
        "storage_schema_migrations",
    }
)
