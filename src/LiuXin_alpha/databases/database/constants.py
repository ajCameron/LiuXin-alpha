
"""
Constants defining the database.
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
        "feeds",
        "hashes",
        "last_read_positions",
        "library_id",
        "metadata_dirtied_books",
        "new_books",
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
        "transform_runs",
        "transform_run_inputs",
        "transform_run_outputs",
        "workflow_states",
        "workflow_steps",
    }
)

# Compatibility catalogs are part of the current schema and remain classified
# as helper tables, but older databases may legitimately predate them.  Their
# read APIs must be allowed to supply inferred defaults until the database is
# migrated; write APIs can still require the physical catalog.
OPTIONAL_HELPER_TABLES = frozenset(
    {
        "column_metadata",
    }
)
