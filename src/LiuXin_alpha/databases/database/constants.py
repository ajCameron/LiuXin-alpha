from __future__ import unicode_literals

HELPER_TABLES = frozenset(
    {
        "conversion_options",
        "compressed_files",
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
        "file_derivations",
        "file_workflow",
        "file_workflow_events",
        "item_workflow",
        "item_workflow_events",
        "transform_runs",
        "transform_run_inputs",
        "transform_run_outputs",
        "workflow_states",
        "workflow_steps",
    }
)
