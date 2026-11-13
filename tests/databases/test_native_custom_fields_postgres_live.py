"""Run the native field contracts unchanged against real PostgreSQL storage."""

import pytest

from tests.databases.test_native_custom_fields import (
    native as native,
)
from tests.databases.test_native_custom_fields import (
    test_atomic_mixed_patch_validation_and_entries as test_atomic_mixed_patch_validation_and_entries,
)
from tests.databases.test_native_custom_fields import (
    test_cleanup_repair_is_explicit_and_fingerprint_covers_ddl as test_cleanup_repair_is_explicit_and_fingerprint_covers_ddl,
)
from tests.databases.test_native_custom_fields import (
    test_composite_relationship_paths_require_aggregation as test_composite_relationship_paths_require_aggregation,
)
from tests.databases.test_native_custom_fields import (
    test_composite_templates_dependencies_and_cycles as test_composite_templates_dependencies_and_cycles,
)
from tests.databases.test_native_custom_fields import (
    test_compound_macro_rollback_and_savepoint_publication as test_compound_macro_rollback_and_savepoint_publication,
)
from tests.databases.test_native_custom_fields import (
    test_field_migration_retirement_and_stale_plans as test_field_migration_retirement_and_stale_plans,
)
from tests.databases.test_native_custom_fields import (
    test_migration_attachment_mapping_and_series_positions as test_migration_attachment_mapping_and_series_positions,
)
from tests.databases.test_native_custom_fields import (
    test_native_core_wire_and_preview_validation as test_native_core_wire_and_preview_validation,
)
from tests.databases.test_native_custom_fields import (
    test_native_datatype_matrix as test_native_datatype_matrix,
)
from tests.databases.test_native_custom_fields import (
    test_native_owner_cleanup_on_independent_connection as test_native_owner_cleanup_on_independent_connection,
)
from tests.databases.test_native_custom_fields import (
    test_native_rollback_commit_failure_and_publication as test_native_rollback_commit_failure_and_publication,
)
from tests.databases.test_native_custom_fields import (
    test_native_schema_failure_is_atomic as test_native_schema_failure_is_atomic,
)
from tests.databases.test_native_custom_fields import (
    test_queries_and_scoped_labels as test_queries_and_scoped_labels,
)
from tests.databases.test_native_custom_fields import (
    test_query_case_and_wrapper_hooks as test_query_case_and_wrapper_hooks,
)
from tests.databases.test_native_custom_fields import (
    test_scoped_metadata_aliases_and_dependencies as test_scoped_metadata_aliases_and_dependencies,
)
from tests.databases.test_native_custom_fields import (
    test_shared_rename_merge_and_delete as test_shared_rename_merge_and_delete,
)
from tests.databases.test_native_custom_fields import (
    test_unsupported_storage_refusal_preserves_ids_and_values as test_unsupported_storage_refusal_preserves_ids_and_values,
)
from tests.databases.test_transaction_ownership_postgres_live import (
    pg_database as pg_database,
)


@pytest.fixture
def db(pg_database):
    return pg_database[0]
