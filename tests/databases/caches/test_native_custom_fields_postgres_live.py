"""Exercise the cache coordinator with real PostgreSQL transactions."""

from tests.databases.caches.test_native_custom_fields import (
    projection as projection,
)
from tests.databases.caches.test_native_custom_fields import (
    test_native_writes_leave_metadata_to_explicit_cache_owner as test_native_writes_leave_metadata_to_explicit_cache_owner,
)
from tests.databases.caches.test_native_custom_fields import (
    test_cache_nested_scopes_publish_final_state_and_discard_savepoints as test_cache_nested_scopes_publish_final_state_and_discard_savepoints,
)
from tests.databases.caches.test_native_custom_fields import (
    test_cache_publication_waits_for_outer_commit_and_rolls_back as test_cache_publication_waits_for_outer_commit_and_rolls_back,
)
from tests.databases.caches.test_native_custom_fields import (
    test_cache_commit_failure_preserves_previous_projection as test_cache_commit_failure_preserves_previous_projection,
)
from tests.databases.caches.test_native_custom_fields import (
    test_cache_publication_failure_reports_committed_write as test_cache_publication_failure_reports_committed_write,
)
from tests.databases.caches.test_native_custom_fields import (
    test_cache_projects_revisions_series_migration_and_retirement as test_cache_projects_revisions_series_migration_and_retirement,
)
from tests.databases.caches.test_native_custom_fields import (
    test_failed_receipt_listener_does_not_skip_cache_publication as test_failed_receipt_listener_does_not_skip_cache_publication,
)
from tests.databases.test_native_custom_fields import native as native
from tests.databases.test_native_custom_fields_postgres_live import db as db
from tests.databases.test_transaction_ownership_postgres_live import (
    pg_database as pg_database,
)
from tests.databases.test_custom_column_unification import (
    test_storage_registry_publishes_schema_changes_only_after_commit as test_storage_registry_publishes_schema_changes_only_after_commit,
)
