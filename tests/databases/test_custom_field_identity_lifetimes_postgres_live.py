"""Exercise identity allocation and transactional DDL against real PostgreSQL."""

from tests.databases.test_custom_field_identity_lifetimes import (
    test_failed_rebuild_preserves_values_and_identity_history as test_failed_rebuild_preserves_values_and_identity_history,
)
from tests.databases.test_custom_field_identity_lifetimes import (
    test_metadata_only_edit_preserves_physical_indexes as test_metadata_only_edit_preserves_physical_indexes,
)
from tests.databases.test_custom_field_identity_lifetimes import (
    test_migration_never_reuses_deleted_identities as test_migration_never_reuses_deleted_identities,
)
from tests.databases.test_native_custom_fields import native as native
from tests.databases.test_native_custom_fields_postgres_live import db as db
from tests.databases.test_transaction_ownership_postgres_live import (
    pg_database as pg_database,
)
