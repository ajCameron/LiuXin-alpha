"""Require the same failed-handle ownership contracts on PostgreSQL."""

from tests.databases.test_database_handle_shutdown import (
    test_constructor_retains_failed_handle_and_preserves_bootstrap_error as test_constructor_retains_failed_handle_and_preserves_bootstrap_error,
)
from tests.databases.test_database_handle_shutdown import (
    test_context_body_error_survives_failed_connection_close as test_context_body_error_survives_failed_connection_close,
)
from tests.databases.test_database_handle_shutdown import (
    test_driver_close_attempts_every_handle_and_reports_all_errors as test_driver_close_attempts_every_handle_and_reports_all_errors,
)
from tests.databases.test_database_handle_shutdown import (
    test_failed_handle_close_keeps_facade_owner_until_retry as test_failed_handle_close_keeps_facade_owner_until_retry,
)
from tests.databases.test_native_custom_fields_postgres_live import db as db
from tests.databases.test_transaction_ownership_postgres_live import (
    pg_database as pg_database,
)
