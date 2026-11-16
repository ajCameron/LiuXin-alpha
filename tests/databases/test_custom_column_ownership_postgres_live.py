"""Native namespace and attachment contracts on PostgreSQL."""

from tests.databases.test_custom_column_ownership import (
    test_native_definitions_require_an_explicit_attachment as test_native_definitions_require_an_explicit_attachment,
    test_native_series_does_not_reserve_a_companion_label as test_native_series_does_not_reserve_a_companion_label,
)
from tests.databases.test_native_custom_fields import native as native
from tests.databases.test_native_custom_fields_postgres_live import db as db
from tests.databases.test_transaction_ownership_postgres_live import (
    pg_database as pg_database,
)
