"""Run the same search parity checks on disposable PostgreSQL schemas."""

from tests.catalog.test_cache_search_differential import (
    collection as collection,
)
from tests.catalog.test_cache_search_differential import (
    test_nullable_comparison_trees_match_database as test_nullable_comparison_trees_match_database,
)
from tests.catalog.test_cache_search_differential import (
    test_wemi_boolean_graph_queries_match_sql_at_every_level as test_wemi_boolean_graph_queries_match_sql_at_every_level,
)
from tests.databases.test_native_custom_fields_postgres_live import db as db
from tests.databases.test_transaction_ownership_postgres_live import (
    pg_database as pg_database,
)
