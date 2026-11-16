"""Run the facade follow-up contracts against a real PostgreSQL schema."""

import pytest

from tests.databases import test_row_write_safety_postgres_live as fixtures
from tests.databases.test_database_facade_followup import (
    test_dupe_interlinks_is_atomic_including_priority_swaps as test_dupe_interlinks_is_atomic_including_priority_swaps,
)
from tests.databases.test_database_facade_followup import (
    test_failed_interlink_readback_cannot_commit_a_relationship as test_failed_interlink_readback_cannot_commit_a_relationship,
)
from tests.databases.test_database_facade_followup import (
    test_get_row_and_scalar_contracts_are_distinct as test_get_row_and_scalar_contracts_are_distinct,
)
from tests.databases.test_database_facade_followup import (
    test_interlink_creation_joins_caller_and_recovers_from_constraint_failure as test_interlink_creation_joins_caller_and_recovers_from_constraint_failure,
)
from tests.databases.test_database_facade_followup import (
    test_nest_rows_is_atomic_and_preserves_unrelated_staged_values as test_nest_rows_is_atomic_and_preserves_unrelated_staged_values,
)
from tests.databases.test_database_facade_followup import (
    test_nest_rows_rejects_invalid_children_before_any_write as test_nest_rows_rejects_invalid_children_before_any_write,
)
from tests.databases.test_database_facade_followup import (
    test_null_search_and_grouping_distinguish_literal_none as test_null_search_and_grouping_distinguish_literal_none,
)
from tests.databases.test_database_facade_followup import (
    test_tree_collection_rejects_legacy_cycles as test_tree_collection_rejects_legacy_cycles,
)
from tests.databases.test_database_facade_followup import (
    test_tree_collection_respects_subtree_boundary as test_tree_collection_respects_subtree_boundary,
)
from tests.databases.test_database_facade_followup import (
    test_unlink_intralinks_constraint_failure_restores_all_links as test_unlink_intralinks_constraint_failure_restores_all_links,
)
from tests.databases.test_database_facade_followup import (
    test_unlink_intralinks_joins_caller_and_keeps_other_links as test_unlink_intralinks_joins_caller_and_keeps_other_links,
)

pg_db = fixtures.pg_db


@pytest.fixture
def db(pg_db):
    return pg_db[0]
