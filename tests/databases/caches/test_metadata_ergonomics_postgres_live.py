"""Exercise metadata ergonomics and shared edit reservations on live PostgreSQL."""

from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest

from LiuXin_alpha.caches.metadata import MetadataAPI
from LiuXin_alpha.catalog.api.metadata_revisions import MetadataConflictError
from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.databases.database import Database
from tests.databases.caches.test_metadata_api import MODES
from tests.databases.caches.test_metadata_ergonomics import (
    test_hydrated_search_pages_and_record_edit_preconditions as _edit_scenario,
)
from tests.databases.caches.test_metadata_ergonomics import (
    test_shared_picker_bulk_reads_and_runtime_capabilities as _picker_scenario,
)
from tests.databases.caches.test_metadata_ergonomics import (
    test_work_memberships_and_role_specific_credit_edits as _relationship_scenario,
)
from tests.databases.test_transaction_ownership_postgres_live import (
    pg_database as pg_database,
)


@pytest.mark.parametrize("mode", MODES)
def test_postgres_metadata_ergonomic_workflows(pg_database, mode):
    db, _, _ = pg_database
    _picker_scenario(db, mode)
    _edit_scenario(db, mode)
    _relationship_scenario(db, mode)


def test_postgres_shared_record_precondition_serializes_independent_writer(pg_database):
    """Hold a shared-record reservation while another connection attempts a stale edit."""
    db, metadata, _ = pg_database
    api = MetadataAPI(Catalog(db))
    tag = api.ensure_tag("Before").result
    token = api.tags.read_for_edit(tag).revision
    entered, release, attempted = Event(), Event(), Event()

    def first():
        def edit(current):
            entered.set()
            assert release.wait(30)
            return current.tags.update(tag, {"name": "First"}).result

        return api.batch(edit, expected=(token,))

    # Construct both handles before reserving tables: database admission itself
    # may inspect/initialize metadata and must not become part of the writer race.
    with Database(
        metadata=dict(metadata),
        db_type="PostgreSQL",
        backup=False,
        repair_bootstrap_rows=False,
        enable_storage_manager=False,
        enable_maintenance=False,
    ) as independent:
        other = MetadataAPI(Catalog(independent))

        def second():
            attempted.set()
            with pytest.raises(MetadataConflictError):
                other.tags.update(tag, {"name": "Stale"}, expected_revision=token)

        with ThreadPoolExecutor(max_workers=2) as pool:
            first_future = pool.submit(first)
            try:
                assert entered.wait(30)
                second_future = pool.submit(second)
                assert attempted.wait(30)
            finally:
                release.set()
            assert first_future.result(timeout=30).persistence == "committed"
            second_future.result(timeout=30)
    assert api.tags.require(tag).fields["name"] == "First"
