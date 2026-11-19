"""Verify native metadata transactions and external-writer reservations on PostgreSQL."""

import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from threading import Event

import pytest

from LiuXin_alpha.caches.metadata import MetadataAPI
from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import (
    connect_postgres,
)
from tests.databases.caches.test_metadata_api import MODES
from tests.databases.caches.test_metadata_extensions import (
    test_custom_field_lifecycle_values_queries_and_native_conflicts as _field_scenario,
)
from tests.databases.caches.test_metadata_extensions import (
    test_optimistic_edits_use_authoritative_metadata_and_detect_relationship_changes as _edit_scenario,
)
from tests.databases.test_transaction_ownership_postgres_live import (
    pg_database as pg_database,
)


@pytest.mark.parametrize("mode", MODES)
def test_postgres_native_metadata_revisions_and_fields(pg_database, mode):
    db, _, _ = pg_database
    _edit_scenario(db, mode)
    _field_scenario(db, mode)


def test_postgres_precondition_reserves_against_raw_writer(pg_database):
    db, metadata, observer = pg_database
    api = MetadataAPI(Catalog(db))
    entity_id = api.works.create({"title": "Before"}).result
    token = api.works.read_for_edit(entity_id).revision
    reserved, release, attempting = Event(), Event(), Event()
    raw_pid = []

    def conditional():
        def edits(service):
            reserved.set()
            assert release.wait(20), "Conditional writer was not released"
            return service.works.update(entity_id, {"title": "Conditional"}).result

        return api.batch(edits, expected=(token,))

    def raw_writer():
        with closing(
            connect_postgres(metadata, prompt_for_password=False)
        ) as connection:
            raw_pid.append(connection.get_backend_pid())
            attempting.set()
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        f'UPDATE "{metadata["schema"]}".works SET work_title=%s WHERE work_id=%s',
                        ("Later raw edit", entity_id),
                    )

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(conditional)
        second = None
        try:
            assert reserved.wait(15)
            second = pool.submit(raw_writer)
            assert attempting.wait(15)
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                assert not second.done(), "Raw writer bypassed the metadata reservation"
                observer.rollback()
                with observer.cursor() as cursor:
                    cursor.execute(
                        "SELECT wait_event_type,wait_event FROM pg_stat_activity WHERE pid=%s",
                        (raw_pid[0],),
                    )
                    if cursor.fetchone() == ("Lock", "relation"):
                        break
                time.sleep(0.01)
            else:
                pytest.fail("Raw writer did not reach a physical table lock")
        finally:
            release.set()
        assert first.result(timeout=20).persistence == "committed"
        assert second is not None
        second.result(timeout=20)
    assert api.works.require(entity_id).work.work_title == "Later raw edit"


def test_postgres_preconditions_reject_preexisting_repeatable_snapshot(pg_database):
    db, _, _ = pg_database
    api = MetadataAPI(Catalog(db))
    entity_id = api.works.create({"title": "Unchanged"}).result
    token = api.works.read_for_edit(entity_id).revision
    with db.macros.transaction() as connection:
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ").close()
        with pytest.raises(RuntimeError, match="READ COMMITTED"):
            api.works.update(entity_id, {"title": "Rejected"}, expected_revision=token)
    assert api.works.require(entity_id).work.work_title == "Unchanged"
