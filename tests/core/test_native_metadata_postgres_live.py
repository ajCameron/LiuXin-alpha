"""Run the public native metadata workflow matrix against disposable PostgreSQL schemas."""

import os
from contextlib import closing
from types import SimpleNamespace
from uuid import uuid4

import pytest

from LiuXin_alpha.core import CoreRuntime
from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import (
    connect_postgres,
)
from tests.core.test_native_metadata_completion import (
    test_batch_connected_creation_rollback_revisions_and_publication as _batch,
)
from tests.core.test_native_metadata_completion import (
    test_complete_native_client_workflow as _workflow,
)
from tests.core.test_native_metadata_shared_boundary import (
    test_shared_records_and_contextual_attachments as _shared,
)
from tests.core.test_native_metadata_shared_boundary import (
    test_tree_cycle_reorder_limits_and_atomic_creation as _trees,
)
from tests.databases.caches.test_metadata_api import MODES


@pytest.fixture
def pg_database():
    """Exercise Core's default bootstrap policy, including reserved system rows."""
    url = os.environ.get("LIUXIN_TEST_POSTGRES_URL", "").strip()
    if not url:
        pytest.skip("LIUXIN_TEST_POSTGRES_URL is not configured.")
    metadata = {"postgres_url": url, "schema": f"liuxin_core_metadata_{uuid4().hex}"}
    with closing(connect_postgres(metadata, prompt_for_password=False)) as cleanup:
        db = None
        try:
            # Keep the Core default. Reserved rows must not interfere with
            # semantic vocabulary ordering or user-created graph operations.
            db = Database(
                metadata=metadata,
                db_type="PostgreSQL",
                create=True,
                backup=False,
                repair_bootstrap_rows=True,
                enable_storage_manager=False,
                enable_maintenance=False,
            )
            yield db, metadata, cleanup
        finally:
            if db is not None:
                db.close()
            cleanup.rollback()
            with cleanup:
                with cleanup.cursor() as cursor:
                    cursor.execute(
                        f'DROP SCHEMA IF EXISTS "{metadata["schema"]}" CASCADE'
                    )


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("remote", (False, True))
def test_postgres_native_client_workflow(pg_database, mode, remote):
    database, _, _ = pg_database
    _workflow(database, mode, remote)


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("remote", (False, True))
def test_postgres_native_batch_failure_recovery(pg_database, mode, remote, monkeypatch):
    database, _, _ = pg_database
    _batch(database, mode, remote, monkeypatch)


@pytest.mark.parametrize("mode", MODES)
def test_postgres_shared_records_and_tree_invariants(pg_database, mode):
    database, _, _ = pg_database
    # PostgreSQL creates this constants table without seeding it. Preserve that
    # observable state, then supply fixture data for the read-only API contract.
    # Core queries must not turn into an implicit bootstrap/migration operation.
    with CoreRuntime(library=SimpleNamespace(database=database)) as runtime:
        assert runtime.query("metadata.shared.list", {"kind": "languages"}) == {
            "records": []
        }
    with database.macros.transaction():
        database.macros.insert_row(
            "languages",
            {
                "language": "English",
                "language_code": "eng",
                "language_iso639_1": "en",
                "language_iso639_2_b": "eng",
                "language_bcp47_primary": "en",
            },
        )
    _shared(database, mode, True)
    for kind in ("genres", "subjects", "series"):
        _trees(database, mode, True, kind)
