"""
Exercise optional real PostgreSQL storage metadata and local filesystem publication.

LIUXIN_TEST_POSTGRES_URL enables a temporary UUID-named schema, concurrent manager
ingests, an injected final metadata failure, and recovery after object reopen in
the same process. Cleanup drops that schema with CASCADE. Absent configuration
skips the live test; no SQL translation fake or process-crash simulation is used.
"""

from __future__ import annotations

import os
import uuid

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import (
    PostgresConnectionAdapter,
    connect_postgres,
)
from LiuXin_alpha.storage.api import DigitalAssetID


pytestmark = pytest.mark.integration


def _postgres_url() -> str:
    """
    Return the stripped LIUXIN_TEST_POSTGRES_URL setting or skip when it is absent/blank. This does
    not connect, validate credentials, or print the configured connection value.

    Example:
        >>> url = _postgres_url()  # doctest: +SKIP


    :return: Nonempty configured PostgreSQL connection URL; missing configuration triggers pytest skip.
    """
    value = os.environ.get("LIUXIN_TEST_POSTGRES_URL", "").strip()
    if not value:
        pytest.skip("LIUXIN_TEST_POSTGRES_URL is not configured.")
    return value


def _drop_schema(metadata: dict[str, object], schema: str) -> None:
    """
    Open a PostgreSQL connection without a password prompt, drop the trusted temporary schema with
    CASCADE inside the connection context, and always close after successful adapter construction.
    The schema identifier is interpolated without escaping embedded quotes; only the generated test
    name is intended.

    Example:
        >>> _drop_schema(metadata, temporary_schema)  # doctest: +SKIP


    :param metadata: Connection metadata passed to connect_postgres, including the configured test database URL.
    :param schema: Trusted generated schema name to drop with all contained objects.
    :return: None after schema cleanup; connection, SQL, transaction, and close failures propagate.
    """
    connection = PostgresConnectionAdapter(
        connect_postgres(metadata, prompt_for_password=False)
    )
    try:
        with connection:
            connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        connection.close()


def test_storage_ingest_concurrency_restart_and_recovery_on_live_postgres(
    tmp_path: Path,
) -> None:
    """
    Exercise real PostgreSQL metadata with two concurrent local-Store ingests and journal recovery.
    Create a UUID-named schema, seed a filesystem Store, open two database/manager objects, and
    submit separate ingests to two workers. Require distinct positive Asset IDs, then inject failure
    at final operation persistence after another publication and check pending status. Close/reopen
    objects in the same process, retry the operation UUID, and verify Asset/Replica identity, actual
    bytes, and absence of pending/failed issues for it. This is not an operating-system crash.

    The outer finally drops the temporary schema with CASCADE. Connection setup and cleanup follow
    the explicit nested try blocks, rather than guaranteeing cleanup for every partially constructed
    database. The URL helper skips before entering the schema-work block when the environment is
    unconfigured.

    Example:
        >>> test_storage_ingest_concurrency_restart_and_recovery_on_live_postgres(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory containing the actual filesystem Store bytes; metadata uses the configured PostgreSQL database.
    :return: None after live concurrency/recovery assertions and schema cleanup; missing URL skips and operational/assertion/cleanup errors propagate.
    """

    schema = f"liuxin_storage_live_{uuid.uuid4().hex}"
    metadata: dict[str, object] = {
        "postgres_url": _postgres_url(),
        "schema": schema,
    }
    store_ref = uuid.uuid4()
    store_root = tmp_path / "postgres-store"
    interrupted_id = uuid.uuid4()
    interrupted_payload = b"postgres publication pending recovery"

    try:
        with Database(
            metadata=metadata,
            db_type="PostgreSQL",
            create=True,
            backup=False,
            enable_storage_manager=False,
        ) as setup:
            setup.macros.insert_row(
                "stores",
                {
                    "store_uuid": str(store_ref),
                    "store_name": "postgres-live",
                    "store_kind": "filesystem",
                    "store_access_protocol": "file",
                    "store_root_uri": store_root.resolve().as_uri(),
                    "store_is_read_only": 0,
                    "store_online_status": "online",
                },
            )

        first = Database(
            metadata=metadata,
            db_type="PostgreSQL",
            create=False,
            backup=False,
            storage_startup_on_add=True,
        )
        second = Database(
            metadata=metadata,
            db_type="PostgreSQL",
            create=False,
            backup=False,
            storage_startup_on_add=True,
        )
        try:
            assert first.storage is not None
            assert second.storage is not None

            def ingest(manager, payload: bytes):
                """
                Run a verified byte ingest through the supplied live manager and return its result
                to the executor. Each submitted call uses a different manager/database connection
                and payload.

                Example:
                    >>> result = ingest(manager, b"manager one")  # doctest: +SKIP


                :param manager: Database-bound storage manager used by this worker.
                :param payload: Byte payload to publish with verify=True.
                :return: DigitalAssetIngestResult after the delegated ingest; errors propagate to the future.
                """
                return manager.ingest_bytes(payload, verify=True)

            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = (
                    pool.submit(ingest, first.storage, b"manager one"),
                    pool.submit(ingest, second.storage, b"manager two"),
                )
                results = tuple(future.result(timeout=30) for future in futures)
            asset_ids = {
                result.asset_record.digital_asset_id for result in results
            }
            assert len(asset_ids) == 2
            assert all(isinstance(value, int) and value > 0 for value in asset_ids)

            operation_cache = first.storage._ingest_operations
            original_upsert = operation_cache._upsert

            def interrupt(_operation) -> None:
                """
                Raise the injected metadata-interruption error instead of persisting the supplied
                completed operation. The already published bytes and journal are left for the tested
                recovery path.

                Example:
                    >>> interrupt(operation)  # doctest: +SKIP


                :param _operation: Ignored operation that the intercepted mapping would otherwise upsert.
                :return: Does not return; always raises RuntimeError with the injected PostgreSQL interruption message.
                """
                raise RuntimeError("simulated PostgreSQL metadata interruption")

            operation_cache._upsert = interrupt
            with pytest.raises(RuntimeError, match="metadata interruption"):
                first.storage.ingest_bytes(
                    interrupted_payload,
                    operation_id=interrupted_id,
                    verify=True,
                )
            operation_cache._upsert = original_upsert
            assert any(
                issue.operation_id == interrupted_id
                for issue in first.storage.get_operational_status().issues_for(
                    "ingest_pending"
                )
            )
        finally:
            first.close()
            second.close()

        with Database(
            metadata=metadata,
            db_type="PostgreSQL",
            create=False,
            backup=False,
            storage_startup_on_add=True,
        ) as restarted:
            assert restarted.storage is not None
            recovered = restarted.storage.ingest_bytes(
                interrupted_payload,
                operation_id=interrupted_id,
                verify=True,
            )
            assert isinstance(recovered.asset_record.digital_asset_id, int)
            assert recovered.asset_record.digital_asset_id == DigitalAssetID(
                recovered.replica_record.digital_asset_id
            )
            assert restarted.storage.read_bytes(recovered.location) == (
                interrupted_payload
            )
            assert not any(
                issue.operation_id == interrupted_id
                for issue in restarted.storage.get_operational_status().issues
                if issue.code in {"ingest_pending", "ingest_failed"}
            )
    finally:
        _drop_schema(metadata, schema)
