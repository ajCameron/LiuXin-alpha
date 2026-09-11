"""
Exercise StoreContainer configuration persistence and facade identity agreement.

The persistence case uses a real temporary SQLite schema through the miniature
database adapter. These tests validate configuration values and routing UUIDs,
without publishing Asset bytes or claiming a full application-manager restart.
"""

from __future__ import annotations

import dataclasses

from pathlib import Path
from uuid import uuid4

from tests.storage._mini_db import build_mini_db

from LiuXin_alpha.storage.api import StoreConfiguration
from LiuXin_alpha.storage.store_container import StoreContainer
from LiuXin_alpha.storage.stores import FilesystemStore


def test_store_container_roundtrips_configuration_and_stable_uuid_via_db(
    tmp_path: Path,
) -> None:
    """
    Save, update, and reload Store configuration through a temporary SQLite catalogue.

    The assigned Store row and routing UUID remain stable while role and tags change. Assertions
    compare both returned configuration values and the stored row. The database connection closes in
    the finally block around the save/update sequence; no Asset payload is published or read.

    Example:
        >>> test_store_container_roundtrips_configuration_and_stable_uuid_via_db(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory holding the SQLite catalogue and configured filesystem Store root.
    :return: None after configuration round-trip, stable UUID, and stored-row assertions pass.
    """
    db = build_mini_db(tmp_path / "store_container.sqlite")
    root = tmp_path / "store"
    configuration = StoreConfiguration(
        store_uuid=uuid4(),
        store_name="primary",
        store_kind="filesystem",
        store_root_uri=root.resolve().as_uri(),
        store_access_protocol="file",
        store_tags=("fast", "local"),
        operational_role="live",
    )
    store = FilesystemStore(root, configuration=configuration)
    container = StoreContainer.from_store(store, db=db)
    try:
        saved = container.save_configuration_to_db()
        original_uuid = saved.store_uuid
        container.configuration = dataclasses.replace(
            saved,
            operational_role="backup",
            store_tags=("local", "backup"),
        )
        updated = container.save_configuration_to_db()
        reloaded = container.reload_configuration_from_db()

        assert container.store_id is not None
        assert updated.store_uuid == original_uuid == configuration.store_uuid
        assert reloaded.operational_role == "backup"
        assert reloaded.store_tags == ("local", "backup")
        row = db.get_row_from_id("stores", container.store_id)
        assert row["store_uuid"] == str(configuration.store_uuid)
        assert row["store_operational_role"] == "backup"
    finally:
        db.conn.close()


def test_store_container_rejects_configuration_for_another_store(tmp_path: Path) -> None:
    """
    Reject a container configuration whose UUID differs from the supplied Store facade.

    The copied configuration changes only identity. The test requires ValueError with the
    UUID-mismatch diagnostic and opens no database or byte stream.

    Example:
        >>> test_store_container_rejects_configuration_for_another_store(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used to configure the filesystem Store root.
    :return: None after the mismatched configuration raises the expected ValueError.
    """
    store = FilesystemStore(tmp_path / "store")
    wrong = dataclasses.replace(store.configuration, store_uuid=uuid4())

    try:
        StoreContainer.from_store(store, configuration=wrong)
    except ValueError as error:
        assert "UUIDs must match" in str(error)
    else:  # pragma: no cover
        raise AssertionError("mismatched Store identity was accepted")
