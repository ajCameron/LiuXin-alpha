"""Shared fixtures for Database-level contract tests.

We reuse the driver selection + torture-corpus fixtures defined for the driver
contract suite. This keeps the matrix of backends consistent, and lets these
tests double as lifecycle proxy tests for each driver.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest


# Reuse driver_contract fixtures (driver_spec, torture corpora, etc.).
pytest_plugins = ("tests.databases.database_driver_plugins.database_driver_contract.conftest",)


@pytest.fixture
def contract_db_name() -> str:
    """Name of the test DB resource used by database-contract tests."""

    return os.environ.get("LIUXIN_TEST_DB_NAME", "test_db_13")


@pytest.fixture
def provisioned_contract_db(provision_test_database, contract_db_name: str):
    """Provision a fresh writable test DB bundle for this test."""

    return provision_test_database(contract_db_name)


@pytest.fixture
def db_metadata(provisioned_contract_db) -> dict:
    """Metadata dict used by Database/driver constructors."""

    return {"database_path": str(provisioned_contract_db.db_path)}


@pytest.fixture
def db_path(db_metadata: dict) -> Path:
    """Concrete on-disk database path for this test."""

    return Path(db_metadata["database_path"])


@pytest.fixture
def open_db(driver_spec, db_metadata: dict):
    """A Database instance that is always fully closed at teardown."""

    from LiuXin_alpha.databases.database import Database

    db = Database(metadata=db_metadata, db_type=driver_spec.db_type, create=False, backup=False)
    try:
        yield db
    finally:
        # Ensure full cleanup (wrapper lock connection + driver connection + thread stop).
        try:
            db.close()
        except Exception:
            pass
