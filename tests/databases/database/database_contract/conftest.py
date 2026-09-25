"""
Provide Database contract resource names, paths, metadata, and backend-parametrized open connections.

The open_db finalizer attempts close and suppresses ordinary cleanup exceptions;
successful teardown does not prove that every underlying resource closed.

Example:
    Exercise these fixtures through their owning tests::

        python -m pytest -q tests/databases/database/database_contract/test_db_add_title_wemi_split.py
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest


@pytest.fixture
def contract_db_name() -> str:
    """
    Read LIUXIN_TEST_DB_NAME without trimming it, defaulting to test_db_13 only when unset.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_add_title_wemi_split.py


    :return: Configured resource name, including an explicitly empty value.
    """

    return os.environ.get("LIUXIN_TEST_DB_NAME", "test_db_13")


@pytest.fixture
def provisioned_contract_db(provision_test_database, contract_db_name: str):
    """
    Provision a writable copy of the selected contract database resource.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_add_title_wemi_split.py


    :param provision_test_database: Fixture factory that copies the requested test
        database into an isolated bundle.
    :param contract_db_name: Fixture resource name read from the environment or the
        default.
    :return: Bundle returned by the shared provisioning fixture; that fixture owns its
        files.
    """

    return provision_test_database(contract_db_name)


@pytest.fixture
def db_metadata(provisioned_contract_db) -> dict:
    """
    Construct fresh database metadata from the provisioned bundle path.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_add_title_wemi_split.py


    :param provisioned_contract_db: Provisioned writable database bundle supplied by the
        fixture factory.
    :return: Dictionary containing database_path as a string.
    """

    return {"database_path": str(provisioned_contract_db.db_path)}


@pytest.fixture
def db_path(db_metadata: dict) -> Path:
    """
    Convert the metadata database_path into a Path without opening it.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_add_title_wemi_split.py


    :param db_metadata: Constructor metadata mapping with a database_path string.
    :return: Path object for the provisioned on-disk database.
    """

    return Path(db_metadata["database_path"])


@pytest.fixture
def open_db(driver_spec, db_metadata: dict):
    """
    Open the existing fixture database with the selected backend and yield it to the test.

    Disable creation and backups. After a successful constructor call, attempt db.close
    in finally and suppress Exception subclasses from close; constructor errors
    propagate.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_add_title_wemi_split.py


    :param driver_spec: Selected database driver specification, including its ID and
        Database db_type.
    :param db_metadata: Constructor metadata mapping with a database_path string.
    :return: Iterator yielding one Database instance.
    """

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
