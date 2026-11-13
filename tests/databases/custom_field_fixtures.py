"""Real native storage for compatibility contract tests with stable node IDs."""

import shutil

import pytest

from LiuXin_alpha.databases.database import Database


@pytest.fixture(scope="module")
def sqlite_template(tmp_path_factory):
    path = tmp_path_factory.mktemp("custom-native-template") / "template.sqlite"
    with Database(
        metadata={"database_path": str(path)},
        db_type="SQLite",
        create=True,
        repair_bootstrap_rows=False,
        enable_maintenance=False,
    ):
        pass
    return path


@pytest.fixture
def sqlite_db(sqlite_template, tmp_path):
    path = tmp_path / "custom-native.sqlite"
    shutil.copyfile(sqlite_template, path)
    with Database(
        metadata={"database_path": str(path)},
        db_type="SQLite",
        repair_bootstrap_rows=False,
        enable_maintenance=False,
    ) as database:
        yield database
