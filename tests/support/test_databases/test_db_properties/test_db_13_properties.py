"""
Declare expected capabilities and contents for test db 13 properties.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise test db 13 properties through a consuming regression::

        python -m pytest -q tests/support/test_databases/test_db_properties/test_db_13_properties.py
"""
from .common_db_properties import (
    CommonDBProperties,
)


class TestDB13Properties(CommonDBProperties):
    """
    Properties for the test_db_13 database.

    Example:
        Exercise TestDB13Properties through a consuming regression::

            python -m pytest -q tests/support/test_databases/test_db_properties/test_db_13_properties.py
    """

    alpha_focus_row_counts = {
        "database_version": 1,
        "works": 0,
        "series": 1,
        "expressions": 0,
        "manifestations": 0,
        "items": 0,
        "files": 0,
        "agents": 1,
        "labels": 0,
    }

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - MAIN TABLES

    theo_title_count = 0

    theo_main_tables = {
        "files",
        "publishers",
        "genres",
        "custom_columns",
        "folder_stores",
        "covers",
        "tags",
        "series",
        "notes",
        "identifiers",
        "devices",
        "folders",
        "languages",
        "last_read_positions",
        "books",
        "comments",
        "synopses",
        "titles",
        "feeds",
        "creators",
        "subjects",
    }

    #
    # ------------------------------------------------------------------------------------------------------------------
