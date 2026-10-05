"""
Declare expected capabilities and contents for test db 14 properties.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise test db 14 properties through a consuming regression::

        python -m pytest -q tests/support/test_databases/test_db_properties/test_db_14_properties.py
"""
from .common_db_properties import (
    CommonDBProperties,
)

# Todo: Need to be able to determine the test coverage - in parallel


class TestDB14Properties(CommonDBProperties):
    """
    Properties for the test_db_14 test database.

    Example:
        Exercise TestDB14Properties through a consuming regression::

            python -m pytest -q tests/support/test_databases/test_db_properties/test_db_14_properties.py
    """

    alpha_focus_row_counts = {
        "database_version": 1,
        "works": 10,
        "series": 1,
        "expressions": 10,
        "manifestations": 10,
        "items": 10,
        "files": 0,
        "agents": 1,
        "labels": 0,
    }

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - MAIN TABLES

    theo_title_count = 9

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
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - TITLES

    theo_title_1_last_modified = "2022-05-23 18:26:24"

    theo_title_1_authors = ("Neal Stephenson",)

    theo_title_1_creator_sort = None

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - BOOKS

    theo_book_1_uuid = "01aa06c6-da59-430f-b039-a9a38808f97416.775476"

    #
    # ------------------------------------------------------------------------------------------------------------------
