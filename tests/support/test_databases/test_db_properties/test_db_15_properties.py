"""
Declare expected capabilities and contents for test db 15 properties.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise test db 15 properties through a consuming regression::

        python -m pytest -q tests/support/test_databases/test_db_properties/test_db_15_properties.py
"""
from .common_db_properties import (
    CommonDBProperties,
)


class TestDB15Properties(CommonDBProperties):
    """
    Properties for the test_db_16 test database.

    Example:
        Exercise TestDB15Properties through a consuming regression::

            python -m pytest -q tests/support/test_databases/test_db_properties/test_db_15_properties.py
    """

    alpha_focus_row_counts = {
        "database_version": 1,
        "works": 20,
        "series": 1,
        "expressions": 20,
        "manifestations": 20,
        "items": 20,
        "files": 0,
        "agents": 1,
        "labels": 0,
    }

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - MAIN TABLE PROPERTIES

    theo_title_count = 3
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
    # - TITLE

    theo_title_1_title_creator_sort = None

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - CREATORS

    title_1_creator_vals = ["Neal Stephenson"]
    title_26_creator_vals = ["Terry Pratchett", "Stephen Baxter"]

    theo_title_1_author_data = [(1, "Neal Stephenson", "Stephenson, Neal", None)]
    theo_title_26_author_data = [
        (2, "Terry Pratchett", "Pratchett, Terry", None),
        (3, "Stephen Baxter", "Baxter, Stephen", None),
    ]

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - COMMENTS

    theo_comment_count = 0

    theo_title_comment_count = 0

    #
    # ------------------------------------------------------------------------------------------------------------------
