from tests.support.test_databases.test_db_properties.common_db_properties import (
    CommonDBProperties,
)


class TestDB15Properties(CommonDBProperties):
    """
    Properties for the test_db_16 test database.
    """

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
