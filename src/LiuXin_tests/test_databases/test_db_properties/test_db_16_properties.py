from tests.support.test_databases.test_db_properties.common_db_properties import (
    CommonDBProperties,
)


# Todo: A test that all db properties inherit from common_db_properties
class TestDB16Properties(CommonDBProperties):
    """
    Properties for the test_db_16 test database.
    """

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - MAIN TABLE PROPERTIES

    theo_title_count = 1
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
    # - COMMENTS

    theo_comment_count = 6

    theo_title_comment_count = 5

    #
    # ------------------------------------------------------------------------------------------------------------------
