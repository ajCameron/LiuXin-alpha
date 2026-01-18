from LiuXin_tests.test_databases.test_db_properties.common_db_properties import (
    CommonDBProperties,
)


class TestDB13Properties(CommonDBProperties):
    """
    Properties for the test_db_13 database.
    """

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
