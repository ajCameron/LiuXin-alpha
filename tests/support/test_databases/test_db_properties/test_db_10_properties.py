from LiuXin_tests.test_databases.test_db_properties.common_db_properties import (
    CommonDBProperties,
)


class TestDB10Properties(CommonDBProperties):

    theo_title_count = 98
    theo_series_count = 42

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

    series_41_val = "Dark Tide Rising"
