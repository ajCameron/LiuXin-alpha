from .common_db_properties import (
    CommonDBProperties,
)


class TestDB10Properties(CommonDBProperties):

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
