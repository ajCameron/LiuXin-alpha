from .common_db_properties import (
    CommonDBProperties,
)


class TestDB2Properties(CommonDBProperties):
    """
    Properties for tests database 2.
    """

    alpha_focus_row_counts = {
        "database_version": 1,
        "works": 1,
        "series": 1,
        "expressions": 1,
        "manifestations": 1,
        "items": 1,
        "files": 0,
        "agents": 1,
        "labels": 0,
    }

    theo_titles_table_hash = "0ffcc8dce5c2985a728076b9da8e8f9a"

    comment_title_link_count = 0
