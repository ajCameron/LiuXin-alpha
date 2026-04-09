from .common_db_properties import (
    CommonDBProperties,
)


class TestDB9Properties(CommonDBProperties):
    """
    Properties for tests database 8.
    """

    alpha_focus_row_counts = {
        "database_version": 1,
        "works": 17,
        "series": 1,
        "expressions": 17,
        "manifestations": 17,
        "items": 17,
        "files": 0,
        "agents": 1,
        "labels": 0,
    }

    theo_titles_table_hash = "f4bef07d520295c18563f175cfd33588"

    comment_title_link_count = 0
