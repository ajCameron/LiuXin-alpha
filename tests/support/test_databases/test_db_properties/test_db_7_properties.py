from .common_db_properties import (
    CommonDBProperties,
)


class TestDB7Properties(CommonDBProperties):
    """
    Properties for tests database 2.
    """

    alpha_focus_row_counts = {
        "database_version": 1,
        "works": 6,
        "series": 1,
        "expressions": 6,
        "manifestations": 6,
        "items": 6,
        "files": 0,
        "agents": 1,
        "labels": 0,
    }

    theo_titles_table_hash = "54fe278b176be88177c2a2aa0b0a9fa4"

    comment_title_link_count = 0
