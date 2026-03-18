from .common_db_properties import (
    CommonDBProperties,
)


class TestDB8Properties(CommonDBProperties):
    """
    Properties for tests database 8.
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

    theo_titles_table_hash = "51a7966ff8d0373608984b3ac104eeb2"

    comment_title_link_count = 0
