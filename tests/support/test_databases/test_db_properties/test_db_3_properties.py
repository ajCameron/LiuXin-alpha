from .common_db_properties import (
    CommonDBProperties,
)


class TestDB3Properties(CommonDBProperties):
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
        "files": 2440,
        "agents": 1,
        "labels": 0,
    }

    theo_titles_table_hash = "282fbf2e169ee56635dc2874807356be"

    comment_title_link_count = 0
