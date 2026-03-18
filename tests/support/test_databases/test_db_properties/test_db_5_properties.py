from .common_db_properties import (
    CommonDBProperties,
)


class TestDB5Properties(CommonDBProperties):
    """
    Properties for tests database 2.
    """

    alpha_focus_row_counts = {
        "database_version": 1,
        "works": 40,
        "series": 1,
        "expressions": 40,
        "manifestations": 40,
        "items": 40,
        "files": 360,
        "agents": 1,
        "labels": 0,
    }

    theo_titles_table_hash = "282fbf2e169ee56635dc2874807356be"

    comment_title_link_count = 0
