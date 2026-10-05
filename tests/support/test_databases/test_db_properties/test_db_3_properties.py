"""
Declare expected capabilities and contents for test db 3 properties.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise test db 3 properties through a consuming regression::

        python -m pytest -q tests/support/test_databases/test_db_properties/test_db_3_properties.py
"""
from .common_db_properties import (
    CommonDBProperties,
)


class TestDB3Properties(CommonDBProperties):
    """
    Properties for tests database 2.

    Example:
        Exercise TestDB3Properties through a consuming regression::

            python -m pytest -q tests/support/test_databases/test_db_properties/test_db_3_properties.py
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
