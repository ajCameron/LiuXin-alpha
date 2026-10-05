"""
Declare expected capabilities and contents for test db 9 properties.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise test db 9 properties through a consuming regression::

        python -m pytest -q tests/support/test_databases/test_db_properties/test_db_9_properties.py
"""
from .common_db_properties import (
    CommonDBProperties,
)


class TestDB9Properties(CommonDBProperties):
    """
    Properties for tests database 8.

    Example:
        Exercise TestDB9Properties through a consuming regression::

            python -m pytest -q tests/support/test_databases/test_db_properties/test_db_9_properties.py
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
