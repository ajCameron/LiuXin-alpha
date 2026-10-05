"""
Declare expected capabilities and contents for test db 7 properties.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise test db 7 properties through a consuming regression::

        python -m pytest -q tests/support/test_databases/test_db_properties/test_db_7_properties.py
"""
from .common_db_properties import (
    CommonDBProperties,
)


class TestDB7Properties(CommonDBProperties):
    """
    Properties for tests database 2.

    Example:
        Exercise TestDB7Properties through a consuming regression::

            python -m pytest -q tests/support/test_databases/test_db_properties/test_db_7_properties.py
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
