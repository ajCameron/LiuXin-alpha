"""
Declare expected capabilities and contents for test db 2 properties.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise test db 2 properties through a consuming regression::

        python -m pytest -q tests/support/test_databases/test_db_properties/test_db_2_properties.py
"""
from .common_db_properties import (
    CommonDBProperties,
)


class TestDB2Properties(CommonDBProperties):
    """
    Properties for tests database 2.

    Example:
        Exercise TestDB2Properties through a consuming regression::

            python -m pytest -q tests/support/test_databases/test_db_properties/test_db_2_properties.py
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
