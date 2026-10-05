"""
Declare expected capabilities and contents for test db 8 properties.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise test db 8 properties through a consuming regression::

        python -m pytest -q tests/support/test_databases/test_db_properties/test_db_8_properties.py
"""
from .common_db_properties import (
    CommonDBProperties,
)


class TestDB8Properties(CommonDBProperties):
    """
    Properties for tests database 8.

    Example:
        Exercise TestDB8Properties through a consuming regression::

            python -m pytest -q tests/support/test_databases/test_db_properties/test_db_8_properties.py
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
