"""
Declare expected capabilities and contents for test db 5 properties.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise test db 5 properties through a consuming regression::

        python -m pytest -q tests/support/test_databases/test_db_properties/test_db_5_properties.py
"""
from .common_db_properties import (
    CommonDBProperties,
)


class TestDB5Properties(CommonDBProperties):
    """
    Properties for tests database 2.

    Example:
        Exercise TestDB5Properties through a consuming regression::

            python -m pytest -q tests/support/test_databases/test_db_properties/test_db_5_properties.py
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
