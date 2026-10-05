
"""
Declare expected capabilities and contents for test db 0 properties.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise test db 0 properties through a consuming regression::

        python -m pytest -q tests/support/test_databases/test_db_properties/test_db_0_properties.py
"""


from .common_db_properties import (
    CommonDBProperties,
)


class TestDB0Properties(CommonDBProperties):

    """
    Declare the expected schema, content and feature properties for TestDB0Properties.

    Example:
        Exercise TestDB0Properties through a consuming regression::

            python -m pytest -q tests/support/test_databases/test_db_properties/test_db_0_properties.py
    """
    from LiuXin_alpha.databases.database_driver_plugins.SQLite import get_SQLite_driver_master_version

    theo_db_version = get_SQLite_driver_master_version()

    alpha_focus_row_counts = {
        "database_version": 1,
        "works": 1,
        "series": 1,
        "expressions": 0,
        "manifestations": 0,
        "items": 0,
        "files": 0,
        "agents": 1,
        "labels": 0,
    }
