
"""
Declare expected capabilities and contents for test db 11 properties.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise test db 11 properties through a consuming regression::

        python -m pytest -q tests/support/test_databases/test_db_properties/test_db_11_properties.py
"""


from .common_db_properties import (
    CommonDBProperties,
)


class TestDB11Properties(CommonDBProperties):

    """
    Declare the expected schema, content and feature properties for TestDB11Properties.

    Example:
        Exercise TestDB11Properties through a consuming regression::

            python -m pytest -q tests/support/test_databases/test_db_properties/test_db_11_properties.py
    """
    from LiuXin_alpha.databases.database_driver_plugins.SQLite import get_SQLite_driver_master_version

    theo_db_version = get_SQLite_driver_master_version()

    alpha_focus_row_counts = {
        "database_version": 1,
        "works": 20,
        "series": 1,
        "expressions": 20,
        "manifestations": 20,
        "items": 20,
        "files": 120,
        "agents": 1,
        "labels": 0,
    }

    theo_titles_table_hash = "223c94924e46a8a19bdd5da3a7131c16"
