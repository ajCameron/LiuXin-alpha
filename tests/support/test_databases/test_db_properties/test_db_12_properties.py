
"""
Declare expected capabilities and contents for test db 12 properties.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise test db 12 properties through a consuming regression::

        python -m pytest -q tests/support/test_databases/test_db_properties/test_db_12_properties.py
"""



from .common_db_properties import (
    CommonDBProperties,
)


class TestDB12Properties(CommonDBProperties):

    """
    Declare the expected schema, content and feature properties for TestDB12Properties.

    Example:
        Exercise TestDB12Properties through a consuming regression::

            python -m pytest -q tests/support/test_databases/test_db_properties/test_db_12_properties.py
    """
    from LiuXin_alpha.databases.database_driver_plugins.SQLite import get_SQLite_driver_master_version

    theo_db_version = get_SQLite_driver_master_version()

    alpha_focus_row_counts = {
        "database_version": 1,
        "works": 21,
        "series": 1,
        "expressions": 21,
        "manifestations": 21,
        "items": 21,
        "files": 0,
        "agents": 1,
        "labels": 0,
    }

    theo_titles_table_hash = "4c635814c5ae0909bb1ff10febbb53bf"
