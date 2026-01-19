
"""
Test properties for DB 11.
"""


from LiuXin_tests.test_databases.test_db_properties.common_db_properties import (
    CommonDBProperties,
)


class TestDB11Properties(CommonDBProperties):

    from LiuXin_alpha.databases.database_driver_plugins.SQLite_apsw import get_SQLite_driver_master_version

    theo_db_version = get_SQLite_driver_master_version()

    theo_titles_table_hash = "223c94924e46a8a19bdd5da3a7131c16"
