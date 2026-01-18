
"""
Represents properties for Test DB 0.
"""


from LiuXin_tests.test_databases.test_db_properties.common_db_properties import (
    CommonDBProperties,
)


class TestDB0Properties(CommonDBProperties):

    from LiuXin_alpha.databases.database_driver_plugins.SQLite import get_SQLite_driver_master_version

    theo_db_version = get_SQLite_driver_master_version()
