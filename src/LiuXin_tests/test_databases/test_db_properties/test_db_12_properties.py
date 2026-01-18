
"""
Properties for the Test DB 12 database.
"""



from LiuXin_tests.test_databases.test_db_properties.common_db_properties import (
    CommonDBProperties,
)


class TestDB12Properties(CommonDBProperties):

    from LiuXin_alpha.databases.database_driver_plugins.SQLite import get_SQLite_driver_master_version

    theo_db_version = get_SQLite_driver_master_version()

    theo_titles_table_hash = "4c635814c5ae0909bb1ff10febbb53bf"
