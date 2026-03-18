
"""
Test properties for DB 11.
"""


from .common_db_properties import (
    CommonDBProperties,
)


class TestDB11Properties(CommonDBProperties):

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
