
"""
Test properties for DB 0.
"""


from .common_db_properties import (
    CommonDBProperties,
)


class TestDB0Properties(CommonDBProperties):

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
