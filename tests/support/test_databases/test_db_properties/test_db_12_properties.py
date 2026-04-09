
"""
Properties for test DB 12.
"""



from .common_db_properties import (
    CommonDBProperties,
)


class TestDB12Properties(CommonDBProperties):

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
