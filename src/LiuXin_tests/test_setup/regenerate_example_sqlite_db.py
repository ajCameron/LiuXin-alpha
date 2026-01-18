

"""
Build the most basic database.
"""


import os

from LiuXin_alpha.constants.paths import LiuXin_base_folder

from LiuXin_alpha.databases.database import Database


def regenerate_base_sqlite_db():
    """
    An example sqlite database is included at the root of the LiuXin directory.

    This is used as a data source for automatic sql completion in PyCharm.
    Generate a blank sqlite database and move it to that location
    :return:
    """
    example_db_path = os.path.join(LiuXin_base_folder, "LiuXin_alpha", "example_sqlite_db.db")

    new_db_md = {"database_path": example_db_path}
    Database(metadata=new_db_md, db_type="SQLite", create=True, backup=False)


if __name__ == "__main__":

    regenerate_base_sqlite_db()
