

"""
Regenerate the example SQLite fixture.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise regenerate example sqlite db through a consuming regression::

        python -m pytest -q tests/support/test_databases/test_legacy_objects_smoke.py
"""


import os

from LiuXin_alpha.constants.paths import LiuXin_base_folder

from LiuXin_alpha.databases.database import Database


def regenerate_base_sqlite_db():
    """
    An example sqlite database is included at the root of the LiuXin directory.

    Example:
        Exercise regenerate base sqlite db through a consuming regression::

            python -m pytest -q tests/support/test_databases/test_legacy_objects_smoke.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    example_db_path = os.path.join(LiuXin_base_folder, "LiuXin_alpha", "example_sqlite_db.db")

    new_db_md = {"database_path": example_db_path}
    Database(metadata=new_db_md, db_type="SQLite", create=True, backup=False)


if __name__ == "__main__":

    regenerate_base_sqlite_db()
