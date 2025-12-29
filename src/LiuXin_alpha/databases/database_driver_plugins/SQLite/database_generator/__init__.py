__author__ = "Cameron"

# Front end for the database_generator
from LiuXin.databases.drivers.SQLite.database_generator.database_generator import (
    create_new_database,
)


class DatabaseIntegrityError(Exception):
    pass
