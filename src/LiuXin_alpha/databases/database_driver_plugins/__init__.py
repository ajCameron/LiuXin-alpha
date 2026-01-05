__author__ = "Cameron"
# On request loads a DatabasePing driver of a certain type (assuming it exists) and returns it.

import os

from LiuXin_alpha.errors import DatabaseDriverError

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

# used to locate this folder on disk so that plugins can be automatically imported
# Todo: Lock the database interface down and add these tests - in the tests folder - not here
__folder__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))

direct_access_methods = (
    "direct_get_tables",
    "direct_get_column_headings",
    "direct_get_tables_and_columns",
    "direct_add_simple_row",
    "direct_add_multiple_simple_rows",
    "direct_get_all_rows",
    "direct_get_row_from_id",
    "direct_search_table",
    "direct_update_row_in_table",
    "direct_get_all_hashes",
    "direct_get_next_book_group",
    "direct_delete_book_group",
    "direct_get_row_count",
)

# Dictionary keyed off the db_name (as all dictionaries here) with values the path to the folder containing the driver
__possible_db_driver_paths__ = dict()
# Contains the direct_access_methods module to actually interact with the database
__valid_db_drivers__ = dict()
# Contains the create_new_database methods
__valid_db_builders__ = dict()


def loadDatabaseDriver(db_type):
    """
    Loads the databasedriver module from within the database driver - which should contain a DatabaseDriver class.
    Returns a handle to the DatabaseDriver class from within that module.
    :param db_type:
    :return:
    """
    driver_path = get_driver_location(db_type)
    module_path = os.path.join(driver_path, "databasedriver.py")
    module_name = db_type + "_databasedriver"
    databasedriver_module = imp.load_source(module_name, module_path)
    return databasedriver_module.DatabaseDriver


def get_direct_access_module(db_driver_name):
    """
    Takes the name of a database driver present in this folder. Checks it exists. Loads the direct_access_methods module
    from it if it does and returns it.
    :param db_driver_name: The name of the driver
    :return direct_access_methods: The direct_access_module for that driver
    """
    if db_driver_name in __valid_db_drivers__:
        return __valid_db_drivers__[db_driver_name]

    db_driver_path = get_driver_location(db_driver_name)
    db_driver_access_path = os.path.join(db_driver_path, "direct_access_methods.py")
    db_driver_module = imp.load_source("db_driver_name", db_driver_access_path)

    # checking the db_driver_module to make sure that it has all the methods needed
    for method in direct_access_methods:
        if not hasattr(db_driver_module, method):
            err_str = "Required module in database direct access methods not found.\n"
            err_str += "Available methods: " + repr(dir(db_driver_module)) + "\n"
            err_str += "Required methods: " + repr(direct_access_methods) + "\n"
            raise DatabaseDriverError(err_str)

    # Caching the loaded module against future calls
    __valid_db_drivers__[db_driver_name] = db_driver_module

    return db_driver_module


# Todo: Add checking that the target_location is valid and not insane - re_write so it takes a DatabasePing object
# As some databases might require access credentials - e..g an internal SQL database need to extend the location
# definition to take account of this
def create_new_database(db_type, target_location):
    """
    Creates a new database of the request type at the requested location.
    :param db_type: The type of database the user wants
    :param target_location: Where the user wants the database
    :return create_new_database:
    """
    if db_type in __valid_db_builders__:
        return __valid_db_builders__[db_type].create_new_database(target_location)

    driver_location = get_driver_location(db_type)
    db_gen_location = os.path.join(driver_location, "database_generator.py")
    gen_module_name = db_type + "_generator"
    db_gen_module = imp.load_source(gen_module_name, db_gen_location)

    # Cache the database creation module
    __valid_db_builders__[db_type] = db_gen_module

    return db_gen_module


# Case insensitive means of getting the folder corresponding to a particular db_type
def get_driver_location(db_type):
    """
    Takes a db_type. Loads the location of that driver folder.
    :param db_type:
    :return db_path:
    """
    if db_type in __possible_db_driver_paths__:
        return __possible_db_driver_paths__[db_type]

    db_type_lower = db_type.lower()
    objects_list = os.listdir(__folder__)
    folders_list = [object for object in objects_list if os.path.isdir(os.path.join(__folder__, object))]
    folders_list_lower = [folder.lower() for folder in folders_list]
    folders_dict = dict(zip(folders_list_lower, folders_list))
    if db_type_lower not in folders_list_lower:
        err_str = "Requested db_driver_location not found.\n"
        err_str += "db_type: " + six_unicode(db_type) + "\n"
        raise DatabaseDriverError(err_str)

    db_type = folders_dict[db_type_lower]
    return os.path.join(__folder__, db_type)
