"""Optional builder modules for test databases.

If you add a module here named after a test database (for example
`test_db_42.py`), the test database provisioner can import it and build the
database on demand.

Supported module contracts:

* def populate_bundle(bundle_dir: pathlib.Path) -> None
    - Create the sqlite DB file and any sidecar resources inside bundle_dir.
    - Preferred when you need more than a single DB file.

* def build(db_path: pathlib.Path) -> None
  def build_database(db_path: pathlib.Path) -> None
  def build_test_database(db_path: pathlib.Path) -> None
    - Create the sqlite DB file at db_path.

The provisioner will prefer `<name>.test_db` but will also accept a single
`*.test_db` file if you generate one with a different filename.
"""

from __future__ import annotations, print_function


import csv
import datetime
import glob
import os
import pprint
import shutil
import time
import string
import sys
from copy import deepcopy
from clint.textui import puts, colored

from LiuXin_alpha.constants.file_extensions import BOOK_EXTENSIONS_DOTTED
from LiuXin_alpha.constants.paths import LiuXin_data_folder, LiuXin_default_database

from LiuXin_alpha.databases.database import Database

from LiuXin_alpha.errors import InputIntegrityError, DatabaseIntegrityError

from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.ptempfiles import get_scratch_folder, get_ramdisk, ScratchFolderManager, unmount_ramdisk
from LiuXin_alpha.utils.storage.local.file_ops import checked_copy
from LiuXin_alpha.utils.terminal import getTerminalSize

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode, memory_range

LiuXin_print = print

# If you want to instruct the build system to construct another test database, then add a module to this folder of the
# form test_db_{number} - then run LiuXin.test.test_setup.build_test_databases. The database will be constructed and
# copied into the test databases folder in the LiuXin_data folder.

# test_db_0 - The base data - simple series - minimal cross linking
# test_db_1 - Richer metadata - more cross linking - more identifier info
# test_db_2 - Based off test_db_1 - but with only five actual titles (six if the unknown title - title 0 is included)
# test_db_3 - Based off test_db_1 - but includes some crude asset data - entries in files, folders and covers
# test_db_4 - Comprehensive test database - generates a test database with as comprehensive as possible collection of
#             metadata
# test_db_5 - A nicer version of test_db_3 - should be merged
# test_db_6 - DatabasePing with some basic custom columns declared - custom columns are empty
# test_db_7 - DatabasePing with no custom columns declared and many of the title rows removed - 0, 1, 5, 6, 7, 18 - are the
#             only ones retained
# test_db_8 - DatabasePing with no custom columns declared and many of the title rows removed - 0, 86, 5, 6, 7, 18 - are
#             the only ones retained
# test_db_9 - DatabasePing with no custom columns and many title rows remove - 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
#             36, 37, 38, 50 are retained
# test_db_10 - DatabasePing with some complex series data and no asset data
# test_db_11 - DatabasePing with complex series and some apparently valid asset data
# test_db_12 - As with db 10 - but with an additional book in one of the leaves of the complex series
# test_db_13 - A completely blank database
# test_db_14 - Only has the first 10 titles

# test_db_16 - Short comprehensive test db (uses the methods from 4) - limited to one title - for testing the
#              comprehensive db generator
# test_db_17 - As test_db_16 - but limited to ten titles
# test_db_18 - As for test_db_4 - artificially generated database
#              By default databases just have many_many - this creates some new main tables with one_one, many_one
#              and one_many links - which are then populated with artificially generated tables
# test_db_19 - As for test_db_18 - but with some additional main tables linked to titles - allows testing of the
#              generic write and table methods
# test_db_20 - As for test_db_4 - but with a more realistic ids count for each title.
# test_db_21 - As for test_db_19 - but also contains a not_series table
# test_db_22 - As for test_db_21 - but also contains some custom columns - in titles and others


# Todo: Create a tets database with faulty folders - the root folder of each folder tree shouldn't have a fs_id set
# Todo: Need to write a test to check the integrity of the test database - as tests are failing due to one of the
#       processes writing corrupt data to the folders


# used to locate this folder on disk so that plugins can be automatically imported
__folder__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
test_data_folder = __folder__

__lx_test_data__ = os.path.join(LiuXin_data_folder, "test_data")
__lx_test_books__ = os.path.join(__lx_test_data__, "test_books")

# Loads and checks test data into the database


def load_data(
    folder_path=None,
    overwrite_db=True,
    base_data=False,
    load_from=None,
    override_scratch_folder=None,
):
    """
    Reads data for the database out of a series of CSV files.
    :param folder_path: Path to the folder containing all the csv files.
                        Takes presidence over all other indicators as to the location of the folder (base_data and
                        load_from - if this is present they are ignored
    :param overwrite_db: True the the default database is overwritten
                         False then the new database is created in a scratch folder and the data loaded into it there.
                         Will consult :param override_scratch_folder: to see if there is a specific scratch folder that
                         should be used.
    :param base_data: Uses the data set which hasn't been filled in.
    :param load_from: Specify an override subfolder in LiuXin.test.test_data to load the data from.
                      This overrides base data.
    :param override_scratch_folder: If not None then this scratch folder will be used instead of the automatically
                                    generated folder to build the new database in
    :return:
    """
    if folder_path is not None:
        if not os.path.exists(folder_path):
            err_str = "load_data has been passed a folder path which doesn't exist.\n"
            err_str = default_log.log_variables(err_str, "ERROR", ("folder_path", folder_path))
            raise InputIntegrityError(err_str)
        target_folder = deepcopy(folder_path)

    else:
        if load_from is None:
            if base_data:
                target_folder = os.path.join(__folder__, "test_db_0")
            else:
                target_folder = os.path.join(__folder__, "test_db_1")
        else:
            target_folder = os.path.join(__folder__, load_from)
            assert os.path.exists(target_folder), "Cannot load - cannot find target folder"

    # Read the csv files out of the given target folder
    if overwrite_db:
        database = Database(create=True)
    else:
        if override_scratch_folder is None:
            # Make a scratch folder and create the database in it
            scratch_folder = get_scratch_folder()
            scratch_db_path = os.path.join(scratch_folder, "temp_db_DELETE_ME.DB")
            database = Database(create=True, metadata={"database_path": scratch_db_path})
        else:
            scratch_db_path = os.path.join(override_scratch_folder, "temp_db_DELETE_ME.DB")
            database = Database(create=True, metadata={"database_path": scratch_db_path})

    # To avoid foreign key constraint errors the main tables have to be loaded first
    main_tables = [t for t in deepcopy(database.main_tables)]

    # The main tables also have to be loaded in a certain order - doing the notes first
    # This prevents foreign key constraints being violated
    main_tables.remove("notes")
    main_tables = [
        "notes",
    ] + main_tables

    # Folders stops the foreign key constraints in files being violated
    main_tables.remove("folders")
    main_tables = [
        "folders",
    ] + main_tables

    # titles stop the foreign key constraints in books being violated
    main_tables.remove("titles")
    main_tables = [
        "titles",
    ] + main_tables

    for table in main_tables:
        database.driver_wrapper.clear(target_table=table)

        table_csv_name = table + ".csv"
        csv_path = os.path.join(target_folder, table_csv_name)

        if os.path.exists(csv_path) and os.path.isfile(csv_path):
            debug_str = "csv data file found - loading: {}".format(six_unicode(csv_path))
            default_log.debug(debug_str)
            with open(csv_path, mode="r+") as table_file:
                for row_dict in csv.DictReader(table_file):

                    new_row_dict = dict()

                    # Process the dictionary to convert all empty columns to None - this should avoid triggering foreign
                    # key constraints

                    for column in row_dict:
                        if column is None:
                            pass
                        elif row_dict[column] == "" or row_dict[column].lower() == "none":
                            new_row_dict[column] = None
                        else:
                            new_row_dict[column] = from_csv(row_dict[column])

                    database.driver_wrapper.add_row(new_row_dict)
        else:
            wrn_str = "Unable to find csv data file: " + six_unicode(csv_path)
            default_log.warning(wrn_str)

    # Loading the interlink, intralink tables and the helper tables
    categorized_tables = dict()
    categorized_tables["interlink"] = deepcopy(database.interlink_tables)
    categorized_tables["intralink"] = deepcopy(database.intralink_tables)
    categorized_tables["helper"] = deepcopy(database.helper_tables)

    for table_cat in categorized_tables:

        typed_tables = categorized_tables[table_cat]

        for table in typed_tables:

            try:
                database.driver_wrapper.clear(target_table=table)
            except DatabaseIntegrityError:
                # We have been blocked from clearing the table - probably protected as database_version
                continue

            table_csv_name = table + ".csv"
            csv_path = os.path.join(target_folder, table_csv_name)

            if os.path.exists(csv_path) and os.path.isfile(csv_path):
                debug_str = "csv data file found - loading: {}".format(six_unicode(csv_path))
                default_log.debug(debug_str)
                with open(csv_path, mode="r+") as table_file:
                    for row_dict in csv.DictReader(table_file):

                        new_row_dict = dict()

                        # Process the dictionary to convert all empty columns to None - this should avoid triggering
                        # foreign key constraints
                        for column in row_dict:
                            if row_dict[column] is None or row_dict[column] == "" or row_dict[column].lower() == "none":
                                new_row_dict[column] = None
                            else:
                                new_row_dict[column] = from_csv(row_dict[column])

                        database.driver_wrapper.add_row(new_row_dict)

            else:
                wrn_str = "Unable to find csv data file: {}".format(six_unicode(csv_path))
                default_log.warning(wrn_str)

    return database


def dump_data(folder_path=None, database=None):
    """
    Dumps the data from the database as a series of csv files in the specified folder.
    :param folder_path: The place to dump the data.
    :param database: The database to read while preforming the dump
    :return:
    """
    if folder_path is not None:
        if not os.path.exists(folder_path):
            err_str = "dump_data has been passed a folder path which doesn't exist.\n"
            default_log.exception(err_str)
            raise InputIntegrityError
        target_folder = deepcopy(folder_path)
    else:
        target_folder = __folder__

    # Dump the tables by type
    if database is None:
        database = Database()
    all_tables = database.all_tables

    for table in all_tables:

        table_csv_name = table + ".csv"
        csv_path = os.path.join(target_folder, table_csv_name)
        with open(csv_path, mode="w+") as table_file:

            table_writer = csv.writer(table_file, delimiter=",", quotechar="|", quoting=csv.QUOTE_MINIMAL)

            # Write the column names into the first line of the file
            column_names = database.get_tables_and_columns()[table]
            table_writer.writerow(column_names)

            # Simple writing the values out produces scrambled results (as dictionaries are unordered like that)
            # Assembling an index of the values in the same order as the column_names and then writing that out
            for row in database.driver_wrapper.get_all_rows(table):

                row_values = [row[k] for k in column_names]

                # Filter to make the output more human readable
                row_values = ["" if k is None else to_csv(k) for k in row_values]

                table_writer.writerow(row_values)


def purge_csv_files(folder_path=None, prompt=True):
    """
    Delete all csv files in the target folder.
    :param folder_path:
    :param prompt:
    :return:
    """
    if prompt:
        wrn_str = "All CSV files at the target location will be deleted.\n"
        wrn_str += "Are you sure?"
        usr_input = LiuXin_print(output=wrn_str, user_input=True)
        if usr_input:
            raise InputIntegrityError("purge_csv_files terminated by user.\n")

    if folder_path is not None:
        if not os.path.exists(folder_path):
            err_str = "dump_data has been passed a folder path which doesn't exist.\n"
            default_log.exception(err_str)
            raise InputIntegrityError
        target_folder = deepcopy(folder_path)
    else:
        target_folder = __folder__

    target_glob = os.path.join(target_folder, "*.csv")
    file_list = glob.glob(target_glob)
    for csv_file in file_list:
        try:
            os.remove(csv_file)
        except OSError as e:
            raise
        if os.path.exists(csv_file):
            critical_str = "File which has been deleted has persisted.\n"
            critical_str += "file_path: " + six_unicode(csv_file) + "\n"
            default_log.critical(critical_str)
            raise OSError


# Take a tree and maps it to the ends of the leaves
# Copies in the list of all ebook files from the constants - creates a dict keyed by the file extensions and valued with
# the target_num of that sort of file to be retrieved
# Every time a file cluster is found copy the entire cluster into the __lx_test_books__ folder - marking off the files
# as they come down
def extract_test_books(books_path, target_num=10, separate_files=False, extract_file_groups=True):
    """
    Takes a path to the root of a tree - walks the tree and tries to extract ten of each type of ebook from it.

    Tree could be a calibre library.
    Needed because I was tired of manually mining rare book formats out a deeply nested file system.
    :param books_path:
    :param target_num: The minimum number of each type of book to be retrieved
    :param separate_files: Should efforts be made to preserve the file structure?
    :return:
    """
    default_log.info("Making test books set - books_path: " + six_unicode(books_path))

    books_path = six_unicode(books_path)
    if not os.path.exists(books_path):
        err_str = "extract_test_books failed - Given books_path does not exist.\n"
        err_str += "books_path: " + six_unicode(books_path)
        default_log.error(err_str)
        raise InputIntegrityError(err_str)

    if not os.path.exists(__lx_test_books__):
        err_str = "test_books folders does not exist.\n"
        err_str += "__lx_test_books__: " + six_unicode(__lx_test_books__)
        default_log.error(err_str)
        raise InputIntegrityError(err_str)

    if separate_files:
        raise NotImplementedError

    book_exts = set([p.lower() for p in BOOK_EXTENSIONS_DOTTED])
    files_count = dict((x, target_num) for x in BOOK_EXTENSIONS_DOTTED)
    needed_exts = set([p.lower() for p in BOOK_EXTENSIONS_DOTTED])

    pprint.pformat(files_count)

    folder_count = 0
    files_count = 0
    for root, dirs, files in os.walk(books_path):

        file_paths_set = set([os.path.join(root, f) for f in files])
        ext_set = set([os.path.splitext(p)[1] for p in file_paths_set if os.path.splitext(p)[1] in book_exts])
        ebook_paths_set = set([p for p in file_paths_set if os.path.splitext(p)[1] in book_exts])

        # Compare the extensions to the extensions which are still needed - if any are still needed then take all the
        # ebooks in the file and dump them in __lx_test_books__
        if ext_set.intersection(needed_exts):

            # Update the files_count and the needed_exts
            for ext in ext_set:
                if ext in files_count.keys():
                    files_count[ext] -= 1
                if files_count[ext] == 0:
                    needed_exts.remove(ext)

            # Copy the files over into the __lx_test_books__ folder
            copied_files = []
            for ebook_path in ebook_paths_set:
                new_path = os.path.join(__lx_test_books__, os.path.split(ebook_path)[1])
                shutil.copyfile(ebook_path, new_path)
                if os.path.exists(new_path):
                    copied_files.append((ebook_path, "green"))
                    files_count += 1
                else:
                    copied_files.append((ebook_path, "red"))
                default_log.print_coloured_list(copied_files)

        folder_count += 1
        LiuXin_print(six_unicode(folder_count) + " folders tested. " + six_unicode(files_count) + " files copied.")


def to_csv(target_string):
    """
    Takes a string - renders it safe for saving in CSV form by replacing all the commas with something safe.
    Should be run after all other pickling/serialization operations have been run.
    :param target_string:
    :return:
    """
    target_string = six_unicode(target_string)
    return target_string.replace(",", "###")


def from_csv(target_string):
    """
    Takes a string - renders it from the csv safe form into something that can be read in, by replacing the placeholder
    for a comma with actual commas.
    :param target_string:
    :return:
    """
    target_string = six_unicode(target_string)
    return target_string.replace("###", ",")


def make_test_data():
    """
    Prepare a test data set from the test_db_0 and some methods from Library.

    The base data was the data set I could be bothered to type out by hand - most of the rest of the data is made by
    using the test add methods from the library test class to add new items to the database.
    :return:
    """
    # Make sure that the database is empty
    from LiuXin_alpha.databases.database import Database

    Database(create=True)

    # Load the base data
    load_data(overwrite_db=True, base_data=True)

    from LiuXin_alpha.library.library2 import Library

    target_library = Library()
    target_db = Database()

    # Add James Heriot to the creators table
    status = test_creator_add(test_library=target_library, test_db=target_db)
    assert status

    # Add Science Fiction and Military Science Fiction to the genres table
    status = test_genre_add(test_library=target_library, test_db=target_db)
    assert status

    # Add Baen books to the publishers table
    status = test_publisher_add(test_library=target_library, test_db=target_db)
    assert status

    # Add Star Trek to the series table
    status = test_add_series(test_library=target_library, test_db=target_db)
    assert status

    # Add Military History and Armour Development to the subjects table
    status = test_add_subject(test_library=target_library, test_db=target_db)
    assert status

    # Add the rational fiction tag to the library
    status = test_add_tag(test_library=target_library, test_db=target_db)
    assert status

    # Add two more titles to the Library
    status = test_add_title(test_library=target_library, test_db=target_db)
    assert status

    # Apply a note to Cryptonomicon
    status = test_apply_note(test_library=target_library, test_db=target_db)
    assert status

    # Apply some tags to all the titles and series
    status = test_apply_tags(test_library=target_library, test_db=target_db)
    assert status

    # Apply publishers to many of the titles
    status = test_apply_publisher(test_library=target_library, test_db=target_db)
    assert status

    # Apply synopsis to some of the titles and series
    status = test_apply_synopsis(test_library=target_library, test_db=target_db)
    assert status

    # Add a language to the languages table - English
    status = test_add_language(test_library=target_library, test_db=target_db)
    assert status

    # Apply English to all the titles
    status = test_apply_language(test_library=target_library, test_db=target_db)
    assert status

    # Make a book row for every title
    status = create_all_books(test_library=target_library, test_db=target_db)
    assert status

    # Dump the data to the test_data folder
    dump_data(database=target_db)

    # Some tables cause problems - for example, titles_aggregate - which is supposed to be generated from triggers
    # based off the titles table - so remove them before they can cause problems
    drop_files = ["database_metadata.csv", "titles_aggregate.csv"]
    for file_name in drop_files:
        file_path = os.path.join(__folder__, file_name)
        os.remove(file_path)


# Todo: Currently overwrites the database in the default position - this needs to be corrected
def make_test_database_backup(base_data=False, override_dst_name=None):
    """
    Makes a test database - loads it with all the data - then copies it to a secure location.
    Do not use if you're altering the pirmary table sql.
    :param base_data: The test database is assembled in two stages. First a basic set of data is loaded out of the CSV
                      files in LiuXin.test.library.test_data.test_db_0 - then additional methods are run on it using
                      the library add methods (as manually typing out those additions to the CSV files seemed entirely
                      too much like work. If True this generates a database using that base data. If False then
                      generates a test database using the full data set.
                      If True, the new database is stored in the LiuXin_data_folder, as test_database_base.db
                      If False, the new database is stored in the LiuXin_data_folder as test_database.db
    :param override_dst_name: If provided will override the output name from the default to the provided value
    :return:
    """
    puts(colored.green("About to create the test database"))
    live_database = load_data(base_data=base_data)

    # Shutdown the live database - needed to allow a clean move
    del live_database

    from LiuXin_alpha.constants import LiuXin_data_folder
    import os
    import shutil

    if override_dst_name:
        test_db_backup_path = os.path.join(LiuXin_data_folder, override_dst_name)
    elif base_data:
        test_db_backup_path = os.path.join(LiuXin_data_folder, "test_database_base.db")
    else:
        test_db_backup_path = os.path.join(LiuXin_data_folder, "test_database.db")

    if os.path.exists(LiuXin_default_database):
        shutil.copyfile(src=LiuXin_default_database, dst=test_db_backup_path)
    if os.path.exists(test_db_backup_path):
        puts(colored.green("Test database successfully created and transferred to - {}".format(test_db_backup_path)))

    return test_db_backup_path


########################################################################################################################
# - METHODS TO MAKE COPIES OF THE TEST DATABASE FILES
########################################################################################################################


def file_load_comprehensive_test_database_backup():
    """
    Copies the test database from where it resides in the LiuXin_data_folder to the database path. The database at that
    location, if any, is overwritten.
    :param base: If True, then loads the base data set (the test database without books, and the addition of some
                 creators and series). If False, then loads the complete test data set
    :param scratch: Instead of replacing the main database copies the database into a scratch folder and returns the
                    path to the database.
    :return final_db_path: The path to the replaced database - where ever that may be
    """
    info_str = "About to copy the comprehensive test database to a scratch folder"
    puts(colored.green(info_str))

    from LiuXin_alpha.constants.paths import LiuXin_data_folder
    import os
    import shutil

    test_db_backup_path = os.path.join(LiuXin_data_folder, "comprehensive_test_database.db")
    assert os.path.exists(test_db_backup_path), "comprehensive test database file not found"

    from LiuXin_alpha.utils.ptempfiles import get_scratch_folder

    db_scratch_folder = get_scratch_folder()
    final_db_path = os.path.join(db_scratch_folder, "scratch_database.db")
    shutil.copyfile(src=test_db_backup_path, dst=final_db_path)

    if os.path.exists(final_db_path):
        info_str = "comprehensive test database scratch folder created"
        puts(colored.green(info_str))
    else:
        puts(colored.red("Final database could not be generated - final_db_path: {}".format(final_db_path)))

    return final_db_path


# Todo: This needs to be seriously reconsidered - or outright removed
def file_load_test_database_backup(base=False, scratch=False, override_scratch_folder=None):
    """
    Copies the test database from where it resides in the LiuXin_data_folder to the database path. The database at that
    location, if any, is overwritten.
    :param base: If True, then loads the base data set (the test database without books, and the addition of some
                 creators and series). If False, then loads the complete test data set
    :param scratch: Instead of replacing the main database copies the database into a scratch folder and returns the
                    path to the database.
    :param override_scratch_folder: If provided, then the new database will be generated in the given folder
    :return final_db_path: The path to the replaced database - where ever that may be
    """
    info_str = "About to copy the {}test database to {}".format(
        "full " if not base else "base ",
        "the actual database position" if not scratch else "a scratch folder",
    )
    puts(colored.green(info_str))

    if not base:
        final_db_path = file_load_named_database(
            database_name="test_db_1.test_db",
            scratch=scratch,
            override_scratch_folder=override_scratch_folder,
        )
    else:
        final_db_path = file_load_named_database(
            database_name="test_db_0.test_db",
            scratch=scratch,
            override_scratch_folder=override_scratch_folder,
        )

    if os.path.exists(final_db_path):
        info_str = "{} successfully generated with {}".format(
            "Main" if not scratch else "Scratch",
            "base data set" if base else "full data set",
        )
        puts(colored.green(info_str))
    else:
        puts(colored.red("Final database could not be generated - final_db_path: {}".format(final_db_path)))

    return final_db_path


def file_load_named_database(database_name="test_db_1.test_db", scratch=True, override_scratch_folder=None):
    """
    Load the named database either into a scratch folder or replace the default database.

    Return a path to wherever the database ended up.
    :param database_name:
    :param scratch:
    :param override_scratch_folder: If given, then the scratch database will be generated in this folder.
    :return:
    """
    import os
    import shutil
    from LiuXin_alpha.constants import LiuXin_data_folder
    from LiuXin_alpha.constants import LiuXin_default_database

    test_db_backup_path = os.path.join(LiuXin_data_folder, "test_databases", database_name)

    # This should almost never be used - it deletes and replaces the default database
    if not scratch:
        if os.path.exists(test_db_backup_path):
            try:
                os.remove(LiuXin_default_database)
            except OSError:
                # File has (probably) already been deleted - continuing
                puts(colored.yellow("DatabasePing was not found to delete - continuing"))
        final_db_path = deepcopy(LiuXin_default_database)
    else:
        from LiuXin_alpha.utils.ptempfiles import get_scratch_folder

        db_scratch_folder = get_scratch_folder() if override_scratch_folder is None else override_scratch_folder
        final_db_path = os.path.join(db_scratch_folder, "scratch_database.db")
    shutil.copyfile(src=test_db_backup_path, dst=final_db_path)

    return final_db_path


def file_load_test_database_with_file_data(scratch=False):
    """
    Copies the test database (with the test files data) from where it resides in the LiuXin_data_folder to the database
    path. The database at that location, if any, is overwritten.
    :param scratch: Instead of replacing the main database copies the database into a scratch folder and returns the
                    path to the database.
    :return final_db_path: Return a path to where ever the database file ended up
    """
    info_str = "About to copy the test database with the file data"
    puts(colored.green(info_str))

    from LiuXin_alpha.constants import LiuXin_default_database
    from LiuXin_alpha.constants import LiuXin_data_folder
    import os
    import shutil

    test_db_backup_path = os.path.join(LiuXin_data_folder, "test_database_file_data.db")

    if not scratch:
        if os.path.exists(test_db_backup_path):
            try:
                os.remove(LiuXin_default_database)
            except OSError:
                # File has (probably) already been deleted - continuing
                puts(colored.yellow("DatabasePing was not found to delete - continuing"))
        final_db_path = deepcopy(LiuXin_default_database)
    else:
        from LiuXin_alpha.utils.ptempfiles import get_scratch_folder

        db_scratch_folder = get_scratch_folder()
        final_db_path = os.path.join(db_scratch_folder, "scratch_database.db")
    shutil.copyfile(src=test_db_backup_path, dst=final_db_path)

    if os.path.exists(final_db_path):
        info_str = "Test database with file test data has been generated"
        puts(colored.green(info_str))
    else:
        puts(colored.red("Final database could not be generated - final_db_path: {}".format(final_db_path)))

    return final_db_path


########################################################################################################################
# FUNCTIONS WHICH ARE USED TO BUILD THE TEST DATABASE START HERE
########################################################################################################################


def test_add_title(test_library, test_db):
    """
    Test adding a title to the database.
    :param test_library:
    :param test_db:
    :return:
    """
    t1_date = datetime.date(year=1987, month=10, day=1)
    title_row = test_library.add.title(
        title="How Much For Just The Planet?",
        title_sort="How Much For Just The Planet?",
        title_pub_date=t1_date,
        title_copyright_date=t1_date,
        title_wikipedia="https://en.wikipedia.org/wiki/How_Much_for_Just_the_Planet%3F",
        title_fiction_length_category="Novel",
        title_type="novel",
        title_source="local_file",
        title_wordcount=132560,
    )
    title_row_2 = test_db.get_row_from_id(table="titles", row_id=title_row["title_id"])

    if title_row["title"] == title_row_2["title"]:
        puts(colored.green("Title row was successfully added"))
    else:
        puts(colored.red("Title row was not successfully added"))
        return False

    bs_title = test_library.add.title(
        title="Beyond the Stars: A Planet Too Far: a space opera anthology",
        title_sort="Beyond the Stars",
        title_pub_date=None,
        title_wikipedia=None,
        title_fiction_length_category="Novel",
        title_type="anthology",
    )

    vt_title = test_library.add.title(title="Venatoris", title_sort="Venatoris")

    return True


def test_add_tag(test_library, test_db):
    """
    Testing adding a tag to the database.
    :param test_library:
    :param test_db:
    :return:
    """
    tag_row = test_library.add.tag("rational fiction")
    tag_row_2 = test_db.get_row_from_id(table="tags", row_id=tag_row["tag_id"])

    if tag_row["tag"] == tag_row_2["tag"]:
        puts(colored.green("tag was successfully added to the database"))
    else:
        puts(colored.green("tag was not successfully added to the database"))
        return False

    return True


def test_add_subject(test_library, test_db):
    """
    Testing adding a subject to the database.
    :param test_library:
    :param test_db:
    :return:
    """
    mh_sub = test_library.add.subject(subject="Military History")
    mh_sub_2 = test_db.get_row_from_id(table="subjects", row_id=mh_sub["subject_id"])

    if mh_sub["subject"] == mh_sub_2["subject"]:
        puts(colored.green("Subject row from the created row and the row retrieved from the db match"))
    else:
        puts(colored.red("Subject row from the created row and the row retrieved from the db do not match"))
        return False

    # Create a child subject for Military History
    ah_sub = test_library.add.subject(subject="Armour Development", subject_parent=mh_sub)
    if str(ah_sub["subject_parent"]) == str(mh_sub["subject_id"]):
        puts(colored.green("Subject parent was properly set"))
    else:
        puts(colored.red("Subject parent was not properly set"))
        return False

    return True


def test_add_series(test_library, test_db):
    """
    Test adding a series to the library.
    :param test_library:
    :param test_db:
    :return:
    """
    gr_creator_row = test_library.add.creator(creator="Gene Roddenberry")
    series_note = test_library.add.note("Something, something optimistic utopia. Commies.")

    st_series_row = test_library.add.series(
        series="Star Trek",
        series_creator=gr_creator_row,
        series_sort=None,
        series_parent=None,
        series_parent_position=None,
        series_note=series_note,
    )
    st_series_row_2 = test_db.get_row_from_id(table="series", row_id=st_series_row["series_id"])

    if st_series_row["series"] == st_series_row["series"]:
        puts(colored.green("The series name and the name from the series row retrieved from the database match"))
    else:
        puts(
            colored.red(
                "Mismatch - created_series_series: {} - db_series_series: {}".format(
                    st_series_row["series"], st_series_row_2["series"]
                )
            )
        )
        return False

    return True


def test_publisher_add(test_library, test_db):
    """
    Tests the library.add.publisher method to make sure it actually adds a publisher to the publisher table.
    :param test_library:
    :param test_db:
    :return:
    """
    pub_row = test_library.add.publisher(
        publisher="Baen Books",
        publisher_description=None,
        publisher_wikipedia="https://en.wikipedia.org/wiki/Baen_Books",
        publisher_website="http://www.baen.com/",
    )
    pub_row_2 = test_db.get_row_from_id(table="publishers", row_id=pub_row.row_id)

    if pub_row["publisher"] == pub_row_2["publisher"]:
        puts(
            colored.green(
                "The publisher created with the library method and retrieved from the database are the same "
                "- publihser: {}".format(pub_row["publisher"])
            )
        )
    else:
        puts(
            colored.red(
                "The publisher created with the library method and retrieved from the database are not the "
                "same - created_publihser: {} - db_publisher: {}".format(pub_row["publisher"], pub_row_2["publisher"])
            )
        )

    # Create the parent row for Tor books, St. Martin's Press
    st_mart_note = test_library.add.note(
        "One of the largest English language publishers, bring to market some 700 " "books a year"
    )
    st_mrt_row = test_library.add.publisher(
        publisher="St. Martin's Press",
        publisher_description=st_mart_note,
        publisher_website="http://us.macmillan.com/smp",
        publisher_wikipedia="https://en.wikipedia.org/wiki/St._Martin's_Press",
    )

    # Test the various ways of creating a publisher_description in a publisher row
    tor_note_row = test_library.add.note("Tor books is the primary imprint of Tom Doherty Associates LLC, based in NY")
    tor_pub_row = test_library.add.publisher(
        publisher="Tor Books",
        publisher_description=tor_note_row,
        publisher_website="http://www.tor.com/",
        publisher_wikipedia="https://en.wikipedia.org/wiki/Tor_Books",
        publisher_parent=st_mrt_row,
    )

    if tor_note_row["note_id"] == tor_pub_row["publisher_description"]:
        puts(colored.green("Load note from note row succeeded"))
    else:
        puts(colored.red("Load note from note row failed"))
        return False

    if tor_pub_row["publisher_parent"] == st_mrt_row["publisher_id"]:
        puts(colored.green("Passing in a row as a publisher_parent set correctly"))
    else:
        err_str = "Passing in a row as the publisher parent did not set correctly\n"
        err_str += "tor_pub_row.publisher_parent : {}\n".format(tor_pub_row["publisher_parent"])
        err_str += "st_mrt_row.publisher_id: {}".format(st_mrt_row["publisher_id"])
        puts(colored.red(err_str))
        return False

    return True


def test_genre_add(test_library, test_db):
    """
    Test the capacity to add a genre.
    :param test_library:
    :param test_db:
    :return:
    """
    gr = test_library.add.genre(genre="Science Fiction")
    gr_2 = test_db.get_row_from_id(table="genres", row_id=gr["genre_id"])

    if gr["genre"] == gr_2["genre"]:
        puts(
            colored.green(
                "Genres from the created row and the row retrieved from the database match - genre: {}".format(
                    gr["genre"]
                )
            )
        )
    else:
        puts(
            colored.red(
                "Genres from the created row and the row retrieved from the database match - "
                "created_genre: {} - retrieved_genre: {}".format(gr["genre"], gr_2["genre"])
            )
        )
        return False

    # Now create a child genre - Military Science Fiction under science fiction
    gr_mil_sf = test_library.add.genre(genre="Military Science Fiction", genre_parent=gr, genre_position=1)
    gr_mil_sf_2 = test_db.get_row_from_id(table="genres", row_id=gr_mil_sf["genre_id"])

    if gr_mil_sf["genre"] == gr_mil_sf_2["genre"]:
        puts(
            colored.green(
                "Genres from the created row and the row retrieved from the database match - genre: {}".format(
                    gr_mil_sf["genre"]
                )
            )
        )
    else:
        puts(
            colored.red(
                "Genres from the created row and the row retrieved from the database match - "
                "created_genre: {} - retrieved_genre: {}".format(gr_mil_sf["genre"], gr_mil_sf_2["genre"])
            )
        )
        return False

    if gr_mil_sf["genre_parent"] == gr_mil_sf_2["genre_parent"]:
        puts(
            colored.green(
                "Genres parents from the two retrieved rows match - genre_parent: {}".format(gr_mil_sf["genre_parent"])
            )
        )
    else:
        puts(
            colored.red(
                "Genres parents from the two retrieved rows do not match - created_genre_parent: {} - "
                "db_genre_parent: {}".format(gr_mil_sf["genre_parent"], gr_mil_sf_2["genre_parent"])
            )
        )
        return False
    return True


def test_creator_add(test_library, test_db):
    """
    Test adding stuff to a test_library.
    :param test_library:
    :return:
    """
    herriot_birth_date = datetime.date(day=3, month=10, year=1916)
    herriot_death_date = datetime.date(day=23, month=2, year=1995)

    cr = test_library.add.creator(
        creator="James Herriot",
        creator_legal_name='James Alfred "Alf" Wight',
        creator_type="author",
        creator_sort="Herriot, James",
        creator_short_name="J Herriott",
        creator_last_name="Herriot",
        creator_birth_date=herriot_birth_date,
        creator_death_date=herriot_death_date,
        creator_wikipedia="https://en.wikipedia.org/wiki/James_Herriot",
        creator_imdb="http://www.imdb.com/name/nm0380713/",
        creator_seminal_work="All Creatures Great and Small",
        creator_language=None,
        creator_bio='Alf Wight ("James Herriot") was born on 3 October 1916 in Sunderland, near '
        "Newcastle. However the family moved to Yoker, a suburb of Glasgow, when Alf "
        "was three weeks old. He attended Glasgow Veterinary School. He moved to "
        "Thirsk, North Yorkshire, in 1940, to work for Donald Sinclair "
        '("Siegfried Farnon") at his practice at 23 Kirkgate. He married Joan '
        'Danbury ("Helen Alderson") on 5 November 1941 at St Mary Magdalene church '
        "in Thirsk. They had two children, Jim (born 1943) and Rosie (born 1947): Jim "
        "is a vet who used to work in the Sinclair/Wight practice and Rosie is a "
        "General Practitioner (family doctor). Alf died on 23 February 1995 of "
        'prostate cancer at his house, "Mirebeck", in the village of Thirlby near '
        'the town of Thirsk that became famous in his books as "Darrowby".',
        creator_link=None,
    )

    cr_2 = test_db.get_row_from_id(table="creators", row_id=cr["creator_id"])

    if cr["creator"] == cr_2["creator"]:
        puts(
            colored.green(
                "Creators for the created row and the row retrieved from the database match - creator:{}".format(
                    cr["creator"]
                )
            )
        )
        return True
    else:
        puts(
            colored.red(
                "Creators from the creator row and the row returned from the database do not match - "
                "created_creator: {} - retrieved_creator: {}".format(cr["creator"], cr_2["creator"])
            )
        )
        return False


def test_apply_note(test_library, test_db):
    """
    Test applying a note to a title in the library.
    :param test_library:
    :param test_db:
    :return:
    """
    title_row = test_db.get_row_from_id("titles", 1)

    note_text = (
        "This is a test note which has been applied to {} as part of the LiuXin test suite. "
        "It can be safely deleted".format(title_row["title"])
    )

    test_library.apply.note(note=note_text, resource=title_row)

    # Retrieve the notes linked to the first title row - check that the note has been retrieved corrected
    title_notes = test_db.get_interlinked_rows(title_row, "notes")
    if len(title_notes) == 0:
        puts(colored.red("Title row isn't linked to any notes"))
        return False
    elif len(title_notes) > 1:
        puts(colored.red("Title row appears to be linked to too many notes"))
        for note_row in title_notes:
            print(note_row["note"])
        return False
    else:
        puts(colored.green("The title row is linked to one note, as expected"))

    # Check that the note text is as expected
    note_row = title_notes[0]
    if note_row["note"] == note_text:
        puts(colored.green("Note text was as expected"))
    else:
        puts(colored.red("Note text was not as expected - note text: {}".format(note_text)))
        return False
    return True


def test_apply_tags(test_library, test_db):
    """
    Test applying a tag to an resource.
    :param test_library:
    :param test_db:
    :return:
    """
    # Test applying tags to a title
    title_row = test_db.get_row_from_id("titles", 1)

    # Test apply a number of tags to the object
    crypt_tags = {
        "alt history",
        "cryptography",
        "World War 2",
        "alchemy",
        "scientist hero",
        "enigma",
        "Nazis",
    }

    for tag_str in crypt_tags:
        test_library.apply.tag(tag=tag_str, resource=title_row)

    tag_rows = test_db.get_interlinked_rows(target_row=title_row, secondary_table="tags")
    tag_val_set = set([r["tag"] for r in tag_rows])
    if crypt_tags == tag_val_set:
        puts(colored.green("tags retrieved from the database where the ones written to it"))
    else:
        err_str = "Tags retrieved from the database where not the ones written to it\n"
        err_str += "crypt_tags: " + six_unicode(crypt_tags) + "\n"
        err_str += "retrieved_tags: " + six_unicode(tag_val_set) + "\n"
        puts(colored.red(err_str))
        return False

    # Test apply a number of tags to a series
    discworld_tags = {
        "epic fantasy",
        "humour",
        "social commentary",
        "bestseller",
        "discworld",
    }
    disc_row = test_db.get_row_from_id("series", 2)

    for tag_str in discworld_tags:
        test_library.apply.tag(tag=tag_str, resource=disc_row)

    series_tag_rows = test_db.get_interlinked_rows(target_row=disc_row, secondary_table="tags")
    series_tag_val = set(r["tag"] for r in series_tag_rows)
    if discworld_tags == series_tag_val:
        puts(colored.green("tags retrieved from the database for a series where the ones written to it"))
    else:
        err_str = "Tags retrieved from the database where not the ones written to it for a series\n"
        err_str += "discworld_tags: " + six_unicode(discworld_tags) + "\n"
        err_str += "retrieved_tags: " + six_unicode(series_tag_val) + "\n"
        puts(colored.red(err_str))
        return False

    # Apply some tags to the rest of the titles
    test_library.apply.tag(
        tag=[
            "diamond",
            "3d_printing",
            "post_scarcity",
            "coming_of_age",
            "victorian_values",
        ],
        resource=test_db.get_row_from_id("titles", row_id=2),
    )
    test_library.apply.tag(
        tag=["pizza_delivery", "cyberpunk", "post_scarcity", "mind_control", "mafia"],
        resource=test_db.get_row_from_id("titles", row_id=3),
    )
    test_library.apply.tag(
        tag=[
            "hamiltonian_mechanics",
            "monks",
            "femtotech",
            "mathematics",
            "first_contact",
        ],
        resource=test_db.get_row_from_id("titles", row_id=4),
    )
    test_library.apply.tag(
        tag=["history", "17th century", "english_history", "newton", "alchemy"],
        resource=test_db.get_row_from_id("titles", row_id=5),
    )
    test_library.apply.tag(
        tag=[
            "history",
            "17th century",
            "royal_society",
            "waterhouse",
            "glorious revolution",
        ],
        resource=test_db.get_row_from_id("titles", row_id=6),
    )
    test_library.apply.tag(
        tag=["rationality", "heists", "royal_society", "george_1", "scientific method"],
        resource=test_db.get_row_from_id("titles", row_id=7),
    )
    test_library.apply.tag(
        tag=[
            "post human",
            "parallel worlds",
            "yellowstone eruption",
            "long earth",
            "ai",
        ],
        resource=test_db.get_row_from_id("titles", row_id=8),
    )
    test_library.apply.tag(
        tag=["science", "botany", "survival", "Mars", "NASA"],
        resource=test_db.get_row_from_id("titles", row_id=9),
    )
    test_library.apply.tag(
        tag=["thieves", "conmen", "Locke Lamorra", "thriller"],
        resource=test_db.get_row_from_id("titles", row_id=10),
    )
    test_library.apply.tag(
        tag=["Granny Weatherwax", "Witches", "coming_of_age", "magic", "elves"],
        resource=test_db.get_row_from_id("titles", row_id=11),
    )
    test_library.apply.tag(
        tag=["popular_science", "xkcd"],
        resource=test_db.get_row_from_id("titles", row_id=12),
    )
    test_library.apply.tag(
        tag=["computing", "gui", "command_line", "historical interest", "essays"],
        resource=test_db.get_row_from_id("titles", row_id=13),
    )
    test_library.apply.tag(
        tag=[
            "fanfiction",
            "space_battles",
            "sufficient_velocity",
            "self_insert",
            "dc_comics",
        ],
        resource=test_db.get_row_from_id("titles", row_id=14),
    )
    test_library.apply.tag(
        tag=[
            "NASA",
            "shuttle",
            "misanthropic",
            "survival",
            "space_voyage",
            "deep_time",
        ],
        resource=test_db.get_row_from_id("titles", row_id=15),
    )
    test_library.apply.tag(
        tag=["deep_time", "time_travel", "paradox", "authorized_sequel", "dyson_shell"],
        resource=test_db.get_row_from_id("titles", row_id=16),
    )
    test_library.apply.tag(
        tag=[
            "misanthropic",
            "space_travel",
            "interstellar_flight",
            "colonization",
            "alternative_universe",
        ],
        resource=test_db.get_row_from_id("titles", row_id=17),
    )
    test_library.apply.tag(
        tag=["misanthropic", "space_travel", "alt_history", "deep_time"],
        resource=test_db.get_row_from_id("titles", row_id=18),
    )
    test_library.apply.tag(
        tag=["xeelee", "hive_mind", "Rome", "poole", "historical_fiction", "cult"],
        resource=test_db.get_row_from_id("titles", row_id=19),
    )
    test_library.apply.tag(
        tag=[
            "xeelee",
            "environmental_collapse",
            "deep_time",
            "time_travel",
            "geological_engineering",
        ],
        resource=test_db.get_row_from_id("titles", row_id=20),
    )
    test_library.apply.tag(
        tag=["xeelee", "time_travel", "deep_time", "FTL_War"],
        resource=test_db.get_row_from_id("titles", row_id=21),
    )

    test_library.apply.tag(
        tag=["apocalyptic", "NASA", "nanotech", "lunar_colonization", "near_future"],
        resource=test_db.get_row_from_id("titles", row_id=25),
    )
    test_library.apply.tag(
        tag=["long_earth", "parallel_world", "colonization", "politics"],
        resource=test_db.get_row_from_id("titles", row_id=26),
    )
    test_library.apply.tag(
        tag=["long_Earth", "parallel_world", "colonization", "America", "airship"],
        resource=test_db.get_row_from_id("titles", row_id=27),
    )
    # The Long Mars
    test_library.apply.tag(
        tag=["Mars", "colonization", "America", "parallel_world", "human_evolution"],
        resource=test_db.get_row_from_id("titles", row_id=28),
    )
    # Anti-Ice
    test_library.apply.tag(
        tag=["Victorian", "steampunk", "anti-matter", "space_travel", "Britain"],
        resource=test_db.get_row_from_id("titles", row_id=29),
    )
    # Wheel of Ice
    test_library.apply.tag(
        tag=["Doctor Who", "space_colony"],
        resource=test_db.get_row_from_id("titles", row_id=30),
    )
    # Pandora's Star
    test_library.apply.tag(
        tag=[
            "space_opera",
            "Nigel Sheldon",
            "Paula Mayo",
            "Commonwealth",
            "Starflyer",
            "Wormholes",
            "Interstellar War",
        ],
        resource=test_db.get_row_from_id("titles", row_id=31),
    )
    # Judas Unchained
    test_library.apply.tag(
        tag=[
            "space_opera",
            "Nigel Sheldon",
            "Paula Mayo",
            "Commonwealth",
            "Starflyer",
            "Wormholes",
            "Interstellar War",
        ],
        resource=test_db.get_row_from_id("titles", row_id=32),
    )

    # Diggers
    test_library.apply.tag(
        tag=["Bromeliad", "Nomes"],
        resource=test_db.get_row_from_id("titles", row_id=36),
    )
    # Truckers
    test_library.apply.tag(
        tag=["Bromeliad", "Nomes", "Diggers"],
        resource=test_db.get_row_from_id("titles", row_id=37),
    )
    # Wings
    test_library.apply.tag(
        tag=["Bromeliad", "Nomes", "Concorde", "Star Ship"],
        resource=test_db.get_row_from_id("titles", row_id=38),
    )

    # The Colour of Magic
    test_library.apply.tag(
        tag=[
            "Discworld",
            "Rincewind",
            "The Wizards",
            "Unseen University",
            "Magic",
            "Great A'tuin",
        ],
        resource=test_db.get_row_from_id("titles", row_id=39),
    )
    # The Light Fantastic
    test_library.apply.tag(
        tag=[
            "Discworld",
            "Rincewind",
            "The Wizards",
            "Unseen University",
            "Magic",
            "Great A'tuin",
        ],
        resource=test_db.get_row_from_id("titles", row_id=40),
    )

    # The Fellowship of the Ring
    test_library.apply.tag(
        tag=["Elves", "Dwarfs", "Middle Earth", "One Ring", "Quest"],
        resource=test_db.get_row_from_id("titles", row_id=83),
    )

    puts(colored.green("Completed applying tags to titles - now applying to series"))

    # The Bromeliad
    test_library.apply.tag(
        tag=["Nomes", "Aliens", "The Thing"],
        resource=test_db.get_row_from_id("series", row_id=1),
    )

    # Discworld
    test_library.apply.tag(
        tag=["Fantasy", "Pastiche", "Social Commentry", "The Disc", "Magic"],
        resource=test_db.get_row_from_id("series", row_id=2),
    )

    # The Science of the Disc
    test_library.apply.tag(
        tag=["Fantasy", "Wizards", "Popular Science", "The Disc", "Magic"],
        resource=test_db.get_row_from_id("series", row_id=3),
    )

    # The Baroque Cycle
    test_library.apply.tag(
        tag=[
            "Baroque",
            "Alternative History",
            "Royal Society",
            "Isaac Newton",
            "Economics",
            "Alchemy",
        ],
        resource=test_db.get_row_from_id("series", row_id=4),
    )

    # The Baroque Cycle
    test_library.apply.tag(
        tag=["Parallel Worlds", "Near Future", "Exploration"],
        resource=test_db.get_row_from_id("series", row_id=5),
    )

    # Locke Lamorra
    test_library.apply.tag(
        tag=[
            "Fantasy",
            "Magic",
            "Thievery",
            "Medieval Fantasy",
            "Conmen",
            "Locke Lamorra",
        ],
        resource=test_db.get_row_from_id("series", row_id=6),
    )

    # Tiffany Aching
    test_library.apply.tag(
        tag=["Magic", "Witches", "Coming of Age", "The Chalk"],
        resource=test_db.get_row_from_id("series", row_id=7),
    )

    # The Farthest Stars
    test_library.apply.tag(
        tag=[
            "Alternative Worlds",
            "Ancient Aliens",
            "Starships",
            "Romans",
            "Misanthropic",
        ],
        resource=test_db.get_row_from_id("series", row_id=8),
    )

    # Xeelee Sequence
    test_library.apply.tag(
        tag=["Deep Time", "Ancient Aliens", "Starships", "Aliens", "Xeeleel"],
        resource=test_db.get_row_from_id("series", row_id=9),
    )

    # Destiny's Children
    test_library.apply.tag(
        tag=[
            "Alternative Worlds",
            "Ancient Aliens",
            "Starships",
            "Romans",
            "Misanthropic",
        ],
        resource=test_db.get_row_from_id("series", row_id=10),
    )

    # The Nasa Trilogy
    test_library.apply.tag(
        tag=[
            "Alternative Worlds",
            "Ancient Aliens",
            "Starships",
            "Romans",
            "Misanthropic",
        ],
        resource=test_db.get_row_from_id("series", row_id=11),
    )

    # The Commonwealth
    test_library.apply.tag(
        tag=["Wormholes", "Ancient Aliens", "Starships", "Commonwealth"],
        resource=test_db.get_row_from_id("series", row_id=12),
    )

    # The Starflyer War
    test_library.apply.tag(
        tag=["Nigel Sheldon", "Starflyer", "Starships", "Ozzie", "Elves", "Aliens"],
        resource=test_db.get_row_from_id("series", row_id=13),
    )

    # The Void Trilogy
    test_library.apply.tag(
        tag=["Nigel Sheldon", "The Void", "Starships", "Ozzie", "Elves", "Aliens"],
        resource=test_db.get_row_from_id("series", row_id=15),
    )

    return True


def test_apply_publisher(test_library, test_db):
    """
    Test apply publishers to the titles in the given library.
    :param test_library:
    :param test_db:
    :return:
    """
    # Add some publishers to the publishers table
    pan_row = test_library.ensure.publisher("Pan")
    corgi_row = test_library.ensure.publisher("Corgi")
    orbit_row = test_library.ensure.publisher("Orbit")
    titan_row = test_library.ensure.publisher("Titan Books")
    gollancz_row = test_library.ensure.publisher("Gollancz")
    sceptre_row = test_library.ensure.publisher("Sceptre")
    touchstone_row = test_library.ensure.publisher("Touchstone")
    dc_row = test_library.ensure.publisher("DC Comics")
    hc_row = test_library.ensure.publisher("HarperCollins")
    wc_row = test_library.ensure.publisher("William Collins")
    gdc_row = test_library.ensure.publisher("Gerald Duckworth & Co Ltd")
    jws_rows = test_library.ensure.publisher("John Wiley & Sons")
    griffin_row = test_library.ensure.publisher("Griffin")
    hp_row = test_library.ensure.publisher("Harper Perennial")
    hodder_row = test_library.ensure.publisher("Hodder Paperbacks")
    arrow_row = test_library.ensure.publisher("Arrow")

    test_library.apply.publisher(publisher=gollancz_row, title_row=test_db.get_row_from_id("titles", 93))

    test_library.apply.publisher(publisher=sceptre_row, title_row=test_db.get_row_from_id("titles", 92))

    test_library.apply.publisher(publisher=titan_row, title_row=test_db.get_row_from_id("titles", 91))

    test_library.apply.publisher(publisher=orbit_row, title_row=test_db.get_row_from_id("titles", 90))

    test_library.apply.publisher(publisher=gollancz_row, title_row=test_db.get_row_from_id("titles", 89))

    test_library.apply.publisher(publisher=touchstone_row, title_row=test_db.get_row_from_id("titles", 88))

    test_library.apply.publisher(publisher=dc_row, title_row=test_db.get_row_from_id("titles", 87))

    test_library.apply.publisher(publisher=dc_row, title_row=test_db.get_row_from_id("titles", 86))

    test_library.apply.publisher(publisher=hc_row, title_row=test_db.get_row_from_id("titles", 85))
    test_library.apply.publisher(publisher=hc_row, title_row=test_db.get_row_from_id("titles", 84))
    test_library.apply.publisher(publisher=hc_row, title_row=test_db.get_row_from_id("titles", 83))
    test_library.apply.publisher(publisher=hc_row, title_row=test_db.get_row_from_id("titles", 82))
    test_library.apply.publisher(publisher=hc_row, title_row=test_db.get_row_from_id("titles", 81))

    test_library.apply.publisher(publisher=wc_row, title_row=test_db.get_row_from_id("titles", 80))

    test_library.apply.publisher(publisher=hc_row, title_row=test_db.get_row_from_id("titles", 79))
    test_library.apply.publisher(publisher=hc_row, title_row=test_db.get_row_from_id("titles", 78))

    test_library.apply.publisher(publisher=gdc_row, title_row=test_db.get_row_from_id("titles", 77))

    test_library.apply.publisher(publisher=jws_rows, title_row=test_db.get_row_from_id("titles", 76))

    test_library.apply.publisher(publisher=pan_row, title_row=test_db.get_row_from_id("titles", 75))

    test_library.apply.publisher(publisher=griffin_row, title_row=test_db.get_row_from_id("titles", 74))

    test_library.apply.publisher(publisher=gollancz_row, title_row=test_db.get_row_from_id("titles", 73))

    test_library.apply.publisher(publisher=gollancz_row, title_row=test_db.get_row_from_id("titles", 72))

    test_library.apply.publisher(publisher=hp_row, title_row=test_db.get_row_from_id("titles", 68))

    test_library.apply.publisher(publisher=gollancz_row, title_row=test_db.get_row_from_id("titles", 67))

    test_library.apply.publisher(publisher=gollancz_row, title_row=test_db.get_row_from_id("titles", 66))

    test_library.apply.publisher(publisher=gollancz_row, title_row=test_db.get_row_from_id("titles", 65))

    test_library.apply.publisher(publisher=gollancz_row, title_row=test_db.get_row_from_id("titles", 64))

    test_library.apply.publisher(publisher=gollancz_row, title_row=test_db.get_row_from_id("titles", 63))

    test_library.apply.publisher(publisher=hodder_row, title_row=test_db.get_row_from_id("titles", 62))
    test_library.apply.publisher(publisher=hodder_row, title_row=test_db.get_row_from_id("titles", 61))
    test_library.apply.publisher(publisher=hodder_row, title_row=test_db.get_row_from_id("titles", 60))
    test_library.apply.publisher(publisher=hodder_row, title_row=test_db.get_row_from_id("titles", 59))
    test_library.apply.publisher(publisher=hodder_row, title_row=test_db.get_row_from_id("titles", 58))

    test_library.apply.publisher(publisher=corgi_row, title_row=test_db.get_row_from_id("titles", 57))

    test_library.apply.publisher(publisher=hodder_row, title_row=test_db.get_row_from_id("titles", 56))
    test_library.apply.publisher(publisher=hodder_row, title_row=test_db.get_row_from_id("titles", 55))

    test_library.apply.publisher(publisher=pan_row, title_row=test_db.get_row_from_id("titles", 54))
    test_library.apply.publisher(publisher=pan_row, title_row=test_db.get_row_from_id("titles", 53))
    test_library.apply.publisher(publisher=pan_row, title_row=test_db.get_row_from_id("titles", 52))

    for i in range(36, 51):
        test_library.apply.publisher(publisher=pan_row, title_row=test_db.get_row_from_id("titles", i))

    for i in range(31, 36):
        test_library.apply.publisher(publisher=pan_row, title_row=test_db.get_row_from_id("titles", i))

    test_library.apply.publisher(publisher=arrow_row, title_row=test_db.get_row_from_id("titles", 1))

    return True


def test_apply_synopsis(test_library, test_db):
    """
    Add synopsis to some of the other resources.
    :param test_library:
    :param test_db:
    :return:
    """
    puts(colored.green("About to try applying a synopsis to a number of titles"))

    crypt_synop = "A ... involved story of crypotgraphy, economics and esponiage during the second world war."
    test_library.apply.synopsis(synopsis=crypt_synop, resource=test_db.get_row_from_id("titles", 1))

    # Add synopsis to a selection of five titles
    da_synopsis = (
        "In a fractured world where the nation state has died an automated education system functions "
        "precisely as desired."
    )
    test_library.apply.synopsis(synopsis=da_synopsis, resource=test_db.get_row_from_id("titles", 2))

    titan_synop = (
        "In a near future where America has been taken over by right wing zealots, NASA is dying a slow "
        "death. A group of scientists hatch an ambicious plan to confirm the existence of life on Titan."
        "Then China screws up and destroy the planet."
    )
    test_library.apply.synopsis(synopsis=titan_synop, resource=test_db.get_row_from_id("titles", 23))

    diggers_synop = (
        "In the climax of the Bromeliad Trilogy, the Gnomes go to California to secure a starship. " "Hilarity ensues."
    )
    test_library.apply.synopsis(synopsis=diggers_synop, resource=test_db.get_row_from_id("titles", 38))

    fac_synop = "Flashman at the Charge sees Flashman at the charge of the Light Brigade"
    test_library.apply.synopsis(synopsis=fac_synop, resource=test_db.get_row_from_id("titles", 78))

    tt_synop = "An epic tale of revenge amongst the planets of the solar system. The Count of Monte Cristo is SPACE!!!"
    test_library.apply.synopsis(synopsis=tt_synop, resource=test_db.get_row_from_id("titles", 93))

    # Add synopsis to a number of series
    puts(colored.green("About to try applying a synopsis to a number of series"))

    br_synopsis = "The story of a bunch of Gnomes having to confront societal change and a series of disasters."
    test_library.apply.synopsis(synopsis=br_synopsis, resource=test_db.get_row_from_id("series", 1))

    dw_synopsis = (
        "One of the great modern fantasy series - a mixture of humour and fantasy by one of the finest "
        "social commentators of all time."
    )
    test_library.apply.synopsis(synopsis=dw_synopsis, resource=test_db.get_row_from_id("series", 2))

    sdw_synopsis = "Using the Wizards of the Disc as a framework to discuss cutting edge popular science"
    test_library.apply.synopsis(synopsis=sdw_synopsis, resource=test_db.get_row_from_id("series", 3))

    bc_synopsis = "An epic story of the rise of the scientific method and the dawn of the age of reason."
    test_library.apply.synopsis(synopsis=bc_synopsis, resource=test_db.get_row_from_id("series", 4))

    lw_synopsis = (
        "One day someone open sources a vital new technology. With the ability to walk between parallel "
        "worlds granted to humanity, things rapidly get very strange."
    )
    test_library.apply.synopsis(synopsis=lw_synopsis, resource=test_db.get_row_from_id("series", 5))

    ll_synopsis = "The story of the Gentlemen Bastards - a group of thieves in a fantasy world."
    test_library.apply.synopsis(synopsis=ll_synopsis, resource=test_db.get_row_from_id("series", 6))

    ta_synopsis = "Granny Weatherwax's successor grows into her role and comes of age."
    test_library.apply.synopsis(synopsis=ta_synopsis, resource=test_db.get_row_from_id("series", 7))

    fm_synopsis = "The bully from Tom Brown's schooldays joins the army."
    test_library.apply.synopsis(synopsis=fm_synopsis, resource=test_db.get_row_from_id("series", 16))

    return True


def test_add_language(test_library, test_db):
    """
    Test add a language to the database.
    :param test_library:
    :param test_db:
    :return:
    """
    test_library.add.language(language_name="english", language_code="eng_test")

    # Check that there is only one row on the database
    language_count = test_db.driver_wrapper.get_record_count("languages")
    if language_count == 1:
        puts(colored.green("There is only one entry in the language table - as expected"))
    else:
        puts(
            colored.red(
                "There is more than one entry on the language table - which is not expected"
                " language_count: {}".format(language_count)
            )
        )
        return False

    for lang_row in test_db.get_all_rows("languages"):
        puts(colored.yellow(six_unicode(lang_row.row_dict)))

    return True


def test_apply_language(test_library, test_db):
    """
    Test applying a language to a title
    :param test_library:
    :param test_db:
    :return:
    """
    puts(colored.green("About to apply English to all titles"))
    for title in test_db.get_all_rows(table="titles", iterator_return=True):
        test_library.apply.language(language="english", resource_row=title)

    # Check that there is only one row on the database
    language_count = test_db.driver_wrapper.get_record_count("languages")
    if language_count == 1:
        puts(colored.green("There is only one entry in the language table - as expected"))
    else:
        puts(
            colored.red(
                "There is more than one entry on the language table - which is not expected"
                " language_count: {}".format(language_count)
            )
        )
        for lang in test_db.get_all_rows("languages"):
            print(lang.row_dict)

    return True


def create_all_books(test_library, test_db):
    """
    Create a book for every title on the database.
    :param test_library:
    :param test_db:
    :return:
    """
    for title_row in test_db.get_all_rows("titles"):
        puts(colored.green("Creating book row for {}".format(title_row["title"])))
        test_library.add.book(title_row=title_row)

    return True


# {{{
def generate_test_db(
    library_path=None,
    num_of_records=20000,
    num_of_authors=6000,
    num_of_tags=10000,
    tag_length=7,
    author_length=7,
    title_length=10,
    max_authors=10,
    max_tags=10,
):
    """
    Populates a database with a large amount of random data - useful for speed trials.
    Method adapted from calibre - see test_db_4 for the LiuXin implementation of this - which provides for more detail
    to controle generation of the database topology
    :param library_path:
    :param num_of_records:
    :param num_of_authors:
    :param num_of_tags:
    :param tag_length:
    :param author_length:
    :param title_length:
    :param max_authors:
    :param max_tags:
    :return:
    """
    # Done here to isolate the random module where possible - for there is no guarantee on it that the actual numbers
    # produced are the same between version - unless your using random.random() with identicle seeding

    from LiuXin_alpha.utils.libraries.liuxin_random import LiuXinBadPseudoRandomGenerator

    lx_random = LiuXinBadPseudoRandomGenerator(seed=153)

    from LiuXin_alpha.constants import preferred_encoding

    if not os.path.exists(library_path):
        os.makedirs(library_path)

    letters = string.letters.decode(preferred_encoding)

    def randstr(length):
        return "".join(lx_random.choice(letters) for i in memory_range(length))

    all_tags = [randstr(tag_length) for j in memory_range(num_of_tags)]
    print("Generated", num_of_tags, "tags")
    all_authors = [randstr(author_length) for j in memory_range(num_of_authors)]
    print("Generated", num_of_authors, "authors")
    all_titles = [randstr(title_length) for j in memory_range(num_of_records)]
    print("Generated", num_of_records, "titles")

    testdb = Database(library_path)

    print("Creating", num_of_records, "records...")

    start = time.time()

    for i, title in enumerate(all_titles):
        print(i + 1)
        sys.stdout.flush()
        authors = lx_random.randint(1, max_authors)
        authors = [lx_random.choice(all_authors) for i in memory_range(authors)]
        tags = lx_random.randint(0, max_tags)
        tags = [lx_random.choice(all_tags) for i in memory_range(tags)]
        from LiuXin_alpha.metadata.book.base import calibreMetadata as Metadata

        mi = Metadata(title, authors)
        mi.tags = tags
        testdb.import_book(mi, [])

    t = time.time() - start
    print("\nGenerated", num_of_records, "records in:", t, "seconds")
    print("Time per record:", t / float(num_of_records))


# }}}


class TestDatabaseBuilder(object):
    """
    Builder system for test databases.
    """

    def __init__(
        self,
        dst_file_path,
        csv_folder_path=None,
        dump=False,
        plugin_name=None,
        new_db_uuid="auto",
        test_asset_version=None,
    ):
        """
        Parameters to control the test database generation.
        :param dst_file_path: The location that the database file will be copied to once build is complete
        :param csv_folder_path: The csv files for the database will be loaded from here.
                                If no folder path is provided will default to no data
        :param dump: If True then the database will be dumped in csv format into the folder that it was generated from.
        :param plugin_name: Name of the plugin to be generated
        :param new_db_uuid: When setting the database metadata also set the uuid for the database.
                            If "auto" - a name will be automatically generated and set.
        :param test_asset_version: If provided - will write the current version of the test assets into the database.
                                   If not provided, then will just be a uuid string.
        :return:
        """
        self.dst_file_path = dst_file_path
        self.csv_folder_path = csv_folder_path
        self.dump = dump
        self.plugin_name = plugin_name
        self.new_db_uuid = new_db_uuid
        self.test_asset_version = test_asset_version

        self.build_start_time = None
        self.build_end_time = None
        self.build_delta = None

        # Preform the actual build in a ramdisk - disk will be removed one build is complete (with any attendant files
        # generated during the build process
        self.build_ramdisk = get_ramdisk()
        self.build_scratch_folder_manager = ScratchFolderManager(make_in=self.build_ramdisk)

    # ------------------------------------------------------------------------------------------------------------------
    # - UTILITIES
    def get_internal_rng(self, seed=None):
        """
        Return an internal random number generator - which should be an instant of random.Random prepared so that it
        always produces the same sequence when seeded with the same value.
        From the python docs - there should always been a backwards compatible method for random such that you can
        seed the generator and reliably get the same sequence of numbers back.
        This method is to centralize that function in one place.
        :param seed:
        :return:
        """
        # See rational above for why the import is done here
        from utils.lx_libraries.liuxin_random import LiuXinBadPseudoRandomGenerator

        if seed is None:
            internal_rng = LiuXinBadPseudoRandomGenerator(seed=5565233525)  # This is really terrible
        else:
            internal_rng = LiuXinBadPseudoRandomGenerator(seed=seed)
        return internal_rng

    def get_randint(self, internal_rng, lower_bound, upper_bound):
        """
        Takes the current internal_rng and returns an integer between the lower_bound and the upperbound.
        :param internal_rng:
        :param lower_bound:
        :param upper_bound:
        :return:
        """
        return internal_rng.randint(lower_bound, upper_bound)

    def get_plugin_name(self):
        """
        Return the name of the plugin currently being used to generate this test database.
        :return:
        """
        return self.plugin_name

    @staticmethod
    def get_term_size():
        """
        Return the size of the currently active terminal.
        :return:
        """
        return getTerminalSize()

    @staticmethod
    def okay_print(message_str):
        """
        Prints the given message to screen in green.
        :param message_str:
        :return:
        """
        puts(colored.green(message_str))

    @staticmethod
    def warn_print(message_str):
        """
        Prints the given message to screen in green.
        :param message_str:
        :return:
        """
        puts(colored.yellow(message_str))

    def print_banner(self):
        """
        Print a welcome banner to indicate the test database which is currently being generated.
        :return:
        """
        term_width, term_height = self.get_term_size()
        info_str = [
            "-" * term_width,
            "Generating test database for plugin {}".format(self.get_plugin_name()),
            "-" * term_width,
        ]
        if self.dump:
            info_str.append("dump not currently supported")

        self.okay_print("\n".join(info_str))

    def get_scratch_folder(self):
        """
        Return a scratch folder confined to the build_ramdisk
        :return:
        """
        return self.build_scratch_folder_manager.get_scratch_folder()

    # ------------------------------------------------------------------------------------------------------------------

    def run(self):
        """
        Execute build of the database.
        :return:
        """
        # 1) Set the details of the build on the way in
        self.build_start_time = time.time()

        # Helpful for debugging to know which of the test databases we're trying and failing to generate
        self.print_banner()

        # 2) Load the database - we need a base before building out
        # Acquire the base database to build the test database
        scratch_db = self.load_base_database()

        # 3) Preflight for database detailing
        # Gives you a chance to ensure some of the tables are empty
        self.purge_tables(scratch_db)

        # 4) Detail the database
        # Preform detailing work to transform the database into the final form for saving
        # Todo: Should be detail_database
        self.detail_databases(scratch_db)

        # 4.5) Update the timestamps - to make the timestamps static
        self.write_timestamps(scratch_db)

        # 5) Detail the database metadata
        self.detail_database_metadata(scratch_db)

        # 6) Set the id for the newly created database to something more useful than a random string
        self.set_database_ids(scratch_db)

        # 7) Move the database to it's final position - make the changes needed for storage
        self.save_database(scratch_db)

        # 8) Store and display the final information
        self.build_end_time = time.time()
        self.build_delta = self.build_end_time - self.build_start_time

        self.print_exit_banner()

        # 9) Cleanup
        self.cleanup()

    # Todo: Probably want to merge these two properties - at the moment they seem to do the same thing
    def detail_database_metadata(self, scratch_db):
        """
        Provides the building method a chance to make changes to the database metadata - changing the db uuid e.t.c
        :param scratch_db:
        :return:
        """
        pass

    def write_timestamps(self, scratch_db):
        """
        Manually update the timestamps so that they are static between runs.
        :param scratch_db:
        :return:
        """
        for table in scratch_db.get_tables():
            self._write_one_table_timestamps(scratch_db=scratch_db, target_table=table)

    def _write_one_table_timestamps(self, scratch_db, target_table):
        """
        Write timestamps for one table into the database.
        :param scratch_db:
        :param target_table:
        :return:
        """
        self.okay_print("Generating timestamps for {}".format(target_table))

        created_datestamp_time = "2022-04-24 23:59:11"
        created_datestamp_time = datetime.datetime.strptime(created_datestamp_time, "%Y-%m-%d %H:%M:%S")
        created_datestamp_delta = datetime.timedelta(seconds=1)

        cols_for_update = self._get_datestamp_columns(scratch_db, target_table)

        if not cols_for_update:
            self.warn_print("No columns will be updated!")
            return

        self.okay_print("{} cols will be updated".format(cols_for_update))

        for table_row in scratch_db.get_all_rows(target_table):

            for col in cols_for_update:

                table_row[col] = created_datestamp_time
                created_datestamp_time += created_datestamp_delta

    # Todo: write proceedure for adding a column
    # Todo: Consider including more datestamp cols =as standard in all main tables
    def _get_datestamp_columns(self, scratch_db, target_table):
        """
        Get the datestamped coilumns from the target table to update.

        :return:
        """
        datestamp_cols_maps = {
            "books": ("book_created_datestamp", "book_datestamp"),
            "comments": ("comment_datestamp",),
            "covers": ("cover_created_datestamp", "cover_datestamp"),
            "creators": ("creator_created_datestamp", "creator_datestamp"),
            "devices": ("device_created_datestamp", "device_datestamp"),
            "genres": ("genre_datestamp",),
            "files": ("file_created_datestamp", "file_datestamp"),
            "folder_stores": (
                "folder_store_creation_date",
                "folder_store_created_datestamp",
                "folder_store_datestamp",
            ),
            "folders": ("folder_created_datestamp", "folder_datestamp"),
            "identifiers": ("identifier_datestamp",),
            "languages": ("language_datestamp"),
            "notes": ("note_datestamp",),
            "publishers": ("publisher_created_datestamp", "publisher_datestamp"),
            "ratings": ("rating_datestamp",),
            "series": ("series_datestamp",),
            "subjects": ("subject_datestamp",),
            "synopsis": ("synopsis_datestamp",),
            "tags": ("tag_scratch",),
            "titles": (
                "title_created_datestamp",
                "title_datestamp",
                "title_last_modified",
            ),
        }

        cols_for_update = datestamp_cols_maps.get(target_table, None)

        if cols_for_update is None:

            try:
                return tuple(
                    [
                        scratch_db.driver_wrapper.get_datestamp_column(target_table),
                    ]
                )
            except InputIntegrityError:
                return tuple()

        return cols_for_update

    def set_database_ids(self, scratch_db):
        """
        Write the new, consistent identifiers into the database - to note that this
        """
        if self.new_db_uuid != "auto":
            scratch_db.uuid = self.new_db_uuid
            scratch_db.library_id = self.new_db_uuid
        else:
            if self.test_asset_version is None:

                import uuid

                scratch_db.uuid = "test_{}_{}".format(self.plugin_name, str(uuid.uuid4()))
                scratch_db.library_id = "test_{}_{}".format(self.plugin_name, str(uuid.uuid4()))
            else:
                scratch_db.uuid = "test_{}_{}".format(self.plugin_name, self.test_asset_version)
                scratch_db.library_id = "test_{}_{}".format(self.plugin_name, self.test_asset_version)

    def cleanup(self):
        """
        Remove the build ramdisk - which should take care of most of the build files as well.
        :return:
        """
        unmount_ramdisk(self.build_ramdisk)

    def print_exit_banner(self):
        """
        Banner to print when the build has completed.
        :return:
        """
        term_width, term_height = self.get_term_size()
        info_str = [
            "-" * term_width,
            "Generation of test database for plugin {} has completed".format(self.get_plugin_name()),
            "wall time: {} seconds".format(self.build_delta),
            "-" * term_width,
        ]
        if self.dump:
            info_str.append("dump not currently supported")

        self.okay_print("\n".join(info_str))

    def load_base_database(self):
        """
        Test databases have to be based on something - this provides that by loading the
        :return scratch_db: The live database file for additional detailing and changes
        """
        if self.csv_folder_path is not None:
            scratch_db = load_data(
                folder_path=self.csv_folder_path,
                overwrite_db=False,
                override_scratch_folder=self.get_scratch_folder(),
            )
            return scratch_db
        else:
            scratch_db_path = os.path.join(self.get_scratch_folder(), "test_db.db")

            # Open a blank test database
            test_db = Database(metadata={"database_path": scratch_db_path}, create=True)
            return test_db

    @staticmethod
    def purge_tables(scratch_db):
        """
        If you want to be sure that nop entries exist for a certain subset of the tables, then this method gives you a
        chance to manually purge them before the main detail method is run.
        :param scratch_db:
        :return:
        """
        pass

    @staticmethod
    def detail_databases(scratch_db):
        """
        Preform the work of modifying the database - making the necessary changes to transform the base database into a
        useful form for saving.
        :param scratch_db:
        :return:
        """
        return scratch_db

    def save_database(self, scratch_db):
        """
        Preform the task of actually saving the database into the scratch folder.
        :param scratch_db:
        :return:
        """
        # Shutdown test database
        scratch_db_path = scratch_db.metadata["database_path"]
        del scratch_db

        # Use checked_copy to move the database to it's new location - this should hash verify the move
        if os.path.exists(scratch_db_path):
            checked_copy(file_in=scratch_db_path, file_out=self.dst_file_path)
        else:
            err_str = (
                "Cannot copy database to designated backup position - "
                "live_database_path doesn't exist: {}".format(scratch_db_path)
            )
            puts(colored.red(err_str))
            raise OSError(err_str)

        # Just-in-case validation and success printing
        if os.path.exists(self.dst_file_path):
            puts(
                colored.green(
                    "Custom column test database successfully created and transferred to - "
                    "{}".format(self.dst_file_path)
                )
            )
        else:
            err_str = "Copy silently failed"
            puts(colored.red(err_str))
            raise OSError(err_str)
