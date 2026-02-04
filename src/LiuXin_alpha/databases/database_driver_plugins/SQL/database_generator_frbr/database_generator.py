# Generates the database from the stored SQL and instructions
# Starts from the SQL code for the main tables. Generates them.
# Takes the default list of interlink tables. Generates the basic SQL syntax for them.
# Does the same for the intralink tables
# Adds any additional columns which have been created by the user

import sqlite3

# When viewing the database certain information needs to be all present and correct in one place. There are two options
# for this
# 1) Views - the sane, professional and reasonable solution. Views execute queries to generate the data requested on the
#            fly - ensuring it is always up to date and accurate. However as the queries need to be executed at run time
#            there will be a performance hit - especially when using slower storage.
# or there is the other way.
# 2) aggregate_tables - put all the information needed in one table which updates itself from other tables using
#                       quite a lot of triggers. Much faster. Needs a lot more code and results in a bloated database
# Hopefully you will have the option to choose.

import sys
import re
import os
import pprint
import pathlib
from copy import deepcopy

from typing import Optional

from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr.constants import \
    __INTERLINK_TABLE_CONSTRAINTS__, __INTERLINK_REQUESTED_COLS__, __ALLOWED_INTERLINK_TYPE_VAL_DICT__, \
    __ALLOWED_INTRALINK_TYPE_VAL_DICT__
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

from LiuXin_alpha.utils.logging import LiuXin_print
from LiuXin_alpha.utils.logging import LiuXin_warning_print
from LiuXin_alpha.utils.language_tools import singular_plural_mapper, plural_singular_mapper

from LiuXin_alpha.databases.database_driver_plugins.SQL.utility_mixins import SQLiteTableLinkingMixin

from LiuXin_alpha.constants.paths import LiuXin_database_folder as __database_folder__

from LiuXin_alpha.databases.api import DatabaseBuilderAPI

from LiuXin_alpha.constants import VERBOSE_DEBUG


# Constraints on the interlink tables - DO NOT IMPORT - dynamically modified at run time


# http://stackoverflow.com/questions/4060221/how-to-reliably-open-a-file-in-the-same-directory-as-a-python-script

__folder__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
__database_file_path__ = os.path.join(__database_folder__, "LiuXin_main_database.db")

# Todo: rename comments to reviews

# Not all columns are needed in all interlink tables - this dictionary provides an easy way to specify the columns
# needed

# Todo: Identify and note the custom columns in database startup
# Todo: How do you want to handle story reviews of other works
# Todo: Need to handle deleting custom tables
# Todo: Might want to make a characters table - possibly as an example? Or an option you can turn on
# Todo: These would make a lot of sense to move to db constants or something like that
# See the docs - interlink_table_explanation for what each of these links should do

# See the docs - explanations for what these are


def create_new_database(connection: sqlite3.Connection) -> None:
    """
    Creates a new blank database using the resources in the database generator folder.

    If the file path is None, then the database generates in LiuXin_data with the default name (LiuXin_main_database).
    :param connection: sqlite3.Connection:
    """
    conn = connection

    builder = SQLiteDatabaseBuilder(conn=conn)
    builder.run()



def get_main_table_sql_files() -> list[pathlib.Path]:
    """
    Walk and return the paths to all the table_sql files to load into the database.

    :return:
    """
    all_sql_files = []

    table_sql_folder = pathlib.Path(__folder__) / "table_sql"

    assert table_sql_folder.is_dir(), "table_sql folder is not a directory"

    for root, dirs, files in table_sql_folder.walk():

        for file in files:

            if os.path.splitext(file)[1] == ".sql":

                all_sql_files.append(pathlib.Path(root) / file)

    return all_sql_files


def get_trigger_sql_files() -> list[pathlib.Path]:
    """
    Walk and return the paths to all the table_sql files to load into the database.

    :return:
    """
    all_sql_files = []

    table_sql_folder = pathlib.Path(__folder__) / "trigger_sql"

    assert table_sql_folder.is_dir(), "trigger_sql folder is not a directory"

    for root, dirs, files in table_sql_folder.walk():

        for file in files:

            if os.path.splitext(file)[1] == ".sql":

                all_sql_files.append(pathlib.Path(root) / file)

    return all_sql_files



class SQLiteDatabaseBuilder(SQLiteTableLinkingMixin, DatabaseBuilderAPI):
    """
    Method to support the construction of a database.
    """

    ALLOWED_INTERLINK_TYPE_VAL_DICT = __ALLOWED_INTERLINK_TYPE_VAL_DICT__

    INTERLINK_TABLE_CONSTRAINTS = __INTERLINK_TABLE_CONSTRAINTS__

    def __init__(self, conn: sqlite3.Connection) -> None:
        """
        A conn object pointing to an empty database.

        :param conn:
        """
        self.conn = conn

        self.main_tables = set()
        self.main_tables_sql_files: list[pathlib.Path] = []

        self.triggers_sql_files: list[pathlib.Path] = []

        self.interlink_tables = set()
        self.interlink_table_pairs = set()

        self.intralink_tables = set()

    def run(self) -> None:
        """
        Actually preforms the build on the database.

        :return:
        """
        # 0) Load resources
        self.main_tables_sql_files = get_main_table_sql_files()
        self.triggers_sql_files = get_trigger_sql_files()

        # Todo: This is clearly not gonna be true at the moment
        #  - Check what we're being commanded to do is, in fact, sane
        self.sanity_check_interlink_inputs()

        # 1) Building the main tables from SQLite - the main tables have to be created by direct SQL execution
        self.create_main_tables()

        # 2) Build the triggers
        self.create_main_triggers()

        assert self.direct_get_tables() is None, sorted(self.direct_get_tables())


        raise NotImplementedError("Let's get the main tables up first.")

        # 2) Creates the interlink tables - these are sufficiently similar that they are amenable to automated creation
        self.interlink_tables_pairs = self.get_requested_interlink_tables()
        for link_pair in self.interlink_tables_pairs:
            self.interlink_tables.add(self.get_interlink_name(link_pair))

        # 3) Validate the constraints which will be applied to the interlink tables
        self.validate_interlink_table_constraints()

        # 4) Validate that the allowed types request is valid
        self.validate_allowed_type_val_dict()

        # 5) Validate the table column requests - the columns that we want added to each of the link tables
        self.validate_interlink_table_column_requests()

        # 6) Build the interlink tables
        for table in self.interlink_tables_pairs:
            self.create_interlink_table(table[0], table[1], connection=self.conn)

        # 7) Read the intralink tables
        self.intralink_tables = self.get_requested_intralink_tables()

        # 8) Build the intralink tables
        self.sanity_check_intralink_inputs()
        for table in self.intralink_tables:
            self.create_intralink_table(table, connection=self.conn)

        # 9) Add the aggregate tables (here mostly views)
        self.create_aggregate_tables()

        # 10) Set the version - so we can check the database and driver version used to build this database
        self.set_database_version()


    # Todo: Add annotations table?
    # Todo: Add a unified tasks table?

    def direct_get_tables(self) -> set[str]:
        """
        Returns a index of the names of all tables in the database.
        :param force_refresh: Force the driver to introspect the database again
        :return:
        """

        stmt = "SELECT name FROM sqlite_master WHERE type = 'table';"
        processed_return = []
        for row in self.conn.execute(stmt):
            processed_return.append(row[0])

        return set(processed_return)

    @staticmethod
    def sanity_check_interlink_inputs() -> None:
        """
        Checks that the inputs being provided in this file are, in fact, sane.

        :return:
        """
        # Check that, if there's a type column, then we also know what values are permitted to be in it
        for link_table in __INTERLINK_REQUESTED_COLS__:

            lt_rq = __INTERLINK_REQUESTED_COLS__[link_table]
            if lt_rq is not None and "type" in lt_rq:
                assert (
                        link_table in __ALLOWED_INTERLINK_TYPE_VAL_DICT__
                ), "If you request a type column for {}, you must specify it's allowed values".format(link_table)

        # Check that, if there's type values specified, there's a table with a type column for them to go in
        for link_table in __ALLOWED_INTERLINK_TYPE_VAL_DICT__:

            try:
                assert (
                    "type" in __INTERLINK_REQUESTED_COLS__[link_table]
                ), "You have specified type values for table {} which does not have a type column for them to go in."
            except KeyError:
                assert (
                    "type" in __INTERLINK_REQUESTED_COLS__[link_table]
                ), "You have specified type values for table {} which does not have a table for them to go in."

    def sanity_check_intralink_inputs(self) -> None:
        """
        Check that the inputs for the intralink tables make sense.

        :return:
        """
        for intralink_table in self.intralink_tables:
            assert intralink_table in __ALLOWED_INTRALINK_TYPE_VAL_DICT__, (
                "table {} has an intralink request but not a corresponding value in allowed_intralink_type_val_dict"
                "".format(intralink_table)
            )

    def create_main_tables(self) -> None:
        """
        Generates and executes the SQL needed to build the main tables.

        :return:
        """
        conn = self.conn
        c = conn.cursor()

        statements = []

        for main_table_sql_file in self.main_tables_sql_files:

            try:
                with main_table_sql_file.open("r") as main_tables_sqlite_file:
                    test = main_tables_sqlite_file.readlines()
            except IOError:
                LiuXin_print(
                    "Error - create_main_tables failed, due to being unable to find the main_tables_sqlite.txt file."
                )
                sys.exit()

            break_count = 0  # counting the number of break statements so far

            current_statement = """ """

            for line in test:

                if line[0:8] == "-- BREAK":
                    break_count += 1

                current_statement += line

                if break_count == 2:
                    break_count = 0
                    statements.append(current_statement)
                    current_statement = """ """

        for statement in statements:
            if VERBOSE_DEBUG:
                LiuXin_print(statement)
            try:
                c.execute(statement)
            except sqlite3.OperationalError as e:
                raise TypeError(f"\n{statement}\n: {e}")
            except sqlite3.ProgrammingError as e:
                raise TypeError(f"\n{statement}\n: {e}")

            conn.commit()

    def create_main_triggers(self) -> None:
        """
        Generates and executes the SQL needed to build the main tables.

        :return:
        """
        conn = self.conn
        c = conn.cursor()

        statements = []

        for main_table_sql_file in self.triggers_sql_files:

            try:
                with main_table_sql_file.open("r") as main_tables_sqlite_file:
                    test = main_tables_sqlite_file.readlines()
            except IOError:
                LiuXin_print(
                    "Error - create_main_tables failed, due to being unable to find the main_tables_sqlite.txt file."
                )
                sys.exit()

            break_count = 0  # counting the number of break statements so far

            current_statement = """ """

            for line in test:

                if line[0:8] == "-- BREAK":
                    break_count += 1

                current_statement += line

                if break_count == 2:
                    break_count = 0
                    statements.append(current_statement)
                    current_statement = """ """

        for statement in statements:
            if VERBOSE_DEBUG:
                LiuXin_print(statement)
            try:
                c.execute(statement)
            except sqlite3.OperationalError as e:
                raise TypeError(f"\n{statement}\n: {e}")
            except sqlite3.ProgrammingError as e:
                raise TypeError(f"\n{statement}\n: {e}")

            conn.commit()

    def get_requested_interlink_tables(self) -> set[str]:
        """
        Parses a link table file for a list of requested tables.

        :return link_tables: A set of tuples of two table names between which a link should be created
        """
        # parses a file for a list of requested link tables
        # interprets these tables to work out the tables it should join
        # generates the SQL of the link table
        # applies any custom SQL to the tables once they exist

        c = self.conn.cursor()
        stmt = "SELECT name FROM sqlite_master WHERE type='table';"
        current_tables = self.main_tables
        for row in c.execute(stmt):
            current_tables.add(row[0])
        link_tables = set()

        try:
            with open(os.path.join(__folder__, "interlink_table_requests.txt"), "r") as requests_file:
                requested_table_names = requests_file.readlines()
        except IOError:
            LiuXin_print("Error - get_requested_interlink_tables failed to find the link table requests text file.")
            raise

        for line in requested_table_names:

            tables = self.extract_main_tables(line)

            if tables is not None:

                c_table1 = tables[0]
                c_table2 = tables[1]

                if (c_table1 not in current_tables) or (c_table2 not in current_tables):
                    warn_str = "Warning - get_requested_interlink_tables reports that you're trying to create a table"
                    warn_str += " that should not be.\n"
                    warn_str += repr(line)
                    LiuXin_warning_print(warn_str)
                else:
                    current_table = (c_table1, c_table2)
                    link_tables.add(current_table)

        return link_tables

    def extract_main_tables(self, interlink_request: str) -> Optional[list[str]]:
        """
        Extract the main tables we're being instructed to link from the main table.

        :param interlink_request:
        :return:
        """
        input_pattern = re.compile(r"\s*([0-9a-zA-Z_]+)-([0-9a-zA-Z]+)_")
        tables = input_pattern.match(interlink_request)

        if tables is None:
            return

        i_table1 = tables.group(1)
        i_table2 = tables.group(2)
        c_table1 = self.match_to_table_name(i_table1)
        c_table2 = self.match_to_table_name(i_table2)

        return sorted([c_table1, c_table2])

    def validate_interlink_table_constraints(self) -> None:
        """
        Check that we're not trying to constrain tables that don't exist.

        :return:
        """
        for link_table in __INTERLINK_TABLE_CONSTRAINTS__.keys():
            assert link_table in self.interlink_tables, self.__constraint_not_found_error(link_table)

    def validate_allowed_type_val_dict(self) -> None:
        """
        Check that the allowed_type_val_dict is keyed with valid link tables which have a type columns requested.

        :return:
        """
        for link_table in __ALLOWED_INTERLINK_TYPE_VAL_DICT__.keys():
            assert link_table in self.interlink_tables

            # If all columns are being created, then a type column will certainly be generated
            if __INTERLINK_REQUESTED_COLS__[link_table] == "all":
                continue

            assert "type" in __INTERLINK_REQUESTED_COLS__[link_table]

    def validate_interlink_table_column_requests(self) -> None:
        """
        Check that we're instructing the database generator to limit the column count of tables that actually exist.

        :return:
        """
        for table_constraint in __INTERLINK_REQUESTED_COLS__.keys():
            # Check that the table to be constrained
            assert table_constraint in self.interlink_tables, self.__constraint_not_found_error(table_constraint)

            # Check that the request is valid
            column_requests = __INTERLINK_REQUESTED_COLS__[table_constraint]
            if column_requests is None:
                continue

            for cr in column_requests:
                assert cr in {"priority", "type", "index"}, "cr {} not valid".format(cr)

    def __constraint_not_found_error(self, link_table: str) -> str:
        err_msg = [
            "{} not found in the known interlink tables".format(link_table),
            "\n{}\n".format(pprint.pformat(self.interlink_tables)),
        ]
        return "\n".join(err_msg)

    @staticmethod
    def get_interlink_name(link_pair: list[str]) -> str:
        """
        Take the pair of tables to be linked and return the name of their interlink table.

        :param link_pair:
        :return:
        """
        link_pair = sorted(link_pair)
        return "{}_{}_links".format(plural_singular_mapper(link_pair[0]), plural_singular_mapper(link_pair[1]))

    def get_interlink_constraint(self, link_pair: list[str]) -> dict[str, str]:
        """
        Takes a pair of tables and returns a link table for it - if it exists.

        :param link_pair:
        :return:
        """
        link_table_name = self.get_interlink_name(link_pair)
        return __INTERLINK_TABLE_CONSTRAINTS__[link_table_name]

    def match_to_table_name(self, candidate_name: str) -> Optional[str]:
        """
        Attempt to fuzzy match the cand name to a known table.

        Tries to match the given string with one that is definitely the name of a table.
        Returns the name of the table - or None if no match can be found.
        :param candidate_name:
        :return:
        """
        name_local = deepcopy(candidate_name)
        name_local = six_unicode(name_local)

        candidate_name = name_local.lower()

        if candidate_name in self.main_tables:
            return candidate_name

        candidate_name = singular_plural_mapper(name_local)
        candidate_name = candidate_name.lower()

        if candidate_name in self.main_tables:
            return candidate_name
        else:
            return None

    def create_interlink_table(self, table1: str, table2: str, connection: sqlite3.Connection) -> None:
        """
        Takes the names of two tables - creates an interlink table between them.

        :param table1:
        :param table2:
        :param connection: The global connection uses throughout this extended method
        :return None: Operation is applied directly to database
        """

        table1_l = deepcopy(table1)
        table1_l = six_unicode(table1_l)
        table2_l = deepcopy(table2)
        table2_l = six_unicode(table2_l)
        conn = connection
        c = conn.cursor()

        table_name, _ = self.get_interlink_table_name(table1, table2)

        requested_cols = "all"
        if table_name in __INTERLINK_REQUESTED_COLS__:
            requested_cols = deepcopy(__INTERLINK_REQUESTED_COLS__[table_name])

        if requested_cols is None:
            requested_cols = set()

        # Check that the table we're building is actually expected
        assert table_name in self.interlink_tables

        # Up to two tables need to be constructed, and one needs to be populated
        # If required, an allowed_type_table will be constructed and populated from the list of statements already
        # created
        att_table_sqlite_list = self.build_interlink_table_sqlite(table1_l, table2_l, requested_cols=requested_cols)
        for att_table_build_stmt in att_table_sqlite_list:
            if VERBOSE_DEBUG:
                LiuXin_print(att_table_build_stmt)
            c.execute(att_table_build_stmt)

        conn.commit()

    # this section deals with adding the intralink tables
    # examples might be authors and their pseudonames.
    # The format is always primary is type of secondary
    def create_intralink_table(self, table_name: str, connection: sqlite3.Connection) -> None:
        """
        Takes the name of a table. Creates an interlink table for that table - tables that link tables to themselves

        :param table_name:
        :param connection:
        :return:
        """
        c = connection.cursor()

        name_local = deepcopy(table_name)
        name_local = six_unicode(name_local)

        sql_list = self.build_intralink_table_sqlite(name_local)

        for intralink_statement in sql_list:
            c.execute(intralink_statement)
            connection.commit()

    def build_intralink_table_sqlite(self, name: str) -> list[str]:
        """
        Takes a table name. Builds the sqlite for a table refering back to the main table.

        :param name:
        :return:
        """
        name_local = deepcopy(name)
        name_local = six_unicode(name_local)

        target_table_name = self.match_to_table_name(name_local)

        target_row_name = plural_singular_mapper(target_table_name)

        row_name = "{}_{}_intralink"
        row_name = row_name.format(target_row_name, target_row_name)

        allowed_type_table_sqlite = self.build_allowed_types_table_intralink(target_table_name)

        columns = """

    -- -----------------------------------------------------
    -- Table `{0}s`
    -- -----------------------------------------------------
    CREATE TABLE IF NOT EXISTS `{0}s` (
      `{0}_id` INTEGER PRIMARY KEY ,
      `{0}_primary_id` INT UNSIGNED NULL,
      `{0}_secondary_id` INT UNSIGNED NULL,
      `{0}_type` TEXT NULL,
      `{0}_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),
      `{0}_scratch` TEXT NULL,

    """
        columns = columns.format(row_name)

        # Add in the foreign key linking out to the allowed_types table
        att_name = self.get_allowed_types_table_name_intralinks(name)
        att_col_name = att_name[:-1]  # Consistently just trimming the s off

        at_foreign_key = """
        CONSTRAINT `{0}_type_is_allowed`
          FOREIGN KEY (`{1}_type`)
          REFERENCES `{2}` (`{3}_type`)

        """.format(
            att_name, row_name, att_name, att_col_name
        )

        first_constraint = """
      CONSTRAINT `{}_primary_id`
        FOREIGN KEY (`{}_primary_id`)
        REFERENCES `{}` (`{}_id`)
        ON DELETE CASCADE
        ON UPDATE CASCADE,

    """
        first_constraint = first_constraint.format(row_name, row_name, target_table_name, target_row_name)

        second_constraint = """
      CONSTRAINT `{}_secondary_id`
        FOREIGN KEY (`{}_secondary_id`)
        REFERENCES `{}` (`{}_id`)
        ON DELETE CASCADE
        ON UPDATE CASCADE)
    ;
        """

        second_constraint = second_constraint.format(row_name, row_name, target_table_name, target_row_name)

        allowed_type_table_sqlite.append(
            columns + at_foreign_key + first_constraint + second_constraint,
        )
        return allowed_type_table_sqlite

    def build_allowed_types_table_intralink(self, for_table: str) -> list[str]:
        """
        Construct an allowed types table - populated with the values from the allowed_type_val_dict.

        :param for_table:
        :return att_sql: A list of SQLite statements which both creates and populates the table
        """
        assert for_table in self.intralink_tables

        if for_table not in __ALLOWED_INTRALINK_TYPE_VAL_DICT__.keys():
            raise NotImplementedError("No allowed types found for intralink table {}".format(for_table))
        allowed_types = __ALLOWED_INTRALINK_TYPE_VAL_DICT__[for_table]

        allowed_table_name = self.get_allowed_types_table_name_intralinks(for_table)
        allowed_table_col_name = allowed_table_name[:-1]

        att_table_sqlite = """
        CREATE TABLE IF NOT EXISTS `{table}` (
          `{column}_id` INTEGER PRIMARY KEY,
          `{column}_type` TEXT NULL,
          `{column}_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
          `{column}_scratch` TEXT NULL,          
          CONSTRAINT `{table}_type_unique`
          UNIQUE({column}_type)
          );

        """.format(
            table=allowed_table_name, column=allowed_table_col_name
        )

        # Add a statement for every element we want to add to the table
        att_add_sqlite = []
        for at in allowed_types:
            at_insert_stmt = 'INSERT INTO {table} ({column}_type) VALUES ("{at}");'.format(
                table=allowed_table_name, column=allowed_table_col_name, at=at
            )
            att_add_sqlite.append(at_insert_stmt)

        return [
            att_table_sqlite,
        ] + att_add_sqlite

    def get_requested_intralink_tables(self) -> set[str]:
        """
        Parses the intralink_table_requests file and gets the names of the table that need to be intralinked

        :return:
        """
        current_tables = self.main_tables

        intralink_tables = set()

        input_pattern = re.compile(r"\s*([^-\s]*[0-9a-zA-Z]+)")

        try:
            with open(os.path.join(__folder__, "intralink_table_requests.txt"), "r") as intra_reqs_file:
                requested_table_names = intra_reqs_file.readlines()
        except IOError:
            LiuXin_print("Error - get_requested_intralink_tables failed to find the link table requests text file.")
            sys.exit()

        for line in requested_table_names:

            tables = input_pattern.match(line)

            if tables is not None:

                i_table = tables.group(1)
                c_table = self.match_to_table_name(i_table)

                if c_table not in current_tables:
                    warn_str = "Error - get_requested_intralink_tables reports that you're trying to create a table"
                    warn_str += " that should not be.\n"
                    warn_str += line
                    LiuXin_warning_print(warn_str)
                else:
                    intralink_tables.add(c_table)

        return intralink_tables

    def create_aggregate_tables(self) -> None:
        """
        Takes a connection to the database. Executes all the SQL it can find in the aggregate_tables file.

        This should include the code to generate the tables themselves, and the code for the triggers to run them.
        :return:
        """
        c = self.conn.cursor()

        try:
            with open(os.path.join(__folder__, "aggregate_tables.txt"), "r") as agg_tables_file:
                test = agg_tables_file.readlines()
        except IOError:
            LiuXin_print(
                "Error - create_aggregate_tables failed, due to being unable to find the main_tables_sqlite.txt file."
            )
            sys.exit()

        break_count = 0  # counting the number of break statements so far

        current_statement = """ """
        statements = []

        for line in test:

            if line[0:8] == "-- BREAK":
                break_count += 1

            current_statement += line

            if break_count == 2:
                break_count = 0
                statements.append(current_statement)
                current_statement = """ """

        for statement in statements:
            if VERBOSE_DEBUG:
                LiuXin_print(statement)
            c.execute(statement)
            self.conn.commit()

    def set_database_version(self) -> None:
        """
        Import the driver version and the database version and set it.

        :return:
        """
        from LiuXin_alpha.databases.database_driver_plugins.SQLite_apsw import get_SQLite_driver_master_version

        version_str = get_SQLite_driver_master_version()

        stmt = "INSERT INTO database_version (database_version_id, database_version_version) VALUES (1, ?);"
        c = self.conn.cursor()
        c.execute(stmt, (version_str,))
        self.conn.commit()

        # Check to see if the insert has actually been written out
        version_val = None
        for row in c.execute("SELECT database_version_version FROM database_version;"):
            version_val = row[0]
        assert version_val == version_str

        ins_stmt_block = """
        CREATE TRIGGER IF NOT EXISTS block_insert_on_database_version_table
        BEFORE INSERT ON database_version
        BEGIN
            SELECT RAISE(ABORT, 'Cannot insert into database_version');
        END;
        """
        c = self.conn.cursor()
        c.execute(ins_stmt_block)
        self.conn.commit()

        upd_stmt_block = """
        CREATE TRIGGER IF NOT EXISTS block_update_on_database_version_table
        BEFORE UPDATE ON database_version
        BEGIN
            SELECT RAISE(ABORT, 'Cannot update on database_version');
        END;
        """
        c = self.conn.cursor()
        c.execute(upd_stmt_block)
        self.conn.commit()

        del_stmt_block = """
        CREATE TRIGGER IF NOT EXISTS block_delete_on_database_version_table
        BEFORE DELETE ON database_version
        BEGIN
            SELECT RAISE(ABORT, 'Cannot delete from database version');
        END;
        """
        c = self.conn.cursor()
        c.execute(del_stmt_block)
        self.conn.commit()


