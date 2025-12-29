from __future__ import unicode_literals, print_function

# Storing some code (full union search code) which might be handy for SQL


import os
import sqlite3
import glob
import re
import sys
import codecs
from copy import deepcopy

from LiuXin.paths import LiuXin_data_sources
from LiuXin.utils.general_ops.io_ops import select_from_options
from LiuXin.utils.general_ops.io_ops import LiuXin_print
from LiuXin.utils.general_ops.io_ops import LiuXin_debug_print

VERBOSE_DEBUG = True
from LiuXin.databases.database import Database
from LiuXin.databases.row import Row
from LiuXin.databases.SQLite.databasedriver import DatabaseDriver
from LiuXin.utils.search_query_parser import SearchQueryParser
from LiuXin.utils.general_ops.language_tools import plural_singular_mapper

from LiuXin.exceptions import LogicalError
from LiuXin.exceptions import InputIntegrityError
from LiuXin.exceptions import DatabaseIntegrityError

from LiuXin.utils.lx_libraries.liuxin_six import six_unicode


class DatabaseSearch(object):
    """
    Test bench for a class to search the database.
    """

    def __init__(self):
        self.final_stmt = None
        self.locations = {
            "all": None,
            "covers": ("cover_original_name",),
            "creators": ("creator", "creator_canonical"),
            "creator_type": ("creator_type",),
            "genres": ("genre",),
            "folders": ("folder_original_name",),
            "identifiers": ("identifier",),
            "identifier_type": ("identifier_type",),
            "languages": ("language_name", "language_code"),
            "notes": ("note",),
            "publishers": ("publisher", "publisher_description"),
            "series": ("series", "series_creator"),
            "synopsis": ("synopsis",),
            "tags": ("tag", "tag_search"),
            "titles": ("title", "title_simple_search"),
        }

        self.test_query = "((titles:thing or creators:david) and simon) or genres:thing or genres:thing"
        self.parser = SearchQueryParser(self.locations)
        self.parsed_test = self.parser.parse(self.test_query)
        self.locations = self.populate_all_locations(self.locations)
        print(self.locations)

        # Properties of the database are needed when constructing the SQL for the locations search
        self.db = Database()
        self.categorized_tables = self.db.get_categorized_tables()
        self.tables = (
            self.categorized_tables["main"]
            .union(self.categorized_tables["intralink"])
            .union(self.categorized_tables["interlink"])
        )
        self.tables_and_columns = self.db.get_tables_and_columns()

        self.locational_search(self.parsed_test)

        print(self.categorized_tables["interlink"])
        self.build_total_joined_table()

        print(self.final_stmt)

    # Ideally this would be done with an outer join - but that functionality is not available in SQLITE
    # Instead two left joins are used, and the statement is broken down into a load of individual statements for
    # execution.
    # The (horrendous) algorithm goes as follows (though only one statement should ever be executed on each table -
    # which should, at least, get the execution cost down a little from the original plan - which was to do an OUTER
    # JOIN over every table and search in each location that way).
    # Starting from the innermost token e.g all:"David Weber". This will have form [u'token', u'all', u'"David Weber"']
    # The following statements need to be executed.
    # FROM `titles` SELECT * WHERE titles.title = "David Weber"
    def locational_search(self, parsed_query):
        """
        Takes an index parsed from a search query - builds an appropriate search query from that parsed query and
        executes it on the database.
        Currently, if the row is not linked to a title row, it will be ignored.
        :param parsed_query: A query parsed by the SearchQueryParser.
        :return:
        """
        parsed_query = deepcopy(parsed_query)
        locations = self.locations
        if self.locations is None:
            wrn_str = "DatabaseDriver doesn't have locations loaded.\n"
            LiuXin_debug_print(wrn_str)

        # The tables which will be needed to include in the inner join can be calculated from the required locations
        required_locations = set()

        # Scans down looking for an instance of an index of the form ['string', 'string', 'string'] to transform them
        while not isinstance(parsed_query, unicode):
            # index_location - used to specify a position within the parsed query tree structure
            index_location = []
            current_level = parsed_query
            while not self.can_index_be_transformed(current_level):
                for i in range(len(current_level)):
                    token = current_level[i]
                    if hasattr(token, "__iter__"):
                        current_level = token
                        index_location.append(i)
                        break
                else:
                    err_str = "Attempt to parse query has failed.\n"
                    err_str += "parsed_query: " + repr(parsed_query) + "\n"
                    raise LogicalError(err_str)

            # Including the location in the list of required locations
            if current_level[0] == "token":
                required_locations.add(current_level[1])

            # Using the index_location as a guide to build some code to actually change the value (because the number of
            # indices is variable and this seems to be the best way to access it)
            transformed_index = self.transform_index(current_level)
            python_stmt = "parsed_query"
            for value in index_location:
                python_stmt += six_unicode("[" + six_unicode(value) + "]")
            python_stmt += " = transformed_index"
            exec(python_stmt)

        # Todo: Really need to review and removpe most of the exec statements

        # With the required locations known the search can now be conducted
        # 1) A join table is constructed containing every location needed for the search
        # 2) The search is preformed over this joined table. The title_ids produced are returned
        inner_joins = self.build_total_joined_table()

        final_stmt = "SELECT titles.title_id FROM `titles` \n\n" + inner_joins + " WHERE " + parsed_query + ";"
        self.final_stmt = final_stmt

        print(inner_joins)
        print(parsed_query)
        print(required_locations)

    @staticmethod
    def can_index_be_transformed(target_index):
        """
        Tests to see if an index can be transformed into pure string form.
        :param target_index:
        :return:
        """
        if not hasattr(target_index, "__iter__"):
            return False

        if len(target_index) != 3:
            err_str = "can_index_be_transformed in locational_search has been passed a poorly formed index.\n"
            err_str += "target_index: " + repr(target_index) + "\n"
            raise InputIntegrityError(err_str)
        if hasattr(target_index[1], "__iter__") or hasattr(target_index[2], "__iter__"):
            return False
        else:
            return True

    def transform_index(self, target_index):
        """
        Takes an index - transforms it into intermediate form.
        :param target_index:
        :return:
        """

        if target_index[0] == "token":

            if target_index[1] not in self.locations:
                err_str = "Unable to parse requested token.\n"
                err_str += "location could not be found.\n"
                err_str += "target_index: " + repr(target_index) + "\n"
                err_str += "location: " + repr(target_index[1]) + "\n"
                raise InputIntegrityError(err_str)

            searchable_columns = self.locations[target_index[1]]
            print(searchable_columns)
            search_index = []
            for column in searchable_columns:
                this_term = ""
                column_table = self.__identify_table_from_column(column)
                this_term += column_table + "." + column + "=" + "'" + target_index[2] + "'"
                search_index.append(this_term)
            search_term = "( " + " OR ".join(search_index)
            return search_term

        elif target_index[0] == "or":

            return "( " + target_index[1] + " OR " + target_index[2] + " )"

        elif target_index[0] == "and":

            return "( " + target_index[1] + " AND " + target_index[2] + " )"

        else:
            err_str = "transform_index in locational_search has failed while trying to parse a query.\n"
            err_str += "target_index: " + repr(target_index) + "\n"
            raise LogicalError(err_str)

    @staticmethod
    def populate_all_locations(locations_dict):
        """
        Receives a location_dict - populates the all field (creating it if it isn't set)
        :return:
        """
        if "all" in locations_dict:
            del locations_dict["all"]

        all_columns_set = set()
        for location in locations_dict:
            columns = locations_dict[location]
            if hasattr(columns, "__iter__"):
                for column in columns:
                    all_columns_set.add(column)
            else:
                if VERBOSE_DEBUG:
                    wrn_str = "Location dictionary has value without an __iter__ method.\n"
                    wrn_str += "This is assumed to be a string.\n"
                    LiuXin_debug_print(wrn_str)
                    all_columns_set.add(columns)
                else:
                    all_columns_set.add(columns)

        locations_dict["all"] = tuple(all_columns_set)
        return locations_dict

    def build_total_joined_table(self):
        """
        Builds a SQL syntax for a joined table containing columns from every main table on the database.
        At the moment every table is joined to titles - as this table links to all the others and the assumption is that
        this method is being used as part of a locational search which will return title_ids
        :param target_table:
        :return:
        """
        target_table = "titles"
        target_table_id_column = self.get_id_column(target_table)

        # Take a copy of the main tables and ensure the to_be_joined table is a set
        main_tables = self.categorized_tables["main"]
        interlink_tables = self.categorized_tables["interlink"]
        to_be_joined = deepcopy(main_tables)
        to_be_joined = set([_ for _ in to_be_joined])

        if target_table not in main_tables:
            err_str = "Unable to build_total_joined_table.\n"
            err_str += "Target table was not found in the main tables for this database.\n"
            err_str += "target_table: " + repr(target_table) + "\n"
            raise InputIntegrityError(err_str)
        to_be_joined.remove(target_table)

        stmt = ""
        for this_table in to_be_joined:
            interlink_table = self.get_link_table_name(target_table, this_table)
            # If the location can't be linked to the target table it's discarded
            # Todo: Account for the fact that this might make screw up the syntax for some tables
            # This shouldn't be a problem with titles, as everything links to titles (or should)
            if interlink_table not in interlink_tables:
                continue
            interlink_table_column_name = self.get_row_name_from_table_name(interlink_table)
            this_table_id_column = self.get_id_column(this_table)

            current_bit = "OUTER JOIN " + interlink_table + "\n" + "   ON "
            current_bit += target_table + "." + target_table_id_column + " = "
            current_bit += interlink_table + "." + interlink_table_column_name + "_" + target_table_id_column + "\n"

            current_bit += "OUTER JOIN " + this_table + "\n" + "   ON "
            current_bit += interlink_table + "." + interlink_table_column_name + "_" + this_table_id_column + " = "
            current_bit += this_table + "." + this_table_id_column + "\n"
            stmt += " " + current_bit + " \n"

        return stmt

    def get_link_table_name(self, table1, table2):
        """
        Takes two tables. Returns their link table (if one exists). Returns false otherwise
        :param table1:
        :param table2:
        :return link_table_name/False: The name of the link table, if valid, or false if the table doesn't exist.
        """
        table1 = six_unicode(table1).lower()
        table2 = six_unicode(table2).lower()
        valid_tables = self.tables

        if table1 != table2:
            table1_row_name = self.get_row_name_from_table_name(table1)
            table2_row_name = self.get_row_name_from_table_name(table2)
            tables = [table1_row_name, table2_row_name]
            tables.sort()
            link_table_name = "{}_{}_links"
            link_table_name = link_table_name.format(tables[0], tables[1])

            if link_table_name not in valid_tables:
                return False
            else:
                return link_table_name
        else:
            table_row_name = self.get_row_name_from_table_name(table1)
            link_table_name = "{}_{}_intralinks"
            link_table_name = link_table_name.format(table_row_name, table_row_name)
            return link_table_name

    @staticmethod
    def get_row_name_from_table_name(table_name):
        """
        Takes the name of a table. Returns the singular form (which should be the start of all the column names).
        :param table_name:
        :return column_name:
        """
        table_name = six_unicode(table_name)
        return plural_singular_mapper(table_name)

    def get_id_column(self, table):
        """
        Every table in the database should have an id column.
        Currently assumes that there is a column with a name ending in id and that if this is true for multiple rows
        that the shortest string ending in id is the id string. Should be tested every time a new column is added.
        :param table:
        :return:
        """
        tables_and_columns = self.tables_and_columns
        headings = tables_and_columns[table]

        candidate_ids = []
        for heading in headings:
            if heading.endswith("id"):
                candidate_ids.append(heading)
        if len(candidate_ids) > 1:
            candidate_ids = sorted(candidate_ids, key=len)
            return candidate_ids[0]
        elif len(candidate_ids) == 0:
            if VERBOSE_DEBUG:
                err_str = "Error - get_id_column failed - no column with a name ending in id found"
                err_str += "table: " + repr(table) + "\n"
                err_str += "headings: " + repr(headings) + "\n"
                raise DatabaseIntegrityError(err_str)
            else:
                raise DatabaseIntegrityError
        else:
            return candidate_ids[0]

    def __identify_table_from_column(self, column_heading, headings_and_columns=None, print_error=True):
        """
        Takes a column heading (and optionally a headings and columns dict). Works out the table it falls into.
        :param column_heading: Each column heading should be unique in the database
        :param headings_and_columns: COMPLETELY SUPERFLUOUS
        :param print_error: Will be replaced with LiuXin debug print
        :return:
        """
        headings_and_columns_local = self.tables_and_columns
        tables = headings_and_columns_local.keys()

        for table in tables:
            column_headings = headings_and_columns_local[table]
            if column_heading in column_headings:
                return table
        else:
            if VERBOSE_DEBUG:
                err_str = "identify_table_from_column failed.\n"
                err_str += repr(column_heading) + " was not recognized.\n"
                raise InputIntegrityError(err_str)
            else:
                raise InputIntegrityError

    @staticmethod
    def sanitize_string_for_sql(string):
        """
        Makes a string safe for searching in the database.
        :param string:
        :return:
        """
        string = deepcopy(string)
        string = string.strip()
