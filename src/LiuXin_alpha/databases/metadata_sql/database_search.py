
"""
Retain the unfinished location-aware SQL search prototype.

This module preserves legacy query transformation and conventional-name helpers. It
is not a working parameterized search interface: transformation interpolates raw
terms and the search method raises instead of executing SQL.
"""

# Todo: Actually write this mess.


from __future__ import unicode_literals, print_function, annotations

# Storing some code (full union search code) which might be handy for SQL

from typing import TYPE_CHECKING

from copy import deepcopy

from LiuXin_alpha.constants import VERBOSE_DEBUG

from LiuXin_alpha.utils.search_query_parser import SearchQueryParser
from LiuXin_alpha.utils.language_tools import plural_singular_mapper

from LiuXin_alpha.errors import LogicalError, InputIntegrityError, DatabaseIntegrityError

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI


class DatabaseSearch:
    """
    Retain the unfinished location-aware SQL search prototype.

    Construction parses a fixed sample query, loads schema groups and immediately
    attempts the incomplete search. It may raise before yielding a usable object.
    Standalone static helpers can be used without that constructor.

    Example:
        >>> DatabaseSearch.populate_all_locations({"titles": ("title",)})["all"]
        ('title',)
    """

    db: "DatabaseAPI"

    def __init__(self, db: "DatabaseAPI") -> None:
        """
        Load hard-coded search locations and attempt an unfinished sample query.

        Parses test_query, populates all locations, and reads categorized tables and
        headings. Calls locational_search during construction, so incomplete transformation
        errors propagate before the final join-builder call.

        Example:
            >>> search = DatabaseSearch(database)  # doctest: +SKIP


        :param db: Database host providing categorized tables and column headings.
        :return: None if initialization completes.
        """

        self.db = db

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

        # Properties of the database are needed when constructing the SQL for the locations search
        self.categorized_tables = self.db.get_categorized_tables()

        self.tables = (
            self.categorized_tables["main"]
            .union(self.categorized_tables["intralink"])
            .union(self.categorized_tables["interlink"])
        )
        self.tables_and_columns = self.db.get_tables_and_columns()

        self.locational_search(self.parsed_test)

        self.build_total_joined_table()

    # Ideally this would be done with an outer join - but that functionality is not available in SQLITE
    # Instead two left joins are used, and the statement is broken down into a load of individual statements for
    # execution.

    # The (horrendous) algorithm goes as follows (though only one statement should ever be executed on each table -
    # which should, at least, get the execution cost down a little from the original plan - which was to do an OUTER
    # JOIN over every table and search in each location that way).

    # Starting from the innermost token e.g all:"David Weber". This will have form [u'token', u'all', u'"David Weber"']
    # The following statements need to be executed.
    # FROM `titles` SELECT * WHERE titles.title = "David Weber"
    def locational_search(self, parsed_query: list[str]):
        """
        Attempt query-tree transformation and raise instead of executing a search.

        Copies the input; absent locations raises NotImplementedError. For tree input,
        attempts to locate a transformable node, builds an assignment string and raises
        NotImplementedError rather than executing it. String input builds/stores final_stmt
        and raises with that SQL. Malformed trees can fail earlier; Python strings count as
        iterable in the transformation predicate.

        Example:
            >>> search.locational_search(parsed_query)  # doctest: +SKIP


        :param parsed_query: Legacy parsed query tree, or already-transformed SQL text.
        :return: No normal search result; this prototype raises.
        """
        parsed_query = deepcopy(parsed_query)
        if self.locations is None:
            wrn_str = "DatabaseDriver doesn't have locations loaded.\n"
            raise NotImplementedError(wrn_str)

        # The tables which will be needed to include in the inner join can be calculated from the required locations
        required_locations = set()

        # Scans down looking for an instance of an index of the form ['string', 'string', 'string'] to transform them
        while not isinstance(parsed_query, six_unicode):

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
            # exec(python_stmt)
            raise NotImplementedError(python_stmt)

        # Todo: Really need to review and remove most of the exec statements

        # With the required locations known the search can now be conducted
        # 1) A join table is constructed containing every location needed for the search
        # 2) The search is preformed over this joined table. The title_ids produced are returned
        inner_joins = self.build_total_joined_table()

        final_stmt = "SELECT titles.title_id FROM `titles` \n\n" + inner_joins + " WHERE " + parsed_query + ";"
        self.final_stmt = final_stmt

        raise NotImplementedError(final_stmt)

    @staticmethod
    def can_index_be_transformed(target_index):
        """
        Check whether a three-element candidate has non-iterable second and third values.

        Non-iterable inputs return False; iterable inputs with length other than three raise
        InputIntegrityError. Strings are iterable, so ordinary text token values produce
        False. Sized/indexable input is assumed.

        Example:
            >>> DatabaseSearch.can_index_be_transformed(["token", "titles", "x"])
            False
            >>> DatabaseSearch.can_index_be_transformed(["token", 1, 2])
            True


        :param target_index: Candidate three-element token or logical-expression container.
        :return: Boolean describing this legacy predicate, not SQL validity.
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
        Format one token or logical operation as unbound SQL text.

        Token locations must exist, otherwise InputIntegrityError. Prints their column
        collection and builds raw equality predicates using unescaped values; the token
        branch leaves its opening parenthesis unclosed. or/and branches join operands with
        surrounding parentheses; unknown operators raise LogicalError.

        Example:
            >>> from types import SimpleNamespace
            >>> DatabaseSearch.transform_index(SimpleNamespace(), ["and", "x=1", "y=2"])
            '( x=1 AND y=2 )'


        :param target_index: Candidate three-element token or logical-expression container.
        :return: SQL fragment; no query is executed and values are not escaped.
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
                column_table = self._identify_table_from_column(column)
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
        Replace all with the union of columns in every other location.

        Mutates and returns the original mapping. Iterables are expanded elementwise
        (including strings as characters); non-iterables are inserted directly into a set.
        Values must be hashable and tuple ordering is unspecified.

        Example:
            >>> mapping = {"titles": ("title",), "all": ("old",)}
            >>> DatabaseSearch.populate_all_locations(mapping) is mapping
            True
            >>> mapping["all"]
            ('title',)


        :param locations_dict: Mutable mapping from location names to column collections.
        :return: Same mapping with an all tuple.
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

                    all_columns_set.add(columns)
                else:
                    all_columns_set.add(columns)

        locations_dict["all"] = tuple(all_columns_set)
        return locations_dict

    def build_total_joined_table(self):
        """
        Build legacy OUTER JOIN fragments around the titles table.

        Requires titles among loaded main tables. Iterates other main tables in set order
        and skips unregistered interlinks. Uses conventional endpoint headings and literal
        OUTER JOIN syntax without validating it against SQLite; neither executes nor returns
        a full SELECT.

        Example:
            >>> search.build_total_joined_table()  # doctest: +SKIP


        :return: Concatenated join fragments.
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
        Construct a conventional link name from lowercased endpoint names.

        For different tables, sorts their singular bases and returns False if the generated
        _links table is not in self.tables. Equal endpoints return a repeated-base
        _intralinks name without checking existence.

        Example:
            >>> search.get_link_table_name("creators", "titles")  # doctest: +SKIP


        :param table1: First endpoint table name, lowercased after Unicode conversion.
        :param table2: Second endpoint table name, lowercased after Unicode conversion.
        :return: Cross-table name or False; unchecked generated name for equal endpoints.
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
        Return the shared singular-name mapping for a Unicode table name.

        Example:
            >>> DatabaseSearch.get_row_name_from_table_name("titles")
            'title'


        :param table_name: Table name converted to text before singularization.
        :return: Mapped singular name used as a conventional column prefix.
        """
        table_name = six_unicode(table_name)
        return plural_singular_mapper(table_name)

    def get_id_column(self, table):
        """
        Choose the shortest loaded heading ending with the case-sensitive suffix id.

        Ties preserve heading order. Unknown tables raise KeyError; no candidates raises
        DatabaseIntegrityError. Does not inspect actual primary-key constraints.

        Example:
            >>> from types import SimpleNamespace
            >>> DatabaseSearch.get_id_column(SimpleNamespace(tables_and_columns={"items": ["long_owner_id", "item_id"]}), "items")
            'item_id'


        :param table: Key in the loaded table-to-headings mapping.
        :return: Selected heading.
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

    # Todo: Move over to the generic names mixin
    def _identify_table_from_column(self, column_heading: str) -> str:
        """
        Find the first loaded table containing a heading.

        Uses mapping order for ambiguous headings and raises InputIntegrityError when none
        match.

        Example:
            >>> from types import SimpleNamespace
            >>> DatabaseSearch._identify_table_from_column(SimpleNamespace(tables_and_columns={"items": ["item_id"]}), "item_id")
            'items'


        :param column_heading: Heading to locate in the loaded table mapping.
        :return: First matching table name.
        """
        headings_and_columns_local = self.tables_and_columns
        tables = headings_and_columns_local.keys()

        for table in tables:
            column_headings = headings_and_columns_local[table]
            if column_heading in column_headings:
                return table
        else:
            err_str = "identify_table_from_column failed.\n"
            err_str += repr(column_heading) + " was not recognized.\n"
            raise InputIntegrityError(err_str)

    # Todo: This should actually, y'know, work
    @staticmethod
    def sanitize_string_for_sql(string: str) -> str:
        """
        Copy a string and strip outer whitespace without SQL escaping.

        The name overstates its protection: the result is not safe for SQL interpolation.
        Use parameter binding for query values.

        Example:
            >>> DatabaseSearch.sanitize_string_for_sql("  O'Reilly  ")
            "O'Reilly"


        :param string: Text whose outer whitespace is removed; quotes and SQL metacharacters
            remain.
        :return: Whitespace-stripped string with its contents otherwise unchanged.
        """
        string = deepcopy(string)
        string = string.strip()
        return string
