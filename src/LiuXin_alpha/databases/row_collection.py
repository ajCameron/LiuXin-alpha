from __future__ import unicode_literals

__author__ = "Cameron"

# Intended to serve as an intermediary between metadata and the database itself.
# Once the plugin architecture is written, the manager should bear at least the methods below to this method

# Todo: Impliment this interface - or not - whateves
# Class intended to represent the metadata assigned to a title in the database.
# Additional classes will be defined for Authors and for Files.
# Ultimately LiuXin is focused on managing collections of files. These collections - multiple different files which consititute versions of a document - should always be contained in an individual folder. These colections can then be grouped into larger collections - representing series or the works of an individual author
# However there are times when the file centric view breaks down (when you want to actually find something, for example.
# Thus there exists various classes of metadata

# TODO: Note that collections are the basis of LiuXin.
# Interface will take the following form
# Row_Collection.row() - gives the row that this instance of
# Row_Collection["table"].composite_rows() - returns the composite rows corresponding to the entries in that table
# Row_Collection["table"].key() - returns an index sorted by the key
# key("") priority of the interlink consisting solely of rows of that type
# Row_Collection["table"].target_rows() - returns the target rows
# Row_Collection["table"].relationship_types() - returns a set of allowable relationship types linking the base table and "table"
# Row_Collection.rows() - throws back all the rows connected to the base row
# Row_Collection["table"]["table_id"]["id"] returns the specified row (if possible should be a pointer to any instance of the row that has been called before
# Row_Collection["table"]["table_id"]["column_name"] - returns the specified column
# Row_Collection["table"]["something"]["column_name"] - creates a new row

from copy import deepcopy
import pprint

# A RowCollection is intended to be a a store of Row objects. Errors will be thrown if you attempt to add something
# which is not a row object or try and store one improperly
from LiuXin.databases.row import Row

from LiuXin.utils.general_ops.io_ops import LiuXin_debug_print

from LiuXin.constants import VERBOSE_DEBUG

from LiuXin.exceptions import InputIntegrityError
from LiuXin.exceptions import LogicalError

from LiuXin.utils.lx_libraries.liuxin_six import six_unicode


class RowCollection(object):
    """
    Intended to serve as an "elegant" bridge between the database itself and the metadata object.
    Intended to act as a container for all relevant rows off the database.
    Can be seeded with a title row - or left blank.
    """

    def __init__(self, seed_row, target_database=None):
        """
        Takes either a seed row, or nothing. If a seed row is present (and is not on the DO_NOT_SEED list) calls the
        database and picks up all rows which reference the given object. Puts them all in one place for easy access.
        :param: seed_row: If a Row is given the collection is all rows which link to that row. If None is given then
        the collection is left blank.
        :param seed_row: Either a row_dict, or a Row. If a Row uses the primary_row_dict instead.
        :param target_database:
        """
        # Processing the input to get all the data needed. After this stage RowCollection must have a target_database
        # and a seed_row_dict.
        if isinstance(seed_row, dict):
            self.seed_row_dict = deepcopy(seed_row)
            if target_database is not None:
                self.db = target_database
            else:
                if VERBOSE_DEBUG:
                    err_str = "Attempt to create a RowCollection failed.\n"
                    err_str += "seed_row was a row_dict, and no target_database was given. Hence aborting.\n"
                    raise InputIntegrityError(err_str)
                else:
                    raise InputIntegrityError
        # If a Row is passed in tries to use the DatabasePing from it - if no DatabasePing is set for the Row tries to use the
        # target_database field
        elif isinstance(seed_row, Row):
            self.seed_row_dict = seed_row.primary_row
            if seed_row.db is not None:
                self.db = seed_row.db
            elif target_database is not None:
                self.db = target_database
            else:
                if VERBOSE_DEBUG:
                    err_str = "Attempt to create a RowCollection failed.\n"
                    err_str += "seed_row: " + six_unicode(seed_row) + "\n"
                    err_str += "target_database: " + six_unicode(target_database) + "\n"
                    err_str += "No valid target_database found in either."
                    raise InputIntegrityError(err_str)
                else:
                    raise InputIntegrityError
        else:
            raise NotImplementedError

        # Setting basic attributes - making sure that RowCollection has a index corresponding the name of every table
        # in the database
        self.categorized_tables = self.db.get_categorized_tables()
        self.main_tables = self.categorized_tables["main"]
        _data = deepcopy(dict((table, []) for table in self.main_tables))

        # Used as a switch to tell if the row has been loaded off the database
        self.loaded_tables = deepcopy(dict((table, None) for table in self.main_tables))
        object.__setattr__(self, "_data", _data)

        # Properties of the seed row and the table associated with it are loaded here
        self.seed_table_name = self.db.identify_table_from_row_dict(self.seed_row_dict)
        self.seed_table_id_column = self.db.get_id_column(self.seed_table_name)
        self.seed_row_display_column = self.db.get_display_column(self.seed_table_name)
        self.seed_row_display_value = self.seed_row_dict[self.seed_row_display_column]
        self.seed_row_id = self.seed_row_dict[self.seed_table_id_column]

        # Removing the main table name from the _data and loaded tables - if a call is put in for the seed row dict it
        # will return the seed_row (in an index - for the sake of compatibility)
        del _data[self.seed_table_name]
        del self.loaded_tables[self.seed_table_name]

        # Some of the processor intensive options are cached locally - so that they only need to be generated for the
        # object once during it's lifetime
        self.__fingerprint = None

    def __str__(self):
        """
        String representation of the RowCollection - to be used with the print function.
        :return:
        """
        return self.__unicode__().encode("utf-8")

    def __unicode__(self):
        """
        Returns a unicode representation of this RowCollection.
        This is only a summary of the information stored in the RowCollection - for everything the RowCollection
        contains use self.uni_full_dump()
        :return:
        """
        ans = []
        _data = object.__getattribute__(self, "_data")

        def uni_format(x, y):
            candidate = None
            try:
                candidate = "%-20s: %s" % (six_unicode(x), six_unicode(y))
                # ans.append(u'%-20s: %s'%(unicode(x), unicode(y)))
            except UnicodeDecodeError:
                # Todo: Use the default encoding here
                candidate = "%-20s: %s" % (
                    six_unicode(x, "utf-8"),
                    six_unicode(y, "utf-8"),
                )
                # ans.append(u'%-20s: %s'%(unicode(x,'utf-8'), unicode(y,'utf-8')))
            finally:
                if candidate is None:
                    ans.append("%-20s: %s" % (six_unicode(x), repr(y)))
                else:
                    ans.append(candidate)

        ans += ["RowCollection\n"]

        # Loading information about the seed_row
        uni_format("    seed_row_table", self.seed_table_name)
        uni_format("    seed_row_id", self.seed_row_id)
        uni_format("    seed_row_display", self.seed_row_display_value)

        ans.append("Linked rows:\n")

        # By default doesn't print all of every row - just prints the id and the display name
        for table in _data:

            ans += [six_unicode(table)]
            table_rows = self.__getitem__(table)

            for row in table_rows:

                row_id_column = self.db.get_id_column(table)
                row_display_column = self.db.get_display_column(table)
                row_id = row[row_id_column]
                row_display = row[row_display_column]
                uni_format("  " + six_unicode(row_id_column), row_id)
                uni_format("  " + row_display_column, six_unicode(row_display))
                uni_format("  " + "link_priority", row.get_link_priority())

        return "\n".join(ans)

    def __getattr__(self, item):
        """
        Allows a attribute like interface to some of the derived quantities.
        :param item:
        :return:
        """
        # Check in the standard attributes
        try:
            object.__getattribute__(self, item)
        except AttributeError:
            pass

        if item == self.seed_table_name:
            return Row(row_dict=self.seed_row_dict, database=self.db)

        # Checks against the loaded tables
        _data = object.__getattribute__(self, "_data")
        if item in _data:
            return self.__getitem__(item)

        err_str = "RowCollection.__getattr__ has failed. Item not recognized.\n"
        err_str += "item: " + six_unicode(item) + "\n"
        raise NotImplementedError(err_str)

    def __getitem__(self, table):
        """
        Allows you to access all the link rows between the seed row and this table.
        :param table: A name of a table in the database
        :return link_rows:
        """
        table = deepcopy(table)

        main_tables = object.__getattribute__(self, "main_tables")
        loaded_tables = object.__getattribute__(self, "loaded_tables")
        _data = object.__getattribute__(self, "_data")
        seed_row_dict = object.__getattribute__(self, "seed_row_dict")
        seed_table_name = object.__getattribute__(self, "seed_table_name")

        if table == seed_table_name:
            return [Row(row_dict=seed_row_dict, database=self.db)]

        if table not in main_tables:
            if VERBOSE_DEBUG:
                err_str = "Error - to RowCollection.__getitem__ not recognized.\n"
                err_str += "Table: " + repr(table) + "\n"
                err_str += "Is not a valid table in the database.\n"
                raise InputIntegrityError(err_str)
            else:
                raise InputIntegrityError
        else:
            # If the rows have already been loaded pulling them out the attributes and returning them
            if loaded_tables[table] == "loaded":
                return _data[table]
            elif loaded_tables[table] is None:
                linked_rows = self.db.get_linked_rows(seed_row_dict, table)
                if linked_rows is not None:
                    _data[table] = _data[table] + [r for r in linked_rows]
                    loaded_tables[table] = "loaded"
                    return _data[table]
                elif linked_rows is None:
                    _data[table] = []
                    loaded_tables[table] = "loaded"
                    return _data[table]
                else:
                    raise LogicalError
            else:
                raise LogicalError

    def add_row_dict(self, new_row_dict, link_type=None):
        """
        Takes a row_dict. Adds it into the RowCollection and links it to the seed_row_dict.
        :param new_row_dict:
        :return:
        """
        new_row_dict = deepcopy(new_row_dict)
        row_dict_table = self.db.identify_table_from_row_dict(new_row_dict)
        _data = object.__getattribute__(self, "_data")
        new_row = Row(self.seed_row_dict, self.db)
        new_row.secondary_row = new_row_dict

        _data[row_dict_table] = _data[row_dict_table] + [new_row]

    def sort_all_row_indices(self):
        """
        Sorts all the Row indices.
        :return None:
        """
        _data = object.__getattribute__(self, "_data")
        for table in self.main_tables:
            _data[table] = sorted(_data[table], key=lambda row: row.get_link_priority())

    def get_first_linked_row(self, table_name):
        """
        Gets the first linked row in the given table index. Returns None if there are no rows present.
        :param table_name:
        :return first_linked_row/None:
        """
        if table_name not in self.main_tables:
            if VERBOSE_DEBUG:
                err_str = "get_first_linked_row called with a table_name not in the linked DatabasePing.\n"
                err_str += "table_name: " + repr(table_name) + "\n"
                err_str += "self.tables: " + repr(self.main_tables) + "\n"
        table_rows = self.__getitem__(table_name)
        if not table_rows:
            return None
        else:
            return table_rows[0]

    def get_ids_set(self, target_table):
        """
        Get a set of all the ids for all the rows from one particular table linked to this one.
        :param target_table:
        :return:
        """
        if target_table not in self.main_tables:
            err_str = "given target_table is not a main table on the database.\n"
            err_str += "target_table: " + repr(target_table) + "\n"
            raise InputIntegrityError(err_str)
        table_rows = self.__getitem__(target_table)
        table_id_column = self.db.get_id_column(target_table)
        rtn_set = set(row[table_id_column] for row in table_rows)
        return rtn_set

    # Todo: Cache the fingerprint on the row itself - to save time if it has to be regenerated
    def get_fingerprint(self):
        """
        Returns a fingerprint of the row collection. This fingerprint is a set of values of the form [table]_[table_id]
        and represents all the rows in this row collection.
        This is used for easy comparison of the object the row collection has been seeded around and a fingerprint off
        a folder_store - for example.
        Thus if the row has an undesirable creator it can be immediately excluded.
        :return:
        """
        if self.__fingerprint is not None:
            return self.__fingerprint

        fingerprint = set()
        for table in self.main_tables:
            table_ids = self.get_ids_set(table)
            for term in table_ids:
                row_print = six_unicode(table) + "_" + six_unicode(term)
                fingerprint.add(row_print)
        self.__fingerprint = fingerprint
        return fingerprint

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO ACCESS PROPERTIES OF ANY TREES THE ROW MIGHT BE EMBEDDED IN START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------

    # Todo: Ensure this preforms properly when the target row is of the same type of that seeding row_collection
    def get_root_row(self, target_tree_table):
        """
        Returns the root row of the target tree. For example, in a series tree this would be the top most series.
        :param target_tree_table:
        :return:
        """
        return self.db.get_linear_row_index(self.get_first_linked_row(target_tree_table))[0]

    def get_linear_row_index(self, target_tree_table):
        """
        Returns a linear index of rows working back from the top of the tree to the bottom.
        :param target_tree_table:
        :return:
        """
        if target_tree_table not in self.db.get_categorized_tables()["main"]:
            err_str = "get_linear_row_index has been passed a row which is not a main table.\n"
            err_str += "target_tree_table: " + six_unicode(target_tree_table) + "\n"
            raise InputIntegrityError(err_str)

        o_start_row = self.get_first_linked_row(target_tree_table)
        if o_start_row is None or not o_start_row:
            return []
        start_row_dict = o_start_row.get_row(target_tree_table)
        n_start_row = Row(row_dict=start_row_dict, database=self.db)
        return self.db.get_linear_row_index(n_start_row)

    def get_linear_column_index(self, target_tree_table, display_column):
        """
        Gets a linear series of the given column.
        :param target_tree_table:
        :param display_column:
        :return:
        """
        linear_row_index = self.get_linear_row_index(target_tree_table)
        linear_row_column_index = []
        for row in linear_row_index:
            linear_row_column_index.append(row[display_column])
        return linear_row_column_index

    # Todo: Add None option to default to the base row the collection is seeded around
    def get_tree_depth(self, target_tree_table):
        """
        Returns the depth of the row most closely associated to the seed row
        :return:
        """
        linked_row = self.get_first_linked_row(target_tree_table)
        linear_row_index = self.db.get_linear_row_index(linked_row)
        return len(linear_row_index)

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - SPECIALIZED METHODS TO DEAL WITH INDIVIDUAL TABLES START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------

    # Todo: Upgrade to intelligently parse the creator_set and return something which seems relevant.
    # Todo: Sort by the creator_thing_link_priority
    def get_creator_role_rows(self, role="all", strict=False):
        """
        Get all the creator row where the Creator has a certain role in this work.
        - If the rows are of interlink type - returns just Rows with the creator as the primary row
        By default returns all the creator rows.
        If not by default, tries to intelligently sort the creators and return the relevant ones.
        The Rows that are returned will have the Creator row_dict as the primary and only row_dict.
        :param role: Return rows where the creator has that role in the seed_row_dict
        :param strict: Should LiuXin attempt to produce "good" results if the requested results don't seem useful
        :return:
        """
        if self.seed_table_name != "creators":
            all_creator_rows = self.__getitem__("creators")
            if role == "all":
                rtn_rows = []
                for row in all_creator_rows:
                    rtn_rows.append(Row(row_dict=row.get_row("creators"), database=self.db))
                return rtn_rows
            if len(all_creator_rows) == 0:
                return None

            # The role of a creator in a particular work is stored in the link table which links the creator to that
            # work
            # This allows for the situation where a creator played a different role in a different work (for example
            # George R. R. Martin was sometimes an editor, and sometimes an author)
            # Sensible defaults are implemented to make sure there is always some return rather than None
            if len(all_creator_rows) == 1:
                creator_row = all_creator_rows[0]
                rtn_rows = [Row(row_dict=creator_row.get_row("creators"), database=self.db)]
                return rtn_rows

            creator_thing_link_table = self.db.get_link_table_name("creators", self.seed_table_name)
            creator_thing_link_column = self.db.table_name_column_name(creator_thing_link_table)
            creator_type_column = creator_thing_link_column + "_type"

            # If strict is True then ONLY returning the creator rows where the role matches what has been passed to the
            # function
            if strict:
                rtn_rows = []
                for row in all_creator_rows:
                    if role == row[creator_type_column]:
                        rtn_rows.append(Row(row_dict=row.get_row("creators"), database=self.db))
                return rtn_rows

            # Building a dict of all the creator roles - keyed by the name of the role and valued by the number of times
            # that role appears in the linked collection - analysis of this dictionary will determine which rows to
            # return
            creators_roles_dict = dict()
            for row in all_creator_rows:
                creator_role = six_unicode(row[creator_type_column])
                if creator_role in creators_roles_dict:
                    creators_roles_dict[creator_role] += 1
                else:
                    creators_roles_dict[creator_role] = 1

            # Analyses the creator roles dict to determine which roles to return
            if creators_roles_dict["None"] == len(all_creator_rows):

                rtn_rows = []
                for row in all_creator_rows:
                    current_row = Row(row_dict=row.get_row("creators"), database=self.db)
                    rtn_rows.append(current_row)
                return rtn_rows

            elif creators_roles_dict["None"] > len(all_creator_rows):

                if VERBOSE_DEBUG:
                    wrn_str = "Apparent logical error in get_creator_role_rows.\n"
                    LiuXin_debug_print(wrn_str)
                rtn_rows = []
                for row in all_creator_rows:
                    current_row = Row(row_dict=row.get_row("creators"), database=self.db)
                    rtn_rows.append(current_row)
                return rtn_rows

            elif role in creators_roles_dict and creators_roles_dict[role] > 0:
                rtn_rows = []
                for row in all_creator_rows:
                    creator_role = six_unicode(row[creator_type_column])
                    if role == creator_role:
                        current_row = Row(row_dict=row.get_row("creators"), database=self.db)
                        rtn_rows.append(current_row)
                return rtn_rows

            else:

                rtn_rows = []
                for row in all_creator_rows:
                    current_row = Row(row_dict=row.get_row("creators"), database=self.db)
                    rtn_rows.append(current_row)
                return rtn_rows
        elif self.seed_table_name == "creators":
            return Row(row_dict=self.seed_row_dict, database=self.db)

    def get_sort_creators(self, creator_role="authors"):
        """
        Filters any creators associated with this row.
        Only creators of the type given in creator_role will be returned.
        If no creator role is set for an individual creator it will be included by default.
        :param creator_role: Only creators with this role will be returned
        :return creator_rows: All rows with the specified role
        """
        all_creator_rows = self.creators
        creator_roles = set(row["creator_role"].lower() for row in all_creator_rows)

        # If no creator roles are set, assume they're all relevant
        if creator_roles == set("None"):
            return all_creator_rows
        # Todo: Make this flexible
        elif "editor" in creator_roles:
            return self.get_creator_role_rows("editor")
        elif "author" in creator_roles:
            return self.get_creator_role_rows("author")
        else:
            return all_creator_rows

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - OUTPUT METHODS START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------

    # The converse method - to take an LX file a return it to the database
    def to_text(self):
        """
        Produces a string representation of the full RowCollection.
        Intended to be used to save chunks of the database to disk with the files they represent.
        :return:
        """
        ans = []
        main_tables = deepcopy(self.main_table)

        # The seed row has to be treated differently
        seed_row_table = self.seed_table_name
        main_tables.remove(seed_row_table)
        ans.append("-- BREAK")
        ans.append(" -- SEED ROW DICT")
        ans.append(pprint.pformat(self.seed_row_dict))
        ans.append("-- BREAK")

        for table in main_tables:
            ans.append("-- BREAK")
            ans.append("-- {} ROWS START HERE".format(table))
            ans.append("-- BREAK")
            table_rows = self.__getitem__(table)
            for link_row in table_rows:
                ans.append("-- BREAK")
                ans.append("-- TARGET ROW")
                ans.append(pprint.pformat(link_row.secondary_row))
                ans.append("-- BREAK")
                ans.append("-- BREAK")
                ans.append("-- LINKING ROW")
                ans.append(pprint.pformat(link_row.link_row))
                ans.append("-- BREAK")

        return "\n".join(ans)
