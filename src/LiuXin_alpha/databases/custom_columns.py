import json
import re

from LiuXin.databases.utils import CUSTOM_DATA_TYPES

from LiuXin.utils.general_ops.json_ops import to_json_str
from LiuXin.utils.logger import default_log
from LiuXin.utils.localization import trans as _

import datetime
import json
import pprint
import re
from functools import partial

from LiuXin.constants import preferred_encoding

from LiuXin.databases import _get_next_series_num_for_list
from LiuXin.databases import _get_series_values
from LiuXin.databases.utils import cleanup_tags

from LiuXin.exceptions import InvalidUpdate
from LiuXin.exceptions import InputIntegrityError

from LiuXin.library.field_metadata import FieldMetadata

from LiuXin.preferences import preferences

from LiuXin import prints
from LiuXin.utils.date import parse_date
from LiuXin.utils.localization import _
from LiuXin.utils.logger import default_log

from LiuXin.utils.general_ops.language_tools import plural_singular_mapper


class CustomColumnDatabaseMixin(object):

    # Todo: Attempt sql injection whenever you can feed into a table
    # Todo: A method to get all the custom columns in a given table
    # Todo: Currently assumes that all custom columns have a link table - which is very far from true
    # Todo: Need to change custom column numbering so that it includes a reference to the table - so it's namespaced
    #       by table
    # Todo: Change target_row to primary_row, in line with ALL THE REST
    def get_interlinked_rows_cc(self, target_row, custom_column, link_table=True):
        """
        Takes a row and a custom column - returns the custom column rows for the given custom column
        :param target_row: A row in a table with a custom column
        :param custom_column: The name of the custom column to retrieve the rows for
                              E.g. "custom_column_2"
        :return:
        """
        if link_table:
            target_table = target_row.table

            cand_cc_link_table = "{}_{}_link".format(target_table, custom_column)

            if cand_cc_link_table not in self.custom_tables:
                err_str = "Cannot get link tables - that target_row and custom column combination is invalid"
                err_str = default_log.log_variables(
                    err_str,
                    "ERROR",
                    ("target_table", target_table),
                    ("cand_cc_link_table", cand_cc_link_table),
                )
                raise InputIntegrityError(err_str)

            cc_col = plural_singular_mapper(cand_cc_link_table)
            cc_link_rows = self.driver_wrapper.search(
                table=cand_cc_link_table,
                column=cc_col + "_book",
                search_term=target_row.row_id,
            )
            if not cc_link_rows:
                return []

            cc_link_rows = sorted(cc_link_rows, key=lambda x: x[cc_col + "_id"])

            # Retrieve the refered to rows and return
            cc_table_rows = []
            for link_row in cc_link_rows:
                target_id = link_row[cc_col + "_value"]
                cc_table_rows.append(self.get_row_from_id(table=custom_column, row_id=target_id))

            return cc_table_rows

        else:

            cc_col = plural_singular_mapper(custom_column)
            cc_rows = self.driver_wrapper.search(
                table=custom_column,
                column=cc_col + "_book",
                search_term=target_row.row_id,
            )
            if not cc_rows:
                return []

            cc_link_rows = sorted(cc_rows, key=lambda x: x[cc_col + "_id"])
            return cc_link_rows


# Todo: Round these up and move them to the custom columns mixin - as with everything else
# Todo: Or, perhaps preferably, move them down into the driver and integrate properly
class CustomColumnsDriverWrapperMixin(object):
    def __init__(self, db, macros):

        # Worker objects
        self.db = db
        self.macros = macros

        # Todo: Might want to rename this to custom_column_tables
        # Stores properties of the database
        self.custom_tables = set()

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - CUSTOM COLUMN METHODS
    def deleted_marked_custom_columns(self):
        """
        Deleted custom columns which have been marked for removal - should be done during a reload or load before the
        custom columns are read off the database.
        :return :
        """
        num_table_lt_map = dict()

        for record in self.db.macros.get_all_cc_ids_marked_for_delete(conn=self.conn):

            num = record
            num_table_lt_map[record] = self.custom_table_names(num)

        self.db.macros.preform_cc_column_delete_from_map(num_table_lt_map, conn=self.conn)

    @property
    def direct_custom_tables(self):
        """
        Get the names of all the custom tables currently registered on the database.
        Replaces the custom_tables property.
        :return:
        """
        return self.db.macros.direct_get_custom_tables(conn=self.db.conn)

    def direct_get_custom_extra(self, link_table, index):
        """
        Return the results of querying the database for an extra column.
        :param link_table:
        :param index:
        :return:
        """
        return self.db.macros.direct_get_custom_and_extra(link_table, index, conn=self.conn)

    def direct_get_custom_id_val_pairs(self, table):
        """
        Retrieve a list of pairs of the ids from the custom table and their values.
        :param table:
        :return:
        """
        return self.db.macros.get_all_cc_id_val_pairs(table, conn=self.conn)

    @staticmethod
    def custom_table_names(num, in_table="books"):
        """
        Makes the names that will be used for a custom column table.
        :param num:
        :param in_table: The table the custom column is linked to - defaults to "books"
        :return:
        """
        try:
            num = int(num)
        except ValueError as e:
            err_str = "Cannot coerce table num (id) to an integer"
            err_str = default_log.log_exception(err_str, e, "ERROR", ("num", num), ("num_type", type(num)))
            raise ValueError(err_str)

        return "custom_column_%d" % num, "%s_custom_column_%d_link" % (in_table, num)

    # Todo: Custom columns needed to be added to the appropriate table name cache after they've been created - check
    #       that this is happening
    def set_custom_column_metadata(self, num, name=None, label=None, is_editable=None, display=None, in_table=None):
        """
        Change the metadata for a custom column - identified with the num
        :param num: The id integer for the custom column
        :param name: The new name of the custom column
        :param label:
        :param is_editable:
        :param display: Used by the interfaces to know what name to give the custom column
        :param in_table: Which table is the custom column being attached to? (defualt is "books")
        :return:
        """
        # Note: the caller is responsible for scheduling a metadata backup if necessary
        changed = self.db.macros.set_custom_column_metadata(
            num=num,
            name=name,
            label=label,
            is_editable=is_editable,
            display=display,
            in_table=in_table,
        )

        # Note: the caller is responsible for scheduling a metadata backup if necessary
        return changed

    # Todo: Restrict multiple to the known values
    # Todo: The combination of name and table should be unique
    # Todo: Change data_type to datatype
    def create_custom_column(
        self,
        name,
        datatype="text",
        is_multiple=False,
        label=None,
        editable=True,
        display=None,
        in_table="books",
    ):
        """
        Add a custom column to the books table.
        :param label:
        :param name:
        :param datatype: Must be one of the following - rating, int, text, comments, series, composite, enumeration,
                         float, datetime, bool
        :param is_multiple:
        :param editable: Is the column editable?
        :param display:
        :param in_table: Which table should the custom column be created in? (Defaults to books for historical reasons)
                         Should be in the main, intralink or interlink tables
        :return:
        """
        # Todo: Somewhere there are allowed cc datatypes - preform a check that we're being given one of them

        assert in_table in self.db.main_tables.union(self.db.interlink_tables).union(
            self.db.intralink_tables
        ), "in_table {} not found in main, intralink or interlink tables".format(in_table)

        # Some datatypes just don't make much sense to be multiple - so throwing an error if we can some combinations
        if is_multiple and datatype in ("rating", "int", "float", "datetime", "bool"):
            err_str = "Cannot have a mutliple column of type {} - makes no sense".format(datatype)
            raise NotImplementedError(err_str)

        if display is None:
            display = {}

        # Update the custom columns table with the new entry - once this has been done it will, at a minimum, be created
        # at the next startup
        label = label if label is not None else "{}__{}".format(in_table, name)

        assert "#" not in label

        if re.match("^\w*$", label) is None or not label[0].isalpha() or label.lower() != label:
            raise ValueError(
                _("The label must contain only lower case letters, digits and underscores, and start " "with a letter")
            )
        if datatype not in CUSTOM_DATA_TYPES:
            raise ValueError("%r is not a supported data type" % datatype)

        # If normalized - a link table is required and generated
        normalized = datatype not in (
            "datetime",
            "comments",
            "int",
            "bool",
            "float",
            "composite",
        )
        is_multiple = is_multiple and datatype in (
            "text",
            "composite",
            "comments",
            "series",
            "enumeration",
        )

        # need_order determines if the custom column needs an additional column to allow for re0ordering of the
        # values
        ordered = False
        if is_multiple and datatype in ("comments", "series"):
            ordered = True

        # In calibre, text might be somewhat badly named - I think it should be "tags" or something similar
        if datatype in ("rating", "int"):
            dt = "INT"
        elif datatype in ("text", "comments", "series", "composite", "enumeration"):
            dt = "TEXT"
        elif datatype in ("float",):
            dt = "REAL"
        elif datatype == "datetime":
            dt = "timestamp"
        elif datatype == "bool":
            dt = "BOOL"
        else:
            err_str = "datatype not recognize and not supported"
            err_str = default_log.log_variables(err_str, "ERROR", ("datatype", datatype))
            raise NotImplementedError(err_str)

        # Todo: Really rating should point over to a rating table of some sort
        cc_row_dict = self.db.driver_wrapper.get_blank_row("custom_columns")
        cc = "custom_column_"
        cc_row_dict[cc + "label"] = label
        cc_row_dict[cc + "name"] = name
        cc_row_dict[cc + "datatype"] = datatype
        cc_row_dict[cc + "is_multiple"] = is_multiple
        cc_row_dict[cc + "editable"] = editable
        cc_row_dict[cc + "display"] = to_json_str(display)  # display is a dict, and so has to be serialized
        cc_row_dict[cc + "normalized"] = normalized
        cc_row_dict[cc + "in_table"] = in_table
        cc_row_dict[cc + "ordered"] = ordered
        self.db.driver_wrapper.update_row(cc_row_dict)

        num = cc_row_dict["custom_column_id"]

        collate = "COLLATE NOCASE" if dt == "TEXT" else ""
        cc_table, link_table = self.custom_table_names(num, in_table=in_table)

        self.db.macros.create_cc_table(
            normalized=normalized,
            datatype=datatype,
            dt=dt,
            table=cc_table,
            link_table=link_table,
            collate=collate,
            in_table=in_table,
            ordered=ordered,
        )

        # Refresh the conn - to ensure that it's not stale
        # Todo: Add a refresh method to the driver to do this stuff for us
        self.conn = self.db.driver.get_connection()
        self.db.driver.conn = self.db.driver.get_connection()

        # Todo: Need to notify the database that the custom columns have been updated

        # Update the tables name cache in the database to reflect the fact that new tables have just been created
        if normalized:
            self.custom_tables.add(cc_table)
            self.custom_tables.add(link_table)
        else:
            self.custom_tables.add(cc_table)

        return num

    def delete_custom_column(self, num):
        """
        Mark a custom column for later deletion.
        :param label:
        :param num:
        :return:
        """
        self.macros.mark_custom_column_for_delete(num=num)

    def _get_custom_column_row(self, in_table, cc_name):
        """
        Return the custom column row for the given custom column.
        :return:
        """
        pass

    def update_custom_column(self, in_table, cc_name, value):
        """
        Preform an update on the given custom column - loading the data into the backend.
        :param in_table: Table the custom column is in
        :param cc_name: Name of the custom column in the table
        :param value: Values to load into the database
        :return:
        """
        raise NotImplementedError


#
# ----------------------------------------------------------------------------------------------------------------------


CUSTOM_DATA_TYPES = frozenset(
    [
        "rating",
        "text",  # Actually the equivalent of tags
        "comments",
        "datetime",
        "int",
        "float",
        "bool",
        "series",
        "composite",
        "enumeration",
    ]
)

# NOTES ON CUSTOM_DATA_TYPES
# enumeration - can take any of a pre set range of values - this range is stored in the view field of the custom columns
#               converted to text on return. Is normalized (has a link table which connects the entries in the
#               custom_column table to the main table) - so there will only ever be, in the custom columns table, a
#               number of values equal to the number of distinct possibilities in the custom_columns table
# text - A block of text - free choice as to what it contains. If two books have degenerate text then they will be
#        linked to the same entry in the custom column table.
# comments - Like text, but un-normalized. Free choice as to what the contains. If two entries are degenerate they will
#            not be linked. This is for when you want to assign as many blocks of text as you like to an object
# datetime - Datetime objects - probably tzinfo strings - one of them can be assigned to each of the entries.
#            This datatype is not normalized - each element in the table the custom column is in has one, and only one
#            element. These need not be unique.
# int - Assign an integer to an object. One to one. Need not be unique.
# float - As integer
# bool - True, False or (optionally) None is assigned to all the objects as a custom column
# rating - Behaves like a rating - an enumerate series of integers that you can link objects to
# series - Series like objects - with the calibre series properties (many to one), a position, optionally extra
#          information additionally stored
# composite - Draws information from multiple different columns. A view column which does not necessarily exist.


# The base mixin handles all the database custom columns stuff - this class adds in all the preference e.t.c
# update logic which needs to be done to the library when custom column changes occur
class CustomColumns(CustomColumnsDriverWrapperMixin):
    """
    Represents the custom_columns (specifically on the books table).
    In calibre this class was originally intended to be a mixing for the LibraryDatabase2 object - some work has been
    done to it so that it can be invoked independently - for easier testing.
    """

    CUSTOM_DATA_TYPES = frozenset(
        [
            "rating",
            "text",
            "comments",
            "datetime",
            "int",
            "float",
            "bool",
            "series",
            "composite",
            "enumeration",
        ]
    )

    @property
    def custom_tables(self):
        return self.direct_custom_tables

    def __init__(
        self,
        db,
        conn=None,
        table="books",
        field_metadata=None,
        data=None,
        field_map=None,
        embed=False,
    ):
        """
        Represents the custom columns for a given table. Defaults to books.
        :param db: The database containing the custom columns
        :param conn: Connection object to allow executing SQL on the database
        :param table: Defaults to books.
        :param field_metadata: This is an object which stores the field metadata for the books table - if you have a
                               field_metadata object for the class your using this in already, then add it here.
        :param field_map: Keyed with the name of the field in the row and valued with the position of that field in the
                          row (which corresponds to the number of columns from the left in that table)
        :param embed: Is this class to be used as part of multiple inheritance for another class? True for yes, False
                      for No. If True then certain methods are not declared to stop them overwriting methods which it's
                      presumed are present in the other class.
        :return:
        """
        self.embed = embed

        self.db = db
        if conn is None:
            self.conn = self.db.driver.conn
        if field_metadata is None:
            self.field_metadata = FieldMetadata()
        else:
            self.field_metadata = field_metadata
        if data is None:
            self.data = {}
        else:
            self.data = data
        if field_map is None:
            # This is the default field map for the meta2 view - if the field map changes elsewhere it also HAS to be
            # changed here
            # Todo: Move it to library constants - rename it META2_FIELD_MAP
            self.FIELD_MAP = {
                "id": 0,
                "title": 1,
                "authors": 2,
                "timestamp": 3,
                "size": 4,
                "rating": 5,
                "tags": 6,
                "comments": 7,
                "series": 8,
                "publisher": 9,
                "series_index": 10,
                "sort": 11,
                "author_sort": 12,
                "formats": 13,
                "path": 14,
                "pubdate": 15,
                "uuid": 16,
                "cover": 17,
                "au_map": 18,
                "last_modified": 19,
                "identifiers": 20,
                "languages": 21,
            }
        else:
            self.FIELD_MAP = field_map

        # Verify that CUSTOM_DATA_TYPES is a (possibly improper) subset of VALID_DATA_TYPES
        if len(self.CUSTOM_DATA_TYPES - FieldMetadata.VALID_DATA_TYPES) > 0:
            raise ValueError("Unknown custom column type in set")

        # Delete marked custom columns
        self.deleted_marked_custom_columns()

        # Load metadata for custom columns
        # label - the name of the column
        # num - id of the custom column in the custom_columns table
        self.custom_column_label_map, self.custom_column_num_map = {}, {}
        self.triggers = []
        self.remove = []
        self.refresh_db_custom_columns_metadata()
        remove = self.remove
        triggers = self.triggers

        if remove:
            with self.conn:
                for data in remove:
                    prints("WARNING: Custom column %r not found, removing." % data["label"])
                    self.db.macros.do_custom_column_delete_by_num(data["num"])

        if triggers:
            # Todo: This will almost certainly not actually work as intended - as it's assuming that the custom columns
            #       are only in books

            with self.conn:
                self.db.driver_wrapper.execute(
                    """\
                    CREATE TEMP TRIGGER custom_books_delete_trg
                        AFTER DELETE ON books
                        BEGIN
                        %s
                        END;
                    """
                    % (" \n".join(triggers))
                )

        # Setup data adapters
        def adapt_text(x, d):
            if d["is_multiple"]:
                if x is None:
                    return []
                if isinstance(x, (str, unicode, bytes)):
                    x = x.split(d["multiple_seps"]["ui_to_list"])
                try:
                    x = [y.strip() for y in x if y.strip()]
                except Exception as e:
                    err_str = "Cannot process - error while trying to strip individual tokens"
                    err_str = default_log.log_exception(err_str, e, "ERROR", ("x", x))
                    raise InvalidUpdate(err_str)

                x = [y.decode(preferred_encoding, "replace") if not isinstance(y, unicode) else y for y in x]
                return [" ".join(y.split()) for y in x]
            else:
                if x is None or isinstance(x, (str, unicode, bytes)):
                    return x if x is None or isinstance(x, unicode) else x.decode(preferred_encoding, "replace")
                else:
                    raise InvalidUpdate("Invalid update type for this adaptor - x: {} - d: {}".format(x, d))

        # Todo: Upgrade to also handle unix datestamps
        def adapt_datetime(x, d):
            """
            Adapt a string into a datetime object
            :param x:
            :param d:
            :return:
            """
            if isinstance(x, (str, unicode, bytes)):
                try:
                    x = parse_date(x, assume_utc=False, as_utc=False)
                except:
                    raise InvalidUpdate("Unexpected case passed to adapt_datetime - x: {} - d: {}".format(x, d))
            elif x is True or x is False:
                raise InvalidUpdate("Unexpected case passed to adapt_datetime - bool - x: {} - d: {}".format(x, d))
            elif isinstance(x, (int, float)):
                raise InvalidUpdate(
                    "Unexpected case passed to adapt_datetime - int or float - x: {} - d: {}" "".format(x, d)
                )
            return x

        def adapt_bool(x, d):
            """
            Attempts to adapt a string into a bool.
            :param x:
            :param d:
            :return:
            """
            if isinstance(x, (str, unicode, bytes)):
                x = x.lower()
                if x == "true" or x == "1":
                    x = True
                elif x == "false" or x == "0":
                    x = False
                elif x == "none":
                    x = None
                else:
                    try:
                        x = bool(int(x))
                    except:
                        raise InvalidUpdate("adapt_bool has failed - x: {} - d: {}".format(x, d))
            elif isinstance(x, float):
                raise InvalidUpdate("adapt_bool has failed - x: {} - d: {}".format(x, d))
            elif isinstance(x, datetime.datetime):
                raise InvalidUpdate("adapt_bool has failed - x: {} - d: {}".format(x, d))
            return x

        def adapt_enum(x, d):
            """
            Adapt a enummeration type field - which is just text, so calls adapt_text instead/
            :param x:
            :param d:
            :return:
            """
            v = adapt_text(x, d)
            if not v:
                v = None
            return v

        def adapt_number(x, d):
            if x is None:
                return None
            if x is True or x is False:
                raise InvalidUpdate("adapt_number has been passed a bool - {}".format(x))
            if isinstance(x, (str, unicode, bytes)):
                if x.lower() == "none":
                    return None
            if d["datatype"] == "int":
                try:
                    return int(x)
                except:
                    raise InvalidUpdate(
                        "adapt_number has been passed an object it can't deal with - x: {} - d: {}" "".format(x, d)
                    )

            try:
                return float(x)
            except:
                raise InvalidUpdate(
                    "adapt_number has been passed an object it can't deal with - x: {} - d: {}" "".format(x, d)
                )

        def adapt_rating(x, d):
            if x is None:
                return None
            if x is True or x is False:
                raise InvalidUpdate("Unexpected update type - x: {} - d: {}".format(x, d))
            try:
                return min(10.0, max(0.0, float(x)))
            except (ValueError, TypeError):
                raise InvalidUpdate("Unexpected update type - x: {} - d: {}".format(x, d))

        self.custom_data_adapters = {
            "float": adapt_number,
            "int": adapt_number,
            "rating": adapt_rating,
            "bool": adapt_bool,
            "comments": lambda x, d: adapt_text(x, {"is_multiple": False}),
            "datetime": adapt_datetime,
            "text": adapt_text,
            "series": adapt_text,
            "enumeration": adapt_enum,
        }

        # Create Tag Browser categories for custom columns
        for k in sorted(self.custom_column_label_map.iterkeys()):
            v = self.custom_column_label_map[k]
            if v["normalized"]:
                is_category = True
            else:
                is_category = False
            is_m = v["multiple_seps"]
            tn = "custom_column_{0}".format(v["num"])
            self.field_metadata.add_custom_field(
                label=v["label"],
                table=tn,
                column="value",
                datatype=v["datatype"],
                colnum=v["num"],
                name=v["name"],
                display=v["display"],
                is_multiple=is_m,
                is_category=is_category,
                is_editable=v["editable"],
                is_csp=False,
            )

        # This class was originally embedded into the Library2 class - it's been spun off to allow easier testing
        # The methods here replace the actual methods that should be here when this class is being used it it's original
        # context.
        if not embed:
            self.dirtied = partial(dummy_dirtied, cc_class=self)
            self.notify = partial(dummy_notify, cc_class=self)

        # Note that tag browser categories for the custom columns have been, in fact, created
        self.cc_tag_browser_categories_made = True

    def refresh_db_custom_columns_metadata(self):
        """
        Re-read the data from the custom_columns table.
        :return:
        """
        custom_tables = self.custom_tables
        self.custom_column_label_map, self.custom_column_num_map = {}, {}

        remove = self.remove
        triggers = self.triggers

        cc = "custom_column_"

        for record in self.db.driver_wrapper.get_all_rows(table="custom_columns"):

            # At the moment data comes back from the database as a string - thus if you've stored a bool as a 0 or a 1
            # in the database what will come back is '0' or '1' - which always evaluates to True when you call bool with
            # it - which means EVERYTHING evaluates as a bool. Which is clearly wrong. Overcome this by coercing to int
            # before running bool
            # Todo: Merge with the adapters defined for the data types above.
            try:

                data = {
                    "label": record[cc + "label"],
                    "name": record[cc + "name"],
                    "datatype": record[cc + "datatype"],
                    "editable": bool(int(record[cc + "editable"])),
                    "display": json.loads(record[cc + "display"]),
                    "normalized": bool(int(record[cc + "normalized"])),
                    "num": int(record[cc + "id"]),
                    "is_multiple": bool(int(record[cc + "is_multiple"])),
                    "in_table": record[cc + "in_table"],
                }

            except Exception as e:
                err_str = "Parsing the record into a dict failed - deleting the record and continuing"
                default_log.log_exception(err_str, e, "ERROR", ("record", record))
                self.db.macros.do_custom_column_delete_by_id(record["custom_column_id"])
                continue

            if data["display"] is None:
                data["display"] = {}
            # set up the is_multiple separator dict
            if data["is_multiple"]:
                if data["display"].get("is_names", False):
                    seps = {
                        "cache_to_list": "|",
                        "ui_to_list": "&",
                        "list_to_ui": " & ",
                    }
                elif data["datatype"] == "composite":
                    seps = {"cache_to_list": ",", "ui_to_list": ",", "list_to_ui": ", "}
                else:
                    seps = {"cache_to_list": "|", "ui_to_list": ",", "list_to_ui": ", "}
            else:
                seps = {}
            data["multiple_seps"] = seps

            table, lt = self.custom_table_names(data["num"])
            # If a table is not normalized, we only need to check that it exists
            # If a table is normalized both it and it's link table need to be checked to exist
            if table not in custom_tables or (data["normalized"] and lt not in custom_tables):
                info_str = "The necessary tables where not found for a custom column - marking it for removal"
                default_log.log_variables(
                    info_str,
                    "INFO",
                    ("table", table),
                    ("lt", lt),
                    ("custom_tables", custom_tables),
                    ("data", data),
                )
                remove.append(data)
                continue

            self.custom_column_label_map[data["label"]] = data["num"]
            self.custom_column_num_map[data["num"]] = self.custom_column_label_map[data["label"]] = data

            # Create Foreign Key triggers
            if data["normalized"]:
                trigger = self.db.macros.get_foreign_key_replacement_trigger(target_table=lt)
            else:
                trigger = self.db.macros.get_foreign_key_replacement_trigger(target_table=table)
            triggers.append(trigger)

    # Begin Convenience methods for getting and setting custom data - {{{
    def get_custom(self, idx, label=None, num=None, index_is_id=False):
        """
        Returns the value for a given custom column with the given index or id.
        Reads it out of the results cache - which is based off reading the meta2 view.
        :param idx: Either the index of the row in the current sorting of data, or the id of the book row - determined
                    by the index_is_id switch
        :param label: The label on the custom column - one of either label or num must be filled so the system knows
                      which custom column to read from.
        :param num: The number of the custom columns
        :param index_is_id:
        :return:
        """
        if label is not None:
            data = self.custom_column_label_map[label]
        elif num is not None:
            data = self.custom_column_num_map[num]
        else:
            raise NotImplementedError("There is no information here to designate the custom column")

        row = self.data._data[idx] if index_is_id else self.data[idx]
        ans = row[self.FIELD_MAP[data["num"]]]
        if data["is_multiple"] and data["datatype"] == "text":
            ans = ans.split(data["multiple_seps"]["cache_to_list"]) if ans else []
            if data["display"].get("sort_alpha", False):
                ans.sort(cmp=lambda x, y: cmp(x.lower(), y.lower()))

        return ans

    def get_custom_extra(self, idx, label=None, num=None, index_is_id=False):
        """
        Reads the extra column from the link table for the particular book and returns it.
        Currently the only type of custom column which has a extra column is the link table to a custom column with
        datatype series - if the datatype is not series there is no attempt to retrieve the result - just returns None.
        In a series type custom column extra stores the "series position".
        :param idx:
        :param label:
        :param num:
        :param index_is_id:
        :return:
        """
        if label is not None:
            data = self.custom_column_label_map[label]
        elif num is not None:
            data = self.custom_column_num_map[num]
        else:
            raise NotImplementedError("There is no information here to designate the custom column")

        # add future datatypes with an extra column here
        if data["datatype"] not in ["series"]:
            return None

        ign, lt = self.custom_table_names(data["num"])
        idx = idx if index_is_id else self.id(idx)

        return self.direct_get_custom_extra(lt, idx)

    def get_custom_and_extra(self, idx, label=None, num=None, index_is_id=False):
        """
        Returns the value from the custom column and the extra component from the link table. If the datatype of the
        custom column is not series nothing is returned. See :meth get_custom: and :meth get_custom_extra:
        :param idx:
        :param label:
        :param num:
        :param index_is_id:
        :return:
        """
        if label is not None:
            data = self.custom_column_label_map[label]
        elif num is not None:
            data = self.custom_column_num_map[num]
        else:
            raise NotImplementedError("There is no information here to designate the custom column")

        idx = idx if index_is_id else self.id(idx)
        row = self.data._data[idx]
        ans = row[self.FIELD_MAP[data["num"]]]

        if data["is_multiple"] and data["datatype"] == "text":
            ans = ans.split(data["multiple_seps"]["cache_to_list"]) if ans else []
            if data["display"].get("sort_alpha", False):
                ans.sort(cmp=lambda x, y: cmp(x.lower(), y.lower()))

        # add future datatypes with an extra column here
        if data["datatype"] != "series":
            return ans, None

        ign, lt = self.custom_table_names(data["num"])
        extra = self.direct_get_custom_extra(lt, idx)
        return ans, extra

    def get_custom_items_with_ids(self, label=None, num=None):
        """
        Convenience methods for tag editing
        Some custom columns are stored in a normalized form - with multiple entries in the books table pointing at a
        single entry in the custom column table. This method makes editing those tags easier by providing the id and the
        value at the same time.
        If the data is not normalized - i.e. it is 1-1 with the books table, this method returns None. If the data is
        1-1 with the books table, the id of the data in the custom column doesn't matter. All that matters is the id of
        the book it's associated with.
        :param label:
        :param num:
        :return:
        """
        if label is not None:
            data = self.custom_column_label_map[label]
        elif num is not None:
            data = self.custom_column_num_map[num]
        else:
            raise NotImplementedError("There is no information here to designate the custom column")

        table, lt = self.custom_table_names(data["num"])
        if not data["normalized"]:
            return []
        return self.direct_get_custom_id_val_pairs(table)

    def rename_custom_item(self, old_id, new_name, label=None, num=None):
        """
        Rename an item in one of the custom tables
        :param old_id:
        :param new_name:
        :param label:
        :param num:
        :return:
        """
        if label is not None:
            data = self.custom_column_label_map[label]
        elif num is not None:
            data = self.custom_column_num_map[num]
        else:
            raise NotImplementedError("There is no information here to designate the custom column")

        table, lt = self.custom_table_names(data["num"])

        # Check to see if the item for rename is known to the database
        try:
            db_old_id, db_old_value = self.db.macros.get_cc_id_value_from_cc_id(table, old_id)
        except IndexError:
            raise InvalidUpdate
        if not db_old_id:
            raise InvalidUpdate

        # Adapt the val into a form to be written to the database - the adapters are a dictionary keyed with the vaugue
        # category of the thing to adapt, and valued with a function which takes a tuple of the actual value and the
        # data of that value
        val = self.custom_data_adapters[data["datatype"]](new_name, data)

        # check if item exists
        new_id = self.db.macros.get_cc_id_from_value(table, new_name)
        if new_id is None or old_id == new_id:

            self.db.macros.update_cc_value(cc_column=table, cc_id=old_id, cc_value=new_name)
            new_id = old_id

        else:

            # New id exists. If the column is_multiple, then process like tags, otherwise process like publishers
            # (see database2)
            if data["is_multiple"]:
                books = self.db.macros.get_cc_books_from_link_table(lt, old_id)
                for (book_id,) in books:
                    self.db.macros.break_cc_links_by_book_id_and_value(lt, book_id, new_id)

            # Remove the links from the link table - have to use the same conn for most of these transactions, because
            # we're in the middle of a commit
            self.db.macros.update_cc_lt_value_by_value(lt, new_id, old_id, conn=self.conn)
            # Remove the links from the actual table
            # Todo: A well chosen set of triggers should take care of this instead
            self.db.macros.delete_from_cc_table_by_id(table, old_id, conn=self.conn)

        # Note the change in the relevant places on the database
        data_label = "#" + data["label"]
        book_ids = self.custom_dirty_books_referencing(data_label, new_id, commit=False)
        self.rename_custom_item_in_data(book_ids=book_ids, column_num=data["num"], new_value=new_name)

        # Change the permissible set values in the enumeration type - if that's appropriate
        if data["datatype"] == "enumeration":
            data["display"]["enum_values"].remove(db_old_value)
            data["display"]["enum_values"].append(new_name)

        # Actually update the database
        self.conn.commit()

    def rename_custom_item_in_data(self, book_ids, column_num, new_value):
        """
        Replace all the elements in data with the new value.
        :param book_ids: The books ids to update the value for
        :param column_num: THe CUSTOM COLUMN number
        :param new_value: The new value to write out into the cache
        :return:
        """
        for book_id_tuple in book_ids:
            self.data.set(
                row=book_id_tuple[0],
                col=self.FIELD_MAP[column_num],
                val=new_value,
                row_is_id=True,
            )

    def delete_custom_item_using_id(self, id, label=None, num=None):
        """
        Delete the custom item using it's id
        :param id: The id of the resource to delete
        :param label: The label of the custom column (either this, or the num must be not None, to tell the method which
                      custom column to delete from).
        :param num:
        :return:
        """
        if id:
            if label is not None:
                data = self.custom_column_label_map[label]
            elif num is not None:
                data = self.custom_column_num_map[num]
            else:
                raise NotImplementedError("There is no information here to designate the custom column")

            table, lt = self.custom_table_names(data["num"])

            # Note the change with books_referencing - which allows the books to be updated with the new information
            book_ids = self.custom_dirty_books_referencing("#" + data["label"], id, commit=False)

            # Delete from the link table and the actual table
            self.db.macros.delete_cc_item(table, lt, id)

            self.rename_custom_item_in_data(book_ids=book_ids, column_num=data["num"], new_value=None)

    def is_item_used_in_multiple(self, item, label=None, num=None):
        """
        Is the given item, in the custom column designated with it's label or num, used with multiple books or not?
        :param item: The item to search for
        :param label:
        :param num:
        :return:
        """
        existing_tags = self.all_custom(label=label, num=num)
        return item.lower() in {t.lower() for t in existing_tags}

    def delete_item_from_multiple(self, item, label=None, num=None):
        """
        Delete an item which is reference by multiple books.
        :param item: The item to delete
        :param label: One of label or num must be not None - to indicate which of the custom columns is being
                      referred to
        :param num:
        :return:
        """
        if label is not None:
            data = self.custom_column_label_map[label]
        elif num is not None:
            data = self.custom_column_num_map[num]
        else:
            raise NotImplementedError("There is no information here to designate the custom column")

        if data["datatype"] != "text" or not data["is_multiple"]:
            raise ValueError("Column %r is not text/multiple" % data["label"])

        existing_tags = list(self.all_custom(label=label, num=num))
        lt = [t.lower() for t in existing_tags]
        try:
            idx = lt.index(item.lower())
        except ValueError:
            idx = -1
        books_affected = []
        if idx > -1:
            table, lt = self.custom_table_names(data["num"])
            id_ = self.db.macros.get_cc_id_from_value(table, existing_tags[idx], all=False, conn=self.conn)
            if id_:
                books = self.db.macros.get_cc_lt_books_from_lt_value(lt, value=id_, conn=self.conn)
                if books:
                    books_affected = [b[0] for b in books]
                self.db.macros.delete_from_cc_table_by_value(lt, id_)
                self.db.macros.delete_from_cc_table_by_id(table, id_)
                self.conn.commit()
        return books_affected

    # }}} End Convenience methods

    # Todo: What if the book is already in this series, but in another position in the priority stack
    def get_next_cc_series_num_for(self, series, label=None, num=None):
        """

        :param series:
        :param label:
        :param num:
        :return:
        """
        if label is not None:
            data = self.custom_column_label_map[label]
        elif num is not None:
            data = self.custom_column_num_map[num]
        else:
            raise NotImplementedError("There is no information here to designate the custom column")

        if data["datatype"] != "series":
            return None
        table, lt = self.custom_table_names(data["num"])
        # get the id of the row containing the series string
        series_id = self.db.macros.get_cc_id_from_value(table, series, all=False, conn=self.conn)

        # Todo: Upgrade preferences to use json serialization to solve this mess
        series_index_auto_incr = preferences.parse("series_index_auto_increment", "string", "next")
        if series_id is None:
            if isinstance(series_index_auto_incr, (int, float)):
                return float(series_index_auto_incr)
            return 1.0
        series_indices = self.db.macros.get_cc_series_index_indices(
            cc_series_link_table=lt, series_id=series_id, conn=self.conn
        )

        return self._get_next_series_num_for_list(series_indices)

    @staticmethod
    def _get_next_series_num_for_list(series_indices):
        return _get_next_series_num_for_list(series_indices)

    def all_custom(self, label=None, num=None):
        """
        Returns all values from a custom column.
        :param label: One of label or num must be non-zero to designate the custom column
        :param num:
        :return:
        """
        if label is not None:
            data = self.custom_column_label_map[label]
        elif num is not None:
            data = self.custom_column_num_map[num]
        else:
            raise NotImplementedError("There is no information here to designate the custom column")

        table, lt = self.custom_table_names(data["num"])
        # If the data is already normalized it should already be distinct
        if data["normalized"]:
            ans = self.db.macros.get_all_cc_custom_values(cc_table=table, distinct=False, conn=self.conn)
        else:
            ans = self.db.macros.get_all_cc_custom_values(cc_table=table, distinct=True, conn=self.conn)
        ans = set([x[0] for x in ans])
        return ans

    def set_custom_column_metadata(
        self,
        num,
        name=None,
        label=None,
        is_editable=None,
        display=None,
        in_table=None,
        notify=True,
        update_last_modified=False,
    ):
        """
        Change the metadata for a custom column - identified with the num
        Update the metadata for a custom column - changes the entry in the custom_columns table.
        For all parameters (apart from num) if None, no change will be made.
        :param num: The number of the custom column (the custom column can usually be identified from the num or the
                    name - but you might want change the name
        :param name: The name of the custom column
        :param label: The label - which will be displayed when the custom_columns are presented in the viewer
        :param is_editable:
        :param display:
        :param notify:
        :param update_last_modified:
        :return:
        """
        # Actually update the database with the changes made
        changed = super(CustomColumns, self).set_custom_column_metadata(
            num=num,
            name=name,
            label=label,
            is_editable=is_editable,
            display=display,
            in_table=in_table,
        )

        if is_editable is not None:
            self.custom_column_num_map[num]["is_editable"] = bool(is_editable)

        if notify:
            self.notify("metadata", [])

        return changed

    def set_custom_bulk_multiple(self, ids, add=None, remove=None, label=None, num=None, notify=False):
        """
        Fast algorithm for updating custom column is_multiple datatypes.
        Do not use with other custom column datatypes.
        :param ids:
        :param add:
        :param remove:
        :param label:
        :param num:
        :param notify:
        :return:
        """
        if add is None:
            add = []
        if remove is None:
            remove = []

        if label is not None:
            data = self.custom_column_label_map[label]
        elif num is not None:
            data = self.custom_column_num_map[num]
        else:
            raise NotImplementedError("There is no information here to designate the custom column")

        if not data["editable"]:
            raise ValueError("Column %r is not editable" % data["label"])
        if data["datatype"] != "text" or not data["is_multiple"]:
            raise ValueError("Column %r is not text/multiple" % data["label"])

        add = self.cleanup_tags(add)
        remove = self.cleanup_tags(remove)
        remove = set(remove) - set(add)
        if not ids or (not add and not remove):
            return
        # get custom table names
        custom_table, link_table = self.custom_table_names(data["num"])

        # Add tags that do not already exist into the custom_table
        all_tags = self.all_custom(num=data["num"])
        lt = [t.lower() for t in all_tags]
        new_tags = [t for t in add if t.lower() not in lt]
        if new_tags:
            self.db.macros.insert_multiple_values_into_cc_table(custom_table, new_tags, conn=self.conn)

        # Create the temporary temp_tables to store the ids for books and tags
        # to be operated on
        temp_tables = (
            "temp_bulk_tag_edit_books",
            "temp_bulk_tag_edit_add",
            "temp_bulk_tag_edit_remove",
        )
        self.db.macros.create_cc_temp_tables(temp_tables, conn=self.conn)

        # Populate the books temp custom_table
        self.db.macros.insert_values_into_temp_table("temp_bulk_tag_edit_books", ids, conn=self.conn)

        # Populate the add/remove tags temp temp_tables
        self.db.macros.do_cc_db_bulk_addition(temp_tables, custom_table, link_table, add, remove, conn=self.conn)

        # get rid of the temp tables
        self.db.macros.destroy_cc_temp_tables(temp_tables, conn=self.conn)
        self.dirtied(ids, commit=False)
        self.conn.commit()

        # set the in-memory copies of the tags
        for x in ids:
            tags = self.db.macros.read_cc_value_from_meta_2(data["num"], x, conn=self.conn)
            self.data.set(x, self.FIELD_MAP[data["num"]], tags, row_is_id=True)

        if notify:
            self.notify("metadata", ids)

    def set_custom_bulk(self, ids, val, label=None, num=None, append=False, notify=True, extras=None):
        """
        Change the value of a column for a set of books. The ids parameter is a
        list of book ids to change. The extra field must be None or a list the
        same length as ids.
        :param ids: The ids to set the value for
        :param val: Value to set
        :param label: Either this or the num is used to identify the column to set the value for
        :param num: The id of the column in the custom columns table
        :param append: If possible, the value is appended to the end of the current value in memory
        :param notify: A notification callback
        :param extras: Either None or a dictionary keyed with the positions of the individual ids in the ids itterator
        :return:
        """
        if extras is not None and len(extras) != len(ids):
            raise ValueError("Length of ids and extras is not the same")
        ev = None
        for idx, id in enumerate(ids):
            if extras is not None:
                ev = extras[idx]
            self._set_custom(id, val, label=label, num=num, append=append, notify=notify, extra=ev)
        self.dirtied(ids, commit=False)
        self.conn.commit()

    def set_custom(
        self,
        id,
        val,
        label=None,
        num=None,
        append=False,
        notify=True,
        extra=None,
        commit=True,
        allow_case_change=False,
    ):
        """
        Sets values for a custom column. This method calls the _set_custom method to do the actual work and notes that
        the records in question have been dirtied using self.dirtied.
        Calls self._set_custom with all this information, and dirties the appropriate record.
        :param id: The book id to set the custom column value for
        :param val: The value to set the custom_column to
        :param label: Either this, or the num, is used to specify which custom column to set the value for
        :param num: The id of the custom column in the custom column table (either this or label can be used - label is
                    checked first (this should be swapper around).
        :param append:
        :param notify: A handler to notify the database that the metadata of a book has changed
        :param extra: If the data type is series sets the extra field of the link table to this value - which is the
                      position of the book in the series
        :param commit: Update the database with the newly changed value
        :param allow_case_change: In a case where the data is normalized can case changes be made to use an existing
                                  value?
        :return:
        """
        rv = self._set_custom(
            id,
            val,
            label=label,
            num=num,
            append=append,
            notify=notify,
            extra=extra,
            allow_case_change=allow_case_change,
        )
        self.dirtied({id} | rv, commit=False)
        if commit:
            self.conn.commit()
        return rv

    def _set_custom(
        self,
        id_,
        val,
        label=None,
        num=None,
        append=False,
        notify=True,
        extra=None,
        allow_case_change=False,
    ):
        """
        Does the work of setting a custom column to be a designated value.
        Will return an empty set if the datatype is composite (and, thus, not editable)
        :param id_:
        :param val:
        :param label: Either this, or num, is used to determine the custom_column to operate on
        :param num: The id of the custom column in the custom_columns table
        :param append: Append the val to the current val in that table
        :param notify:
        :param extra: For a 'series' type custom column the link table has an additional column called extra - this can
                      be set using this value.
        :param allow_case_change: In a case where the data is normalized can case changes be made to use an existing
                                  value?
        :return books_to_refresh: A set of the book_ids which now need refreshing (specifically the caches - including
                                  the backup of the metadata about the book) might need to be updated on disk.
        """
        # Todo: Swap the order in which these are checked everywhere
        if label is not None:
            data = self.custom_column_label_map[label]
        elif num is not None:
            try:
                data = self.custom_column_num_map[num]
            except KeyError:
                err_str = "KeyError while calling self.custom_column_num_map"
                default_log.log_variables(
                    err_str,
                    "ERROR",
                    ("self.custom_column_num_map", self.custom_column_num_map),
                )
                raise
        else:
            raise NotImplementedError("There is no information here to designate the custom column")

        # The column is made up from data from other columns - thus changing it makes no sense and is ignored.
        if data["datatype"] == "composite":
            return set([])

        if not data["editable"]:
            raise ValueError("Column %r is not editable" % data["label"])

        # Get the name of the link table and the custom column table to operate on
        table, lt = self.custom_table_names(data["num"])

        # This method will be used to retrieve the values for the given ids - which will be used as part of the updated
        # process
        getter = partial(self.get_custom, id_, num=data["num"], index_is_id=True)

        # Adapt the val into a form to be written to the database - the adapters are a dictionary keyed with the vaugue
        # category of the thing to adapt, and valued with a function which takes a tuple of the actual value and the
        # data of that value
        val = self.custom_data_adapters[data["datatype"]](val, data)

        # Todo: Series lists should be rejected if the series field is not multiple - but this is a chnage from calibre
        #       and needs to be coded
        if data["datatype"] == "series" and extra is None:
            (val, extra) = self._get_series_values(val)
            if extra is None:
                extra = 1.0

        books_to_refresh = set([])
        if data["normalized"] and data["datatype"] != "series":

            # Checks that, if a column is an enumeration type column, that some value is provided and that the values
            # is in the valid enumeration types
            if data["datatype"] == "enumeration" and (val and val not in data["display"]["enum_values"]):
                err_str = "A Custom Column of type enumeration was passed a value not in the allowed write set."
                err_str = default_log.log_variables(
                    err_str,
                    "ERROR",
                    ("data", data),
                    ("val", val),
                    ("type(val)", type(val)),
                    ("data['display']['enum_values']", data["display"]["enum_values"]),
                    (
                        "type(data['display']['enum_values'])",
                        type(data["display"]["enum_values"]),
                    ),
                )
                raise InvalidUpdate(err_str)

            if not append or not data["is_multiple"]:
                self.db.macros.break_cc_links_by_book_id(lt, id_, conn=self.conn)
                self.db.macros.clear_cc_unused_table_entries(table=table, lt=lt, conn=self.conn)
                # Does the work of actually nullifying the value for the stored data
                self.data._data[id_][self.FIELD_MAP[data["num"]]] = None

            set_val = val if data["is_multiple"] else [val] if not isinstance(val, list) else val
            existing = getter()
            if not existing:
                existing = set([])
            else:
                existing = set(existing)

            # preserve the order in set_val
            for x in [v for v in set_val if v not in existing]:
                # normalized types are text and ratings, so we can do this check to see if we need to re-add the value
                if not x:
                    continue
                case_change = False
                existing = list(self.all_custom(num=data["num"]))
                lx = [t.lower() if hasattr(t, "lower") else t for t in existing]

                try:
                    idx = lx.index(x.lower() if hasattr(x, "lower") else x)
                except ValueError:
                    idx = -1

                if idx > -1:
                    ex = existing[idx]
                    xid = self.db.macros.get_cc_id_from_value(table, ex, all=False, conn=self.conn)
                    if allow_case_change and ex != x:
                        case_change = True
                        self.db.macros.update_cc_value(table, x, xid)
                else:
                    xid = self.db.macros.add_cc_table_value(table, x, conn=self.conn)

                if not self.db.macros.check_for_cc_link(lt, id_, xid, self.conn):
                    if data["datatype"] == "series":
                        self.db.macros.add_cc_link_with_extra(lt, id_, xid, extra, conn=self.conn)
                        self.data.set(id_, self.FIELD_MAP[data["num"]] + 1, extra, row_is_id=True)
                    else:
                        self.db.macros.add_cc_link_with_extra(lt, id_, xid, conn=self.conn)

                if case_change:
                    bks = self.db.macros.get_cc_lt_books_from_lt_value(lt, xid, conn=self.conn)
                    books_to_refresh |= set([bk[0] for bk in bks])

            nval = self.db.macros.read_cc_value_from_meta_2(data["num"], id_, conn=self.conn)
            self.data.set(id_, self.FIELD_MAP[data["num"]], nval, row_is_id=True)

        elif data["normalized"] and data["datatype"] == "series":

            if not append or not data["is_multiple"]:
                self.db.macros.break_cc_links_by_book_id(lt, id_, conn=self.conn)
                self.db.macros.clear_cc_unused_table_entries(table=table, lt=lt, conn=self.conn)
                # Does the work of actually nullifying the value for the stored data
                self.data._data[id_][self.FIELD_MAP[data["num"]]] = None

            set_val = val if data["is_multiple"] else [val] if not isinstance(val, list) else val
            existing = getter()
            if not existing:
                existing = set([])
            else:
                existing = set(existing)

            # preserve the order in set_val
            for x in [v for v in set_val if v not in existing]:
                # normalized types are text and ratings, so we can do this check to see if we need to re-add the value
                if not x:
                    continue
                case_change = False
                existing = list(self.all_custom(num=data["num"]))
                lx = [t.lower() if hasattr(t, "lower") else t for t in existing]

                try:
                    idx = lx.index(x.lower() if hasattr(x, "lower") else x)
                except ValueError:
                    idx = -1

                if idx > -1:
                    ex = existing[idx]
                    xid = self.db.macros.get_cc_id_from_value(table, ex, all=False, conn=self.conn)
                    if allow_case_change and ex != x:
                        case_change = True
                        self.db.macros.update_cc_value(table, x, xid)
                else:
                    xid = self.db.macros.add_cc_table_value(table, x, conn=self.conn)

                if not self.db.macros.check_for_cc_link(lt, id_, xid, self.conn):
                    if data["datatype"] == "series":
                        self.db.macros.add_cc_link_with_extra(lt, id_, xid, extra, conn=self.conn)
                        self.data.set(id_, self.FIELD_MAP[data["num"]] + 1, extra, row_is_id=True)
                    else:
                        self.db.macros.add_cc_link_with_extra(lt, id_, xid, conn=self.conn)

                if case_change:
                    bks = self.db.macros.get_cc_lt_books_from_lt_value(lt, xid, conn=self.conn)
                    books_to_refresh |= set([bk[0] for bk in bks])

            nval = self.db.macros.read_cc_value_from_meta_2(data["num"], id_, conn=self.conn)
            self.data.set(id_, self.FIELD_MAP[data["num"]], nval, row_is_id=True)

        else:
            self.db.macros.clear_cc_entries_from_table(table, id_, conn=self.conn)
            if val is not None:
                self.db.macros.add_cc_link_with_extra(lt=table, book_id=id_, value_id=val, conn=self.conn)

            nval = self.db.macros.read_cc_value_from_meta_2(data["num"], id_, conn=self.conn)
            self.data.set(id_, self.FIELD_MAP[data["num"]], nval, row_is_id=True)

        if notify:
            self.notify("metadata", [id_])

        return books_to_refresh

    def clean_custom(self):
        """
        Clean the custom_columns - removes entries which are no longer used.
        :return:
        """
        clean_conn = self.db.driver.get_connection()

        self.db.macros.clean_custom(
            cc_num_map=self.custom_column_num_map,
            cc_table_name_factory=self.custom_table_names,
            conn=clean_conn,
        )

    def custom_columns_in_meta(self, update_field_map=True, field_metadata=None):
        """
        Creates the lines needed to add each of these columns to the view created in meta2.
        Returns lines based on each of the custom columns which will be added to the view.
        Does the order of these lines matter?
        :param update_field_map:
        :param field_metadata: A field metadata object to update at the same time - contains metadata about the fields
                               If None, will be ignored.
        :return: A dictionary keyed with the cc num and valued with a list of the lines that form the view for that
                 custom column in meta2
        """
        lines = {}

        # So, when updating the FIELD_MAP - we know the position to start counting from
        base = max(self.FIELD_MAP.values())

        # Todo: Needs to be generalized to books and titles
        # Todo: Possibly rename meta2 to something concerting books and titles view?
        for data in self.custom_column_label_map.values():

            table, lt = self.custom_table_names(data["num"])
            table_col = plural_singular_mapper(table)
            lt_col = plural_singular_mapper(lt)

            if data["normalized"]:

                query = "{table}.{table_col}_value"

                if data["is_multiple"]:

                    if data["multiple_seps"]["cache_to_list"] == "|":
                        query = "sortconcat_bar(link.{lt_col}_id, {table}.{table_col}_value)"

                    elif data["multiple_seps"]["cache_to_list"] == "&":
                        query = "sortconcat_amper(link.{lt_col}_id, {table}.{table_col}_value)"

                    else:
                        prints(
                            "WARNING: unknown value in multiple_seps",
                            data["multiple_seps"]["cache_to_list"],
                        )
                        query = "sortconcat_bar(link.{lt_col}_id, {table}.{table_col}_value)"

                final_query = query.format(table=table, table_col=table_col, lt_col=lt_col)

                line = """(SELECT {query} FROM {lt} AS link INNER JOIN
                    {table} ON(link.{lt_col}_value={table}.{table_col}_id) WHERE link.{lt_col}_book=books.book_id)
                    custom_{num}
                """.format(
                    query=final_query,
                    lt=lt,
                    lt_col=lt_col,
                    table=table,
                    table_col=table_col,
                    num=data["num"],
                )

                if data["datatype"] == "series":
                    line += """,(SELECT {lt_col}_extra FROM {lt} WHERE {lt}.{lt_col}_book=books.book_id)
                        custom_index_{num}""".format(
                        lt=lt, lt_col=lt_col, num=data["num"]
                    )
            else:
                line = """
                (SELECT {table_col}_value FROM {table} WHERE {table_col}_book=books.book_id) custom_{num}
                """.format(
                    table=table, table_col=table_col, num=data["num"]
                )
            lines[data["num"]] = line

        return lines

    # c.f. calibre.library.databases2 - around line 424
    def update_field_map_from_custom_columns_in_meta(self, lines, update_field_metadata=True):
        """
        The field map exists to provide a mapping between the position of a column in meta2 and the name of that column.
        It is assumed that the lines here have been added to meta2 - thus the field map should also be updated with
        them.
        WARNING - Check that the field map is correct before updating it while calling this method - otherwise it might
        no longer be valid. Check that the length of the field map is the same as a the length of a row retrieve from
        the meta2 view after update - as the field map should have an entry for every column in the view.
        :param lines: The output of custom_columns_in_meta - provides the information needed to update the FIELD_MAP
                      with the new custom columns.
                      Keyed with the number of the custom column (it's id in the custom_columns table). Values with the
                      lines that have to be added to meta2 to represent the object.
        :param update_field_metadata: If False, field_metadata IS NOT UPDATED. THIS SHOULD NEVER BE DONE.
                                      Except for during testing. Maybe.
        :return None: All changes are made internally
        """
        custom_map = lines

        # custom col labels are numbers (the id in the custom_columns table)
        custom_cols = list(sorted(custom_map.keys()))

        # Assume the field map is in it's default state - before any custom columns have been registered to it
        base = max(self.FIELD_MAP.values())

        for col in custom_cols:
            self.FIELD_MAP[col] = base = base + 1
            if update_field_metadata:
                self.field_metadata.set_field_record_index(
                    self.custom_column_num_map[col]["label"], base, prefer_custom=True
                )

            if self.custom_column_num_map[col]["datatype"] == "series":
                # account for the series index column. Field_metadata knows that the series index is one larger than the
                # series. If you change it here, be sure to change it there as well.
                self.FIELD_MAP[str(col) + "_index"] = base = base + 1
                if update_field_metadata:
                    self.field_metadata.set_field_record_index(
                        self.custom_column_num_map[col]["label"] + "_index",
                        base,
                        prefer_custom=True,
                    )

    def create_custom_column(
        self,
        label,
        name,
        datatype,
        is_multiple,
        editable=True,
        display=None,
        in_table="books",
    ):
        """
        Add a custom column to the books table.
        :param label:
        :param name:
        :param datatype: Must be one of the following - rating, int, text, comments, series, composite, enumeration,
                         float, datetime, bool
        :param is_multiple:
        :param editable: Is the column editable?
        :param display:
        :param in_table: Which table should the custom column be created in? (Defaults to books for historical reasons)
        :return:
        """
        num = super(CustomColumns, self).create_custom_column(
            label=label,
            name=name,
            datatype=datatype,
            is_multiple=is_multiple,
            editable=editable,
            display=display,
            in_table=in_table,
        )

        try:
            self.prefs.set("update_all_last_mod_dates_on_start", True)
        except AttributeError:
            pass

        return num

    def custom_field_name(self, label=None, num=None):
        """
        Gets the name for a custom field.
        :param label:
        :param num:
        :return:
        """
        if label is not None:
            return self.field_metadata.custom_field_prefix + label
        return self.field_metadata.custom_field_prefix + self.custom_column_num_to_label_map[num]

    def custom_field_metadata(self, label=None, num=None):
        if label is not None:
            return self.custom_column_label_map[label]
        return self.custom_column_num_map[num]

    def _get_series_values(self, val):
        """
        Takes a calibre formated series string (of the form "series_name [series_number]" e.g. "Rama [1.0]") and returns
        the series name and the desired position.
        Series names with spaces in them should be fine.
        :param val:
        :return:
        """
        if val is None or not val:
            return _get_series_values("")

        if isinstance(val, basestring):
            return _get_series_values(val)

        if isinstance(val, list):
            return _get_series_values(val[-1])

        else:
            raise NotImplementedError("val was not of an expected type {}".format(val))

    def cleanup_tags(self, tags_list):
        return cleanup_tags(tags_list)

    # Todo: This is mostly not going to actually work. Need ... something better.
    def custom_dirty_books_referencing(self, field, id, commit=True):
        """
        Version of the dirty_books_referencing function specifically for custom books.
        :param field:
        :param id:
        :param commit:
        :return:
        """
        # Get the list of books to dirty -- all books that reference the item
        table = self.field_metadata[field]["table"]
        link = self.field_metadata[field]["link_column"]
        bks = self.db.macros.get_cc_books_for_dirtying(table, link, id, conn=self.conn)
        books = []
        for (book_id,) in bks:
            books.append(book_id)
        self.dirtied(books, commit=commit)
        return bks

    def delete_custom_column(self, label=None, num=None):
        """
        Mark a custom column for later deletion.
        :param label:
        :param num:
        :return:
        """
        data = self.custom_field_metadata(label, num)

        self.db.macros.mark_custom_column_for_delete(num=data["num"])


########################################################################################################################
########################################################################################################################
# - DUMMY FUNCTIONS WHICH DO NOTHING


def dummy_notify(event, ids, cc_class):
    """
    Dummy for the notify class
    :param cc_class:
    :param event:
    :param ids:
    :return:
    """
    if cc_class.embed:
        raise NotImplementedError("This method should not be called when the class is embedded")
    else:
        pass


def dummy_dirtied(book_ids, commit, cc_class):
    """
    Dummty for the dirtied class - the original notes that this object has changed in the dirtied table of the database.
    :param cc_class:
    :param book_ids:
    :param commit:
    :return:
    """
    if cc_class.embed:
        raise NotImplementedError("This method should not be called when the class is embedded")
    else:
        pass
