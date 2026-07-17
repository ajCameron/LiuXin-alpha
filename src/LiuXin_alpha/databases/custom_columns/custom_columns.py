
"""
Custom columns allow users to add custom data to the database.

This might, on balance, be more trouble that it's worth. But it's also an expected feature.
(might be worth being able to toggle it on and off)
"""

from __future__ import annotations

import json
import textwrap
from functools import partial

from typing import Iterable, TYPE_CHECKING, Optional, Any, Union

from LiuXin_alpha.databases.adaptors import (
    cc_adapt_text,
    cc_adapt_datetime,
    cc_adapt_bool,
    cc_adapt_enum,
    cc_adapt_number,
    cc_adapt_rating)
from LiuXin_alpha.databases.driver_wrapper.driver_wrapper_custom_columns_mixin import CustomColumnsDriverWrapperMixin
from LiuXin_alpha.databases.notify import dummy_notify, dummy_dirtied

from LiuXin_alpha.databases.utils import cleanup_tags, _get_next_series_num_for_list, _get_series_values

from LiuXin_alpha.errors import InvalidUpdate

from LiuXin_alpha.databases.field_metadata_bridge import FieldMetadata

from LiuXin_alpha.utils.logging import prints, default_log

from LiuXin_alpha.utils.language_tools import plural_singular_mapper

from LiuXin_alpha.utils.libraries.liuxin_six import basestring, iterkeys

from LiuXin_alpha.databases.custom_columns.cc_crud_columns_mixin import CCCRUDColumnsMixin
from LiuXin_alpha.databases.custom_columns.cc_names_mixin import CCNamesMixin
from LiuXin_alpha.databases.custom_columns.cc_set_methods_mixin import CCSetMethodsMixin
from LiuXin_alpha.databases.custom_columns.cc_get_methods_mixin import CCGetMethodsMixin
from LiuXin_alpha.databases.custom_columns.cc_delete_methods_mixin import CCDeleteMethodsMixin

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI
    from LiuXin_alpha.databases.db_types import MainTableName


from LiuXin_alpha.databases.constants import CUSTOM_DATA_TYPES

# Todo: These notes should not be here
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
class CustomColumns(
    CustomColumnsDriverWrapperMixin,
    CCNamesMixin,
    CCSetMethodsMixin,
    CCGetMethodsMixin,
    CCDeleteMethodsMixin,
    CCCRUDColumnsMixin):
    """
    Represents the custom_columns (specifically on the books table).
    In calibre this class was originally intended to be a mixing for the LibraryDatabase2 object - some work has been
    done to it so that it can be invoked independently - for easier testing.
    """

    CUSTOM_DATA_TYPES: frozenset[str] = CUSTOM_DATA_TYPES

    @property
    def custom_tables(self) -> Iterable[str]:
        """
        Return the custom table names defined oin the database.

        :return:
        """
        return self.get_custom_tables()

    def get_custom_tables(self) -> set[str]:
        """
        Return the custom table names defined on the owning database.

        :return:
        """
        return self.db.driver_wrapper.get_custom_tables()

    def __init__(
        self,
        db: "DatabaseAPI",
        conn=None,
        table: "MainTableName" = "books",
        field_metadata=None,
        data=None,
        field_map: dict[str, int] = None,
        embed: bool = False,
    ):
        """
        Represents the custom columns for a given table.

        Defaults to books.
        :param db: The database containing the custom columns
        :param conn: Connection object to allow executing SQL on the database
        :param table: Defaults to books.
        :param field_metadata: This is an object which stores the field metadata for the books table - if you have a
                               field_metadata object for the class your using this in already, then add it here.
        :param field_map: Keyed with the name of the field in the row and valued with the position of that field in the
                          row (which corresponds to the number of columns from the left in that table)
        :param embed: Is this class to be used as part of multiple inheritance for another class?
                      True for yes, False for No.
                      If True then certain methods are not declared to stop them overwriting methods which it's
                      presumed are present in the other class.
        :return:
        """
        self.embed = embed

        self.db = db

        # Calibre compatibility: default table was historically 'books'.
        # In a FRBR/WEMI-first schema that table may not exist.
        if table == "books" and "books" not in getattr(db, "main_tables", set()):
            if "manifestations" in getattr(db, "main_tables", set()):
                table = "manifestations"

        self.table = table

        # Prefer using the driver's live connection (see CustomColumnsDriverWrapperMixin.conn).
        # Accepting an explicit `conn` is kept for compatibility, but we avoid retaining a stale
        # reference when the driver rotates/aliases connections.
        if conn is not None:
            self.conn = conn

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
            # TEMP triggers are per-connection. When custom columns change (or when this class is reinstantiated),
            # we must rebuild the trigger definition to include the latest link tables.
            trigger_name = f"custom_{self.table}_delete_trg"

            with self.conn:
                # Drop/recreate so updates are applied and repeated initialization is idempotent.
                self.db.driver_wrapper.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")
                self.db.driver_wrapper.execute(textwrap.dedent(
                    """
                    CREATE TEMP TRIGGER {trigger_name}
                        AFTER DELETE ON {table}
                        BEGIN
                        {body}
                        END;
                    """.format(trigger_name=trigger_name, table=self.table, body=(" \n".join(triggers))))
                )

        # Setup data adapters
        self.custom_data_adapters = {
            "float": cc_adapt_number,
            "int": cc_adapt_number,
            "rating": cc_adapt_rating,
            "bool": cc_adapt_bool,
            "comments": lambda x, d: cc_adapt_text(x, {"is_multiple": False}),
            "datetime": cc_adapt_datetime,
            "text": cc_adapt_text,
            "series": cc_adapt_text,
            "enumeration": cc_adapt_enum,
        }

        # Create Tag Browser categories for custom columns
        for k in sorted(iterkeys(self.custom_column_label_map)):
            v = self.custom_column_label_map[k]
            # "Tag Browser" in calibre is a misnomer: it's a browser of *categories* (facets).
            # Those categories are (currently) facets over the books table.
            in_table = v.get("in_table") or "books"

            # Calibre behaviour: non-composite normalized columns appear as categories.
            # LiuXin rule: only those attached to books are categories in the calibre-style browser.
            is_category = bool(v.get("normalized") and in_table == "books" and v.get("datatype") != "composite")
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
                in_table=in_table,
            )

        # This class was originally embedded into the Library2 class - it's been spun off to allow easier testing
        # The methods here replace the actual methods that should be here when this class is being used it it's original
        # context.
        if not embed:
            self.dirtied = partial(dummy_dirtied, cc_class=self)
            self.notify = partial(dummy_notify, cc_class=self)

        # Note that tag browser categories for the custom columns have been, in fact, created
        self.cc_tag_browser_categories_made = True

    def refresh_db_custom_columns_metadata(self) -> None:
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

            in_table = data.get("in_table") or "books"
            table, lt = self.custom_table_names(data["num"], in_table=in_table)
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

            # Only load custom columns for the table this instance represents.
            # (Custom columns may exist on other tables, but this CustomColumns object is per-table.)
            if in_table != self.table:
                continue

            self.custom_column_label_map[data["label"]] = data["num"]
            self.custom_column_num_map[data["num"]] = self.custom_column_label_map[data["label"]] = data

            # Create Foreign Key replacement triggers (used to emulate ON DELETE CASCADE behaviour)
            # for custom column tables that reference the parent table.
            search_column = plural_singular_mapper(in_table)
            target_id = self.db.driver_wrapper.get_id_column(in_table)
            target_table = lt if data["normalized"] else table
            trigger = self.db.macros.get_foreign_key_replacement_trigger(
                target_table=target_table,
                search_column=search_column,
                target_id=target_id,
            )
            triggers.append(trigger)

    def rename_custom_item(
            self,
            old_id: int,
            new_name: str,
            label: Optional[str] = None,
            num: Optional[int] = None) -> None:
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

        in_table = data.get("in_table") or "books"
        table, lt = self.custom_table_names(data["num"], in_table=in_table)

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

    # Todo: Test the right item is being set
    # Todo: This is also kinda cursed, ngl
    def rename_custom_item_in_data(
            self,
            book_ids: Union[Iterable[int], Iterable[str]],
            column_num: str, new_value: Any) -> None:
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
                row_is_id = True,
            )

    def is_item_used_in_multiple(
            self,
            item: Any,
            label: Optional[str] = None,
            num: Optional[int] = None) -> bool:
        """
        Is the given item, in the custom column designated with its label or num, used with multiple books or not?

        :param item: The item to search for
        :param label:
        :param num:
        :return:
        """
        existing_tags = self.all_custom(label=label, num=num)
        return item.lower() in {t.lower() for t in existing_tags}

    # }}} End Convenience methods

    @staticmethod
    def _get_next_series_num_for_list(
            series_indices: list[Union[int, float]]) -> Optional[float]:
        """
        Takes a list of indices and tries to work out the "next" value for that list.

        :param series_indices:
        :return:
        """
        return _get_next_series_num_for_list(series_indices)


    def clean_custom(self) -> None:
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

    # Todo: Not sure where this goes, but perhaps not here.
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

            in_table = data.get("in_table") or "books"
            table, lt = self.custom_table_names(data["num"], in_table=in_table)
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
    def update_field_map_from_custom_columns_in_meta(
            self,
            lines: dict[int, str],
            update_field_metadata: bool = True) -> None:
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

        # Assume the field map is in its default state - before any custom columns have been registered to it
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

    # Todo: Probably the custom column metadata should be a dataclas
    def custom_field_metadata(self, label=None, num=None):
        if label is not None:
            return self.custom_column_label_map[label]
        return self.custom_column_num_map[num]

    def _get_series_values(self, val: Any) -> tuple[str, Optional[float]]:
        """
        Takes a calibre formated series string and returns the series name and the desired position.

        (of the form "series_name [series_number]" e.g. "Rama [1.0]")

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

    @staticmethod
    def cleanup_tags(tags_list: list[str]) -> list[str]:
        """
        Preform a clean of the given tags - ready for writing.

        :param tags_list:
        :return:
        """
        return cleanup_tags(tags_list)

    # Todo: This is mostly not going to actually work. Need ... something better.
    def custom_dirty_books_referencing(self, field, id, commit: bool = True) -> Iterable[int]:
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
