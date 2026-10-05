
"""
Compose the legacy custom-column facade, metadata loading and cache hooks.

Constructing CustomColumns can delete marked/broken definitions, install temporary triggers and register custom fields. It does not populate the results cache required by value getters/setters. The first driver-wrapper base shadows the later CRUD wrapper methods.
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

from LiuXin_alpha.catalog.field_metadata import FieldMetadata

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
    Manage one attachment table’s legacy custom-column metadata and operations.

    Share supplied cache, field-map and FieldMetadata objects. Initialization performs schema cleanup and metadata registration; standalone mode installs inert dirty/notify hooks. Default cache data is an empty dictionary, so cached value operations need additional host setup. MRO resolution selects driver-wrapper creation/metadata/deletion methods before CCCRUDColumnsMixin.

    Example:
        Given an open db, CustomColumns(db, table="works") loads and registers custom definitions attached to works, potentially performing cleanup.
    """

    CUSTOM_DATA_TYPES: frozenset[str] = CUSTOM_DATA_TYPES

    @property
    def custom_tables(self) -> Iterable[str]:
        """
        Read the owning database’s global custom-table names through the wrapper.

        Example:
            For a configured facade cc, cc.custom_tables includes physical custom value/link tables across the database.


        :return: Collection returned by get_custom_tables, not restricted to this facade’s attachment table.
        """
        return self.get_custom_tables()

    def get_custom_tables(self) -> set[str]:
        """
        Delegate global custom-table discovery to the owning driver wrapper.

        Example:
            Given cc, cc.get_custom_tables() retrieves the physical names used during definition validation.


        :return: Wrapper result, normally a set of custom value/link table names.
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
        Attach host state, clean obsolete definitions and register custom metadata.

        Delete marked columns, refresh definitions, remove orphan definitions and recreate the parent-delete TEMP trigger on the current connection when snippets exist. Adapter and FieldMetadata setup follows; normalized non-composite columns are categories only for literal books. Setup is not atomic or read-only. Reusing shared FieldMetadata can encounter existing registrations. The instance does not initialize custom_column_num_to_label_map or populate cache rows.

        Example:
            Given db, cc = CustomColumns(db, table="works", field_metadata=metadata) shares metadata and loads Work custom-column definitions.


        :param db: Open database with custom-column wrapper/macros and schema metadata.
        :param conn: Optional compatibility connection assigned through the live-connection property.
        :param table: Attachment table; absent legacy books falls back to manifestations when available.
        :param field_metadata: FieldMetadata object retained by reference, or a new one.
        :param data: Results-cache host retained by reference; None becomes an empty dictionary.
        :param field_map: Field-to-slot mapping retained by reference; None uses the legacy 22-slot layout.
        :param embed: When False, install dummy notification/dirtying hooks; True expects host-provided hooks.
        :return: None; metadata, adapters, temporary triggers and custom field registrations are configured.
        :raises ValueError: CUSTOM_DATA_TYPES includes a type absent from FieldMetadata.VALID_DATA_TYPES.
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
        Rebuild per-table definition maps while collecting removals and trigger snippets.

        Scan every custom_columns record before filtering to this attachment table. Parsing errors delete the definition immediately; missing backing tables queue removals even for other attachment tables. A parsed display=None becomes {}, but another nonmapping display can fail later outside the parsing guard. Shared metadata dictionaries populate both maps. Repeated calls do not clear remove/triggers, install the trigger, execute queued removals or rebuild FieldMetadata registrations.

        Example:
            After external schema changes, cc.refresh_db_custom_columns_metadata() refreshes its maps; recreating the facade is the separate path that applies queued cleanup and trigger installation.


        :return: None; update metadata maps and append to existing remove/triggers lists.
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
        Rename or merge a stored normalized value and update legacy cache references.

        If another value already has the requested spelling, repoint links and remove the old value, avoiding duplicate multiple links. Otherwise update the existing value. The adapted value is discarded; original new_name is used throughout. Dirty referencing owners, update cached values, and alter an enumeration’s in-memory allowed list when applicable. Lookup, SQL and host-hook failures can leave partial changes; no encompassing transaction is supplied.

        Example:
            On a fully configured facade, rename_custom_item(value_id, "Revised", num=column_id) updates that spelling or merges into an existing equal value.


        :param old_id: Existing value-row ID; a false resolved ID is rejected.
        :param new_name: Requested spelling used for lookup/storage even though an adapter is also called.
        :param label: Optional custom-column label; takes precedence over num.
        :param num: Numeric metadata key used when label is None.
        :return: None; SQL/cache/enumeration updates are followed by self.conn.commit on success.
        :raises NotImplementedError: Neither label nor num is supplied.
        :raises KeyError: The selected metadata record is absent.
        :raises InvalidUpdate: Old-value lookup raises IndexError or resolves a false ID.
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
        Replace a cache slot for each owner represented by an indexable row.

        No SQL, dirtying or notification occurs. Flat integer entries fail at element-zero lookup. This signature uses book_ids, so deletion code passing target_ids raises before entering the method.

        Example:
            Given a configured cache, cc.rename_custom_item_in_data([(1,), (2,)], column_id, None) clears the selected custom slot for both owner IDs.


        :param book_ids: Iterable of row-like entries whose first element is the owner ID, despite the flat-ID annotation.
        :param column_num: Custom-column key in FIELD_MAP.
        :param new_value: Value written to the cache without adaptation.
        :return: None; call data.set with row_is_id=True for each entry.
        :raises TypeError: An owner entry cannot be indexed, or the caller uses the unsupported target_ids keyword.
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
        Check whether a case-insensitive value spelling exists in the column inventory.

        Despite its name, this does not count references or require multiple storage. It checks existence only, using lower rather than casefold.

        Example:
            With "Travel" in cc.all_custom(num=column_id), cc.is_item_used_in_multiple("travel", num=column_id) is true even for a value used by only one owner.


        :param item: Value supporting lower().
        :param label: Optional custom-column label; takes precedence over num.
        :param num: Numeric metadata key used when label is None.
        :return: True if its lowercase spelling appears in all_custom.
        """
        existing_tags = self.all_custom(label=label, num=num)
        return item.lower() in {t.lower() for t in existing_tags}

    # }}} End Convenience methods

    @staticmethod
    def _get_next_series_num_for_list(
            series_indices: list[Union[int, float]]) -> Optional[float]:
        """
        Delegate series numbering to the preference-driven legacy helper.

        Default unwrapping remains enabled, so nonempty flat numeric sequences violate the actual input shape despite the annotation.

        Example:
            With the default next policy, CustomColumns._get_next_series_num_for_list([[3.5]]) returns 4.0.


        :param series_indices: Ordered indexable rows containing the numeric index at element zero.
        :return: Next/configured number from the shared helper.
        """
        return _get_next_series_num_for_list(series_indices)


    def clean_custom(self) -> None:
        """
        Run custom-value cleanup using a newly opened driver connection.

        Pass the metadata map and naming factory to the macro, which commits its work. This method does not close the supplied connection or refresh cache values afterward; errors can follow earlier column cleanup.

        Example:
            Given cc, cc.clean_custom() prunes values according to the legacy macro’s normalized-column rules.


        :return: None; delegate each column’s cleanup to db.macros.clean_custom.
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
        Generate legacy meta2 SQL projection strings for the current custom columns.

        Use books.book_id and legacy book/value link columns, even for another attachment table. Multiple normalized values use sorted-concatenation functions; series adds a second index projection. No view is created and FIELD_MAP is not updated. max(FIELD_MAP.values()) is still evaluated, so an empty field map raises even though the calculated value is unused.

        Example:
            Given cc, fragments = cc.custom_columns_in_meta() produces SQL strings for a compatible books-based meta2 view.


        :param update_field_map: Compatibility flag currently ignored.
        :param field_metadata: Compatibility metadata argument currently ignored.
        :return: Dictionary mapping custom-column numbers to SQL projection strings.
        :raises ValueError: FIELD_MAP is empty.
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
        Append custom value/index slots and optionally update FieldMetadata indices.

        Start after the current maximum slot, add each custom value and an adjacent series index where required. Repeated calls append new positions rather than restoring original ones. The caller must ensure the actual view uses this order; failures can leave partial map updates.

        Example:
            After creating a compatible view in sorted custom-number order, cc.update_field_map_from_custom_columns_in_meta(fragments) appends its cache slots.


        :param lines: Mapping whose sorted numeric keys determine custom-column slot order; SQL values are not inspected.
        :param update_field_metadata: Whether to update FieldMetadata alongside FIELD_MAP, default True.
        :return: None; mutate FIELD_MAP and possibly field metadata in place.
        :raises ValueError: FIELD_MAP is empty.
        :raises KeyError: A requested number is missing from custom_column_num_map.
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
        """
        Return the metadata record selected by label first, otherwise number.

        Example:
            Given cc, cc.custom_field_metadata(num=column_id) exposes the record shared with its numeric/label maps.


        :param label: Optional custom-column label; takes precedence over num.
        :param num: Numeric metadata key used when label is None.
        :return: Existing mutable metadata dictionary, not a copy.
        :raises KeyError: The chosen key is absent, including num=None when neither selector is supplied.
        """

        if label is not None:
            return self.custom_column_label_map[label]
        return self.custom_column_num_map[num]

    def _get_series_values(self, val: Any) -> tuple[str, Optional[float]]:
        """
        Parse a series string or the last entry of a supplied list.

        Delegate parsing to the shared bracket-index helper. Earlier list elements are ignored; unsupported truthy types raise rather than being stringified.

        Example:
            For cc, cc._get_series_values(["ignored", "Dune [2]"]) returns ("Dune", 2.0).


        :param val: False value, series string, or nonempty list whose last item can be parsed.
        :return: Parsed (name, optional float index); false input becomes ("", None).
        :raises NotImplementedError: A truthy value is neither a supported string nor a list.
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
        Delegate tag cleanup to the legacy utility, including its current failures.

        The utility misclassifies str as bytes and calls decode; nonempty bytes fail earlier in replace. No facade-specific repair or exception translation is applied.

        Example:
            >>> CustomColumns.cleanup_tags([])
            []


        :param tags_list: Iterable of tag entries passed through unchanged.
        :return: Empty list for empty/all-blank input; ordinary nonblank str or bytes entries currently raise.
        :raises AttributeError: A nonblank ordinary string reaches .decode.
        :raises TypeError: A nonempty bytes entry reaches string-argument replace.
        """
        return cleanup_tags(tags_list)

    # Todo: This is mostly not going to actually work. Need ... something better.
    def custom_dirty_books_referencing(self, field, id, commit: bool = True) -> Iterable[int]:
        """
        Query owners referencing a custom value and pass their IDs to dirtied.

        Expect each result entry to unpack to exactly one owner ID. A one-shot result iterator can be exhausted when returned. Standalone dummy dirtied performs no persistence; embedded hosts determine actual dirtying/commit effects.

        Example:
            On a compatible facade, cc.custom_dirty_books_referencing("#shelf", value_id, commit=False) requests dirtying and returns the macro’s owner rows.


        :param field: FieldMetadata key supplying table and link_column for the macro.
        :param id: Value ID used to find referencing owners.
        :param commit: Flag forwarded to the dirtied hook, default True.
        :return: Original row-shaped macro result after iterating it to extract one-column owner IDs.
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
