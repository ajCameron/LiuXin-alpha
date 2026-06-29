
"""
Set methods for the custom columns.

Responsible for writing value out to custom columns.
"""

from __future__ import annotations

from typing import Iterable, Optional, Any, Union

from functools import partial

from LiuXin_alpha.errors import InvalidUpdate

from LiuXin_alpha.databases.api.custom_columns_api import CustomColumnsAPI

from LiuXin_alpha.utils.logging import default_log


class CCSetMethodsMixin:
    """
    Set values in a custom column.
    """
    def set_custom_bulk_multiple(
        self: "CustomColumnsAPI",
        cc_row_ids: Iterable[int],
        add: Optional[Iterable[Any]] = None,
        remove: Optional[Iterable[str]] = None,
        label: Optional[str] = None,
        num: Optional[int] = None,
        notify: bool = False,
    ) -> None:
        """
        Fast algorithm for updating custom column is_multiple datatypes.

        Do not use with other custom column datatypes.
        :param cc_row_ids:
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
        if not cc_row_ids or (not add and not remove):
            return
        # get custom table names
        in_table = data.get("in_table") or "books"
        custom_table, link_table = self.custom_table_names(data["num"], in_table=in_table)

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
        self.db.macros.insert_values_into_temp_table("temp_bulk_tag_edit_books", cc_row_ids, conn=self.conn)

        # Populate the add/remove tags temp temp_tables
        self.db.macros.do_cc_db_bulk_addition(temp_tables, custom_table, link_table, add, remove, conn=self.conn)

        # get rid of the temp tables
        self.db.macros.destroy_cc_temp_tables(temp_tables, conn=self.conn)
        self.dirtied(cc_row_ids, commit=False)
        self.conn.commit()

        # set the in-memory copies of the tags
        for x in cc_row_ids:
            tags = self.db.macros.read_cc_value_from_meta_2(data["num"], x, conn=self.conn)
            self.data.set(x, self.FIELD_MAP[data["num"]], tags, row_is_id=True)

        if notify:
            self.notify("metadata", cc_row_ids)

    def set_custom_bulk(
        self: "CustomColumnsAPI",
        cc_row_ids: Union[list[int], tuple[int, ...]],
        val: Any,
        label: Optional[str] = None,
        num: Optional[int] = None,
        append: bool = False,
        notify: bool = True,
        extras: list[Any] = None,
    ) -> None:
        """
        Change the value of a column for a set of books.

        The ids parameter is a list of book ids to change. The extra field must be None or a list the same length as
        ids.
        :param cc_row_ids: The ids to set the value for
        :param val: Value to set
        :param label: Either this or the num is used to identify the column to set the value for
        :param num: The id of the column in the custom columns table
        :param append: If possible, the value is appended to the end of the current value in memory
        :param notify: A notification callback
        :param extras: Either None or a dictionary keyed with the positions of the individual ids in the ids itterator
        :return:
        """
        if extras is not None and len(extras) != len(cc_row_ids):
            raise ValueError("Length of ids and extras is not the same")
        ev = None
        for idx, id in enumerate(cc_row_ids):
            if extras is not None:
                ev = extras[idx]
            self._set_custom(id, val, label=label, num=num, append=append, notify=notify, extra=ev)
        self.dirtied(cc_row_ids, commit=False)
        self.conn.commit()

    def set_custom(
        self: "CustomColumnsAPI",
        cc_row_id: int,
        val: Any,
        label: Optional[str] = None,
        num: Optional[int] = None,
        append: bool = False,
        notify: bool = True,
        extra: Any = None,
        commit: bool = True,
        allow_case_change: bool = False,
    ) -> set[int]:
        """
        Sets a single value for a custom column.

        This method calls the _set_custom method to do the actual work and notes that
        the records in question have been dirtied using self.dirtied.
        Calls self._set_custom with all this information, and dirties the appropriate record.
        :param cc_row_id: The book id to set the custom column value for
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
            cc_row_id,
            val,
            label=label,
            num=num,
            append=append,
            notify=notify,
            extra=extra,
            allow_case_change=allow_case_change,
        )
        self.dirtied({cc_row_id} | rv, commit=False)
        if commit:
            self.conn.commit()
        return rv

    def _set_custom(
        self: "CustomColumnsAPI",
        id_: int,
        val: Any,
        label: Optional[str] = None,
        num: Optional[int] = None,
        append: bool = False,
        notify: bool = True,
        extra: Optional[Any] = None,
        allow_case_change: bool = False,
    ) -> set[int]:
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
        in_table = data.get("in_table") or "books"
        table, lt = self.custom_table_names(data["num"], in_table=in_table)

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
