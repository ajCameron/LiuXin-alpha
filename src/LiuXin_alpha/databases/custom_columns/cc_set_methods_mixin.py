
"""
Mutate legacy custom-column SQL values and their results-cache projections.

Hosts supply metadata, adapters, macros, cache access and notification hooks. SQL writes, cache updates, dirtying and commits have separate failure boundaries; these methods do not provide a rollback context spanning the whole operation.
"""

from __future__ import annotations

from typing import Iterable, Optional, Any, Union

from functools import partial

from LiuXin_alpha.errors import InvalidUpdate

from LiuXin_alpha.databases.api.custom_columns_api import CustomColumnsAPI

from LiuXin_alpha.utils.logging import default_log


class CCSetMethodsMixin:
    """
    Coordinate custom value writes with legacy cache and notification hooks.

    Single and bulk writers delegate to _set_custom; the specialized text/multiple path uses temporary SQL tables. Existing adapter, macro and host-interface limitations propagate, including cleanup_tags failures for ordinary nonblank strings.

    Example:
        On a compatible host, set_custom(owner_id, 4, num=column_id, commit=False) stages a scalar update and dirtying while leaving the final explicit commit to its caller.
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
        Apply additions and removals to several editable text/multiple values.

        Validate column editability/type, then clean both tag lists before checking empty owners or a no-op. Current CustomColumns.cleanup_tags fails for ordinary nonblank text. With a compatible override, exact additions win over identical removals, new values are inserted, fixed-name TEMP tables drive link changes, then owners are dirtied and committed before cache refresh. Temporary tables are not removed in a finally block, and one-shot owner iterators can be exhausted before later steps.

        Example:
            With a host supplying working tag cleanup and a reusable ID list, set_custom_bulk_multiple([1, 2], add=["Travel"], num=column_id) adds the tag to both owners.


        :param cc_row_ids: Reusable owner-ID iterable; consumed by several database/cache/notification steps.
        :param add: Tags to add, or None for none.
        :param remove: Tags to remove, or None for none.
        :param label: Optional custom-column label; takes precedence over num.
        :param num: Numeric metadata key used when label is None.
        :param notify: Whether to notify after committing and refreshing the cache, default False.
        :return: None; no affected-owner result is returned.
        :raises NotImplementedError: Neither label nor num is supplied.
        :raises KeyError: The selected metadata record is absent.
        :raises ValueError: The column is noneditable or not text/multiple.
        :raises AttributeError: The current ordinary-string cleanup path attempts .decode on str.
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
        Call the single-owner worker in sequence, then dirty and commit the batch.

        A failure can follow earlier SQL/cache changes; no batch rollback is supplied. After a successful loop, dirty the requested IDs and commit even for an empty sequence. The worker’s additional case-change refresh IDs are not merged into that final dirtying call.

        Example:
            On a compatible host, set_custom_bulk([1, 2], "Saga", num=series_column, extras=[1.0, 2.0]) supplies a separate series position for each owner.


        :param cc_row_ids: Sized sequence of owner IDs, retained for final dirtying.
        :param val: Value passed to every worker call.
        :param label: Optional custom-column label; takes precedence over num.
        :param num: Numeric metadata key used when label is None.
        :param append: Forward append behavior to each worker.
        :param notify: Forward notification policy to each worker, which can notify before the final commit.
        :param extras: Optional sequence of per-owner extras, required to match the ID count.
        :return: None; additional owners returned by individual workers are discarded.
        :raises ValueError: extras and cc_row_ids have different lengths.
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
        Write one owner’s custom value, dirty affected IDs and optionally commit.

        Dirty the union of the requested ID and worker result with commit=False, then optionally commit. A composite worker returns an empty set without writing, but this wrapper still dirties the requested ID and may commit. Errors can follow partial writes or notification.

        Example:
            Given a compatible host, changed = set_custom(owner_id, "Draft", num=column_id, commit=False) returns extra owners requiring refresh without an explicit wrapper commit.


        :param cc_row_id: Owner ID passed to the worker.
        :param val: Value adapted by the worker.
        :param label: Optional custom-column label; takes precedence over num.
        :param num: Numeric metadata key used when label is None.
        :param append: Append only where the selected storage supports multiple values.
        :param notify: Let the worker notify before this method’s dirtying/commit step.
        :param extra: Optional series position; None permits parsing/defaulting.
        :param commit: Commit self.conn after dirtying when True.
        :param allow_case_change: Allow the worker’s legacy case-change path for reused normalized values.
        :return: Worker set of additional affected owner IDs; the requested ID is not automatically added to this return value.
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
        Adapt and persist one custom value, then refresh its cached projection.

        Composite values return an empty set before editability checks. Other writes require editable metadata. Normalized replacement clears links and cached value before adding truthy requested values; an existing series link does not update its extra. Unnormalized storage deletes the old owner value and inserts a non-None replacement. Reload the value through legacy meta2 SQL and update the cache. Current allow_case_change calls update_cc_value(table, value, id), reversing that macro’s ID/value parameters and omitting the explicit connection. No complete-operation commit or rollback is supplied here; delegated macros can have independent effects.

        Example:
            >>> from types import SimpleNamespace
            >>> host = SimpleNamespace(custom_column_num_map={1: {"datatype": "composite"}})
            >>> CCSetMethodsMixin._set_custom(host, 7, "ignored", num=1)
            set()


        :param id_: Owner ID used by SQL and cache lookups.
        :param val: Value passed through the datatype adapter.
        :param label: Optional custom-column label; takes precedence over num.
        :param num: Numeric metadata key used when label is None.
        :param append: Preserve existing links only for multiple normalized storage when True.
        :param notify: Emit metadata notification for this owner after cache refresh.
        :param extra: Series position; when None, parse it from the adapted value or use 1.0.
        :param allow_case_change: Permit case-change updates to an already stored normalized value.
        :return: Set of other owner IDs found when a shared normalized spelling changes.
        :raises NotImplementedError: Neither label nor num is supplied.
        :raises KeyError: The selected metadata record is absent.
        :raises ValueError: A non-composite column is not editable.
        :raises InvalidUpdate: A truthy enumeration value is outside its allowed values.
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
