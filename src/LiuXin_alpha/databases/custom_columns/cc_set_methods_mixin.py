"""
Mutate legacy custom-column SQL values and their results-cache projections.

Hosts supply metadata, adapters, macros, cache access and notification hooks. Public
writers own a complete SQL transaction, restore touched cache rows on failure and
deliver notifications after commit. Caller-owned transactions are rejected before SQL.
"""

from __future__ import annotations

from LiuXin_alpha.databases.custom_columns.value_writes import atomic_custom_write

from LiuXin_alpha.databases.database_driver_plugins._macro_state import (
    require_legacy_macros,
)

from typing import Iterable, Optional, Any, Mapping, Sequence

from functools import partial
from uuid import uuid4

from LiuXin_alpha.errors import DatabaseIntegrityError, InvalidUpdate

from LiuXin_alpha.databases.api.custom_columns_api import CustomColumnsAPI

from LiuXin_alpha.utils.logging import default_log


class CCSetMethodsMixin:
    """
    Coordinate custom value writes with legacy cache and notification hooks.

    Single and bulk writers delegate to _set_custom; the specialized text/multiple path
    uses temporary SQL tables. Existing adapter, macro and host-interface limitations
    propagate.

    Example:
        On a compatible host, set_custom(owner_id, 4, num=column_id) commits a
        scalar update, then notifies listeners. Use SQL macros for caller-owned work.
    """

    @atomic_custom_write
    def set_custom_bulk_multiple(
        self: "CustomColumnsAPI",
        cc_row_ids: Iterable[int],
        add: Optional[Iterable[str | bytes]] = None,
        remove: Optional[Iterable[str]] = None,
        label: Optional[str] = None,
        num: Optional[int] = None,
        notify: bool = False,
    ) -> None:
        """
        Apply additions and removals to several editable text/multiple values.

        Validate column editability/type, then clean both tag lists before checking
        empty owners or a no-op. Tag normalization accepts text and bytes. Exact
        additions win over identical removals. Unique TEMP tables drive link changes;
        SQL, dirtying and cache refresh complete before committing. Failure rolls back
        SQL/TEMP changes and restores touched cache rows. Owner IDs are materialized
        once so every step sees the same IDs.

        Example:
            With a host supplying a reusable ID list, set_custom_bulk_multiple([1, 2],
            add=["Travel"], num=column_id) adds the tag to both owners.


        :param cc_row_ids: Owner-ID iterable, including a single-pass iterator;
            materialized before database/cache/notification steps.
        :param add: Tags to add, or None for none.
        :param remove: Tags to remove, or None for none.
        :param label: Optional custom-column label; takes precedence over num.
        :param num: Numeric metadata key used when label is None.
        :param notify: Whether to notify after committing and refreshing the cache,
            default False.
        :return: None; no affected-owner result is returned.
        :raises NotImplementedError: Neither label nor num is supplied.
        :raises KeyError: The selected metadata record is absent.
        :raises ValueError: The column is noneditable or not text/multiple.
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
            raise NotImplementedError(
                "There is no information here to designate the custom column"
            )

        if not data["editable"]:
            raise ValueError("Column %r is not editable" % data["label"])
        if data["datatype"] != "text" or not data["is_multiple"]:
            raise ValueError("Column %r is not text/multiple" % data["label"])

        add = self.cleanup_tags(add)
        remove = self.cleanup_tags(remove)
        remove = list(set(remove) - set(add))
        # These IDs are reused for SQL, dirtying, cache refresh and notification.
        cc_row_ids = tuple(cc_row_ids)
        if not cc_row_ids or (not add and not remove):
            return
        # get custom table names
        in_table = data.get("in_table") or "books"
        custom_table, link_table = self.custom_table_names(
            data["num"], in_table=in_table
        )

        # Add tags that do not already exist into the custom_table
        all_tags = self.all_custom(num=data["num"])
        lt = [t.lower() for t in all_tags]
        new_tags = [t for t in add if t.lower() not in lt]
        if new_tags:
            require_legacy_macros(self.db.macros).insert_multiple_values_into_cc_table(
                custom_table, new_tags, conn=self.conn
            )

        # Create the temporary temp_tables to store the ids for books and tags
        # to be operated on
        suffix = uuid4().hex
        temp_tables = (
            f"temp_bulk_tag_edit_books_{suffix}",
            f"temp_bulk_tag_edit_add_{suffix}",
            f"temp_bulk_tag_edit_remove_{suffix}",
        )
        require_legacy_macros(self.db.macros).create_cc_temp_tables(
            temp_tables, conn=self.conn
        )

        # Populate the books temp custom_table
        require_legacy_macros(self.db.macros).insert_values_into_temp_table(
            temp_tables[0], cc_row_ids, conn=self.conn
        )

        # Populate the add/remove tags temp temp_tables
        require_legacy_macros(self.db.macros).do_cc_db_bulk_addition(
            temp_tables, custom_table, link_table, add, remove, conn=self.conn
        )

        # get rid of the temp tables
        require_legacy_macros(self.db.macros).destroy_cc_temp_tables(
            temp_tables, conn=self.conn
        )
        self.dirtied(cc_row_ids, commit=False)

        # set the in-memory copies of the tags
        for x in cc_row_ids:
            tags = require_legacy_macros(self.db.macros).read_cc_value_from_meta_2(
                data["num"], x, conn=self.conn
            )
            self.data.set(x, self.FIELD_MAP[data["num"]], tags, row_is_id=True)

        if notify:
            self.notify("metadata", cc_row_ids)

    @atomic_custom_write
    def set_custom_bulk(
        self: "CustomColumnsAPI",
        cc_row_ids: Sequence[int],
        val: Any,
        label: Optional[str] = None,
        num: Optional[int] = None,
        append: bool = False,
        notify: bool = True,
        extras: Sequence[Any] | Mapping[int, Any] | None = None,
    ) -> None:
        """
        Call the single-owner worker in sequence, then dirty and commit the batch.

        One transaction owns the batch. Failure restores earlier owner writes and
        cache rows. Dirty all requested and additionally affected owners, then commit
        before delivering the buffered notification.

        Example:
            On a compatible host, set_custom_bulk([1, 2], "Saga", num=series_column,
            extras=[1.0, 2.0]) supplies a separate series position for each owner.


        :param cc_row_ids: Sized sequence of owner IDs, retained for final dirtying.
        :param val: Value passed to every worker call.
        :param label: Optional custom-column label; takes precedence over num.
        :param num: Numeric metadata key used when label is None.
        :param append: Forward append behavior to each worker.
        :param notify: Notify affected owners once after the batch commits.
        :param extras: Optional sequence of per-owner extras, required to match the ID
            count.
        :return: None; additional affected owners are included in dirtying/notification.
        :raises ValueError: extras and cc_row_ids have different lengths.
        """
        if extras is not None and len(extras) != len(cc_row_ids):
            raise ValueError("Length of ids and extras is not the same")
        ev = None
        affected = set(cc_row_ids)
        for idx, id in enumerate(cc_row_ids):
            if extras is not None:
                ev = extras[idx]
            affected.update(
                self._set_custom(
                    id, val, label=label, num=num, append=append, notify=False, extra=ev
                )
            )
        self.dirtied(affected, commit=False)
        if notify:
            self.notify("metadata", sorted(affected))

    @atomic_custom_write
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
        Write one owner's custom value and dirty affected IDs in an owned transaction.

        Dirty the union of the requested ID and worker result with commit=False.
        SQL/cache failures roll back the write. Notify after commit; listener errors
        carry a note that the write already committed. A composite worker still
        dirties the requested ID. Borrowed transactions and commit=False are refused.

        Example:
            Given a compatible host, changed = set_custom(owner_id, "Draft",
            num=column_id) returns extra affected owners after committing the write.


        :param cc_row_id: Owner ID passed to the worker.
        :param val: Value adapted by the worker.
        :param label: Optional custom-column label; takes precedence over num.
        :param num: Numeric metadata key used when label is None.
        :param append: Append only where the selected storage supports multiple values.
        :param notify: Deliver metadata notification after the transaction commits.
        :param extra: Optional series position; None permits parsing/defaulting.
        :param commit: Must be True; False is rejected before mutation. Use SQL
            macros for caller-owned transactions without a cached facade.
        :param allow_case_change: Allow the worker’s legacy case-change path for reused
            normalized values.
        :return: Worker set of additional affected owner IDs; the requested ID is not
            automatically added to this return value.
        """
        if not commit:
            raise ValueError(
                "Cached custom-column writes do not support commit=False; use SQL macros for caller-owned transactions"
            )
        rv = self._set_custom(
            cc_row_id,
            val,
            label=label,
            num=num,
            append=append,
            notify=False,
            extra=extra,
            allow_case_change=allow_case_change,
        )
        if rv:
            metadata = (
                self.custom_column_label_map[label]
                if label is not None
                else self.custom_column_num_map[num]
                if num is not None
                else None
            )
            if metadata is not None:
                for owner in rv:
                    value = require_legacy_macros(
                        self.db.macros
                    ).read_cc_value_from_meta_2(metadata["num"], owner, conn=self.conn)
                    self.data.set(
                        owner, self.FIELD_MAP[metadata["num"]], value, row_is_id=True
                    )
        self.dirtied({cc_row_id} | rv, commit=False)
        if notify:
            self.notify("metadata", sorted({cc_row_id} | rv))
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

        Composite values return an empty set before editability checks. Other writes
        require editable metadata. Normalized replacement clears links and cached value
        before adding truthy requested values; an existing series link does not update
        its extra. Unnormalized storage deletes the old owner value and inserts a
        non-None replacement. Reload the value through legacy meta2 SQL and update the
        cache. Case changes pass the resolved ID and replacement value explicitly;
        missing lookup/insert IDs raise DatabaseIntegrityError before link creation.
        No complete-operation commit or rollback is supplied here; delegated macros can
        have independent effects.

        Example:
            >>> from types import SimpleNamespace
            >>> host = SimpleNamespace(
            ...     custom_column_num_map={1: {"datatype": "composite"}}
            ... )
            >>> CCSetMethodsMixin._set_custom(host, 7, "ignored", num=1)
            set()


        :param id_: Owner ID used by SQL and cache lookups.
        :param val: Value passed through the datatype adapter.
        :param label: Optional custom-column label; takes precedence over num.
        :param num: Numeric metadata key used when label is None.
        :param append: Preserve existing links only for multiple normalized storage when
            True.
        :param notify: Emit metadata notification for this owner after cache refresh.
        :param extra: Series position; when None, parse it from the adapted value or use
            1.0.
        :param allow_case_change: Permit case-change updates to an already stored
            normalized value.
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
            raise NotImplementedError(
                "There is no information here to designate the custom column"
            )

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

        books_to_refresh: set[int] = set()
        if data["normalized"] and data["datatype"] != "series":
            # Checks that, if a column is an enumeration type column, that some value is provided and that the values
            # is in the valid enumeration types
            if data["datatype"] == "enumeration" and (
                val and val not in data["display"]["enum_values"]
            ):
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
                require_legacy_macros(self.db.macros).break_cc_links_by_book_id(
                    lt, id_, conn=self.conn
                )
                require_legacy_macros(self.db.macros).clear_cc_unused_table_entries(
                    table=table, lt=lt, conn=self.conn
                )
                # Does the work of actually nullifying the value for the stored data
                self.data._data[id_][self.FIELD_MAP[data["num"]]] = None

            set_val = (
                val
                if data["is_multiple"]
                else [val]
                if not isinstance(val, list)
                else val
            )
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
                    xid = require_legacy_macros(self.db.macros).get_cc_id_from_value(
                        table, ex, all=False, conn=self.conn
                    )
                    if xid is None:
                        raise DatabaseIntegrityError(
                            f"Custom value {ex!r} disappeared from {table!r}"
                        )
                    if allow_case_change and ex != x:
                        case_change = True
                        require_legacy_macros(self.db.macros).update_cc_value(
                            table, cc_id=xid, cc_value=x, conn=self.conn
                        )
                else:
                    xid = require_legacy_macros(self.db.macros).add_cc_table_value(
                        table, x, conn=self.conn
                    )

                if xid is None:
                    raise DatabaseIntegrityError(
                        f"Custom value {x!r} has no ID in {table!r}"
                    )
                if not require_legacy_macros(self.db.macros).check_for_cc_link(
                    lt, id_, xid, self.conn
                ):
                    if data["datatype"] == "series":
                        require_legacy_macros(self.db.macros).add_cc_link_with_extra(
                            lt, id_, xid, extra, conn=self.conn
                        )
                        self.data.set(
                            id_, self.FIELD_MAP[data["num"]] + 1, extra, row_is_id=True
                        )
                    else:
                        require_legacy_macros(self.db.macros).add_cc_link_with_extra(
                            lt, id_, xid, conn=self.conn
                        )

                if case_change:
                    bks = require_legacy_macros(
                        self.db.macros
                    ).get_cc_lt_books_from_lt_value(lt, xid, conn=self.conn)
                    books_to_refresh |= set([bk[0] for bk in bks])

            nval = require_legacy_macros(self.db.macros).read_cc_value_from_meta_2(
                data["num"], id_, conn=self.conn
            )
            self.data.set(id_, self.FIELD_MAP[data["num"]], nval, row_is_id=True)

        elif data["normalized"] and data["datatype"] == "series":
            if not append or not data["is_multiple"]:
                require_legacy_macros(self.db.macros).break_cc_links_by_book_id(
                    lt, id_, conn=self.conn
                )
                require_legacy_macros(self.db.macros).clear_cc_unused_table_entries(
                    table=table, lt=lt, conn=self.conn
                )
                # Does the work of actually nullifying the value for the stored data
                self.data._data[id_][self.FIELD_MAP[data["num"]]] = None

            set_val = (
                val
                if data["is_multiple"]
                else [val]
                if not isinstance(val, list)
                else val
            )
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
                    xid = require_legacy_macros(self.db.macros).get_cc_id_from_value(
                        table, ex, all=False, conn=self.conn
                    )
                    if xid is None:
                        raise DatabaseIntegrityError(
                            f"Custom value {ex!r} disappeared from {table!r}"
                        )
                    if allow_case_change and ex != x:
                        case_change = True
                        require_legacy_macros(self.db.macros).update_cc_value(
                            table, cc_id=xid, cc_value=x, conn=self.conn
                        )
                else:
                    xid = require_legacy_macros(self.db.macros).add_cc_table_value(
                        table, x, conn=self.conn
                    )

                if xid is None:
                    raise DatabaseIntegrityError(
                        f"Custom value {x!r} has no ID in {table!r}"
                    )
                if not require_legacy_macros(self.db.macros).check_for_cc_link(
                    lt, id_, xid, self.conn
                ):
                    if data["datatype"] == "series":
                        require_legacy_macros(self.db.macros).add_cc_link_with_extra(
                            lt, id_, xid, extra, conn=self.conn
                        )
                        self.data.set(
                            id_, self.FIELD_MAP[data["num"]] + 1, extra, row_is_id=True
                        )
                    else:
                        require_legacy_macros(self.db.macros).add_cc_link_with_extra(
                            lt, id_, xid, conn=self.conn
                        )

                if case_change:
                    bks = require_legacy_macros(
                        self.db.macros
                    ).get_cc_lt_books_from_lt_value(lt, xid, conn=self.conn)
                    books_to_refresh |= set([bk[0] for bk in bks])

            nval = require_legacy_macros(self.db.macros).read_cc_value_from_meta_2(
                data["num"], id_, conn=self.conn
            )
            self.data.set(id_, self.FIELD_MAP[data["num"]], nval, row_is_id=True)

        else:
            require_legacy_macros(self.db.macros).clear_cc_entries_from_table(
                table, id_, conn=self.conn
            )
            if val is not None:
                require_legacy_macros(self.db.macros).add_cc_link_with_extra(
                    lt=table, book_id=id_, value_id=val, conn=self.conn
                )

            nval = require_legacy_macros(self.db.macros).read_cc_value_from_meta_2(
                data["num"], id_, conn=self.conn
            )
            self.data.set(id_, self.FIELD_MAP[data["num"]], nval, row_is_id=True)

        if notify:
            self.notify("metadata", [id_])

        return books_to_refresh
