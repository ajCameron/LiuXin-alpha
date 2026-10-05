
"""
Delete legacy custom-column values and collect their affected owner IDs.

These helpers require a host with custom metadata, naming/macros and, for cache-aware deletion, dirtying and cache-update hooks. Their SQL, cache and notification steps do not share a transaction established by this mixin.
"""

from __future__ import annotations

from typing import Any, TYPE_CHECKING, Optional

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.custom_columns_api import CustomColumnsAPI


class CCDeleteMethodsMixin:
    """
    Offer value deletion for custom-column facades with legacy host hooks.

    The methods use label-first metadata selection and stored in_table for naming. Cache refresh and transaction behavior differ between the two deletion paths.

    Example:
        A configured facade can use delete_item_from_multiple("old tag", num=column_id) for a text/multiple column, subject to the legacy macro naming conventions.
    """

    def delete_custom_item_using_id(
            self: "CustomColumnsAPI",
            idx: Optional[int],
            label: Optional[str] = None,
            num: Optional[int] = None) -> None:
        """
        Dirty referencing owners, delete a custom value and request cache replacement.

        Resolve table names, call custom_dirty_books_referencing with commit=False, then delete_cc_item. Finally call rename_custom_item_in_data(target_ids=..., column_num=..., new_value=None). Current CustomColumns names that first parameter book_ids, so this final call raises TypeError after deletion has already occurred. The SQL delete macro may commit independently; no rollback or compensation is provided here.

        Example:
            Given a host with a compatible target_ids cache hook, delete_custom_item_using_id(value_id, num=column_id) removes the value and replaces cached references; the current CustomColumns hook needs a signature fix before that full workflow succeeds.


        :param idx: Truthy value-row ID; None, zero and other false values return immediately.
        :param label: Optional label selecting custom_column_label_map; takes precedence over num.
        :param num: Numeric custom-column ID used when label is None.
        :return: None; false IDs perform no work.
        :raises NotImplementedError: Neither label nor num is supplied.
        :raises KeyError: The chosen metadata key is absent.
        :raises TypeError: The cache replacement hook rejects target_ids, as current CustomColumns does.
        """
        # Todo: Whhyyyyyyyy?
        if idx:

            if label is not None:
                data = self.custom_column_label_map[label]

            elif num is not None:
                data = self.custom_column_num_map[num]
            else:
                raise NotImplementedError("There is no information here to designate the custom column")

            # Link table naming depends on the table the custom column is attached to.
            in_table = data.get("in_table") or "books"
            table, lt = self.custom_table_names(data["num"], in_table=in_table)

            # Note the change with books_referencing - which allows the books to be updated with the new information
            book_ids = self.custom_dirty_books_referencing("#" + data["label"], idx, commit=False)

            # Delete from the link table and the actual table
            self.db.macros.delete_cc_item(table, lt, idx)

            self.rename_custom_item_in_data(target_ids=book_ids, column_num=data["num"], new_value=None)

    # Todo: We seem to be assuming items are strings a lot - this feels like custom tags
    def delete_item_from_multiple(
            self: "CustomColumnsAPI",
            item: str,
            label: Optional[str] = None,
            num: Optional[int] = None) -> list[int]:
        """
        Remove the first case-insensitive matching value from a text/multiple column.

        Require datatype text and is_multiple. Search the facade’s unordered all_custom result and choose its first lowercase match. Query referencing owners, delete link/value rows and commit self.conn when a truthy ID is found. This path does not dirty owners or update the results cache. Legacy macros assume compatible book/value column names, even for alternate attachment tables.

        Example:
            For a configured text/multiple facade, affected = delete_item_from_multiple("travel", num=column_id) returns owners found before the matching value is removed.


        :param item: Tag text compared with lower rather than casefold.
        :param label: Optional label selecting custom_column_label_map; takes precedence over num.
        :param num: Numeric custom-column ID used when label is None.
        :return: List of owner IDs extracted from macro result rows, or an empty list when no usable value ID is found.
        :raises NotImplementedError: Neither label nor num is supplied.
        :raises KeyError: The chosen metadata key is absent.
        :raises ValueError: The selected column is not text with multiple values.
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
            in_table = data.get("in_table") or "books"
            table, lt = self.custom_table_names(data["num"], in_table=in_table)
            id_ = self.db.macros.get_cc_id_from_value(table, existing_tags[idx], all=False, conn=self.conn)
            if id_:
                books = self.db.macros.get_cc_lt_books_from_lt_value(lt, value=id_, conn=self.conn)
                if books:
                    books_affected = [b[0] for b in books]
                self.db.macros.delete_from_cc_table_by_value(lt, id_)
                self.db.macros.delete_from_cc_table_by_id(table, id_)
                self.conn.commit()

        return books_affected
