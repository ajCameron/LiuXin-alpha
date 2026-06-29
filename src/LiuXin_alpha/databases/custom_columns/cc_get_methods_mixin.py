
"""
Method to get values from a custom columns table.
"""

from __future__ import annotations

from functools import partial
from typing import Optional, TYPE_CHECKING, Any

from LiuXin_alpha.preferences import preferences
from LiuXin_alpha.utils.libraries.liuxin_six import six_cmp as cmp

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.custom_columns_api import CustomColumnsAPI


class CCGetMethodsMixin:
    """
    Set values in a custom column.
    """

    # Begin Convenience methods for getting and setting custom data - {{{
    def get_custom(
            self: "CustomColumnsAPI",
            idx: int,
            label: Optional[str] = None,
            num: Optional[int] = None,
            index_is_id: bool = False) -> Any:
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
                ans.sort(cmp = lambda x, y: cmp(x.lower(), y.lower()))

        return ans

    def get_custom_extra(
            self: "CustomColumnsAPI",
            idx: int,
            label: Optional[str] = None,
            num: Optional[int] = None,
            index_is_id: bool = False) -> Any:
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

        in_table = data.get("in_table") or "books"
        ign, lt = self.custom_table_names(data["num"], in_table=in_table)
        idx = idx if index_is_id else self.id(idx)

        return self.direct_get_custom_extra(lt, idx)

    def get_custom_and_extra(
            self: "CustomColumnsAPI",
            idx: int,
            label: Optional[str] = None,
            num: Optional[int] = None,
            index_is_id: bool = False) -> tuple[Any, Any]:
        """
        Returns the value from the custom column and the extra component from the link table.

        If the datatype of the custom column is not series nothing is returned.
        See :meth get_custom: and :meth get_custom_extra:
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

        in_table = data.get("in_table") or "books"
        ign, lt = self.custom_table_names(data["num"], in_table=in_table)
        extra = self.direct_get_custom_extra(lt, idx)
        return ans, extra

    def get_custom_items_with_ids(
            self: "CustomColumnsAPI",
            label: Optional[str] = None,
            num: Optional[int] = None) -> list[tuple[int, Any]]:
        """
        Convenience methods for tag editing.

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

        in_table = data.get("in_table") or "books"
        table, lt = self.custom_table_names(data["num"], in_table=in_table)
        if not data["normalized"]:
            return []
        return self.direct_get_custom_id_val_pairs(table)

    # Todo: What if the book is already in this series, but in another position in the priority stack
    def get_next_cc_series_num_for(
            self: "CustomColumnsAPI",
            series: str,
            label: Optional[str] = None,
            num: Optional[int] = None) -> Optional[float]:
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
        in_table = data.get("in_table") or "books"
        table, lt = self.custom_table_names(data["num"], in_table=in_table)
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

    def all_custom(
            self: "CustomColumnsAPI",
            label: Optional[str] = None,
            num: Optional[int] = None) -> set[Any]:
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

        in_table = data.get("in_table") or "books"
        table, lt = self.custom_table_names(data["num"], in_table=in_table)
        # If the data is already normalized it should already be distinct
        if data["normalized"]:
            ans = self.db.macros.get_all_cc_custom_values(cc_table=table, distinct=False, conn=self.conn)
        else:
            ans = self.db.macros.get_all_cc_custom_values(cc_table=table, distinct=True, conn=self.conn)
        ans = set([x[0] for x in ans])
        return ans
