
"""
Read legacy custom values from results-cache rows and custom SQL tables.

Hosts must supply metadata maps, FIELD_MAP and compatible data/id accessors for cached reads, plus naming and macro helpers for SQL-backed reads. These helpers do not refresh the cache before accessing it.
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
    Expose cached custom values, series extras and stored value inventories.

    Select metadata by label before num. A configured results cache is required for positional/ID reads; the plain empty data dictionary used by standalone CustomColumns construction does not provide the complete interface. Multiple text reads retain a Python 2 cmp-sort call when sort_alpha is enabled.

    Example:
        A host with a populated results cache can call get_custom(book_id, num=column_id, index_is_id=True) to read that owner’s current cached value.
    """

    # Begin Convenience methods for getting and setting custom data - {{{
    def get_custom(
            self: "CustomColumnsAPI",
            idx: int,
            label: Optional[str] = None,
            num: Optional[int] = None,
            index_is_id: bool = False) -> Any:
        """
        Read one custom value from a positional or ID-indexed cache row.

        Locate the slot through FIELD_MAP[data["num"]]. If display.sort_alpha is true for multiple text, list.sort(cmp=...) raises TypeError on Python 3. Other datatypes return the stored object unchanged.

        Example:
            >>> from types import SimpleNamespace
            >>> host = SimpleNamespace(custom_column_num_map={1: {"num": 1, "is_multiple": False, "datatype": "int"}}, data=[[7]], FIELD_MAP={1: 0})
            >>> CCGetMethodsMixin.get_custom(host, 0, num=1)
            7


        :param idx: Cache position, or owner ID when index_is_id=True.
        :param label: Optional label selecting custom_column_label_map; takes precedence over num.
        :param num: Numeric custom-column ID used when label is None.
        :param index_is_id: Use self.data._data[idx] instead of self.data[idx] when True.
        :return: Cached value, with text/multiple strings split into a new list; empty such values become [].
        :raises NotImplementedError: Neither label nor num is supplied.
        :raises KeyError: The chosen metadata key is absent.
        :raises TypeError: Alphabetical multiple-text sorting passes the unsupported cmp keyword.
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
        Read the link-table extra value for a series column.

        Use stored in_table or books to derive the link-table name, then direct_get_custom_extra on the host’s connection. No cached custom value is read here.

        Example:
            >>> from types import SimpleNamespace
            >>> host = SimpleNamespace(custom_column_num_map={1: {"datatype": "text"}})
            >>> CCGetMethodsMixin.get_custom_extra(host, 7, num=1) is None
            True


        :param idx: Cache position or owner ID.
        :param label: Optional label selecting custom_column_label_map; takes precedence over num.
        :param num: Numeric custom-column ID used when label is None.
        :param index_is_id: Treat idx as an owner ID when True; otherwise resolve it through self.id.
        :return: Delegated series extra, usually a scalar or None; None immediately for non-series columns.
        :raises NotImplementedError: Neither label nor num is supplied.
        :raises KeyError: The chosen metadata key is absent.
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
        Return a cached custom value together with its optional series extra.

        Resolve owner ID first and read data._data. Apply the same multiple-text splitting and unsupported cmp sorting as get_custom. For series, query the link extra separately, so cached value and SQL extra need not form a consistent snapshot.

        Example:
            Given a populated host cache, get_custom_and_extra(owner_id, num=column_id, index_is_id=True) returns (cached_value, None) for a non-series column.


        :param idx: Cache position or owner ID.
        :param label: Optional label selecting custom_column_label_map; takes precedence over num.
        :param num: Numeric custom-column ID used when label is None.
        :param index_is_id: Skip self.id conversion when True.
        :return: Pair (value, extra), using None for the extra on non-series columns.
        :raises NotImplementedError: Neither label nor num is supplied.
        :raises KeyError: The chosen metadata key is absent.
        :raises TypeError: Alphabetical multiple-text sorting uses the unsupported cmp keyword.
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
        Read ID/value pairs for a normalized custom-column value table.

        Derive table names before checking normalized. The host macro controls the returned collection type and query ordering; this method does not sort or coerce it to a list.

        Example:
            Given a normalized tag-like column, get_custom_items_with_ids(num=column_id) retrieves stored value IDs alongside their display values.


        :param label: Optional label selecting custom_column_label_map; takes precedence over num.
        :param num: Numeric custom-column ID used when label is None.
        :return: Delegated pair collection for normalized storage; [] for an unnormalized column.
        :raises NotImplementedError: Neither label nor num is supplied.
        :raises KeyError: The chosen metadata key is absent.
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
        Suggest an index for a named custom series using legacy preferences.

        A missing series uses a numeric preference only if parse returns an actual int/float; otherwise return 1.0. For an existing series, fetch ordered index rows and delegate to _get_next_series_num_for_list, whose default expects indexable rows. The SQL macro gathers indices for books referencing the series and can include their other series links; this is not a strict maximum over only the selected value’s links.

        Example:
            For a configured series column whose named value does not yet exist, get_next_cc_series_num_for("New sequence", num=column_id) normally suggests 1.0.


        :param series: Series display value looked up through the value-table macro.
        :param label: Optional label selecting custom_column_label_map; takes precedence over num.
        :param num: Numeric custom-column ID used when label is None.
        :return: None for non-series columns; otherwise the configured/computed index, often 1.0 for a missing series.
        :raises NotImplementedError: Neither label nor num is supplied.
        :raises KeyError: The chosen metadata key is absent.
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
        Collect distinct stored custom values into a Python set.

        Use DISTINCT SQL only for unnormalized columns; normalized storage is already expected to be unique, though the final set deduplicates either result. No deterministic ordering or unused-value filtering is provided. The macro must return row-shaped entries, not a flat sequence of scalar values.

        Example:
            On a configured facade, all_custom(num=column_id) returns the value-table inventory without reading individual cache rows.


        :param label: Optional label selecting custom_column_label_map; takes precedence over num.
        :param num: Numeric custom-column ID used when label is None.
        :return: Set of hashable values extracted as element zero of each macro result row.
        :raises NotImplementedError: Neither label nor num is supplied.
        :raises KeyError: The chosen metadata key is absent.
        :raises TypeError: Returned values are unhashable or macro entries cannot be indexed.
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
