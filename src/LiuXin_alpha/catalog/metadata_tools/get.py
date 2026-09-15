
"""
Read linked legacy comments, synopses and Series with mode-dependent results.
"""


class BackendGetter:
    """
    Retain a database handle for legacy row-oriented relationship reads.

    Result order comes from the database. First-result operations catch KeyError
    but not IndexError, so an empty list can raise instead of returning None.
    No encompassing read transaction is opened.

    Example:
        Request ``getter.comment(resource_row, all=True, rows=False)`` for text values.
    """

    def __init__(self, db):
        """
        Retain the caller's database without opening or validating it.

        Example:
            >>> database = object()
            >>> BackendGetter(database).db is database
            True


        :param db: Borrowed database handle exposing relationship getters.
        :return: None; stores the reference.
        """
        self.db = db

    def comment(self, resource_row, all=False, rows=True):
        """
        Read linked Comments in the legacy all/rows mode.

        With all=False and rows=False, return None after fetching links. A first
        row KeyError is suppressed; an empty list's IndexError propagates. All-text
        projection errors and database failures propagate.

        Example:
            ``getter.comment(resource_row, all=True, rows=False)`` returns stored comment
            text values, including an empty list when there are no links.


        :param resource_row: Resource Row used as the primary endpoint.
        :param all: True selects all results; false selects the first row when rows=True.
        :param rows: True returns Rows; false returns text only in the all-results mode.
        :return: All linked Rows, a list of comment values, one Row, or None by mode.
        """
        cn_rows = self.db.get_interlinked_rows(primary_row=resource_row, secondary_table="comments")

        if all:
            if rows:
                return cn_rows
            else:
                return [sn["comment"] for sn in cn_rows]
        else:
            try:
                if rows:
                    return cn_rows[0]
                else:
                    return None
            except KeyError:
                return None

    def series(self, resource_row, all=False, rows=True):
        """
        Read linked Series together with their link rows or index values.

        The single values mode deliberately retains the Series Row as the second
        member. Empty-list IndexError is not caught. The single-row mode still
        looks up the index column even when returning Rows, so column discovery
        can fail independently. Each linked Series requires a separate link read.

        Example:
            Unpack ``link_row, series_row = getter.series(resource_row)``; the link
            row precedes the Series row.


        :param resource_row: Resource Row whose Series links are requested.
        :param all: True returns a list; false selects the database's first linked Series.
        :param rows: True returns (link Row, Series Row); false projects index values.
        :return: All rows: list of (link Row, Series Row); all values: list of (index, Series text).
            Single rows: (link Row, Series Row); single values: (index, Series Row);
            None only for a caught KeyError in first-row/link retrieval.
        """
        rs_rows = self.db.get_interlinked_rows(primary_row=resource_row, secondary_table="series")

        rs_row_list = []
        if all:
            if rows:
                for rs_row in rs_rows:
                    rs_link_row = self.db.get_interlink_row(primary_row=resource_row, secondary_row=rs_row)
                    rs_row_list.append((rs_link_row, rs_row))
                return rs_row_list
            else:
                rs_index_col = self.db.driver_wrapper.get_link_column(
                    table1=resource_row.table, table2="series", column_type="index"
                )
                for rs_row in rs_rows:
                    rs_link_row = self.db.get_interlink_row(primary_row=resource_row, secondary_row=rs_row)
                    rs_row_list.append((rs_link_row[rs_index_col], rs_row["series"]))
                return rs_row_list
        else:
            try:
                rs_row = rs_rows[0]
                rs_link_row = self.db.get_interlink_row(primary_row=resource_row, secondary_row=rs_row)
            except KeyError:
                return None

            rs_index_col = self.db.driver_wrapper.get_link_column(
                table1=resource_row.table, table2="series", column_type="index"
            )
            if rows:
                return rs_link_row, rs_row
            else:
                return rs_link_row[rs_index_col], rs_row

    def synopsis(self, resource_row, all=False, rows=True):
        """
        Read linked Synopses in the legacy all/rows mode.

        With all=False and rows=False, return None after fetching links. A first
        row KeyError is suppressed; an empty list's IndexError propagates. All-text
        projection errors and database failures propagate.

        Example:
            ``getter.synopsis(resource_row, all=True, rows=False)`` returns stored synopsis
            text values, including an empty list when there are no links.


        :param resource_row: Resource Row used as the primary endpoint.
        :param all: True selects all results; false selects the first row when rows=True.
        :param rows: True returns Rows; false returns text only in the all-results mode.
        :return: All linked Rows, a list of synopsis values, one Row, or None by mode.
        """
        sn_rows = self.db.get_interlinked_rows(primary_row=resource_row, secondary_table="synopses")

        if all:
            if rows:
                return sn_rows
            else:
                return [sn["synopsis"] for sn in sn_rows]
        else:
            try:
                if rows:
                    return sn_rows[0]
                else:
                    return None
            except KeyError:
                return None
