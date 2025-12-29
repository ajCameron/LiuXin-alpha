class BackendGetter(object):
    """
    Provides convenience methods to get objects and information from the database.
    e.g. Can take a title row and return the synopsis for that title.
    Centralizes retrieval methods here so that if the storage methadology changes it's easier to re-write to
    accommodate that.
    """

    def __init__(self, db):
        """
        Exists as convenience methods to retrieve data from the database - so should only need a connection to the
        database.
        :param db:
        :return:
        """
        self.db = db

    def comment(self, resource_row, all=False, rows=True):
        """
        Returns tje comments for a title.
        :param resource_row:
        :type resource_row: A LiuXin row object
        :param all: If True then returns all the rows - if False then just returns the first one
        :type all: bool
        :param rows:
        :return:
        """
        cn_rows = self.db.get_interlinked_rows(target_row=resource_row, secondary_table="comments")

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
        Return the series for a title.
        :param resource_row:
        :param all:
        :param rows: If True returns the rows as a list of tuples - the first element being the link row between the
                     resource and the series, and the second element being the series row.
        :return:
        """
        rs_rows = self.db.get_interlinked_rows(target_row=resource_row, secondary_table="series")

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
        Returns the synopsis for a title.
        :param resource_row:
        :type resource_row: A LiuXin row object
        :param all: If True then returns all the rows - if False then just returns the first one
        :type all: bool
        :return:
        """
        sn_rows = self.db.get_interlinked_rows(target_row=resource_row, secondary_table="synopses")

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
