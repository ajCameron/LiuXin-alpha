


from LiuXin_alpha.errors import InputIntegrityError, DatabaseIntegrityError

from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

from LiuXin_alpha.databases.row import Row


class DatabaseIntralinkRowsMixin:
    """
    Intralink rows on the database.
    """

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO WRITE TO INTRALINK TABLES START HERE

    # Todo: Need to extend to account for the other interlink data types
    def intralink_rows(self, primary_row, secondary_row, link_type):
        """
        Intralink two rows - with an allowed link_type.
        :param primary_row: This will be entered as the primary row
        :param secondary_row: This will be entered as the secondary row
        :param link_type:
        :return:
        """
        link_type = six_unicode(link_type).lower().strip()
        if not primary_row.table == secondary_row.table:
            err_str = "Cannot intralink rows from different table types"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("primary_row", primary_row),
                ("secondary_row", secondary_row),
                ("link_type", link_type),
            )
            raise InputIntegrityError(err_str)
        table = primary_row.table

        if primary_row.row_id is None or secondary_row.row_id is None:
            err_str = "Both rows must have ids set before they can be linked."
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("primary_row", primary_row),
                ("secondary_row", secondary_row),
                ("link_type", link_type),
            )
            raise InputIntegrityError(err_str)

        # Checks that the intralink type is one of those allowed for this table in preferences
        # Todo: Move this to init for performance
        allowed_types_name = "allowed_{0}_intralink_types".format(primary_row.table)
        try:
            allowed_link_types = self.preferences[allowed_types_name]
        except KeyError:
            info_str = "Allowed type name not found in preferences - no restrictions applied to intralink type"
            default_log.log_variables(info_str, "INFO", ("allowed_type_name", allowed_types_name))
        else:
            allowed_link_types = frozenset([six_unicode(lt).lower().strip() for lt in allowed_link_types])
            if link_type not in allowed_link_types:
                err_str = "Unable to intralink rows - link type not recognized"
                err_str = default_log.log_variables(
                    err_str,
                    "ERROR",
                    ("primary_row", primary_row),
                    ("secondary_row", secondary_row),
                    ("link_type", link_type),
                    ("allowed_link_types", allowed_link_types),
                )
                raise InputIntegrityError(err_str)

        intralink_row = dict()
        primary_col = self.driver_wrapper.get_intralink_column(table, "primary_id")
        secondary_col = self.driver_wrapper.get_intralink_column(table, "secondary_id")
        type_col = self.driver_wrapper.get_intralink_column(table, "type")
        intralink_row[primary_col] = primary_row.row_id
        intralink_row[secondary_col] = secondary_row.row_id
        intralink_row[type_col] = link_type

        intralink_row = Row(row_dict=intralink_row, database=self)
        intralink_row.ensure_row_has_id()
        intralink_row.sync()

        return intralink_row

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO READ INTRALINKED ROWS START HERE

    def get_intralink_row(self, primary_row, secondary_row):
        """
        Get the intralink row connecting the primary and secondary row - if any.
        :param primary_row:
        :param secondary_row:
        :return:
        """
        primary_table = primary_row.table
        secondary_table = secondary_row.table

        link_table_name = self.driver_wrapper.get_link_table_name(primary_table, secondary_table)
        if not link_table_name or (primary_table != secondary_table):
            err_str = "Given tables cannot be connected - or you have used an interlink method, not the intralink one"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("primary_row", primary_row),
                ("secondary_row", secondary_row),
                ("link_table_name", link_table_name),
            )
            raise InputIntegrityError(err_str)

        primary_id_col = self.driver_wrapper.get_link_column(primary_table, primary_table, "primary_id")
        secondary_id_col = self.driver_wrapper.get_link_column(primary_table, primary_table, "secondary_id")

        candidate_rows = []
        # Search the table using the primary_id - refine using the secondary to return the actually desired result
        primary_id = six_unicode(primary_row.row_id)
        secondary_id = six_unicode(secondary_row.row_id)
        for row in self.search(table=link_table_name, column=primary_id_col, search_term=primary_id):
            if secondary_id == six_unicode(row[secondary_id_col]):
                candidate_rows.append(row)

        if len(candidate_rows) == 0:
            return None
        elif len(candidate_rows) == 1:
            return candidate_rows[0]
        else:
            err_str = "Rows are joined by more than one intralink row - which shouldn't happen."
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("candidate_rows", candidate_rows),
                ("primary_row", primary_row),
                ("secondary_row", secondary_row),
            )
            raise DatabaseIntegrityError(err_str)

    def get_intralink_rows(self, row, primary=True, secondary=True, link_type_filter=None):
        """
        Returns all intralink rows involving the given row.
        :param row:
        :param primary: If True return link rows where this row is the primary
        :type primary: bool
        :param secondary: If True return lik rows where this row is the secondary
        :type secondary: bool
        :param link_type_filter: Filter to remove any links but the ones with this type
        :return:
        """
        table = row.table
        row_id = six_unicode(row.row_id)

        intralink_table = self.driver_wrapper.get_link_table_name(table, table)
        intralink_table_primary_row = self.driver_wrapper.get_link_column(table, table, "primary_id")
        intralink_table_secondary_row = self.driver_wrapper.get_link_column(table, table, "secondary_id")

        row_pool = []
        # Search the intralink table for mentions of the id in the primary column
        if primary:
            primary_intralink_rows = self.search(
                table=intralink_table,
                column=intralink_table_primary_row,
                search_term=row_id,
            )
            row_pool.extend([r for r in primary_intralink_rows])

        # Search the intralink table for mentions of the id in the secondary column
        if secondary:
            secondary_intralink_rows = self.search(
                table=intralink_table,
                column=intralink_table_secondary_row,
                search_term=row_id,
            )
            row_pool.extend([r for r in secondary_intralink_rows])

        if link_type_filter is None:
            return row_pool
        else:
            intralink_table_link_type = self.driver_wrapper.get_link_column(table, table, "type")
            filtered_row_pool = [
                r for r in row_pool if six_unicode(r[intralink_table_link_type]) == six_unicode(link_type_filter)
            ]
            return filtered_row_pool

    def get_intralinked_rows(self, primary_row, secondary_row):
        """
        Get any rows intralinked to the given primary row.
        The row must be primary in the link - if it's secondary that means something different.
        If the primary_row is not None, and the secondary row is None, returns every title linked to that row with that
        row as the primary_id (so returns purely secondary rows).
        If the secondary_row is not None, and the primary row is None, returns all the title linked to that row with
        that row as the secondary_id (so returns purely secondary rows).
        If both the primary and the secondary rows are not None - errors. You probably want the intralink_row. There's
        a specific method for that and everything.
        :param primary_row:
        :param secondary_row:
        :return:
        """
        if primary_row is not None and secondary_row is not None:
            err_str = "You seem to have both the title rows that you could want - do you want the intralink row itself?"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("primary_row", primary_row),
                ("secondary_row", secondary_row),
            )
            raise InputIntegrityError(err_str)
        if primary_row is None and secondary_row is None:
            err_str = "Both primary and secondary rows supplied to get_intralinked_rows where null"
            default_log.error(err_str)
            raise InputIntegrityError(err_str)

        # Get every row with a the primary_row_id as it's primary - return that
        if primary_row is not None:
            table = primary_row.table
            primary_row_id = six_unicode(primary_row.row_id)

            intralink_table = self.driver_wrapper.get_link_table_name(table, table)
            intralink_table_primary_row = self.driver_wrapper.get_link_column(table, table, "primary_id")
            intralink_table_secondary_row = self.driver_wrapper.get_link_column(table, table, "secondary_id")

            intralink_rows = self.search(
                table=intralink_table,
                column=intralink_table_primary_row,
                search_term=primary_row_id,
            )

            intralinked_rows = []
            for link_row in intralink_rows:
                secondary_id = link_row[intralink_table_secondary_row]
                intralinked_rows.append(self.get_row_from_id(table=table, row_id=secondary_id))
            return intralink_rows

        # Get every row with a the secondary_row_id as it's primary - return that
        elif secondary_row is not None:
            table = secondary_row.table
            secondary_row_id = six_unicode(secondary_row.row_id)

            intralink_table = self.driver_wrapper.get_link_table_name(table, table)
            intralink_table_primary_row = self.driver_wrapper.get_link_column(table, table, "primary_id")
            intralink_table_secondary_row = self.driver_wrapper.get_link_column(table, table, "secondary_id")

            intralink_rows = self.search(
                table=intralink_table,
                column=intralink_table_secondary_row,
                search_term=secondary_row_id,
            )

            intralinked_rows = []
            for link_row in intralink_rows:
                primary_id = link_row[intralink_table_primary_row]
                intralinked_rows.append(self.get_row_from_id(table=table, row_id=primary_id))
            return intralink_rows

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO DELETE INTRALINK ROWS START HERE

    # Todo: Consider renaming - unlink_intralink
    def unlinked_intralink(self, primary_row, secondary_row):
        """
        Unlink two rows that have been interlinked.
        If primary_row and secondary_row are both not None, removes any interlink between the primary and the
        secondary row.
        If the primary_row is not None - deletes any intralink rows with that row as the primary.
        If the secondary_row is not None - deletes any intralink rows with that row as secondary.
        If both are None - errors.
        :param primary_row:
        :param secondary_row:
        :return:
        """
        if primary_row is not None and secondary_row is not None:

            link_row = self.get_intralink_row(primary_row=primary_row, secondary_row=secondary_row)
            # Deal with the case where there is no link to remove
            if link_row is None:
                return
            self.delete(link_row)

        elif primary_row is not None and secondary_row is None:

            table = primary_row.table
            primary_id = primary_row.row_id

            # Search the intralink table for any rows with the given primary_id - delete them
            intralink_table = self.driver_wrapper.get_link_table_name(table1=table, table2=table)
            intralink_table_primary = self.driver_wrapper.get_link_column(
                table1=table, table2=table, column_type="primary_id"
            )
            link_rows = self.search(
                table=intralink_table,
                column=intralink_table_primary,
                search_term=primary_id,
            )

            [self.delete(l_r) for l_r in link_rows]

        elif primary_row is None and secondary_row is not None:

            table = primary_row.table
            secondary_id = secondary_row.row_id

            # Search the intralink table for any rows with the given primary_id - delete them
            intralink_table = self.driver_wrapper.get_link_table_name(table1=table, table2=table)
            intralink_table_primary = self.driver_wrapper.get_link_column(
                table1=table, table2=table, column_type="secondary_id"
            )
            link_rows = self.search(
                table=intralink_table,
                column=intralink_table_primary,
                search_term=secondary_id,
            )

            [self.delete(l_r) for l_r in link_rows]

        elif primary_row is None and secondary_row is None:

            err_str = "unlink_intralink called without content"
            default_log.error(err_str)
            raise InputIntegrityError(err_str)

    #
    # ----------------------------------------------------------------------------------------------------------------------
