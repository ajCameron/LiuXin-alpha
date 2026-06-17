
"""

"""

from copy import deepcopy
from numbers import Number

from LiuXin_alpha.databases.row import Row

from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError

from LiuXin_alpha.utils.logging import default_log

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode


class DatabaseInterlinkRowsMixin:
    """
    Mixin method to deal with interlinked rows.
    """

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO READ INTERLINK TABLES START HERE

    def get_interlink_row(self, primary_row, secondary_row, onelink=True):
        """
        Get the row connecting the primary_row and the secondary row. Errors if there is more than one. Returns None if
        there is less than one.
        If the tables can't be linked, errors.
        :param primary_row:
        :param secondary_row:
        :param onelink: If True assumes that there should be either one or zero links between the given two rows.
                        If False then there can be any number of links. Returns all of them as a list.
        :return:
        """
        primary_table = primary_row.table
        secondary_table = secondary_row.table

        link_table_name = self.driver_wrapper.get_link_table_name(primary_table, secondary_table)
        if not link_table_name or (primary_table == secondary_table):
            err_str = "Given tables cannot be connected - or you have used an interlink method, not the intralink one"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("primary_row", primary_row),
                ("secondary_row", secondary_row),
                ("link_table_name", link_table_name),
            )
            raise InputIntegrityError(err_str)

        # Search the interlink table for a row which matches the required criteria
        interlink_table = self.driver_wrapper.get_link_table_name(primary_table, secondary_table)
        primary_link_col = self.driver_wrapper.get_link_column(
            primary_table,
            secondary_table,
            self.driver_wrapper.get_id_column(primary_table),
        )
        secondary_link_col = self.driver_wrapper.get_link_column(
            primary_table,
            secondary_table,
            self.driver_wrapper.get_id_column(secondary_table),
        )

        # Search for links which reference the primary_row
        candidate_rows = []
        link_rows = self.search(
            table=interlink_table,
            column=primary_link_col,
            search_term=primary_row.row_id,
        )
        secondary_id = six_unicode(secondary_row.row_id)
        for row in link_rows:
            if secondary_id == six_unicode(row[secondary_link_col]):
                candidate_rows.append(row)

        if len(candidate_rows) == 0:
            return None
        elif len(candidate_rows) == 1:
            if onelink:
                return candidate_rows[0]
            else:
                return candidate_rows
        else:
            if onelink:
                err_str = "Only one link is permitted between each row pair"
                err_str = default_log.log_variables(
                    err_str,
                    "ERROR",
                    ("primary_row", primary_row),
                    ("secondary_row", secondary_row),
                    ("link_table_name", link_table_name),
                    ("candidate_rows", candidate_rows),
                )
                raise DatabaseIntegrityError(err_str)
            else:
                return candidate_rows

    def get_interlink_rows(self, primary_row, secondary_table):
        """
        Get all the interlink rows connecting the primary row and any row in the secondary table.
        :param primary_row:
        :param secondary_table:
        :return:
        """
        primary_table = primary_row.table

        link_table_name = self.driver_wrapper.get_link_table_name(primary_table, secondary_table)
        if not link_table_name or (primary_table == secondary_table):
            err_str = "Given tables cannot be connected - or you have used an interlink method, not the intralink one"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("primary_row", primary_row),
                ("secondary_table", secondary_table),
                ("link_table_name", link_table_name),
            )
            raise InputIntegrityError(err_str)

        # Search the interlink table for a row which matches the required criteria
        interlink_table = self.driver_wrapper.get_link_table_name(primary_table, secondary_table)
        primary_link_col = self.driver_wrapper.get_link_column(
            primary_table,
            secondary_table,
            self.driver_wrapper.get_id_column(primary_table),
        )

        link_rows = self.search(
            table=interlink_table,
            column=primary_link_col,
            search_term=primary_row.row_id,
        )
        try:
            priority_col = self.driver_wrapper.get_link_column(primary_table, secondary_table, "priority")
        except DatabaseIntegrityError:
            pass
        else:
            link_rows = sorted(link_rows, key=lambda x: x[priority_col])
        return link_rows

    def get_interlinked_rows(self, target_row=None, secondary_table=None, type_filter=None, **kwargs):
        """
        Takes a row and the name of another table. Finds all the rows in the second table linked to the given row.
        Returns them as an index ordered by their priority.
        :param target_row:
        :param secondary_table:
        :param type_filter: Only results which are linked to the target_row with a link of this type will be retured
        :return row_list (ordered by priority)/[]:
        """

        # Backwards/forwards compatibility: some callers (notably contract tests) use
        # primary_row=<Row> to mean the same as target_row=<Row>.
        if target_row is None and "primary_row" in kwargs:
            target_row = kwargs.pop("primary_row")

        # Defensive: if a caller passed unexpected keywords, fail loudly with a helpful message.
        if kwargs:
            unexpected = ", ".join(sorted(kwargs.keys()))
            raise TypeError(f"get_interlinked_rows() got unexpected keyword argument(s): {unexpected}")

        if secondary_table is None:
            raise TypeError("get_interlinked_rows() missing required argument: 'secondary_table'")

        if not isinstance(target_row, Row):
            err_str = "Input to the DatabasePing class has to be in the form of Rows"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("target_row", target_row),
                ("secondary_table", secondary_table),
            )
            raise InputIntegrityError(err_str)

        if secondary_table not in self.main_tables and secondary_table not in self.helper_tables:
            err_str = "Secondary table needs to be in either the main tables or the helper tables"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("target_row", target_row),
                ("secondary_table", secondary_table),
            )
            raise InputIntegrityError(err_str)

        if target_row.table == secondary_table:
            err_str = "This method is for interlink rows, not intralink rows."
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("target_row", target_row),
                ("secondary_table", secondary_table),
            )
            raise InputIntegrityError(err_str)

        primary_table = target_row.table
        primary_id = target_row.row_id
        primary_id_col = self.driver_wrapper.get_id_column(primary_table)

        secondary_id_col = self.driver_wrapper.get_id_column(secondary_table)

        # Get the name of the link table - check to see if it exists (if it doesn't, returns None) - signalling that no
        # link exists
        link_table = self.driver_wrapper.get_link_table_name(primary_table, secondary_table)
        if not link_table:
            return []

        link_table_col = self.driver_wrapper.get_column_base(link_table)
        primary_table_link_col = link_table_col + "_" + primary_id_col
        secondary_table_link_col = link_table_col + "_" + secondary_id_col
        link_priority_col = link_table_col + "_priority"

        link_rows = self.driver_wrapper.search(table=link_table, column=primary_table_link_col, search_term=primary_id)
        if not link_rows:
            return []

        # The highest priority rows will be the first in the list - if there is a priority row to order them
        try:
            link_rows = sorted(link_rows, key=lambda x: x[link_priority_col], reverse=True)
        except KeyError:
            pass

        if type_filter is None:
            secondary_ids = [r[secondary_table_link_col] for r in link_rows]
            secondary_rows = [self.get_row_from_id(table=secondary_table, row_id=r_id) for r_id in secondary_ids]
            return secondary_rows
        else:
            link_type_column = link_table_col + "_type"
            secondary_ids = [r[secondary_table_link_col] for r in link_rows if r[link_type_column] == type_filter]
            secondary_rows = [self.get_row_from_id(table=secondary_table, row_id=r_id) for r_id in secondary_ids]
            return secondary_rows

    def get_interlink_values(self, target_row, secondary_column):
        """
        Takes a row and a column - in a table linked to the row.

        Returns a set of every value of that column in a row linked to the given target row - for example, searching with a title_row "creator" yields every creator linked
        to that target row.
        :param target_row:
        :param secondary_column:
        :return values_set:
        """
        secondary_table = self.driver_wrapper.direct_identify_table_from_column(secondary_column)
        linked_rows = self.get_interlinked_rows(target_row=target_row, secondary_table=secondary_table)
        return set([r[secondary_column] for r in linked_rows])

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO WRITE TO INTERLINK TABLES START HERE

    def _check_for_link_table_priority(self, link_table_name, primary_link_table_name, secondary_link_table_name):
        """
        Check to see if the link table has a priority column.
        :param link_table_name:
        :return:
        """
        if link_table_name in self._link_has_priority:
            return self._link_has_priority[link_table_name]
        else:
            try:
                self.driver_wrapper.get_link_column(primary_link_table_name, secondary_link_table_name, "priority")
            except DatabaseIntegrityError:
                self._link_has_priority[link_table_name] = False
                return False

            self._link_has_priority[link_table_name] = True
            return True

    # Todo: Remain type to link type
    def interlink_rows(self, primary_row, secondary_row, priority="highest", type=None, **col_value_pairs):
        """
        Link two rows - col_value_pairs provide a means of adding more information to the link - they can include such
        things as index and type.
        priority accepts integer values, or highest/lowest. This will set the priority to the highest/lowest value in
        that column of the link table. Which is crude, but can be prettified later.
        :param primary_row:
        :param secondary_row:
        :param priority:
        :param type: The type of link
        :param col_value_pairs:
        :return link_row:
        """
        # Check that the tables can be interlinked
        primary_row_table = primary_row.table
        secondary_row_table = secondary_row.table
        link_table = self.driver_wrapper.get_link_table_name(primary_row_table, secondary_row_table)
        if not link_table:
            err_str = "Tables cannot be linked - no such link table exists"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("primary_row", primary_row),
                ("secondary_row", secondary_row),
            )
            raise InputIntegrityError(err_str)

        # Check that both the rows have ids
        primary_id = primary_row.row_id
        secondary_id = secondary_row.row_id
        if primary_id is None or secondary_id is None:
            err_str = "Table cannot be linked - one of the rows doesn't have an id"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("primary_row", primary_row),
                ("secondary_row", secondary_row),
            )
            raise InputIntegrityError(err_str)

        link_row = dict()
        for col in col_value_pairs:
            link_row_col = self.driver_wrapper.get_link_column(primary_row_table, secondary_row_table, col)
            link_row[link_row_col] = col_value_pairs[col]

        # Make the link dict - do not add it as yet
        primary_row_id_col = self.driver_wrapper.get_id_column(primary_row_table)
        primary_link_col = self.driver_wrapper.get_link_column(
            primary_row_table, secondary_row_table, primary_row_id_col
        )

        secondary_row_id_col = self.driver_wrapper.get_id_column(secondary_row_table)
        secondary_link_col = self.driver_wrapper.get_link_column(
            primary_row_table, secondary_row_table, secondary_row_id_col
        )

        link_row[primary_link_col] = primary_id
        link_row[secondary_link_col] = secondary_id

        # Process the priority - only numbers can be written into the priority column
        if priority != "not_set":

            if self._check_for_link_table_priority(link_table, primary_row_table, secondary_row_table):
                priority_col = self.driver_wrapper.get_link_column(primary_row_table, secondary_row_table, "priority")

                # Set the priority of the link if the table has a priority column
                if priority_col is not None:
                    priority_key = six_unicode(priority).lower().strip()
                    if priority is None:
                        link_row[priority_col] = 0
                    elif priority_key == "highest" or priority_key == "lowest":
                        priority_num = (
                            self.get_max(priority_col) if priority_key == "highest" else self.get_min(priority_col)
                        )
                        try:
                            priority_val = int(priority_num) + 1 if priority_key == "highest" else int(priority_num) - 1
                        except (ValueError, TypeError) as e:
                            # Correct a bug which throws an error when t a link table is empty
                            link_row_count = self.driver_wrapper.get_record_count(target_table=link_table)
                            if link_row_count != 0:
                                err_str = (
                                    "get_max for a priority column appears to have returned something not a number"
                                )
                                err_str = default_log.log_exception(
                                    err_str,
                                    e,
                                    "ERROR",
                                    ("priority_num", priority_num),
                                    ("primary_row", primary_row),
                                    ("secondary_row", secondary_row),
                                    ("priority", priority),
                                )
                                raise DatabaseIntegrityError(err_str)
                            else:
                                info_str = "Link table appeared to be empty - setting piority_val to 1 and continuing"
                                default_log.log_variables(info_str, "INFO")
                                priority_val = 1
                        link_row[priority_col] = priority_val

                    elif isinstance(priority, Number):
                        link_row[priority_col] = priority

                    else:
                        err_str = "priority type not recognized and cannot be parsed"
                        err_str = default_log.log_variables(
                            err_str,
                            "ERROR",
                            ("primary_row", primary_row),
                            ("secondary_row", secondary_row),
                            ("priority", priority),
                        )
                        raise InputIntegrityError(err_str)

        # Process the type - Todo: Add checking that the type is valid for that combination
        if type is not None:
            type_col = self.driver_wrapper.get_link_column(primary_row_table, secondary_row_table, "type")
            link_row[type_col] = type

        # Acquire an id for the link row and add it
        link_table_id = self.driver_wrapper.get_id_column(link_table)
        blank_link_row = self.driver_wrapper.get_blank_row(link_table)
        link_row[link_table_id] = blank_link_row[link_table_id]


        # If priority wasn't explicitly set but this link table has a priority column with a non-NULL default,
        # multiple links for the same primary row may collide with UNIQUE(primary_id, priority).
        # In that case, auto-assign the next available priority for this primary (and type, if relevant).
        if priority == "not_set":
            if self._check_for_link_table_priority(link_table, primary_row_table, secondary_row_table):
                priority_col = self.driver_wrapper.get_link_column(primary_row_table, secondary_row_table, "priority")
                if priority_col is not None:
                    blank_default = blank_link_row.get(priority_col, None)
                    # If the DB default is NULL, UNIQUE constraints won't collide on it in SQLite (multiple NULLs allowed).
                    if blank_default is not None:
                        type_col = None
                        if type is not None:
                            try:
                                type_col = self.driver_wrapper.get_link_column(
                                    primary_row_table, secondary_row_table, "type"
                                )
                            except Exception:
                                type_col = None

                        blank_default_cmp = blank_default
                        if isinstance(blank_default_cmp, str):
                            try:
                                blank_default_cmp = float(blank_default_cmp) if "." in blank_default_cmp else int(blank_default_cmp)
                            except Exception:
                                blank_default_cmp = blank_default

                        where = "`{}` = ? AND `{}` = ?".format(primary_link_col, priority_col)
                        vals = [primary_id, blank_default_cmp]
                        if type is not None and type_col is not None:
                            where += " AND `{}` = ?".format(type_col)
                            vals.append(type)

                        exists = self.driver_wrapper.get(
                            "SELECT 1 FROM `{}` WHERE {} LIMIT 1;".format(link_table, where),
                            vals,
                            all=False,
                        )
                        if exists is not None:
                            where2 = "`{}` = ?".format(primary_link_col)
                            vals2 = [primary_id]
                            if type is not None and type_col is not None:
                                where2 += " AND `{}` = ?".format(type_col)
                                vals2.append(type)

                            max_row = self.driver_wrapper.get(
                                "SELECT MAX(`{}`) FROM `{}` WHERE {};".format(priority_col, link_table, where2),
                                vals2,
                                all=False,
                            )
                            max_val = None
                            if max_row is not None and len(max_row) > 0:
                                max_val = max_row[0]

                            try:
                                if max_val is None:
                                    next_val = 1
                                else:
                                    if isinstance(max_val, str):
                                        max_val = float(max_val) if "." in max_val else int(max_val)
                                    next_val = max_val + 1
                            except Exception:
                                next_val = 1

                            link_row[priority_col] = next_val

        # Todo: This is pretty inefficient - try and tidy it up
        # Sync the new data back to the database
        link_row = Row(row_dict=link_row, database=self)
        try:
            link_row.sync()
        except DatabaseIntegrityError:
            self.delete(link_row)
            raise

        # The Row instance created above may not include every link-table column (e.g. columns with DB defaults
        # like priority). Many callers expect those defaults to be visible immediately, so reload from the DB.
        try:
            link_row.load_row_from_id(row_id=link_row.row_id, table=link_table)
        except Exception:
            # Best-effort only: if reload fails, still return the successfully-created link.
            pass

        return link_row

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO UPDATE A LINK BETWEEN TWO ROWS START HERE

    def dupe_interlinks(
            self,
            src_row,
            dst_row,
            swap_priorities=False,
            restrict_to_tables=None,
            force_priority=None,
    ):
        """
        Duplicates the interlinks from one row and applied them to another.
        The dst row will end up having a higher priority in the links that the src row.
        :param src_row: Interlinks from this row will be applied to the dst_row
        :param dst_row:
        :param swap_priorities: If true then swap the priorities of the two rows so that src_row ends up higher
                                priority than dst_row
        :param restrict_to_tables: If not None then only interlinks from these tables will be copies
        :type restrict_to_tables: None or an iterable of table names
        :param force_priority: If force_priority is not None then the string is passed into the interlink_rows method
        :type force_priority: None, or a priority string acceptable as the priority argument of the interlink_rows
                              method.
        :return:
        """
        # So this method only tries to handle interlinks
        if restrict_to_tables is None:
            other_main_tables = set(t for t in deepcopy(self.main_tables))
            other_main_tables.remove(src_row.table)
        else:
            other_main_tables = restrict_to_tables

        # Identify all the rows linked to the src_row - then link them to the dst row
        for main_table in other_main_tables:
            src_linked_rows = self.get_interlinked_rows(target_row=src_row, secondary_table=main_table)
            src_linked_rows.reverse()
            for src_linked_row in src_linked_rows:

                if force_priority is None:
                    self.interlink_rows(primary_row=dst_row, secondary_row=src_linked_row)
                else:
                    self.interlink_rows(
                        primary_row=dst_row,
                        secondary_row=src_linked_row,
                        priority=force_priority,
                    )

                if swap_priorities:
                    self.swap_priorities(src_row=src_linked_row, dst_row_1=src_row, dst_row_2=dst_row)

    def swap_priorities(self, src_row, dst_row_1, dst_row_2):
        """
        Swap the priorities of two rows linked to the same src row.
        :param src_row: The row which is linked to dst_row_1 and dst_row_2
        :param dst_row_1:
        :param dst_row_2:
        :return:
        """
        src_row_table = src_row.table
        dst_table = dst_row_1.table
        link_priority_column = self.driver_wrapper.get_link_column(src_row_table, dst_table, "priority")

        dst_row_1_link = self.get_interlink_row(primary_row=src_row, secondary_row=dst_row_1)
        dst_row_2_link = self.get_interlink_row(primary_row=src_row, secondary_row=dst_row_2)

        priority_hold = dst_row_1_link[link_priority_column]
        dst_row_1_link[link_priority_column] = dst_row_2_link[link_priority_column]
        dst_row_2_link[link_priority_column] = priority_hold

        # Need this to get around the uniquen constraint
        dst_row_1_link[link_priority_column] = None
        dst_row_1_link.sync()

        # Actually do the work of writing the change out
        dst_row_1_link.sync()
        dst_row_2_link.sync()

    # Todo: Need tests for the other col-value pairs
    def update_interlink(self, primary_row, secondary_row, priority="unchanged", **col_value_pairs):
        """
        Update the link row connecting the primary_row and the secondary_row.
        Errors if there is no link to update.
        :param primary_row: The primary row in the link
        :param secondary_row: The secondary row in the link
        :param priority: highest, lowest or unchanged
        :param col_value_pairs: Pass an other link variables you want updated as keywords
        :return interlink_row: The updated row, with the updates having been written out to the database
        """
        interlink_row = self.get_interlink_row(primary_row=primary_row, secondary_row=secondary_row)
        primary_row_table = primary_row.table
        secondary_row_table = secondary_row.table

        # Update the priority to the newly given quantity
        # Process the priority - only numbers can be written into the priority column
        priority_col = self.driver_wrapper.get_link_column(primary_row_table, secondary_row_table, "priority")
        priority_key = six_unicode(priority).lower().strip()
        if priority is None:
            interlink_row[priority_col] = 0
        elif priority_key == "unchanged":
            pass
        elif priority_key == "highest" or priority_key == "lowest":
            priority_num = self.get_max(priority_col) if priority_key == "highest" else self.get_min(priority_col)
            try:
                priority_val = int(priority_num) + 1 if priority_key == "highest" else int(priority_num) - 1
            except (ValueError, TypeError) as e:
                err_str = "get_max for a priority column appears to have returned something not a number"
                err_str = default_log.log_exception(
                    err_str,
                    e,
                    "ERROR",
                    ("priority_num", priority_num),
                    ("primary_row", primary_row),
                    ("secondary_row", secondary_row),
                    ("priority", priority),
                )
                raise DatabaseIntegrityError(err_str)
            else:
                interlink_row[priority_col] = priority_val
        elif isinstance(priority, Number):
            interlink_row[priority_col] = priority
        else:
            err_str = "priority type not recognized and cannot be parsed"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("primary_row", primary_row),
                ("secondary_row", secondary_row),
                ("priority", priority),
                ("priority_type", type(priority)),
            )
            raise InputIntegrityError(err_str)

        # Update everything else specified by the keyword pairs
        for col in col_value_pairs:
            link_row_col = self.driver_wrapper.get_link_column(primary_row_table, secondary_row_table, col)
            interlink_row[link_row_col] = col_value_pairs[col]

        interlink_row.sync()
        return interlink_row

    # Todo: Test this with both a tuple and list of ids
    def update_interlink_priority(self, primary_row, secondary_table, ordered_ids):
        """
        Re-write the priorities of all the rows in a secondary table that are linked to a primary row.
        :param primary_row: All the rows linked to this row from the secondary table will have their priorities updated
        :param secondary_table: All rows, linked to the primary row, in this secondary table will be updated
        :param ordered_ids: The order of the ids - the rows in the secondary table will be re-ordered so they have this
                            order.
        :return:
        """
        secondary_rows = self.get_interlinked_rows(target_row=primary_row, secondary_table=secondary_table)
        assert len(secondary_rows) == len(ordered_ids)

        secondary_row_map = dict((int(r.row_id), r) for r in secondary_rows)

        # Add the rows in the order specified by the ordered_ids
        ordered_ids = [_ for _ in deepcopy(ordered_ids)]
        ordered_ids.reverse()

        for row_id in ordered_ids:
            secondary_row = secondary_row_map[int(row_id)]
            self.update_interlink(primary_row, secondary_row, priority="highest")

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHOD TO UNLINK TWO ROWS STARTS HERE

    def unlink_interlink(self, primary_row, secondary_row):
        """
        Remove any interlink rows linking the priamry_row and the secondary_row.
        Errors if there is not such row to delete.
        :param primary_row:
        :param secondary_row:
        :return:
        """
        link_row = self.get_interlink_row(primary_row=primary_row, secondary_row=secondary_row)
        self.delete(link_row)

    # Todo: Test on a table like ratings, where we can have multiple links between the same title and rating but with
    #       different types. That caused this method to error.
    # Todo: Test on multiple different type filters - including types filters which are lists
    def unlink_all(self, primary_row, secondary_table, type_filter=None):
        """
        Removes every interlink between the primary row and any row in the secondary table.
        :param primary_row:
        :param secondary_table:
        :param type_filter: If provided, then only links with this type will be removed
        :return:
        """
        linked_to_rows = self.get_interlinked_rows(target_row=primary_row, secondary_table=secondary_table)
        if type_filter is None:
            for linked_row in linked_to_rows:
                interlink_row = self.get_interlink_row(primary_row=primary_row, secondary_row=linked_row)
                self.delete(interlink_row)
        else:
            interlink_column = self.driver_wrapper.get_link_column(primary_row.table, secondary_table, "type")
            for linked_row in linked_to_rows:
                try:
                    interlink_row = self.get_interlink_row(primary_row=primary_row, secondary_row=linked_row)
                    interlink_rows = [
                        interlink_row,
                    ]
                except DatabaseIntegrityError:
                    # We might be dealing with a table like ratings
                    interlink_rows = self.get_interlink_row(
                        primary_row=primary_row, secondary_row=linked_row, onelink=False
                    )

                for ilr in interlink_rows:
                    if ilr[interlink_column] == type_filter:
                        self.delete(ilr)

    #
    # ----------------------------------------------------------------------------------------------------------------------
