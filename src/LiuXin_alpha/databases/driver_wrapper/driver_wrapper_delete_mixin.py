


class DriverWrapperDeleteMixin:
    """
    Delete methods for the driver wrapper.
    """

    def delete(self, target_table, column, value):
        """
        Deletes all the entries which equal that column value in the table.

        :param target_table:
        :param column:
        :param value: If is a list, or set, deletes all the elements in that list or set.
        :return:
        """
        if isinstance(value, (list, set)):
            return self.driver.direct_delete_many(target_table=target_table, column=column, values=value)
        else:
            return self.driver.direct_delete(target_table=target_table, column=column, value=value)

    def delete_by_id(self, target_table, row_id):
        """
        Deletes all the entries which have that id from that table.
        :param target_table:
        :param row_id: If is a list or set, deletes all the elements in that list or set.
        :return:
        """
        if isinstance(row_id, (list, set)):
            return self.driver.direct_delete_many_by_ids(target_table, row_id)
        else:
            return self.driver.direct_delete_row_by_id(target_table, row_id)

    def nullify_column(self, table, row_id, column):
        """
        Delete an entry for in a specified column for a specified row in a table
        :param table:
        :param row_id:
        :param column:
        :return:
        """
        return self.update_column(table, row_id, column, None)

    def clear(self, target_table):
        """
        A separate method - so as to reduce the chance of accidentally
        :param target_table:
        :return:
        """
        return self.driver.direct_clear_table(target_table)
