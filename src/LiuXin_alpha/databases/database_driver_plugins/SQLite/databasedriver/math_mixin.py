


class MathFunctionsMixin:
    """
    Math functions which can be run on columns.
    """

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - DIRECT SQLITE EXECUTION METHODS START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------

    def direct_get_max(self, column: str):
        """
        Get the maximum value from a column and return it.
        """
        col_table = self.identify_table_from_column(column)
        stmt = "SELECT MAX({}) FROM {};".format(column, col_table)

        conn = self.get_connection()
        max_val = next(conn.execute(stmt))[0]
        return max_val

    def direct_get_min(self, column: str):
        """
        Get the maximum value from a column and return it.
        """
        col_table = self.identify_table_from_column(column)
        stmt = "SELECT MIN({}) FROM {};".format(column, col_table)

        conn = self.get_connection()
        min_val = next(conn.execute(stmt))[0]
        return min_val
