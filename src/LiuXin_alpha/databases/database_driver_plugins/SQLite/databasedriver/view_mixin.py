

from LiuXin_alpha.utils.libraries.liuxin_six import force_cmp, user_input, force_unicode

from LiuXin_alpha.utils.logging import default_log

from LiuXin_alpha.errors import DatabaseIntegrityError


class ViewMixin:
    """
    Provides methods to manipulate views.
    """
    def direct_get_view_row_dict_from_id(self, view, row_id):
        """
        Retrieve a row from a view and return it as a dictionary, keyed with the column headings of the row and valued
        with the values of that column.
        :param view:
        :param row_id:
        :return:
        """
        view = force_unicode(view)
        row_id = force_unicode(row_id)

        conn = self.get_connection()
        c = conn.cursor()

        headings = self.direct_get_view_column_headings(view)
        table_id_name = "id"

        stmt = "SELECT * FROM {} WHERE {} = ?".format(view, table_id_name)

        rows = []
        result = dict()
        for row in c.execute(stmt, (row_id,)):
            for i in range(len(headings)):
                if not isinstance(headings[i], set):
                    result[headings[i]] = force_unicode(row[i])
                else:
                    result[headings[i]] = row[i]
            rows.append(result)

        if len(rows) > 1:
            err_str = "Error - search yielded multiple rows. Aborting.\n"
            err_str += repr(rows)
            default_log.error(err_str)
            conn.close()
            raise DatabaseIntegrityError(err_str)
        elif len(rows) == 0:
            info_str = "Warning - search yielded no results. Consider sources of logical error."
            default_log.log_variables(info_str, "INFO", ("table", view), ("row_id", row_id))
            conn.close()
            return False
        else:
            conn.close()
            return result

    def direct_get_view_column_headings(self, view):
        """
        Returns the column headings for the given view.
        :param view:
        :return:
        """
        # Todo: Add checking against injection attacks
        stmt = "PRAGMA TABLE_INFO({})".format(view)

        conn = self.get_connection()
        c = conn.cursor()

        view_columns = []
        for i in c.execute(stmt):
            view_columns.append(i[1])

        return view_columns

