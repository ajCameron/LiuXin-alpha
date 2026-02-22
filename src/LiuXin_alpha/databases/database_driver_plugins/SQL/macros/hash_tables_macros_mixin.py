


class HashTablesMacrosMixin:
    """
    Hash methods - which create gists and hashes of tables.
    """

    def hash_table(self, target_table, columns):
        """
        Construct a hash of the given table using the given columns.

        Used to tag snapshots of the current state of the db.
        :param target_table:
        :param columns:
        :return:
        """
        columns = tuple(columns)

        import hashlib

        m = hashlib.md5()

        for row in self.db.get_all_rows(target_table):
            current_row_list = []
            for col in columns:
                current_row_list.append(row[col])

            current_row_tuple = tuple(current_row_list)

            m.update(str(current_row_tuple))

        return m.hexdigest()
