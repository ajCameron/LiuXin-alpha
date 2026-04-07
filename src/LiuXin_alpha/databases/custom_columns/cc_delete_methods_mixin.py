


class CCDeleteMethodsMixin:
    """
    Methods to delete entries from custom columns.
    """

    def delete_custom_item_using_id(self, idx, label=None, num=None):
        """
        Delete the custom item using its id

        :param idx: The id of the resource to delete
        :param label: The label of the custom column (either this, or the num must be not None, to tell the method which
                      custom column to delete from).
        :param num:
        :return:
        """
        if idx:
            if label is not None:
                data = self.custom_column_label_map[label]
            elif num is not None:
                data = self.custom_column_num_map[num]
            else:
                raise NotImplementedError("There is no information here to designate the custom column")

            # Link table naming depends on the table the custom column is attached to.
            in_table = data.get("in_table") or "books"
            table, lt = self.custom_table_names(data["num"], in_table=in_table)

            # Note the change with books_referencing - which allows the books to be updated with the new information
            book_ids = self.custom_dirty_books_referencing("#" + data["label"], idx, commit=False)

            # Delete from the link table and the actual table
            self.db.macros.delete_cc_item(table, lt, idx)

            self.rename_custom_item_in_data(book_ids=book_ids, column_num=data["num"], new_value=None)

    def delete_item_from_multiple(self, item, label=None, num=None):
        """
        Delete an item which is reference by multiple books.

        :param item: The item to delete
        :param label: One of label or num must be not None - to indicate which of the custom columns is being
                      referred to
        :param num:
        :return:
        """
        if label is not None:
            data = self.custom_column_label_map[label]
        elif num is not None:
            data = self.custom_column_num_map[num]
        else:
            raise NotImplementedError("There is no information here to designate the custom column")

        if data["datatype"] != "text" or not data["is_multiple"]:
            raise ValueError("Column %r is not text/multiple" % data["label"])

        existing_tags = list(self.all_custom(label=label, num=num))
        lt = [t.lower() for t in existing_tags]
        try:
            idx = lt.index(item.lower())
        except ValueError:
            idx = -1
        books_affected = []
        if idx > -1:
            in_table = data.get("in_table") or "books"
            table, lt = self.custom_table_names(data["num"], in_table=in_table)
            id_ = self.db.macros.get_cc_id_from_value(table, existing_tags[idx], all=False, conn=self.conn)
            if id_:
                books = self.db.macros.get_cc_lt_books_from_lt_value(lt, value=id_, conn=self.conn)
                if books:
                    books_affected = [b[0] for b in books]
                self.db.macros.delete_from_cc_table_by_value(lt, id_)
                self.db.macros.delete_from_cc_table_by_id(table, id_)
                self.conn.commit()

        return books_affected
