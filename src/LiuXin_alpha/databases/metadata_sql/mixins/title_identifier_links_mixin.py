"""Metadata SQL macros for title-to-identifier relationships."""




class CMIdentifierTitleLinks:
    """Implement title-to-identifier relationship macros."""



    def delete_title_identifiers(self, title_id, id_type=None):
        """
        Delete all the identifiers associated with a given title.
        :param title_id:
        :param id_type: If id_type is not None, then all the identifiers of this type for the title will be removed
        :return:
        """
        if id_type is None:
            del_stmt = """
            DELETE FROM identifiers 
            WHERE identifier_id IN (
            SELECT identifier_id
            FROM identifiers INNER JOIN identifier_title_links
            ON identifiers.identifier_id = identifier_title_links.identifier_title_link_identifier_id
            WHERE identifier_title_link_title_id = ?
            );
            """
            self.execute(del_stmt, title_id)
        else:
            del_stmt = """
            DELETE FROM identifiers 
            WHERE identifier_id IN (
            SELECT identifier_id
            FROM identifiers INNER JOIN identifier_title_links
            ON identifiers.identifier_id = identifier_title_links.identifier_title_link_identifier_id
            WHERE identifier_title_link_title_id = ? AND identifier_type = ?
            );
            """
            self.execute(del_stmt, (title_id, id_type))

    def add_title_identifier(self, title_id, id_type, id_val):
        """
        Add an new identifier to a title specified by the title id
        :param title_id: The id of the book to add the identifier to
        :param id_type: The type of the identifier to add
        :param id_val: The value of the identifier to add
        :return:
        """
        title_row = self.db.get_row_from_id("titles", row_id=title_id)

        new_id_row = self.db.get_blank_row("identifiers")
        new_id_row["identifier_type"] = id_type
        new_id_row["identifier"] = id_val
        new_id_row.sync()

        self.db.interlink_rows(primary_row=title_row, secondary_row=new_id_row, type=id_type)
