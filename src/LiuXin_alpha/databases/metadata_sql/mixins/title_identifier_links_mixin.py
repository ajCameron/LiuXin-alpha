"""
Provide metadata SQL operations for title identifier links.

These helpers target the stored schema named in their SQL. The host supplies db
and/or execution methods. Per-method notes distinguish explicit live-connection
commits from delegated transaction handling; filesystem assets are never moved by
these helpers.
"""




class CMIdentifierTitleLinks:
    """
    Implement the title identifier links operations used by MetadataSQL.

    Requires a compatible owner database or host query methods. Backend/schema errors
    propagate except where a method explicitly documents suppression.

    Example:
        >>> metadata_sql.delete_title_identifiers(1, "isbn")  # doctest: +SKIP
    """



    def delete_title_identifiers(self, title_id, id_type=None):
        """
        Delete identifier value rows linked to a title, optionally filtering identifier_type.

        This deletes shared value rows, not merely title links; foreign-key effects belong
        to the schema. The unfiltered path forwards a scalar title binding, while the
        filtered path binds title and type together.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> metadata_sql.delete_title_identifiers(1)  # doctest: +SKIP


        :param title_id: Title identifier bound to the operation; batch handling, where
            supported, is described above.
        :param id_type: Identifier scheme/type; normalization and validation depend on this
            method.
        :return: None.
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
        Reserve an identifier row, set type/value, sync it and link it to the title.

        Does not normalize or deduplicate values. Creation and linking are separate
        operations, so linking failure can leave the new identifier row behind.

        Example:
            >>> metadata_sql.add_title_identifier(1, "isbn", "9780306406157")  # doctest: +SKIP


        :param title_id: Title identifier bound to the operation; batch handling, where
            supported, is described above.
        :param id_type: Identifier scheme/type; normalization and validation depend on this
            method.
        :param id_val: Identifier value; false-value clearing applies only to
            set_title_identifier.
        :return: None.
        """
        title_row = self.db.get_row_from_id("titles", row_id=title_id)

        new_id_row = self.db.get_blank_row("identifiers")
        new_id_row["identifier_type"] = id_type
        new_id_row["identifier"] = id_val
        new_id_row.sync()

        self.db.interlink_rows(primary_row=title_row, secondary_row=new_id_row, type=id_type)
