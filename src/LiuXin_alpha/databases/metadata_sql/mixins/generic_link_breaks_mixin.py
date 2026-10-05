"""
Provide metadata SQL operations for generic link breaks.

These helpers target the stored schema named in their SQL. The host supplies db
and/or execution methods. Per-method notes distinguish explicit live-connection
commits from delegated transaction handling; filesystem assets are never moved by
these helpers.
"""




class CMLangTitleLinkMixin:
    """
    Implement the generic link breaks operations used by MetadataSQL.

    Requires a compatible owner database or host query methods. Backend/schema errors
    propagate except where a method explicitly documents suppression.

    Example:
        >>> metadata_sql.break_lang_title_links(1)  # doctest: +SKIP
    """


    # ------------------------------------------------------------------------------------------------------------------
    #
    # - LINK BREAKING METHODS

    def break_lang_title_links(self, title_id, link_type=None):
        """
        Delete a title's language links, optionally restricting the link type.

        The title ID is bound, but a non-None link_type is interpolated into quoted SQL and
        must be trusted. None removes every language link for the title.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> metadata_sql.break_lang_title_links(1)  # doctest: +SKIP


        :param title_id: Title identifier bound to the operation; batch handling, where
            supported, is described above.
        :param link_type: Optional relationship type; binding/interpolation behavior is
            described above.
        :return: None.
        """
        if link_type is not None:
            stmt = (
                "DELETE FROM language_title_links WHERE language_title_link_title_id = ? "
                "AND language_title_link_type = '{}';".format(link_type)
            )
            self.execute(stmt, (title_id,))
        else:
            stmt = "DELETE FROM language_title_links WHERE language_title_link_title_id = ?;".format(link_type)
            self.execute(stmt, (title_id,))

    # Todo: This is a bad name - it breaks generic LINKS
    def break_generic_link(self, link_table, link_col, remove_id, link_type=None):
        """
        Delete generic links matching one ID or untyped batch bindings.

        Interpolates trusted table/column identifiers. A supplied link_type is bound using
        the conventionally derived type column. Typed integer deletion is supported; typed
        batch deletion raises NotImplementedError before execution.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> metadata_sql.break_generic_link("creator_title_links", "creator_title_link_title_id", 1)  # doctest: +SKIP


        :param link_table: Trusted relationship-table name interpolated into SQL.
        :param link_col: Trusted match-column name interpolated into SQL.
        :param remove_id: Integer match ID or batch bindings; typed batch removal is
            unsupported.
        :param link_type: Optional relationship type; binding/interpolation behavior is
            described above.
        :return: None.
        """
        if link_type is None:
            stmt = "DELETE FROM {0} WHERE {1} = ?;".format(link_table, link_col)
            if isinstance(remove_id, int):
                self.execute(stmt, (remove_id,))
            else:
                self.executemany(stmt, remove_id)
        else:
            link_table_col = self.db.driver_wrapper.get_column_base(link_table)
            link_table_type_col = "{}_type".format(link_table_col)
            stmt = "DELETE FROM {0} WHERE {1} = ? AND {2} = ?;".format(link_table, link_col, link_table_type_col)
            if isinstance(remove_id, int):
                self.execute(stmt, (remove_id, link_type))
            else:
                # self.executemany(stmt, remove_id)
                raise NotImplementedError

    def break_generic_single_link(self, link_table, left_link_col, right_link_col, left_id, right_id):
        """
        Delete generic links matching both bound endpoint IDs.

        Interpolates trusted table and endpoint-column names; no type filter is added.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> metadata_sql.break_generic_single_link("creator_title_links", "creator_title_link_creator_id", "creator_title_link_title_id", 1, 1)  # doctest: +SKIP


        :param link_table: Trusted relationship-table name interpolated into SQL.
        :param left_link_col: Trusted first endpoint column interpolated into SQL.
        :param right_link_col: Trusted second endpoint column interpolated into SQL.
        :param left_id: First endpoint ID, parameter-bound.
        :param right_id: Second endpoint ID, parameter-bound.
        :return: None.
        """
        del_stmt = "DELETE FROM {0} WHERE {1} = ? AND {2} = ?;".format(link_table, left_link_col, right_link_col)
        self.execute(del_stmt, (left_id, right_id))

    #
    # ------------------------------------------------------------------------------------------------------------------
