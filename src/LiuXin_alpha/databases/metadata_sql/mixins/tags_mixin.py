"""
Provide metadata SQL operations for tags.

These helpers target the stored schema named in their SQL. The host supplies db
and/or execution methods. Per-method notes distinguish explicit live-connection
commits from delegated transaction handling; filesystem assets are never moved by
these helpers.
"""




class CMTagsMixin:
    """
    Implement the tags operations used by MetadataSQL.

    Requires a compatible owner database or host query methods. Backend/schema errors
    propagate except where a method explicitly documents suppression.

    Example:
        >>> metadata_sql.delete_tag_by_value("example")  # doctest: +SKIP
    """


    def delete_tag_by_value(self, tag):
        """
        Resolve a canonical tag identity, delete its row if found and commit.

        An absent identity returns without committing. Does not itself remove linked rows;
        schema constraints/cascades apply.

        Example:
            >>> metadata_sql.delete_tag_by_value("example")  # doctest: +SKIP


        :param tag: Tag spelling resolved through the database canonical-identity service.
        :return: None.
        """
        identity = self.db.get_canonical_identity("tags", "tag", tag)
        if identity is None:
            return
        self.db.driver.conn.execute(
            "DELETE FROM tags WHERE tag_id=?;",
            (identity.row_id,),
        )
        self.db.driver.conn.commit()

    def get_tag_id_from_value(self, tag):
        """
        Resolve a tag through the database's canonical-identity policy.

        Example:
            >>> metadata_sql.get_tag_id_from_value("example")  # doctest: +SKIP


        :param tag: Tag spelling resolved through the database canonical-identity service.
        :return: Canonical row ID, or None when no identity matches.
        """
        identity = self.db.get_canonical_identity("tags", "tag", tag)
        return None if identity is None else identity.row_id


    def add_tag(self, tag_value):
        """
        Ensure a canonical tags.tag value through the portable macro provider.

        Example:
            >>> metadata_sql.add_tag("example")  # doctest: +SKIP


        :param tag_value: Value passed to ensure_table_value for canonical tag
            lookup/insertion.
        :return: Identifier returned by ensure_table_value; an existing canonical match may
            be reused.
        """
        return self.db.macros.ensure_table_value("tags", "tag", tag_value)
