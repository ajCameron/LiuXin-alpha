"""
Provide metadata SQL operations for identifiers.

These helpers target the stored schema named in their SQL. The host supplies db
and/or execution methods. Per-method notes distinguish explicit live-connection
commits from delegated transaction handling; filesystem assets are never moved by
these helpers.
"""




class CMIdentifiersMixin:
    """
    Implement the identifiers operations used by MetadataSQL.

    Requires a compatible owner database or host query methods. Backend/schema errors
    propagate except where a method explicitly documents suppression.

    Example:
        >>> metadata_sql.read_all_identifiers()  # doctest: +SKIP
    """


    def read_all_identifiers(self):
        """
        Read linked title IDs, relationship types and identifier values.

        Orders by descending identifier/title link priority. The type comes from the link,
        not identifier_type on the value row.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> metadata_sql.read_all_identifiers()  # doctest: +SKIP


        :return: Execution cursor/iterable of three-cell rows, not an eagerly built
            snapshot.
        """
        stmt = """
                SELECT identifier_title_links.identifier_title_link_title_id, 
                identifier_title_links.identifier_title_link_type,
                identifiers.identifier
                FROM identifier_title_links JOIN identifiers
                ON identifier_title_links.identifier_title_link_identifier_id = identifiers.identifier_id
                ORDER BY identifier_title_links.identifier_title_link_priority DESC;
                """
        return self.execute(stmt)
