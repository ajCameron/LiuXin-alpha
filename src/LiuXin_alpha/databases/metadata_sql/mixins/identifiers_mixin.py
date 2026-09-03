"""Metadata SQL macros for identifier catalogue rows."""




class CMIdentifiersMixin:
    """Implement identifier-row metadata macros."""


    def read_all_identifiers(self):
        """
        Reads all the identifiers from the identifiers table into memory.
        Returns a tuple of the form book, typ, val - the book id for the identifier - the type of the identifier and the
        value of the identifier.
        :return:
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
