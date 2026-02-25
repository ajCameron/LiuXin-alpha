from __future__ import unicode_literals


class IdentifierAdderMixin:
    """
    Add methods for rows in the ``identifiers`` table.
    """

    def identifier(self, identifier, identifier_type):
        """
        Ensure an identifier row.
        """
        return self.ensure.identifier(identifier, identifier_type)
