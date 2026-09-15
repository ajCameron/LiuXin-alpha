"""
Delegate identifier creation to the wired Ensure helper.
"""

from __future__ import unicode_literals


class IdentifierAdderMixin:
    """
    Supply identifier creation to a legacy Add host.

    The host provides the database and any peers required by the method.
    Validation and synchronization failures propagate to the caller.

    Example:
        The default Ensure error policy propagates duplicate insertion errors; this is not unconditional reuse.
    """

    def identifier(self, identifier, identifier_type):
        """
        Delegate identifier creation to the wired Ensure helper.

        Example:
            The default Ensure error policy propagates duplicate insertion errors; this is not unconditional reuse.


        :param identifier: Identifier text; scheme validation belongs to Ensure.
        :param identifier_type: Identifier scheme passed unchanged.
        :return: Row returned by ensure.identifier; missing peer wiring raises AttributeError.
        """
        return self.ensure.identifier(identifier, identifier_type)
