"""
Delegate same-table relationship creation through legacy named helpers.
"""

# The corresponding class for interlinked is apply
# Todo: Update a bunch of the doc strings to actually reflect reality


class Intralinker(object):
    """
    Wrap database intralink_rows without additional family or type validation.

    All named methods and generic perform the same delegation. Database policy
    determines supported tables, link types, reuse and transaction behavior.

    Example:
        Use a named method to express expected row families; the database still
        validates whether that relationship is supported.
    """

    def __init__(self, database):
        """
        Retain the caller's database for later intralink operations.

        Example:
            >>> database = object()
            >>> Intralinker(database).db is database
            True


        :param database: Borrowed database handle; not opened or validated.
        :return: None; stores the reference.
        """

        self.db = database

    def creator_creator(self, primary, secondary, link_type=None):
        """
        Delegate an intralink between two Creator/Agent rows.

        No priority argument or local schema check is added. Backend failures
        propagate and this wrapper opens no transaction.

        Example:
            The supplied primary and secondary order is retained; this helper does
            not retry with reversed endpoints.


        :param primary: Primary endpoint Row passed unchanged.
        :param secondary: Secondary endpoint Row passed unchanged.
        :param link_type: Relationship type passed unchanged; None uses database policy.
        :return: Link result returned by database.intralink_rows.
        """
        row = self.db.intralink_rows(primary_row=primary, secondary_row=secondary, link_type=link_type)
        return row

    def cover_cover(self, primary, secondary, link_type=None):
        """
        Delegate an intralink between two Cover rows.

        No priority argument or local schema check is added. Backend failures
        propagate and this wrapper opens no transaction.

        Example:
            The supplied primary and secondary order is retained; this helper does
            not retry with reversed endpoints.


        :param primary: Primary endpoint Row passed unchanged.
        :param secondary: Secondary endpoint Row passed unchanged.
        :param link_type: Relationship type passed unchanged; None uses database policy.
        :return: Link result returned by database.intralink_rows.
        """
        row = self.db.intralink_rows(primary_row=primary, secondary_row=secondary, link_type=link_type)
        return row

    def file_file(self, primary, secondary, link_type=None):
        """
        Delegate an intralink between two File rows.

        No priority argument or local schema check is added. Backend failures
        propagate and this wrapper opens no transaction.

        Example:
            The supplied primary and secondary order is retained; this helper does
            not retry with reversed endpoints.


        :param primary: Primary endpoint Row passed unchanged.
        :param secondary: Secondary endpoint Row passed unchanged.
        :param link_type: Relationship type passed unchanged; None uses database policy.
        :return: Link result returned by database.intralink_rows.
        """
        row = self.db.intralink_rows(primary_row=primary, secondary_row=secondary, link_type=link_type)
        return row

    def folder_store_folder_store(self, primary, secondary, link_type=None):
        """
        Delegate an intralink between two folder/store rows.

        No priority argument or local schema check is added. Backend failures
        propagate and this wrapper opens no transaction.

        Example:
            The supplied primary and secondary order is retained; this helper does
            not retry with reversed endpoints.


        :param primary: Primary endpoint Row passed unchanged.
        :param secondary: Secondary endpoint Row passed unchanged.
        :param link_type: Relationship type passed unchanged; None uses database policy.
        :return: Link result returned by database.intralink_rows.
        """
        row = self.db.intralink_rows(primary_row=primary, secondary_row=secondary, link_type=link_type)
        return row

    def identifier_identifier(self, primary, secondary, link_type=None):
        """
        Delegate an intralink between two Identifier rows.

        No priority argument or local schema check is added. Backend failures
        propagate and this wrapper opens no transaction.

        Example:
            The supplied primary and secondary order is retained; this helper does
            not retry with reversed endpoints.


        :param primary: Primary endpoint Row passed unchanged.
        :param secondary: Secondary endpoint Row passed unchanged.
        :param link_type: Relationship type passed unchanged; None uses database policy.
        :return: Link result returned by database.intralink_rows.
        """
        row = self.db.intralink_rows(primary_row=primary, secondary_row=secondary, link_type=link_type)
        return row

    def tag_tag(self, primary, secondary, link_type=None):
        """
        Delegate an intralink between two Tag rows.

        No priority argument or local schema check is added. Backend failures
        propagate and this wrapper opens no transaction.

        Example:
            The supplied primary and secondary order is retained; this helper does
            not retry with reversed endpoints.


        :param primary: Primary endpoint Row passed unchanged.
        :param secondary: Secondary endpoint Row passed unchanged.
        :param link_type: Relationship type passed unchanged; None uses database policy.
        :return: Link result returned by database.intralink_rows.
        """
        row = self.db.intralink_rows(primary_row=primary, secondary_row=secondary, link_type=link_type)
        return row

    def title_title(self, primary, secondary, link_type=None):
        """
        Delegate an intralink between two Title rows.

        No priority argument or local schema check is added. Backend failures
        propagate and this wrapper opens no transaction.

        Example:
            The supplied primary and secondary order is retained; this helper does
            not retry with reversed endpoints.


        :param primary: Primary endpoint Row passed unchanged.
        :param secondary: Secondary endpoint Row passed unchanged.
        :param link_type: Relationship type passed unchanged; None uses database policy.
        :return: Link result returned by database.intralink_rows.
        """
        row = self.db.intralink_rows(primary_row=primary, secondary_row=secondary, link_type=link_type)
        return row

    def publisher_publisher(self, primary, secondary, link_type=None):
        """
        Delegate an intralink between two publisher/organisation rows.

        No priority argument or local schema check is added. Backend failures
        propagate and this wrapper opens no transaction.

        Example:
            The supplied primary and secondary order is retained; this helper does
            not retry with reversed endpoints.


        :param primary: Primary endpoint Row passed unchanged.
        :param secondary: Secondary endpoint Row passed unchanged.
        :param link_type: Relationship type passed unchanged; None uses database policy.
        :return: Link result returned by database.intralink_rows.
        """
        row = self.db.intralink_rows(primary_row=primary, secondary_row=secondary, link_type=link_type)
        return row

    def generic(self, primary, secondary, link_type=None):
        """
        Delegate an intralink between two compatible same-family rows.

        No priority argument or local schema check is added. Backend failures
        propagate and this wrapper opens no transaction.

        Example:
            The supplied primary and secondary order is retained; this helper does
            not retry with reversed endpoints.


        :param primary: Primary endpoint Row passed unchanged.
        :param secondary: Secondary endpoint Row passed unchanged.
        :param link_type: Relationship type passed unchanged; None uses database policy.
        :return: Link result returned by database.intralink_rows.
        """
        row = self.db.intralink_rows(primary_row=primary, secondary_row=secondary, link_type=link_type)
        return row
